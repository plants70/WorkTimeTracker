from __future__ import annotations

import datetime as dt
import logging
import threading
from enum import Enum, auto
from typing import Any, Dict

from PyQt5.QtCore import QObject, QTimer, pyqtSignal

from consts import STATUS_ACTIVE, STATUS_FORCE_LOGOUT
from telemetry import trace_time
from user_app import session as session_state
from user_app.services import Services, services

logger = logging.getLogger(__name__)


class AppState(Enum):
    BOOT = auto()
    LOGGED_OUT = auto()
    AUTHENTICATING = auto()
    ACTIVE = auto()
    LOGOUT_IN_PROGRESS = auto()
    RETURNING_TO_LOGIN = auto()


class AppController(QObject):
    state_changed = pyqtSignal(object, object)
    login_started = pyqtSignal()
    login_failed = pyqtSignal(str)
    login_succeeded = pyqtSignal(dict)
    logout_started = pyqtSignal(str)
    logout_finished = pyqtSignal(str)
    remote_logout = pyqtSignal(str)

    def __init__(self, service_provider: Services | None = None) -> None:
        super().__init__()
        provider = service_provider or services
        self.services = provider
        self.state = AppState.BOOT
        self._transition_guard = threading.Lock()
        self.login_window = None
        self.main_window = None
        self._current_user: dict[str, Any] | None = None
        self._current_session_id: str | None = None
        self._pending_finish_reason: str | None = None
        self._remote_logout_triggered = False
        self._last_message: str | None = None
        self._last_known_group: str | None = None

        self.session_signals = self.services.session_signals
        self.sync_signals = self.services.sync_signals
        self.session_signals.sessionFinished.connect(self._handle_session_finished)
        if hasattr(self.session_signals, "sessionFinalized"):
            self.session_signals.sessionFinalized.connect(self._handle_session_finalized)
        self.sync_signals.force_logout.connect(
            lambda: self.handle_remote_force_logout("remote_force_logout")
        )

        self.login_succeeded.connect(self._on_login_success)
        self.login_failed.connect(self._on_login_failed)
        self.logout_finished.connect(self._on_logout_finished)
        self.remote_logout.connect(self._on_remote_logout)

    # --- FSM helpers --------------------------------------------------
    def _set_state(self, new_state: AppState) -> None:
        if self.state is new_state:
            return
        old_state = self.state
        self.state = new_state
        self.state_changed.emit(old_state, new_state)
        logger.info("App state transition: %s → %s", old_state.name, new_state.name)

    def _enter_transition(self) -> bool:
        acquired = self._transition_guard.acquire(blocking=False)
        if not acquired:
            logger.debug("Transition suppressed due to guard")
        return acquired

    def _exit_transition(self) -> None:
        if self._transition_guard.locked():
            self._transition_guard.release()

    # --- Public API ---------------------------------------------------
    def start(self) -> None:
        self.to_logged_out()

    def start_login(self, email: str) -> None:
        normalized = (email or "").strip()
        if not normalized:
            self.login_failed.emit("Введите email адрес")
            return
        if "@" not in normalized:
            self.login_failed.emit("Некорректный формат email")
            return
        if self.state not in {AppState.LOGGED_OUT, AppState.RETURNING_TO_LOGIN}:
            logger.debug("Login attempt ignored in state %s", self.state)
            return

        if not self._enter_transition():
            return

        try:
            self._set_state(AppState.AUTHENTICATING)
            self.login_started.emit()
            future = self.services.submit(self._login_worker, normalized)
            future.add_done_callback(self._on_login_future_done)
        finally:
            self._exit_transition()

    def request_logout(self, reason: str = "local_logout") -> None:
        if self.state != AppState.ACTIVE:
            return
        if not self._enter_transition():
            return
        try:
            self._pending_finish_reason = reason
            self._set_state(AppState.LOGOUT_IN_PROGRESS)
            self.logout_started.emit(reason)
        finally:
            self._exit_transition()

    def to_logged_out(self, reason: str = "") -> None:
        if not self._enter_transition():
            return
        try:
            self.services.auto_sync.stop()
            self.services.heartbeat.stop()
            self._remote_logout_triggered = False
            self._pending_finish_reason = None
            self._set_state(AppState.LOGGED_OUT)
            self._show_login_window(message=reason)
        finally:
            self._exit_transition()

    def to_authenticating(self) -> None:
        if not self._enter_transition():
            return
        try:
            self._set_state(AppState.AUTHENTICATING)
        finally:
            self._exit_transition()

    def to_active(self, session_id: str) -> None:
        if not self._enter_transition():
            return
        try:
            self._set_state(AppState.ACTIVE)
            self._current_session_id = session_id
            offline_mode = bool((self._current_user or {}).get("offline_mode"))
            if self._current_user and not offline_mode:
                email = self._current_user.get("email") or ""
                self.services.heartbeat.start(
                    email=email,
                    session_id=session_id,
                    callback=self.handle_remote_force_logout,
                )
            else:
                if offline_mode:
                    logger.info(
                        "Heartbeat skipped for session %s (offline mode)", session_id
                    )
            self.services.auto_sync.start(
                signals=self.sync_signals,
                session_signals=self.session_signals,
                offline_mode=offline_mode,
                remote_callback=self.handle_remote_force_logout,
            )
        finally:
            self._exit_transition()

    def to_logout_in_progress(self, reason: str = "local_logout") -> None:
        if not self._enter_transition():
            return
        try:
            self._set_state(AppState.LOGOUT_IN_PROGRESS)
            self.logout_started.emit(reason)
        finally:
            self._exit_transition()

    def to_returning_to_login(self, reason: str) -> None:
        if not self._enter_transition():
            return
        try:
            self._set_state(AppState.RETURNING_TO_LOGIN)
            self.services.auto_sync.stop()
            self.services.heartbeat.stop()
            self._pending_finish_reason = None
            self.logout_finished.emit(reason)
            self._cleanup_session_state()
            self._show_login_window(message=self._last_message)
            self._set_state(AppState.LOGGED_OUT)
        finally:
            self._exit_transition()

    # --- Internal slots -----------------------------------------------
    def _on_login_future_done(self, future) -> None:  # type: ignore[override]
        try:
            user_data = future.result()
        except Exception as exc:  # pragma: no cover - propagated via signal
            logger.debug("Login future failed: %s", exc)
            self.login_failed.emit(str(exc))
        else:
            self.login_succeeded.emit(user_data)

    def _on_login_success(self, user_data: Dict[str, Any]) -> None:
        self._current_user = user_data
        session_id = user_data.get("session_id") or ""
        session_state.set_session_id(session_id)
        session_state.set_user_email(user_data.get("email", ""))
        self._last_known_group = user_data.get("group", "")

        self._create_main_window(user_data)
        self.to_active(session_id)
        try:
            self.services.schedule_worklog_sort(self._last_known_group or "")
        except Exception:
            logger.debug("Worklog sort scheduling failed after login", exc_info=True)

    def _on_login_failed(self, message: str) -> None:
        self._current_user = None
        self._current_session_id = None
        self._set_state(AppState.LOGGED_OUT)
        self._show_login_window(message=message)

    def _on_logout_finished(self, reason: str) -> None:
        self._cleanup_session_state()
        if reason:
            self._last_message = self._logout_message(reason)
        else:
            self._last_message = None

    def _on_remote_logout(self, reason: str) -> None:
        self._last_message = self._logout_message(reason)
        self.to_returning_to_login(reason)

    def _handle_session_finished(self, reason: str) -> None:
        normalized = (reason or "").strip().lower() or "local_logout"
        if normalized.startswith("remote"):
            self.handle_remote_force_logout(normalized)
        else:
            self.to_returning_to_login(normalized)

    def _handle_session_finalized(self, reason: str) -> None:
        normalized = (reason or "").strip()
        message = self._logout_message(normalized)
        self._last_message = message
        if self.login_window:
            self.login_window.show_info(message)
        logger.info("Logout finalized with reason=%s", normalized or "<empty>")

    # --- Remote handling ----------------------------------------------
    def handle_remote_force_logout(self, reason: str) -> None:
        if self._remote_logout_triggered:
            return
        self._remote_logout_triggered = True
        logout_reason = reason or "remote_force_logout"
        session_id = self._current_session_id
        email = (self._current_user or {}).get("email")
        if session_id and email:
            try:
                self.services.db.finish_session(
                    session_id,
                    email=email,
                    status=STATUS_FORCE_LOGOUT,
                    reason=STATUS_FORCE_LOGOUT,
                    comment="Сессия завершена администратором",
                    logout_time=dt.datetime.now(dt.UTC),
                    user_group=(self._current_user or {}).get("group") or None,
                )
            except Exception as exc:  # pragma: no cover - best effort
                logger.debug("Failed to mark FORCE_LOGOUT locally: %s", exc)

            sheets = self.services.sheets
            if hasattr(sheets, "ack_remote_command"):

                def _ack() -> None:
                    try:
                        sheets.ack_remote_command(email=email, session_id=session_id)
                    except Exception as exc:  # pragma: no cover
                        logger.debug("ACK remote command failed: %s", exc)

                self.services.submit(_ack)

        self.remote_logout.emit(logout_reason)
        try:
            self.services.schedule_worklog_sort(self._last_known_group or "")
        except Exception:
            logger.debug("Failed to schedule WorkLog sort after remote logout", exc_info=True)

    # --- Helpers ------------------------------------------------------
    def _create_main_window(self, user_data: Dict[str, Any]) -> None:
        from user_app.gui import EmployeeApp

        if self.login_window:
            self.login_window.hide()

        def on_logout(reason: str | None = None) -> None:
            self.to_returning_to_login(reason or "local_logout")

        self.main_window = EmployeeApp(
            email=user_data.get("email", ""),
            name=user_data.get("name", ""),
            role=user_data.get("role", ""),
            shift_hours=user_data.get("shift_hours", ""),
            telegram_login=user_data.get("telegram_login", ""),
            session_id=user_data.get("session_id"),
            login_was_performed=user_data.get("login_was_performed", True),
            group=user_data.get("group", ""),
            session_signals=self.session_signals,
            on_session_finish_requested=self._on_session_finish_requested,
            session_started_at=user_data.get("session_started_at"),
            services=self.services,
            controller=self,
            on_logout_callback=on_logout,
        )
        self.main_window.show()

    def _on_session_finish_requested(self, reason: str) -> None:
        normalized = (reason or "").strip().lower() or "local_logout"
        self._pending_finish_reason = normalized
        self.to_logout_in_progress(normalized)

    def _show_login_window(self, message: str | None = None) -> None:
        from user_app.login_window import LoginWindow

        if self.main_window:
            try:
                self.main_window.close()
            except Exception:  # pragma: no cover
                pass
            self.main_window = None

        if not self.login_window:
            self.login_window = LoginWindow(controller=self)
        if message:
            self.login_window.show_info(self._logout_message(message))
        else:
            self.login_window.show_info("")
        self.login_window.show()
        self.login_window.raise_()

    def _cleanup_session_state(self) -> None:
        self.services.heartbeat.stop()
        self.services.auto_sync.stop()
        session_state.set_session_id("")
        session_state.set_user_email("")
        self._current_session_id = None
        self._current_user = None
        self._remote_logout_triggered = False

    def _logout_message(self, reason: str) -> str:
        mapping = {
            "remote_force_logout": "Сессия завершена администратором",
            "local_logout": "Смена завершена",
            "local_logout_offline": "Смена завершена (оффлайн режим)",
        }
        return mapping.get(reason, reason)

    # --- Background workers -----------------------------------------
    def _login_worker(self, email: str) -> Dict[str, Any]:
        with trace_time("login"):
            sheets = self.services.sheets
            required = [
                "get_user_by_email",
                "get_active_session",
                "set_active_session",
                "finish_active_session",
            ]
            for method_name in required:
                if not hasattr(sheets, method_name):
                    raise RuntimeError(
                        f"SheetsAPI object has no attribute '{method_name}'"
                    )

            from config import ALLOW_SELF_SIGNUP, validate_config

            validate_config()

            try:
                from sync.network import is_internet_available

                online = bool(is_internet_available())
            except Exception:
                online = True

            offline_hint = not online
            user_data: dict[str, Any] | None = None
            if not offline_hint:
                try:
                    user_data = sheets.get_user_by_email(email)
                except Exception as exc:
                    logger.warning("Remote user lookup failed for %s: %s", email, exc)
                    offline_hint = True

            if not user_data and offline_hint:
                cached = self.services.db.get_user_from_cache(email)
                if cached:
                    user_data = {
                        "Email": cached["email"],
                        "Name": cached["name"],
                        "Role": cached["role"],
                        "ShiftHours": cached["shift_hours"],
                        "Telegram": cached["telegram_login"],
                        "Group": cached["group"],
                        "_offline": True,
                    }

            if not user_data and ALLOW_SELF_SIGNUP and not offline_hint:
                user_data = sheets.add_user_if_absent(email)

            if not user_data:
                if offline_hint:
                    raise RuntimeError(
                        "Нет подключения к интернету. Повторите попытку или используйте оффлайн режим после успешного входа."
                    )
                raise RuntimeError(
                    "Пользователь не найден. Проверьте email или обратитесь к администратору."
                )

            normalized = self._normalize_user_data(user_data, email)
            offline_mode = bool(user_data.get("_offline"))

            if not offline_mode:
                try:
                    self.services.db.update_user_cache(
                        {
                            "email": normalized["email"],
                            "name": normalized["name"],
                            "role": normalized["role"],
                            "group": normalized.get("group"),
                            "shift_hours": normalized["shift_hours"],
                            "telegram_login": normalized["telegram_login"],
                        }
                    )
                except Exception as exc:  # pragma: no cover
                    logger.debug("Failed to update local cache: %s", exc)

            login_dt = dt.datetime.now(dt.UTC)
            session_id = session_state.generate_session_id(email, login_dt)
            login_time_iso = login_dt.isoformat()

            if not offline_mode:
                active_session = None
                if hasattr(sheets, "get_active_session"):
                    active_session = sheets.get_active_session(email)
                if active_session:
                    existing_sid = active_session.get("SessionID")
                    if existing_sid:
                        with trace_time("finish_active_session"):
                            sheets.finish_active_session(email, existing_sid)

                sheets.set_active_session(
                    email,
                    normalized.get("name", ""),
                    session_id,
                    login_time_iso,
                )
            else:
                logger.info("Login in offline mode for %s", email)

            record_id, created = self.services.db.mark_session_active(
                session_id,
                email=normalized.get("email", email),
                name=normalized.get("name", email),
                status=STATUS_ACTIVE,
                started_at=login_dt,
                comment="Начало смены",
                user_group=normalized.get("group") or None,
            )
            if created and record_id:
                QTimer.singleShot(0, lambda: logger.debug("Local session recorded"))

            try:
                self.services.replicate_session_start(
                    {
                        "session_id": session_id,
                        "email": normalized.get("email", email),
                        "name": normalized.get("name", ""),
                        "status": STATUS_ACTIVE,
                        "started_at": login_time_iso,
                        "group": normalized.get("group") or None,
                    }
                )
            except Exception:  # pragma: no cover - replication is best-effort
                logger.debug("Failed to replicate session start to server DB", exc_info=True)

            session_state.set_session_id(session_id)
            session_state.set_user_email(normalized.get("email", email))

            return {
                "email": normalized.get("email", email),
                "name": normalized.get("name", ""),
                "role": normalized.get("role", ""),
                "shift_hours": normalized.get("shift_hours", ""),
                "telegram_login": normalized.get("telegram_login", ""),
                "group": normalized.get("group", ""),
                "login_was_performed": True,
                "session_id": session_id,
                "session_started_at": login_time_iso,
                "offline_mode": offline_mode,
            }

    def _normalize_user_data(
        self, user_data: Dict[str, Any], email: str
    ) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {}
        for key, value in user_data.items():
            normalized_key = self._to_snake_case(key)
            normalized[normalized_key] = value
        normalized.setdefault("email", email)
        normalized.setdefault("name", "")
        normalized.setdefault("role", "специалист")
        normalized.setdefault("shift_hours", "8 часов")
        normalized.setdefault("telegram_login", "")
        normalized.setdefault("group", "")
        return normalized

    def _to_snake_case(self, column_name: str) -> str:
        import re

        normalized = column_name.strip().lower()
        normalized = re.sub(r"[\s-]+", "_", normalized)
        normalized = re.sub(r"[^a-z0-9_]", "", normalized)
        return normalized
