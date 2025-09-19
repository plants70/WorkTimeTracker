from __future__ import annotations

import logging
import sys
import time
from concurrent.futures import Future
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import MAX_COMMENT_LENGTH, STATUS_GROUPS
from consts import STATUS_ACTIVE, STATUS_LOGOUT, normalize_session_status
from sheets_api import SheetsAPIError
from telemetry import trace_time
from user_app import session as session_state
from user_app.db_local import LocalDBError, write_tx
from user_app.services import Services
from user_app.signals import SessionSignals

try:
    from sync.notifications import Notifier
except ImportError:
    try:
        from .sync.notifications import Notifier
    except ImportError:
        from notifications import Notifier

from typing import TYPE_CHECKING

from PyQt5.QtCore import Qt, QTimer, pyqtSignal
from PyQt5.QtGui import QIcon, QPixmap
from PyQt5.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from user_app.app_controller import AppController

logger = logging.getLogger(__name__)


class EmployeeApp(QWidget):
    status_changed = pyqtSignal(str)
    app_closed = pyqtSignal(str)

    def __init__(
        self,
        email: str,
        name: str,
        role: str = "специалист",
        group: str = "",
        shift_hours: str = "8 часов",
        telegram_login: str = "",
        on_logout_callback: Optional[Callable] = None,
        session_id: Optional[str] = None,
        login_was_performed: bool = True,
        session_signals: Optional[SessionSignals] = None,
        on_session_finish_requested: Optional[Callable[[str], None]] = None,
        session_started_at: Optional[str] = None,
        *,
        services: Services,
        controller: "AppController",
    ):
        super().__init__()
        self.services = services
        self.controller = controller
        self.email = email
        self.name = name
        self.role = role
        self.group = group
        self.shift_hours = shift_hours
        self.telegram_login = telegram_login
        self.on_logout_callback = on_logout_callback
        self.session_signals = session_signals
        self.on_session_finish_requested = on_session_finish_requested

        now_local = datetime.now()
        self.current_status = STATUS_ACTIVE
        self.status_start_time = now_local
        self.shift_start_time = now_local
        self.last_sync_time = None
        self.shift_ended = False
        self._logout_in_progress = False

        # Логика закрытия: None, "admin_logout", "user_close", "auto_logout"
        self._closing_reason = "user_close"  # по умолчанию

        self._session_started_at = session_started_at
        self.shift_start_time = self._get_local_session_start(self.shift_start_time)
        if session_id is not None:
            self.session_id = session_id
            self._continue_existing_session = True
        else:
            self.session_id = session_state.generate_session_id(self.email)
            self._continue_existing_session = False
            try:
                session_state.set_session_id(self.session_id)
            except Exception:
                logger.debug("Unable to persist generated session id")
        self.status_buttons = {}
        self._status_in_progress = False
        self._last_status_click = 0.0
        self._last_finish_click = 0.0
        self._executor = services.executor

        self.login_was_performed = login_was_performed

        # Shared services
        self.sheets_api = services.sheets

        self._init_db()
        self._init_ui()
        self._init_timers()
        self._init_shift_check_timer()

    def get_user(self):
        return {
            "Email": self.email,
            "Name": self.name,
            "Role": self.role,
            "Telegram": self.telegram_login,
            "ShiftHours": self.shift_hours,
            "Group": self.group,
        }

    def _make_action_payload_from_row(self, row):
        # Порядок столбцов в logs:
        # 0:id 1:session_id 2:email 3:name 4:status 5:action_type 6:comment
        # 7:timestamp 8:synced 9:sync_attempts 10:last_sync_attempt 11:priority
        # 12:status_start_time 13:status_end_time 14:reason 15:user_group
        return {
            "session_id": row[1],
            "email": row[2],
            "name": row[3],
            "status": row[4],
            "action_type": row[5],
            "comment": row[6],
            "timestamp": row[7],
            "status_start_time": row[12],
            "status_end_time": row[13],
            "reason": row[14] if len(row) > 14 else None,
        }

    def _get_local_session_start(self, fallback: Optional[datetime] = None) -> datetime:
        """Convert stored session start to local naive datetime for timers."""

        fallback_dt = fallback or datetime.now()
        raw_value = self._session_started_at
        candidate: Optional[datetime]

        if isinstance(raw_value, datetime):
            candidate = raw_value
        elif isinstance(raw_value, str):
            raw_text = raw_value.strip()
            if not raw_text:
                return fallback_dt
            try:
                candidate = datetime.fromisoformat(raw_text)
            except ValueError:
                logger.debug("Invalid session start format: %s", raw_value)
                return fallback_dt
        else:
            return fallback_dt

        if candidate.tzinfo is not None:
            try:
                candidate = candidate.astimezone()
            except Exception as exc:
                logger.debug("Failed to convert session start timezone: %s", exc)
                try:
                    candidate = candidate.astimezone(timezone.utc)
                except Exception:
                    return fallback_dt

        try:
            return candidate.replace(tzinfo=None)
        except Exception:
            return fallback_dt

    def _send_action_to_sheets(self, record_id, user_group=None):
        self._executor.submit(self._send_action_to_sheets_worker, record_id, user_group)

    def _send_action_to_sheets_worker(self, record_id, user_group=None):
        try:
            row = self.db.get_action_by_id(record_id)
            if not row:
                logger.error(
                    f"Не удалось найти запись с id={record_id} для отправки в Sheets"
                )
                return

            action = self._make_action_payload_from_row(row)
            target_group = user_group or self.group or None
            try:
                with trace_time("worklog_write"):
                    self.sheets_api.log_user_actions(
                        email=action["email"],
                        action=action.get("action_type", ""),
                        status=action.get("status", ""),
                        group=target_group,
                        timestamp_utc=action.get("timestamp"),
                        start_utc=action.get("status_start_time"),
                        end_utc=action.get("status_end_time"),
                        session_id=action.get("session_id"),
                        group_at_start=target_group,
                    )
            except SheetsAPIError as exc:
                logger.warning(
                    "Sheets: не удалось записать действие в WorkLog — %s", exc
                )
            else:
                self.db.mark_actions_synced([record_id])
                self.last_sync_time = datetime.now()
        except Exception as e:
            logger.warning(f"Ошибка отправки действия в Google Sheets: {e}")
            Notifier.show(
                "Оффлайн режим", "Данные будут отправлены при появлении интернета."
            )

    def _finish_and_send_previous_status(self):
        prev_result = self.db.finish_last_status(self.email, self.session_id)
        if isinstance(prev_result, tuple):
            prev_id = prev_result[0]
        else:
            prev_id = prev_result
        if prev_id and prev_id > 0:
            self._executor.submit(self._finish_and_send_previous_status_worker, prev_id)

    def _finish_and_send_previous_status_worker(self, prev_id):
        row = self.db.get_action_by_id(prev_id)
        if not row:
            return
        try:
            action = self._make_action_payload_from_row(row)
            target_group = self.group or None
            try:
                with trace_time("worklog_write"):
                    self.sheets_api.log_user_actions(
                        email=action["email"],
                        action=action.get("action_type", ""),
                        status=action.get("status", ""),
                        group=target_group,
                        timestamp_utc=action.get("timestamp"),
                        start_utc=action.get("status_start_time"),
                        end_utc=action.get("status_end_time"),
                        session_id=action.get("session_id"),
                        group_at_start=target_group,
                    )
            except SheetsAPIError as exc:
                logger.warning(
                    "Sheets: не удалось синхронизировать завершённый статус — %s",
                    exc,
                )
            else:
                self.db.mark_actions_synced([prev_id])
                self.last_sync_time = datetime.now()
                try:
                    payload = dict(action)
                    payload["group"] = target_group
                    self.services.replicate_action(payload)
                except Exception:  # pragma: no cover - replication best effort
                    logger.debug("Server DB action replication failed", exc_info=True)
                try:
                    self.services.schedule_worklog_sort(self.group)
                except Exception:
                    logger.debug("Failed to schedule WorkLog sort", exc_info=True)
        except Exception as e:
            logger.warning(f"Ошибка отправки завершённого статуса в Sheets: {e}")
            Notifier.show(
                "Оффлайн режим", "Предыдущий статус будет синхронизирован позже."
            )

    def _notify_session_finish_requested(self, reason: str) -> None:
        if callable(self.on_session_finish_requested):
            try:
                self.on_session_finish_requested(reason)
            except Exception as exc:
                logger.debug("on_session_finish_requested failed: %s", exc)

    def _disable_post_logout(self) -> None:
        self.shift_ended = True
        self.finish_btn.setEnabled(False)

    def _emit_session_finished(self, reason: str) -> None:
        self._logout_in_progress = False
        if self.session_signals:
            try:
                self.session_signals.sessionFinished.emit(reason)
                return
            except Exception as exc:
                logger.debug("sessionFinished emit failed: %s", exc)
        if callable(self.on_logout_callback):
            try:
                self.on_logout_callback(reason)
            except TypeError:
                self.on_logout_callback()

    def _start_logout_worker(self, mode: str) -> Future[str]:
        future: Future[str] = self._executor.submit(self._logout_worker, mode)
        future.add_done_callback(self._on_logout_worker_done)
        return future

    def _logout_worker(self, mode: str) -> str:
        reason = "local_logout"
        try:
            if mode == "local":
                success, offline = self._finish_remote_session_with_retry()
                if offline and not success:
                    reason = "local_logout_offline"
                else:
                    reason = "local_logout"
            elif mode == "remote":
                self._ack_remote_command_with_retry()
                reason = "remote_force_logout"
            else:
                reason = mode
        except Exception as exc:  # pragma: no cover - network failures tolerated
            logger.debug("Logout worker encountered error: %s", exc)
            reason = "local_logout_offline"
        try:
            if reason in {"local_logout", "local_logout_offline"}:
                try:
                    self.services.schedule_worklog_sort(self.group)
                except Exception:
                    logger.debug("Failed to schedule WorkLog sort on logout", exc_info=True)
        finally:
            return reason

    def _on_logout_worker_done(self, future: Future[str]) -> None:
        try:
            reason = future.result()
        except Exception as exc:  # pragma: no cover - executor errors
            logger.debug("Logout worker future failed: %s", exc)
            reason = "local_logout_offline"
        if self.session_signals and hasattr(self.session_signals, "sessionFinalized"):
            try:
                self.session_signals.sessionFinalized.emit(reason)
            except Exception as signal_exc:  # pragma: no cover
                logger.debug("sessionFinalized emit failed: %s", signal_exc)

    def _finish_remote_session_with_retry(self) -> tuple[bool, bool]:
        """Возвращает (success, offline_hint)."""

        if not self.sheets_api:
            return False, False

        last_exception: Exception | None = None
        for attempt in range(1, 4):
            try:
                logout_time = datetime.now().isoformat()
                with trace_time("finish_active_session"):
                    ok = self.sheets_api.finish_active_session(
                        self.email,
                        self.session_id,
                        logout_time,
                    )
                if ok:
                    logger.info(
                        "finish_active_session succeeded for %s (attempt %s)",
                        self.session_id,
                        attempt,
                    )
                    return True, False

                with trace_time("check_user_session_status"):
                    raw_status = self.sheets_api.check_user_session_status(
                        self.email, self.session_id
                    )
                status = normalize_session_status(raw_status)
                if status and status != STATUS_ACTIVE:
                    logger.info(
                        "finish_active_session skipped: remote status %s for %s",
                        status,
                        self.session_id,
                    )
                    return True, False
            except Exception as exc:
                last_exception = exc
                logger.warning(
                    "finish_active_session attempt %s failed: %s",
                    attempt,
                    exc,
                )
            time.sleep(2)

        offline = last_exception is not None
        if offline:
            logger.warning(
                "finish_active_session failed after retries for %s: %s",
                self.session_id,
                last_exception,
            )
        else:
            logger.warning(
                "finish_active_session returned False for %s after retries",
                self.session_id,
            )
        return False, offline

    def _ack_remote_command_with_retry(self) -> None:
        if not hasattr(self.sheets_api, "ack_remote_command"):
            return
        for attempt in range(1, 4):
            try:
                ok = self.sheets_api.ack_remote_command(
                    email=self.email, session_id=self.session_id
                )
                if ok:
                    logger.info(
                        "ACK remote command sent for %s on attempt %s",
                        self.session_id,
                        attempt,
                    )
                    return
            except Exception as exc:
                logger.debug("ack_remote_command attempt %s failed: %s", attempt, exc)
            time.sleep(2)

    def _init_db(self):
        try:
            self.db = self.services.db
            if self.login_was_performed:
                if self._session_started_at:
                    started_at_arg: datetime | str = self._session_started_at
                else:
                    started_dt = datetime.now(timezone.utc)
                    started_at_arg = started_dt
                    self._session_started_at = started_dt.isoformat()

                record_id, created = self.db.mark_session_active(
                    self.session_id,
                    email=self.email,
                    name=self.name,
                    status=STATUS_ACTIVE,
                    started_at=started_at_arg,
                    comment="Начало смены",
                    user_group=self.group or None,
                )
                if created and record_id and record_id > 0:
                    self._send_action_to_sheets(
                        record_id, user_group=self.group or None
                    )
                self.shift_start_time = self._get_local_session_start(
                    self.shift_start_time
                )
                self.status_start_time = datetime.now()
        except LocalDBError as e:
            logger.error(f"Ошибка инициализации БД: {e}")
            QMessageBox.critical(
                self, "Ошибка", "Не удалось инициализировать локальную базу данных"
            )
            raise

    def _init_ui(self):
        self.setWindowTitle("🕓 Учёт рабочего времени")
        self.setWindowIcon(QIcon(str(Path(__file__).parent / "sberhealf.png")))
        self.resize(500, 440)
        self.setMinimumSize(400, 350)

        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(15, 15, 15, 15)
        main_layout.setSpacing(15)

        header_layout = QHBoxLayout()
        logo_label = QLabel()
        logo_path = Path(__file__).parent / "sberhealf.png"
        if logo_path.exists():
            pixmap = QPixmap(str(logo_path))
            pixmap = pixmap.scaled(180, 80, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            logo_label.setPixmap(pixmap)
        header_layout.addWidget(logo_label)

        title_label = QLabel("Учёт рабочего времени")
        title_label.setStyleSheet("font-size: 18px; font-weight: bold;")
        header_layout.addWidget(title_label, alignment=Qt.AlignCenter)
        main_layout.addLayout(header_layout)

        self.info_label = QLabel()
        self.info_label.setStyleSheet(
            "QLabel { background-color: #f5f5f5; border-radius: 5px; padding: 10px; }"
        )
        self._update_info_text()
        main_layout.addWidget(self.info_label)

        self.comment_input = QTextEdit()
        self.comment_input.setPlaceholderText("Введите комментарий...")
        self.comment_input.setMaximumHeight(80)
        self.comment_input.setStyleSheet(
            "QTextEdit { border: 1px solid #ddd; border-radius: 5px; padding: 5px; }"
        )
        main_layout.addWidget(self.comment_input)

        self.time_label = QLabel("⏱ Время в статусе: 00:00:00")
        self.time_label.setAlignment(Qt.AlignCenter)
        self.time_label.setStyleSheet("font-size: 14px;")
        main_layout.addWidget(self.time_label)

        self.shift_timer_label = QLabel("⏰ Время смены: 00:00:00")
        self.shift_timer_label.setAlignment(Qt.AlignCenter)
        self.shift_timer_label.setStyleSheet("font-size: 14px; color: #0069c0;")
        main_layout.addWidget(self.shift_timer_label)

        for group in STATUS_GROUPS:
            btn_layout = QHBoxLayout()
            btn_layout.setSpacing(10)
            for status in group:
                btn = QPushButton(status)
                btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
                btn.clicked.connect(lambda _, s=status: self.set_status(s))
                btn_layout.addWidget(btn)
                self.status_buttons[status] = btn
            main_layout.addLayout(btn_layout)

        self.finish_btn = QPushButton("Завершить смену")
        self.finish_btn.setStyleSheet(
            """
            QPushButton {
                padding: 10px;
                border-radius: 5px;
                background-color: #f44336;
                color: white;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #d32f2f;
            }
        """
        )
        self.finish_btn.clicked.connect(self.finish_shift)
        main_layout.addWidget(self.finish_btn)

        self.setLayout(main_layout)
        self._update_button_states()

    def _init_timers(self):
        self.status_timer = QTimer(self)
        self.status_timer.timeout.connect(self._update_time_display)
        self.status_timer.start(1000)

        self.sync_timer = QTimer(self)
        self.sync_timer.timeout.connect(self._check_sync_status)
        self.sync_timer.start(60000)

    def _init_shift_check_timer(self):
        self.shift_check_timer = QTimer(self)
        self.shift_check_timer.timeout.connect(self._auto_check_shift_ended)
        self.shift_check_timer.start(30000)  # каждые 30 сек
        self._auto_check_shift_ended()

    def _auto_check_shift_ended(self):
        if self.shift_ended:
            return

        # 1) локальная проверка
        if self._is_shift_ended():
            self.shift_ended = True
            self.finish_btn.setEnabled(False)
            for btn in self.status_buttons.values():
                btn.setEnabled(False)
            Notifier.show(
                "WorkLog", "Смена завершена (автоматически, по данным системы)."
            )
            logger.info(f"[AUTO_LOGOUT_DETECT] Локально найден LOGOUT для {self.email}")
            return

        # Проверку удалённого статуса теперь выполняет контроллер

    def force_logout_by_admin(self):
        """
        Слот под SyncSignals.force_logout.
        Вызывается без аргументов. Показываем предупреждение и закрываемся.
        """
        if self._logout_in_progress:
            return

        self._logout_in_progress = True
        self._closing_reason = "remote_force_logout"
        self._notify_session_finish_requested("remote_force_logout")
        self._disable_post_logout()
        self.comment_input.clear()
        try:
            QMessageBox.warning(
                self,
                "Отключение",
                "Вы были отключены администратором. Сессия будет завершена.",
            )
        except Exception:  # pragma: no cover - Qt errors ignored
            pass
        Notifier.show("WorkLog", "Сессия завершается администратором")
        self._executor.submit(self._ack_remote_command_with_retry)
        self.controller.handle_remote_force_logout("remote_force_logout")

    def _update_info_text(self):
        info_text = (
            f"<b>Сотрудник:</b> {self.name}<br>"
            f"<b>Должность:</b> {self.role}<br>"
            f"<b>Группа:</b> {self.group}<br>"
            f"<b>Смена:</b> {self.shift_hours}<br>"
            f"<b>Текущий статус:</b> <span style='color: #2e7d32;'>{self.current_status}</span>"
        )
        self.info_label.setText(info_text)
        self.status_changed.emit(self.current_status)
        self._update_button_states()

    def _update_button_states(self):
        for status, btn in self.status_buttons.items():
            if status == self.current_status:
                btn.setStyleSheet(
                    """
                    QPushButton {
                        padding: 8px;
                        border-radius: 5px;
                        background-color: #b3ffb3;
                        font-weight: bold;
                        border: 2px solid #2e7d32;
                    }
                    QPushButton:hover {
                        background-color: #a1e6a1;
                    }
                """
                )
                btn.setEnabled(False)
            else:
                btn.setStyleSheet(
                    """
                    QPushButton {
                        padding: 8px;
                        border-radius: 5px;
                        background-color: #e0e0e0;
                    }
                    QPushButton:hover {
                        background-color: #d0d0d0;
                    }
                """
                )
                btn.setEnabled(True)
            if self.shift_ended:
                btn.setEnabled(False)

    def _update_time_display(self):
        time_in_status = datetime.now() - self.status_start_time
        hours, remainder = divmod(time_in_status.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        self.time_label.setText(
            f"⏱ Время в статусе: {hours:02d}:{minutes:02d}:{seconds:02d}"
        )

        shift_time = datetime.now() - self.shift_start_time
        h, rem = divmod(shift_time.seconds, 3600)
        m, s = divmod(rem, 60)
        self.shift_timer_label.setText(f"⏰ Время смены: {h:02d}:{m:02d}:{s:02d}")

    def _check_sync_status(self):
        if self.last_sync_time:
            time_since_sync = datetime.now() - self.last_sync_time
            if time_since_sync > timedelta(hours=1):
                Notifier.show("WorkLog", "Данные не синхронизировались более часа")

    def _is_shift_ended(self) -> bool:
        try:
            return self.db.check_existing_logout(self.email, session_id=self.session_id)
        except Exception as e:
            logger.error(f"Ошибка проверки завершения сменя: {e}")
            return False

    def set_status(self, new_status: str):
        if self.shift_ended:
            QMessageBox.warning(self, "Ошибка", "Смена уже завершена")
            return

        if new_status == self.current_status:
            QMessageBox.information(
                self, "Информация", "Вы уже находитесь в этом статусе"
            )
            return

        now_ts = time.monotonic()
        if now_ts - self._last_status_click < 0.8:
            return
        if self._status_in_progress:
            return
        self._status_in_progress = True
        self._last_status_click = now_ts

        try:
            comment = self.comment_input.toPlainText().strip()
            if len(comment) > MAX_COMMENT_LENGTH:
                QMessageBox.warning(
                    self,
                    "Ошибка",
                    f"Комментарий слишком длинный (максимум {MAX_COMMENT_LENGTH} символов)",
                )
                return

            if not comment:
                comment = "Смена статуса"

            self._finish_and_send_previous_status()

            now = datetime.now().isoformat()
            try:
                with write_tx() as conn:
                    record_id = self.db.log_action_tx(
                        conn=conn,
                        email=self.email,
                        name=self.name,
                        status=new_status,
                        action_type="STATUS_CHANGE",
                        comment=comment,
                        session_id=self.session_id,
                        status_start_time=now,
                        status_end_time=None,
                        reason=None,
                    )
                self._send_action_to_sheets(record_id)
            except Exception as e:
                logger.error(f"Ошибка записи нового статуса: {e}")
                QMessageBox.critical(self, "Ошибка", "Не удалось сохранить статус")
                return

            self.current_status = new_status
            self.status_start_time = datetime.fromisoformat(now)
            self.comment_input.clear()
            self._update_info_text()
            Notifier.show("WorkLog", f"Статус изменён на: {new_status}")
        finally:
            self._status_in_progress = False

    def _log_shift_end(
        self, comment: str, reason: str = "LOGOUT", sync_to_sheets: bool = True
    ) -> int:
        status_value = reason or STATUS_LOGOUT
        record_id = -1
        try:
            logout_moment = datetime.now(timezone.utc)
            record_id, created = self.db.finish_session(
                self.session_id,
                email=self.email,
                name=self.name,
                status=status_value,
                comment=comment,
                reason=status_value,
                logout_time=logout_moment,
                user_group=self.group or None,
            )
            if record_id and record_id > 0:
                if sync_to_sheets and created:
                    self._send_action_to_sheets(record_id, user_group=self.group)
                elif not sync_to_sheets:
                    try:
                        self.db.mark_actions_synced([record_id])
                    except Exception as sync_exc:
                        logger.debug(
                            "mark_actions_synced failed for record %s: %s",
                            record_id,
                            sync_exc,
                        )
            try:
                self.services.replicate_session_finish(
                    {
                        "session_id": self.session_id,
                        "email": self.email,
                        "name": self.name,
                        "logout_time": logout_moment.isoformat(),
                        "reason": status_value,
                        "comment": comment,
                        "group": self.group or None,
                    }
                )
            except Exception:  # pragma: no cover - best effort replication
                logger.debug("Failed to replicate session finish", exc_info=True)
            return record_id if record_id is not None else -1
        except Exception as e:
            logger.error(f"Ошибка записи завершения смены: {e}")
            raise

    def finish_shift(self):
        if self.shift_ended:
            QMessageBox.information(self, "Информация", "Смена уже завершена")
            return

        now_ts = time.monotonic()
        if now_ts - self._last_finish_click < 0.8:
            return

        if self._logout_in_progress:
            QMessageBox.information(self, "Информация", "Операция уже выполняется")
            return

        self._last_finish_click = now_ts

        comment = self.comment_input.toPlainText().strip()
        if not comment:
            comment = "Завершение смены"

        self._logout_in_progress = True
        self._closing_reason = "local_logout"
        self._notify_session_finish_requested("local_logout")

        try:
            self._log_shift_end(comment, reason=STATUS_LOGOUT, sync_to_sheets=True)
        except Exception:
            self._logout_in_progress = False
            self._closing_reason = "user_close"
            QMessageBox.critical(self, "Ошибка", "Не удалось завершить смену")
            return

        self._disable_post_logout()
        self.comment_input.clear()
        Notifier.show("WorkLog", "Завершение смены выполняется в фоне")
        self._start_logout_worker("local")
        self._emit_session_finished("local_logout")

    def closeEvent(self, event):
        if not self.shift_ended:
            reply = QMessageBox.question(
                self,
                "Подтверждение",
                "Вы уверены, что хотите закрыть приложение? Смена не завершена.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if reply == QMessageBox.No:
                event.ignore()
                return

        if callable(self.on_logout_callback):
            # передаём "причину" если колбэк принимает аргумент; иначе — без него
            try:
                self.on_logout_callback(self._closing_reason)
            except TypeError:
                self.on_logout_callback()
        super().closeEvent(event)


if __name__ == "__main__":  # pragma: no cover - manual testing
    app = QApplication(sys.argv)
    from user_app.app_controller import AppController
    from user_app.services import services as _services

    controller = AppController(_services)
    window = EmployeeApp(
        email="test@example.com",
        name="Тестовый Сотрудник",
        role="специалист",
        group="Тестовая группа",
        on_logout_callback=lambda reason: print(f"Logout reason: {reason}"),
        services=_services,
        controller=controller,
    )
    window.show()
    sys.exit(app.exec())
