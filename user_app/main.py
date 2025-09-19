from __future__ import annotations

# user_app/main.py
import atexit
import datetime as dt
import logging
import sys
import threading
import traceback
from pathlib import Path
from typing import Any, Dict

from PyQt5.QtCore import QObject, QThread, pyqtSignal
from PyQt5.QtWidgets import QApplication, QMessageBox

# Добавляем корень проекта в sys.path
ROOT = Path(__file__).parent.parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auto_sync import SyncManager  # ← добавили
from consts import (
    STATUS_ACTIVE,
    STATUS_FORCE_LOGOUT,
    STATUS_LOGOUT,
    normalize_session_status,
)

# Инициализация логирования через единый модуль
from config import (
    DB_FALLBACK_PATH,
    DB_MAIN_PATH,
    HEARTBEAT_PERIOD_SEC,
    LOG_DIR,
    get_credentials_file,
)
from logging_setup import setup_logging
from notifications.engine import start_background_poller
from sheets_api import SheetsAPI  # Явный импорт класса SheetsAPI
from user_app import db_local
from user_app import session as session_state  # ← добавили импорт
from user_app.api import UserAPI
from user_app.signals import SessionSignals, SyncSignals

atexit.register(db_local.close_connection)


# ----- Сигналы приложения -----
class ApplicationSignals(QObject):
    app_started = pyqtSignal()
    app_shutdown = pyqtSignal()
    login_attempt = pyqtSignal(str)
    login_success = pyqtSignal(dict)
    login_failed = pyqtSignal(str)
    sync_status_changed = pyqtSignal(bool)
    sync_progress = pyqtSignal(int, int)
    sync_finished = pyqtSignal(bool)


def _hb_loop(
    api: UserAPI,
    email: str | None,
    session_id: str,
    stop_evt: threading.Event,
    period_sec: int,
    session_signals: "SessionSignals" | None = None,
    suppress_evt: threading.Event | None = None,
) -> None:
    logger = logging.getLogger(__name__)

    try:
        period = int(period_sec)
    except (TypeError, ValueError):
        period = 60
    if period <= 0:
        period = 60

    remote_emitted = False

    def _check_remote() -> None:
        nonlocal remote_emitted
        if remote_emitted:
            return
        if suppress_evt and suppress_evt.is_set():
            return
        try:
            raw_status = api.get_session_status(
                session_id=session_id, email=email
            )
        except Exception as exc:  # pragma: no cover - защита от сетевых ошибок
            logger.debug("Heartbeat remote check failed: %s", exc)
            return

        normalized_status = normalize_session_status(raw_status)
        logger.info(
            "heartbeat status=%s session=%s",
            normalized_status or "<unknown>",
            session_id,
        )

        if not session_signals or not email:
            return

        if normalized_status and normalized_status != STATUS_ACTIVE:
            remote_emitted = True
            logger.info(
                "Heartbeat detected non-active status (session=%s, status=%s)",
                session_id,
                normalized_status,
            )
            session_signals.sessionFinished.emit("remote_force_logout")
            stop_evt.set()

    def _send_once() -> None:
        try:
            api.heartbeat_session(session_id=session_id)
            _check_remote()
        except Exception as exc:
            logger.warning("Heartbeat thread error for session %s: %s", session_id, exc)

    if not stop_evt.is_set():
        _send_once()

    while not stop_evt.wait(period):
        _send_once()


# ----- Менеджер приложения -----
class ApplicationManager(QObject):
    def __init__(self):
        super().__init__()
        self.app = QApplication(sys.argv)
        self.app.setStyle("Fusion")
        self.app.setApplicationName("WorkTimeTracker")
        self.app.setApplicationVersion("1.0.0")

        self.login_window = None
        self.main_window = None
        self.signals = ApplicationSignals()
        self.session_signals = SessionSignals()
        self.session_signals.sessionFinished.connect(self._handle_session_finished)

        self.sync_thread: QThread | None = None
        self.sync_worker: SyncManager | None = None
        self.sync_signals = (
            SyncSignals()
        )  # сигналы доступны и для GUI, и для SyncManager
        self._sync_running = False
        self._sync_offline_mode = False

        sys.excepthook = self.handle_uncaught_exception

        self.user_api: UserAPI | None = None
        self._heartbeat_stop_evt: threading.Event | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_period = HEARTBEAT_PERIOD_SEC
        self._current_session_id: str | None = None
        self._current_user_email: str | None = None
        self._pending_finish_reason: str | None = None
        self._returning_to_login = False
        self._suppress_remote_checks = threading.Event()
        self._session_already_terminated = False

        offline_mode = False
        try:
            self._initialize_resources()
        except Exception as e:
            logging.getLogger(__name__).error("Init resources failed: %s", e)
            QMessageBox.warning(
                None,
                "Офлайн режим",
                "Не удалось подключиться к серверу.\nПриложение запущено в офлайн-режиме.",
            )
            offline_mode = True
        self._start_sync_service(offline_mode=offline_mode)
        self.signals.app_started.emit()

    # --- Инициализация ресурсов ---
    def _initialize_resources(self):
        creds_path = get_credentials_file()
        if not creds_path.exists():
            raise FileNotFoundError(f"Credentials file not found: {creds_path}")

        # Инициализация клиента Google Sheets + расширенная диагностика
        try:
            import sheets_api as _sheets_api_mod

            logging.getLogger(__name__).info(
                "Using sheets_api module: %s",
                getattr(_sheets_api_mod, "__file__", "<unknown>"),
            )
        except Exception:
            _sheets_api_mod = None

        try:
            self.sheets_api = SheetsAPI()
            logging.getLogger(__name__).info(
                "SheetsAPI.has(check_credentials)=%s; has(test_connection)=%s",
                hasattr(SheetsAPI, "check_credentials"),
                hasattr(self.sheets_api, "test_connection"),
            )
            self.user_api = UserAPI(self.sheets_api)
        except Exception as e:
            logging.getLogger(__name__).error("SheetsAPI init failed: %s", e)
            raise

        # Не валимся, если в используемой версии класса нет метода check_credentials.
        ok = False
        try:
            if hasattr(self.sheets_api, "check_credentials"):
                ok = self.sheets_api.check_credentials()
            elif hasattr(self.sheets_api, "test_connection"):
                logging.getLogger(__name__).warning(
                    "SheetsAPI.check_credentials() отсутствует, используем test_connection()"
                )
                ok = self.sheets_api.test_connection()
            else:
                logging.getLogger(__name__).warning(
                    "У SheetsAPI нет check_* методов — пропускаем health-check"
                )
                ok = True
        except Exception as e:
            logging.getLogger(__name__).error("Sheets credentials/test failed: %s", e)
            ok = False

        if not ok:
            raise RuntimeError("Invalid Google Sheets credentials")

    def _start_session_heartbeat(self, session_id: str | None) -> None:
        self._stop_session_heartbeat()
        if not session_id or not self.user_api:
            return

        stop_evt = threading.Event()
        try:
            period_value = int(self._heartbeat_period)
        except (TypeError, ValueError):
            period_value = HEARTBEAT_PERIOD_SEC
        if period_value <= 0:
            period_value = HEARTBEAT_PERIOD_SEC

        thread = threading.Thread(
            target=_hb_loop,
            args=(
                self.user_api,
                self._current_user_email,
                session_id,
                stop_evt,
                period_value,
                self.session_signals,
                self._suppress_remote_checks,
            ),
            daemon=True,
            name="session-heartbeat",
        )
        self._heartbeat_stop_evt = stop_evt
        self._heartbeat_thread = thread
        self._current_session_id = session_id
        thread.start()
        logging.getLogger(__name__).info(
            "Session heartbeat started (session=%s, period=%s)",
            session_id,
            period_value,
        )

    def _stop_session_heartbeat(self) -> None:
        stop_evt = self._heartbeat_stop_evt
        thread = self._heartbeat_thread
        session_id = self._current_session_id

        self._heartbeat_stop_evt = None
        self._heartbeat_thread = None
        self._current_session_id = None

        if stop_evt:
            stop_evt.set()
        if thread and thread.is_alive():
            thread.join(timeout=2.0)

        if session_id:
            logging.getLogger(__name__).info(
                "Session heartbeat stopped (session=%s)", session_id
            )

    # --- Фоновая синхронизация ---
    def _start_sync_service(self, offline_mode: bool = False):
        try:
            logger = logging.getLogger(__name__)
            logger.info("=== ЗАПУСК СЕРВИСА СИНХРОНИЗАЦИИ ===")
            # QThread + worker
            self.sync_thread = QThread(self)
            self.sync_worker = SyncManager(
                signals=self.sync_signals,
                background_mode=True,
                session_signals=self.session_signals,
            )
            if offline_mode:
                # мягкий режим восстановления сети
                self.sync_worker._is_offline_recovery = True
                self.sync_worker._sync_interval = 10
            self.sync_worker.moveToThread(self.sync_thread)
            self.sync_thread.started.connect(
                self.sync_worker.run
            )  # run() есть в SyncManager
            self.sync_thread.start()
            logger.info("Sync service thread started (offline_mode=%s)", offline_mode)
            self._sync_running = True
            self._sync_offline_mode = offline_mode
        except Exception as e:
            logger.error(f"Failed to start sync service: {e}")

    def _stop_sync_service(self) -> None:
        logger = logging.getLogger(__name__)
        if not self.sync_thread and not self.sync_worker:
            self._sync_running = False
            return

        try:
            if self.sync_worker:
                try:
                    self.sync_worker.stop()
                except Exception as exc:  # pragma: no cover - безопасная остановка
                    logger.debug("Sync worker stop error: %s", exc)
        finally:
            if self.sync_thread and self.sync_thread.isRunning():
                self.sync_thread.quit()
                self.sync_thread.wait(2000)

        self.sync_thread = None
        self.sync_worker = None
        self._sync_running = False

    # --- UI потоки ---
    def show_login_window(self):
        try:
            from user_app.login_window import LoginWindow

            self.login_window = LoginWindow()
            self.login_window.login_success.connect(self.handle_login_success)
            self.login_window.login_failed.connect(self.handle_login_failed)
            self.login_window.show()
        except Exception as e:
            self._show_error("Login Error", f"Cannot show login window: {e}")
            self.quit_application()

    def handle_login_success(self, user_data: Dict[str, Any]):
        try:
            from user_app.gui import EmployeeApp

            # закрыть окно логина
            if self.login_window:
                try:
                    self.login_window.close()
                except Exception:
                    pass

            # достаём данные, которые LoginWindow уже собирает
            session_id = None
            login_was_performed = True
            if user_data.get("unfinished_session"):
                session_id = user_data["unfinished_session"].get("session_id")
            if "login_was_performed" in user_data:
                login_was_performed = bool(user_data["login_was_performed"])

            self._session_already_terminated = False
            session_started_at = user_data.get("session_started_at")

            self._suppress_remote_checks.clear()
            self._pending_finish_reason = None

            def on_logout_wrapper(reason: str | None = None):
                """
                Колбэк из EmployeeApp при закрытии/логауте.
                reason: "admin_logout" | "user_close" | "auto_logout" | None
                Сейчас логика простая — просто корректно завершаем приложение.
                При желании здесь можно добавить отдельную запись в логи/Sheets.
                """
                if reason == "return_to_login":
                    return
                self.quit_application()

            # создаём главное окно как раньше
            self.main_window = EmployeeApp(
                email=user_data["email"],
                name=user_data["name"],
                role=user_data["role"],
                shift_hours=user_data["shift_hours"],
                telegram_login=user_data.get("telegram_login", ""),
                on_logout_callback=on_logout_wrapper,
                session_id=session_id,
                login_was_performed=login_was_performed,
                group=user_data.get("group", ""),
                session_signals=self.session_signals,
                on_session_finish_requested=self.handle_session_finish_requested,
                session_started_at=session_started_at,
            )
            self.main_window.show()

            # подключаем «принудительный разлогин» из сервиса синхронизации
            try:
                self.sync_signals.force_logout.disconnect()
            except TypeError:
                pass
            self.sync_signals.force_logout.connect(self._emit_remote_force_logout)
            logger = logging.getLogger(__name__)
            logger.info("force_logout сигнал подключён к sessionFinished")

            actual_session_id = getattr(self.main_window, "session_id", session_id)
            self._current_session_id = actual_session_id
            self._current_user_email = user_data.get("email")
            self._start_session_heartbeat(actual_session_id)
            try:
                session_state.set_session_id(actual_session_id or "")
                session_state.set_user_email(self._current_user_email or "")
            except Exception:
                logger.debug("Unable to persist session id in session_state")

            if not self._sync_running:
                self._start_sync_service(offline_mode=self._sync_offline_mode)

        except Exception as e:
            self._show_error("Main Window Error", f"Cannot show main window: {e}")
            self.quit_application()

    def handle_login_failed(self, message: str):
        self._show_error("Login Failed", message)

    def handle_session_finish_requested(self, reason: str):
        logger = logging.getLogger(__name__)
        normalized = (reason or "").strip().lower()
        self._pending_finish_reason = normalized
        if normalized.startswith("local"):
            logger.info("Session finish requested (local)")
            self._suppress_remote_checks.set()
        elif normalized.startswith("remote"):
            logger.info("Session forced remotely (admin)")
        else:
            logger.info("Session finish requested (%s)", normalized or "unknown")
        self._stop_session_heartbeat()

    def _handle_session_finished(self, reason: str) -> None:
        logger = logging.getLogger(__name__)
        normalized = (reason or "").strip().lower() or "local_logout"
        pending = (self._pending_finish_reason or "").strip().lower()
        if normalized.startswith("remote") and not pending.startswith("remote"):
            logger.info("Session forced remotely (admin)")
        if normalized.startswith("local") and not pending.startswith("local"):
            logger.info("Session finish requested (local)")
        self.return_to_login(normalized)

    def _emit_remote_force_logout(self) -> None:
        self.session_signals.sessionFinished.emit("remote_force_logout")

    def _finalize_local_session(self, reason: str) -> None:
        logger = logging.getLogger(__name__)
        window = self.main_window
        email = getattr(window, "email", None) or self._current_user_email
        session_id = getattr(window, "session_id", None) or self._current_session_id
        if not email or not session_id:
            return

        status_value = (
            STATUS_FORCE_LOGOUT if reason.startswith("remote") else STATUS_LOGOUT
        )
        comment = (
            "Сессия завершена администратором"
            if reason.startswith("remote")
            else "Смена завершена"
        )

        try:
            db = db_local.LocalDB()
        except Exception as exc:
            logger.debug("LocalDB unavailable on finalize: %s", exc)
            return

        record_id: int | None = None
        created = False
        try:
            name_value = getattr(window, "name", "") or email
            group_value = getattr(window, "group", None)
            record_id, created = db.finish_session(
                session_id,
                email=email,
                name=name_value,
                status=status_value,
                comment=comment,
                reason=status_value,
                logout_time=dt.datetime.now(dt.UTC),
                user_group=group_value,
            )
        except Exception as exc:
            logger.warning(
                "Finalize local session failed (session=%s): %s", session_id, exc
            )
            return

        if (
            reason.startswith("remote")
            and record_id
            and record_id > 0
            and created
        ):
            try:
                db.mark_actions_synced([record_id])
            except Exception as exc:
                logger.debug("mark_actions_synced failed for %s: %s", record_id, exc)

    def _ack_remote_command_async(self, email: str, session_id: str) -> None:
        if not hasattr(self.sheets_api, "ack_remote_command"):
            return

        logger = logging.getLogger(__name__)

        def _worker() -> None:
            try:
                ok = self.sheets_api.ack_remote_command(
                    email=email, session_id=session_id
                )
                logger.info("ACK remote command for session %s -> %s", session_id, ok)
            except Exception as exc:
                logger.warning(
                    "Failed to ACK remote command (session=%s, email=%s): %s",
                    session_id,
                    email,
                    exc,
                )

        threading.Thread(target=_worker, name="remote-ack", daemon=True).start()

    def _show_logout_message(self, reason: str) -> None:
        reason_key = (reason or "").strip().lower()
        messages = {
            "local_logout": "Смена завершена",
            "local_logout_offline": "Смена будет завершена при восстановлении сети.",
            "remote_force_logout": "Сессия завершена администратором",
        }
        message = messages.get(reason_key)
        if not message:
            return

        try:
            if reason_key.startswith("remote"):
                QMessageBox.warning(None, "Смена", message)
            else:
                QMessageBox.information(None, "Смена", message)
        except (
            Exception
        ) as exc:  # pragma: no cover - показ сообщения может не удаться в headless
            logging.getLogger(__name__).debug("Failed to show logout message: %s", exc)

    # --- Общее ---
    def _show_error(self, title: str, message: str):
        QMessageBox.critical(None, title, message)
        logger = logging.getLogger(__name__)
        logger.error("%s: %s", title, message)

    def return_to_login(self, reason: str) -> None:
        logger = logging.getLogger(__name__)
        normalized = (reason or "").strip().lower() or "local_logout"
        if self._session_already_terminated:
            logger.debug(
                "Session already terminated, skip return_to_login (reason=%s)",
                normalized,
            )
            return
        self._session_already_terminated = True
        if self._returning_to_login:
            logger.debug(
                "Return to login already in progress (skip reason=%s)", normalized
            )
            return

        self._returning_to_login = True
        try:
            logger.info("Return to login (reason=%s)", normalized)
            self._stop_session_heartbeat()
            self._finalize_local_session(normalized)

            if (
                normalized.startswith("remote")
                and self._current_user_email
                and self._current_session_id
            ):
                self._ack_remote_command_async(
                    self._current_user_email,
                    self._current_session_id,
                )

            self._stop_sync_service()

            try:
                session_state.set_session_id("")
                session_state.set_user_email("")
            except Exception:
                logger.debug("Unable to reset session state")

            self._pending_finish_reason = None
            self._suppress_remote_checks.clear()

            if self.main_window:
                try:
                    setattr(self.main_window, "_closing_reason", "return_to_login")
                except Exception:
                    pass
                try:
                    self.main_window.on_logout_callback = None
                except Exception:
                    pass
                try:
                    self.main_window.close()
                except Exception as exc:
                    logger.error("Error on main_window.close(): %s", exc)
                self.main_window = None

            if self.login_window:
                try:
                    self.login_window.close()
                except Exception as exc:
                    logger.debug("Error closing login window before restart: %s", exc)
                self.login_window = None

            self._current_session_id = None
            self._current_user_email = None

            self.show_login_window()
            self._show_logout_message(normalized)
        finally:
            self._returning_to_login = False

    def handle_uncaught_exception(self, exc_type, exc_value, exc_traceback):
        logger = logging.getLogger(__name__)
        logger.critical(
            "Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback)
        )
        self._show_error(
            "Critical Error", f"An unexpected error occurred:\n\n{exc_value}"
        )
        self.quit_application()

    def quit_application(self):
        logger = logging.getLogger(__name__)
        logger.info("Shutting down application.")
        self.signals.app_shutdown.emit()

        self._stop_session_heartbeat()

        # закрываем окна
        if self.main_window:
            try:
                self.main_window.close()
            except Exception as e:
                logger.error("Error on main_window.close(): %s", e)
            self.main_window = None

        if self.login_window:
            try:
                self.login_window.close()
            except Exception as e:
                logger.error("Error on login_window.close(): %s", e)
            self.login_window = None

        # останавливаем сервис синхронизации
        self._stop_sync_service()
        db_local.close_connection()
        self.app.quit()

    # точка входа UI
    def run(self):
        self.show_login_window()
        sys.exit(self.app.exec_())


# ----- CLI -----
def main():
    poller_stop = None
    try:
        # единый логгер
        log_path = setup_logging(app_name="wtt-user", log_dir=LOG_DIR)
        logger = logging.getLogger(__name__)
        logger.info("Logging initialized (path=%s)", log_path)

        # один раз при старте
        db_local.init_db(DB_MAIN_PATH, DB_FALLBACK_PATH)

        # Запускаем фоновый опросчик уведомлений
        poller_stop = start_background_poller(60)

        app_manager = ApplicationManager()
        app_manager.run()
    except Exception as e:
        logging.critical(f"Fatal error: %s\n{traceback.format_exc()}", e)
        QMessageBox.critical(None, "Fatal Error", f"Application failed to start:\n{e}")
        sys.exit(1)
    finally:
        logger.info("Shutting down application.")
        if poller_stop:
            poller_stop.set()


if __name__ == "__main__":
    main()
