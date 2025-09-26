# user_app/main.py
import sys
import logging
from pathlib import Path
from typing import Dict, Any
from PyQt5.QtWidgets import QApplication, QMessageBox
from PyQt5.QtCore import QObject, pyqtSignal, QThread
import traceback
import atexit

# Добавляем корень проекта в sys.path
ROOT = Path(__file__).parent.parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Инициализация логирования через единый модуль
from config import LOG_DIR, get_credentials_file
from logging_setup import setup_logging
from user_app.signals import SyncSignals
from sheets_api import SheetsAPI  # Явный импорт класса SheetsAPI
from auto_sync import SyncManager  # ← добавили

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

        self.sync_thread: QThread | None = None
        self.sync_worker: SyncManager | None = None
        self.sync_signals = SyncSignals()  # сигналы доступны и для GUI, и для SyncManager

        sys.excepthook = self.handle_uncaught_exception

        try:
            self._initialize_resources()
            self._start_sync_service()
            self.signals.app_started.emit()
        except Exception as e:
            self._show_error("Initialization Error", f"Failed to initialize: {e}")
            sys.exit(1)

    # --- Инициализация ресурсов ---
    def _initialize_resources(self):
        creds_path = get_credentials_file()
        if not creds_path.exists():
            raise FileNotFoundError(f"Credentials file not found: {creds_path}")
        
        # Инициализация клиента Google Sheets
        try:
            self.sheets_api = SheetsAPI()
        except Exception as e:
            logging.getLogger(__name__).error("SheetsAPI init failed: %s", e)
            raise
        
        if not self.sheets_api.check_credentials():
            raise RuntimeError("Invalid Google Sheets credentials")

    # --- Фоновая синхронизация ---
    def _start_sync_service(self):
        try:
            logger = logging.getLogger(__name__)
            logger.info("=== ЗАПУСК СЕРВИСА СИНХРОНИЗАЦИИ ===")
            
            # Запускаем сервис синхронизации в фоне
            self.sync_manager = SyncManager(signals=self.sync_signals, background_mode=True)
            if hasattr(self.sync_manager, "start"):
                self.sync_manager.start()
            elif hasattr(self.sync_manager, "start_background"):
                self.sync_manager.start_background()
            
            logger.info("Sync service started")
        except Exception as e:
            logger.error(f"Failed to start sync service: {e}")

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

            def on_logout_wrapper():
                # корректно завершаем приложение по запросу из EmployeeApp
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
                group=user_data.get("group", "")
            )
            self.main_window.show()

            # подключаем «принудительный разлогин» из сервиса синхронизации
            self.sync_signals.force_logout.connect(self.main_window.force_logout_by_admin)
            logger = logging.getLogger(__name__)
            logger.info("force_logout сигнал подключён к force_logout_by_admin")

        except Exception as e:
            self._show_error("Main Window Error", f"Cannot show main window: {e}")
            self.quit_application()

    def handle_login_failed(self, message: str):
        self._show_error("Login Failed", message)

    # --- Общее ---
    def _show_error(self, title: str, message: str):
        QMessageBox.critical(None, title, message)
        logger = logging.getLogger(__name__)
        logger.error("%s: %s", title, message)

    def handle_uncaught_exception(self, exc_type, exc_value, exc_traceback):
        logger = logging.getLogger(__name__)
        logger.critical(
            "Unhandled exception",
            exc_info=(exc_type, exc_value, exc_traceback)
        )
        self._show_error("Critical Error", f"An unexpected error occurred:\n\n{exc_value}")
        self.quit_application()

    def quit_application(self):
        logger = logging.getLogger(__name__)
        logger.info("Shutting down application.")
        self.signals.app_shutdown.emit()

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
        try:
            if self.sync_worker:
                self.sync_worker.stop()
        except Exception:
            pass
        if self.sync_thread and self.sync_thread.isRunning():
            self.sync_thread.quit()
            self.sync_thread.wait()

        self.app.quit()

    # точка входа UI
    def run(self):
        self.show_login_window()
        sys.exit(self.app.exec_())

# ----- CLI -----
def main():
    try:
        # единый логгер
        log_path = setup_logging(app_name="wtt-user", log_dir=LOG_DIR)
        logger = logging.getLogger(__name__)
        logger.info("Logging initialized (path=%s)", log_path)
        
        app_manager = ApplicationManager()
        app_manager.run()
    except Exception as e:
        logging.critical(f"Fatal error: {e}\n{traceback.format_exc()}")
        QMessageBox.critical(None, "Fatal Error", f"Application failed to start:\n{e}")
        sys.exit(1)

if __name__ == "__main__":
    main()