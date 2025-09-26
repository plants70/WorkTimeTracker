# main.py
import sys
import os
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from PyQt5.QtWidgets import QApplication, QMessageBox
from PyQt5.QtCore import QObject, pyqtSignal, QThread
import signal
import traceback
import atexit

# Добавляем корень проекта в sys.path для поиска auto_sync.py
sys.path.insert(0, str(Path(__file__).parent.parent))

# --- Импортируем logger из config.py ---
from config import LOG_DIR, CREDENTIALS_FILE
# Настройка логирования должна быть в config.py
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'app.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info("Logging initialized (DEBUG mode)")
logger.info(f"Logs directory: {LOG_DIR}")
# --- Конец настройки логов ---

from auto_sync import SyncManager, SyncSignals

class ApplicationSignals(QObject):
    app_started = pyqtSignal()
    app_shutdown = pyqtSignal()
    login_attempt = pyqtSignal(str)
    login_success = pyqtSignal(dict)
    login_failed = pyqtSignal(str)
    sync_status_changed = pyqtSignal(bool)
    sync_progress = pyqtSignal(int, int)
    sync_finished = pyqtSignal(bool)

class ApplicationManager(QObject):
    def __init__(self):
        super().__init__()
        self.app = QApplication(sys.argv)
        self.app.setStyle('Fusion')
        self.app.setApplicationName("WorkTimeTracker")
        self.app.setApplicationVersion("1.0.0")

        self.login_window = None
        self.main_window = None
        self.signals = ApplicationSignals()
        self.sync_thread = None
        self.sync_worker = None
        self._shift_ended_by_user = False

        self.sync_signals = SyncSignals()

        sys.excepthook = self.handle_uncaught_exception

        try:
            self._initialize_resources()
            self._start_sync_service()
            self.signals.app_started.emit()
        except Exception as e:
            self._show_error("Initialization Error", f"Failed to initialize: {str(e)}")
            sys.exit(1)

    def _initialize_resources(self):
        # CREDENTIALS_FILE уже определен в config.py
        from sheets_api import SheetsAPI
        if not CREDENTIALS_FILE.exists():
            raise FileNotFoundError(f"Credentials file not found: {CREDENTIALS_FILE}")
        self.sheets_api = SheetsAPI()
        if not self.sheets_api.check_credentials():
            raise RuntimeError("Invalid Google Sheets credentials")
        # validate_config() уже вызван в config.py

    def _start_sync_service(self):
        try:
            logger.info("=== ЗАПУСК СЕРВИСА СИНХРОНИЗАЦИИ ===")
            self.sync_thread = QThread()
            
            # Создаем SyncManager (теперь он QObject)
            self.sync_worker = SyncManager(signals=self.sync_signals, background_mode=True)
            self.sync_worker.moveToThread(self.sync_thread)
            
            # --- ПОДКЛЮЧАЕМ ТОЛЬКО started ---
            self.sync_thread.started.connect(self.sync_worker.run_service)
            # ---
            
            self.sync_thread.finished.connect(self.sync_thread.deleteLater)
            
            atexit.register(lambda: self.sync_worker.stop())
            self.sync_thread.start()
            
            logging.info("Sync service started")
        except Exception as e:
            logging.error(f"Failed to start sync service: {e}")

    def show_login_window(self):
        try:
            from user_app.login_window import LoginWindow
            self.login_window = LoginWindow()
            self.login_window.login_success.connect(self.handle_login_success)
            self.login_window.login_failed.connect(self.handle_login_failed)
            self.login_window.show()
        except Exception as e:
            self._show_error("Login Error", f"Cannot show login window: {str(e)}")
            self.quit_application()

    def handle_login_success(self, user_data: Dict[str, Any]):
        try:
            from user_app.gui import EmployeeApp
            if self.login_window:
                self.login_window.close()
            
            session_id = None
            login_was_performed = True
            if "unfinished_session" in user_data and user_data["unfinished_session"]:
                session_id = user_data["unfinished_session"].get("session_id")
            if "login_was_performed" in user_data:
                login_was_performed = user_data["login_was_performed"]
            
            def on_logout_wrapper():
                self._shift_ended_by_user = True
                self.quit_application()
            
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

            self.sync_signals.force_logout.connect(self.main_window.force_logout_by_admin)
            logging.info("force_logout сигнал подключён к force_logout_by_admin")
        except Exception as e:
            self._show_error("Main Window Error", f"Cannot show main window: {str(e)}")
            self.quit_application()

    def handle_login_failed(self, message: str):
        self._show_error("Login Failed", message)

    def _show_error(self, title: str, message: str):
        QMessageBox.critical(None, title, message)
        logging.error(f"{title}: {message}")

    def handle_uncaught_exception(self, exc_type, exc_value, exc_traceback):
        logging.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))
        self._show_error("Critical Error", f"An unexpected error occurred:\n\n{str(exc_value)}")
        self.quit_application()

    def quit_application(self):
        logging.info("Shutting down application...")
        self.signals.app_shutdown.emit()
        if self.main_window:
            try:
                self.main_window.close()
            except Exception as e:
                logging.error(f"Error on main_window.close(): {e}")
        if self.login_window:
            try:
                self.login_window.close()
            except Exception as e:
                logging.error(f"Error on login_window.close(): {e}")
        if self.sync_worker:
            self.sync_worker.stop()
        if self.sync_thread and self.sync_thread.isRunning():
            # Вместо quit() используем wait()
            self.sync_thread.quit()
            self.sync_thread.wait() # Ждем завершения потока
        self.app.quit()

    def run(self):
        self.show_login_window()
        sys.exit(self.app.exec_())

def main():
    try:
        # Логирование уже настроено
        app_manager = ApplicationManager()
        app_manager.run()
    except Exception as e:
        # Даже если что-то пошло не так, мы можем залогировать
        logging.critical(f"Fatal error: {e}\n{traceback.format_exc()}")
        QMessageBox.critical(None, "Fatal Error", f"Application failed to start:\n{str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()