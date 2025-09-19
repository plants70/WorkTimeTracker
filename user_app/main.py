from __future__ import annotations

import logging
import sys
import traceback

from PyQt5.QtWidgets import QApplication, QMessageBox

from config import DB_FALLBACK_PATH, DB_MAIN_PATH, LOG_DIR
from logging_setup import setup_logging
from notifications.engine import start_background_poller
from user_app import db_local
from user_app.app_controller import AppController
from user_app.services import services


def _handle_uncaught(exc_type, exc_value, exc_traceback) -> None:
    logger = logging.getLogger(__name__)
    logger.critical(
        "Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback)
    )
    QMessageBox.critical(
        None,
        "Critical Error",
        f"An unexpected error occurred:\n\n{exc_value}",
    )


def _create_app() -> QApplication:
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setApplicationName("WorkTimeTracker")
    app.setApplicationVersion("1.0.0")
    return app


def main() -> None:
    poller_stop = None
    app: QApplication | None = None
    try:
        log_path = setup_logging(app_name="wtt-user", log_dir=LOG_DIR)
        logger = logging.getLogger(__name__)
        logger.info("Logging initialized (path=%s)", log_path)

        db_local.init_db(DB_MAIN_PATH, DB_FALLBACK_PATH)

        poller_stop = start_background_poller(60)

        app = _create_app()
        sys.excepthook = _handle_uncaught

        controller = AppController(services)
        controller.start()

        exit_code = app.exec_()
        services.shutdown()
        if poller_stop:
            poller_stop.set()
        db_local.close_connection()
        sys.exit(exit_code)
    except Exception as exc:  # pragma: no cover - startup failures
        logging.critical(
            "Fatal error: %s\n%s", exc, traceback.format_exc(), exc_info=True
        )
        QMessageBox.critical(
            None,
            "Fatal Error",
            f"Application failed to start:\n{exc}",
        )
        if poller_stop:
            poller_stop.set()
        if app:
            app.quit()
        sys.exit(1)


if __name__ == "__main__":
    main()
