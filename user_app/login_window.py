from __future__ import annotations

import logging
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QFont, QIcon, QPixmap
from PyQt5.QtWidgets import (
    QDialog,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QSpacerItem,
    QVBoxLayout,
)

if TYPE_CHECKING:  # pragma: no cover - only for type checkers
    from user_app.app_controller import AppController

logger = logging.getLogger(__name__)


class LoginWindow(QDialog):
    def __init__(self, controller: "AppController", parent=None):
        super().__init__(parent)
        self.controller = controller
        self.setWindowTitle("Вход в систему")
        self.setWindowIcon(QIcon(self._resource_path("user_app/sberhealf.png")))
        self.setFixedSize(440, 360)
        self.auth_in_progress = False
        self._showing_error = False
        logger.debug("LoginWindow: initialized")
        self._init_ui()
        self._setup_shortcuts()
        self._connect_controller_signals()
        self._slow_login_timer = QTimer(self)
        self._slow_login_timer.setSingleShot(True)
        self._slow_login_timer.timeout.connect(self._show_slow_login_hint)

    # --- UI setup -----------------------------------------------------
    def _resource_path(self, relative_path: str) -> str:
        if hasattr(sys, "_MEIPASS"):
            base_path = Path(sys._MEIPASS)
        else:
            base_path = Path(__file__).parent.parent
        return str(base_path / relative_path)

    def _init_ui(self) -> None:
        self.setFont(QFont("Segoe UI", 11))
        layout = QVBoxLayout()
        layout.setContentsMargins(30, 25, 30, 25)
        layout.setSpacing(18)

        logo_label = QLabel()
        try:
            pixmap = QPixmap(self._resource_path("user_app/sberhealf.png"))
            pixmap = pixmap.scaled(170, 70, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            logo_label.setPixmap(pixmap)
            logo_label.setAlignment(Qt.AlignCenter)
            layout.addWidget(logo_label)
        except Exception as exc:  # pragma: no cover - best effort
            logger.debug("Failed to load login logo: %s", exc)

        title_label = QLabel("Вход в систему")
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setStyleSheet(
            "font-size: 22px; font-weight: bold; color: #222; margin-bottom: 15px;"
        )
        layout.addWidget(title_label)

        layout.addSpacerItem(
            QSpacerItem(20, 10, QSizePolicy.Minimum, QSizePolicy.Fixed)
        )

        self.email_input = QLineEdit()
        self.email_input.setPlaceholderText("Корпоративный email")
        self.email_input.setStyleSheet(
            """
            QLineEdit {
                padding: 11px;
                border: 1.5px solid #ccc;
                border-radius: 8px;
                font-size: 15px;
                min-width: 290px;
                max-width: 350px;
            }
        """
        )
        self.email_input.setMinimumWidth(290)
        self.email_input.setMaximumWidth(350)
        layout.addWidget(self.email_input, alignment=Qt.AlignCenter)

        layout.addSpacerItem(QSpacerItem(20, 8, QSizePolicy.Minimum, QSizePolicy.Fixed))

        self.login_btn = QPushButton("Войти")
        self.login_btn.setStyleSheet(
            """
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 13px;
                font-size: 16px;
                border-radius: 9px;
                min-width: 180px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """
        )
        self.login_btn.setMinimumHeight(40)
        self.login_btn.setMaximumWidth(220)
        self.login_btn.clicked.connect(self._try_login)
        layout.addWidget(self.login_btn, alignment=Qt.AlignCenter)

        self.status_label = QLabel()
        self.status_label.setStyleSheet(
            "color: #666; font-size: 13px; margin-top: 12px; min-height: 18px;"
        )
        self.status_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.status_label)

        layout.addStretch(1)
        self.setLayout(layout)

    def _setup_shortcuts(self) -> None:
        self.email_input.returnPressed.connect(self._try_login)

    def _connect_controller_signals(self) -> None:
        self.controller.login_started.connect(self._on_login_started)
        self.controller.login_failed.connect(self._on_login_failed)
        self.controller.login_succeeded.connect(self._on_login_succeeded)

    # --- Handlers -----------------------------------------------------
    def _validate_email(self, email: str) -> bool:
        pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        return re.match(pattern, email) is not None

    def _try_login(self) -> None:
        if self.auth_in_progress:
            return

        email = self.email_input.text().strip()
        if not email:
            self._show_error_once("Введите email адрес")
            return
        if not self._validate_email(email):
            self._show_error_once("Некорректный формат email")
            return

        self.auth_in_progress = True
        self._set_loading_state(True, "Проверка данных...")
        self.controller.start_login(email)

    def _on_login_started(self) -> None:
        self._set_loading_state(True, "Выполняется вход...")
        self._slow_login_timer.start(3000)

    def _on_login_failed(self, message: str) -> None:
        self.auth_in_progress = False
        self._set_loading_state(False)
        self._slow_login_timer.stop()
        if message:
            self._show_error_once(message)

    def _on_login_succeeded(self, _: dict) -> None:
        self.auth_in_progress = False
        self._set_loading_state(False)
        self._slow_login_timer.stop()
        self.status_label.setText("")
        self.hide()

    def show_info(self, message: str) -> None:
        self.status_label.setText(message or "")

    # --- Helpers ------------------------------------------------------
    def _set_loading_state(self, loading: bool, message: str | None = None) -> None:
        self.login_btn.setDisabled(loading)
        self.email_input.setReadOnly(loading)
        self.login_btn.setText("Подключение..." if loading else "Войти")
        if message:
            self.status_label.setText(message)
        elif not loading:
            self.status_label.setText("")

    def _show_slow_login_hint(self) -> None:
        if self.auth_in_progress:
            self.status_label.setText("Подключение продолжается в фоне...")

    def _show_error_once(self, message: str) -> None:
        if self._showing_error:
            return
        self._showing_error = True
        try:
            QMessageBox.warning(self, "Ошибка", message)
        finally:
            self.status_label.setText(f'<span style="color: red;">{message}</span>')
            self._showing_error = False

    def keyPressEvent(self, event) -> None:  # pragma: no cover - Qt runtime
        if event.key() in (Qt.Key_Return, Qt.Key_Enter):
            self._try_login()
        else:
            super().keyPressEvent(event)
