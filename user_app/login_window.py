import re
import logging
import sys
from pathlib import Path
from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QLabel,
    QLineEdit, QPushButton, QMessageBox, QSpacerItem, QSizePolicy
)
from PyQt5.QtCore import Qt, pyqtSignal, QDateTime
from PyQt5.QtGui import QIcon, QPixmap, QFont

try:
    from config import validate_config
    from sheets_api import SheetsAPI
    from user_app.db_local import LocalDB
    from user_app import session as session_state
except ImportError:
    try:
        from roma.config import validate_config
        from roma.sheets_api import SheetsAPI
        from roma.user_app.db_local import LocalDB
        from roma.user_app import session as session_state
    except ImportError:
        from config import validate_config
        from sheets_api import SheetsAPI
        from user_app.db_local import LocalDB
        from user_app import session as session_state

logger = logging.getLogger(__name__)

class LoginWindow(QDialog):
    login_success = pyqtSignal(dict)
    login_failed = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Вход в систему")
        self.setWindowIcon(QIcon(self._resource_path("user_app/sberhealf.png")))
        self.setFixedSize(440, 360)
        self.user_data = None
        self.sheets_api = SheetsAPI()
        self.auth_in_progress = False
        self._success_emitted = False
        self._showing_error = False
        logger.debug("LoginWindow: инициализация окна входа")
        self._init_ui()
        self._setup_shortcuts()

    def _resource_path(self, relative_path):
        if hasattr(sys, '_MEIPASS'):
            base_path = Path(sys._MEIPASS)
        else:
            base_path = Path(__file__).parent.parent
        return str(base_path / relative_path)

    def _init_ui(self):
        self.setFont(QFont("Segoe UI", 11))
        layout = QVBoxLayout()
        layout.setContentsMargins(30, 25, 30, 25)
        layout.setSpacing(18)

        logo_label = QLabel()
        try:
            pixmap = QPixmap(self._resource_path("user_app/sberhealf.png"))
            target_width = 170
            target_height = 70
            pixmap = pixmap.scaled(
                target_width, target_height,
                Qt.KeepAspectRatio, Qt.SmoothTransformation
            )
            logo_label.setPixmap(pixmap)
            logo_label.setAlignment(Qt.AlignCenter)
            layout.addWidget(logo_label)
        except Exception as e:
            logger.warning(f"Не удалось загрузить логотип: {e}")

        title_label = QLabel("Вход в систему")
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setStyleSheet("""
            font-size: 22px;
            font-weight: bold;
            color: #222;
            margin-bottom: 15px;
        """)
        layout.addWidget(title_label)

        layout.addSpacerItem(QSpacerItem(20, 10, QSizePolicy.Minimum, QSizePolicy.Fixed))

        self.email_input = QLineEdit()
        self.email_input.setPlaceholderText("Корпоративный email")
        self.email_input.setStyleSheet("""
            QLineEdit {
                padding: 11px;
                border: 1.5px solid #ccc;
                border-radius: 8px;
                font-size: 15px;
                min-width: 290px;
                max-width: 350px;
            }
        """)
        self.email_input.setMinimumWidth(290)
        self.email_input.setMaximumWidth(350)
        layout.addWidget(self.email_input, alignment=Qt.AlignCenter)

        layout.addSpacerItem(QSpacerItem(20, 8, QSizePolicy.Minimum, QSizePolicy.Fixed))

        self.login_btn = QPushButton("Войти")
        self.login_btn.setStyleSheet("""
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
        """)
        self.login_btn.setMinimumHeight(40)
        self.login_btn.setMaximumWidth(220)
        self.login_btn.clicked.connect(self._try_login)
        layout.addWidget(self.login_btn, alignment=Qt.AlignCenter)

        self.status_label = QLabel()
        self.status_label.setStyleSheet("""
            color: #666;
            font-size: 13px;
            margin-top: 12px;
            min-height: 18px;
        """)
        self.status_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.status_label)

        layout.addStretch(1)
        self.setLayout(layout)
        logger.debug("LoginWindow: интерфейс инициализирован")

    def _setup_shortcuts(self):
        self.email_input.returnPressed.connect(self._try_login)

    def _validate_email(self, email: str) -> bool:
        logger.debug(f"LoginWindow: валидация email '{email}'")
        pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        return re.match(pattern, email) is not None

    def _try_login(self):
        logger.info("LoginWindow: старт логина")
        if self.auth_in_progress or self._success_emitted:
            logger.debug(f"LoginWindow: пропуск попытки логина (auth_in_progress={self.auth_in_progress}, _success_emitted={self._success_emitted})")
            return
        self.auth_in_progress = True

        email = self.email_input.text().strip()
        logger.info(f"LoginWindow: введён email: {email}")

        if not email:
            error_msg = "Введите email адрес"
            logger.warning(f"LoginWindow: {error_msg}")
            self._show_error_once(error_msg)
            self.login_failed.emit(error_msg)
            self.auth_in_progress = False
            return

        if not self._validate_email(email):
            error_msg = "Некорректный формат email"
            logger.warning(f"LoginWindow: {error_msg}")
            self._show_error_once(error_msg)
            self.login_failed.emit(error_msg)
            self.auth_in_progress = False
            return

        self._set_loading_state(True)

        try:
            logger.debug("LoginWindow: вызов validate_config")
            validate_config()
            logger.debug("LoginWindow: вызов get_user_by_email")
            user_data = self.sheets_api.get_user_by_email(email)

            if user_data:
                logger.info("LoginWindow: пользователь найден, продолжаем")
                
                # --- ВСЕГДА ЗАВЕРШАЕМ СТАРУЮ СЕССИЮ И НАЧИНАЕМ НОВУЮ ---
                # 1. Находим активную сессию
                active_session = self.sheets_api.get_active_session(email)
                if active_session:
                    session_id = active_session.get("SessionID")
                    login_time = active_session.get("LoginTime")
                    # Автоматически завершаем старую сессию без вопроса
                    logger.info(f"LoginWindow: Автоматически завершаем старую сессию от {login_time}")
                    logout_time = QDateTime.currentDateTime().toString(Qt.ISODate)
                    self.sheets_api.finish_active_session(email, session_id, logout_time)
                
                # 2. Создаем новую сессию
                session_id = f"{email[:8]}_{QDateTime.currentDateTime().toString('yyyyMMddHHmmss')}"
                self.sheets_api.set_active_session(
                    email,
                    user_data.get("name", ""),
                    session_id,
                    QDateTime.currentDateTime().toString(Qt.ISODate)
                )
                login_was_performed = True
                # --- КОНЕЦ ---
                
                # Формируем данные для передачи в GUI
                self.user_data = {
                    "email": user_data["email"],
                    "name": user_data["name"],
                    "role": user_data["role"],
                    "shift_hours": user_data["shift_hours"],
                    "telegram_login": user_data.get("telegram_login", ""),
                    "group": user_data.get("group", ""),
                    "login_was_performed": login_was_performed,
                    "session_id": session_id
                }
                
                # сохраняем email текущего пользователя для всех подсистем
                try:
                    email_value = (self.email_input.text() if hasattr(self, "email_input") else email).strip()
                    session_state.set_user_email(email_value)
                except Exception:
                    pass
                # испускаем login_success
                if not self._success_emitted:
                    logger.debug("LoginWindow: испускаем login_success")
                    self._success_emitted = True
                    self.login_success.emit(self.user_data)
                else:
                    logger.debug("LoginWindow: login_success уже испущен")
                self.accept()
            else:
                error_msg = "Пользователь не найден. Проверьте email или обратитесь к администратору."
                logger.error(f"LoginWindow: {error_msg}")
                self._show_error_once(error_msg)
                self.login_failed.emit(error_msg)

        except Exception as e:
            logger.error(f"LoginWindow: Ошибка авторизации: {e}")
            error_msg = f"Ошибка подключения: {str(e).replace("'", "")}"
            self._show_error_once(error_msg)
            self.login_failed.emit(error_msg)
        finally:
            logger.debug("LoginWindow: завершение попытки логина")
            self._set_loading_state(False)
            self.auth_in_progress = False

    def _set_loading_state(self, loading: bool):
        logger.debug(f"LoginWindow: установка состояния loading={loading}")
        self.login_btn.setDisabled(loading)
        self.email_input.setReadOnly(loading)
        self.login_btn.setText("Проверка..." if loading else "Войти")
        self.status_label.setText("Идет проверка данных..." if loading else "")

    def _show_error_once(self, message: str):
        logger.debug(f"LoginWindow: _show_error_once вызван с message='{message}', _showing_error={self._showing_error}")
        if self._showing_error:
            logger.warning("LoginWindow: попытка повторного показа ошибки, пропуск")
            return
        self._showing_error = True
        logger.info(f"LoginWindow: показываем QMessageBox.warning с текстом: {message}")
        QMessageBox.warning(self, "Ошибка", message)
        self.status_label.setText(f'<span style="color: red;">{message}</span>')
        self._showing_error = False

    def keyPressEvent(self, event):
        if event.key() in (Qt.Key_Return, Qt.Key_Enter):
            logger.debug("LoginWindow: нажатие Enter/Return, пробуем логин")
            self._try_login()
        else:
            super().keyPressEvent(event)

if __name__ == "__main__":
    from PyQt5.QtWidgets import QApplication
    logging.basicConfig(level=logging.DEBUG)
    app = QApplication(sys.argv)
    window = LoginWindow()
    window.show()
    sys.exit(app.exec_())