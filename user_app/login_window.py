from __future__ import annotations

import datetime as dt
import logging
import re
import sys
from pathlib import Path

from PyQt5.QtCore import QDateTime, Qt, pyqtSignal
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

try:
    from config import validate_config
    from sheets_api import get_sheets_api
    from user_app import session as session_state
    from user_app.db_local import LocalDB
except ImportError:
    try:
        from roma.config import validate_config
        from roma.sheets_api import get_sheets_api
        from roma.user_app import session as session_state
        from roma.user_db_local import LocalDB
    except ImportError:
        from config import validate_config
        from sheets_api import get_sheets_api
        from user_app import session as session_state
        from user_app.db_local import LocalDB

from consts import STATUS_ACTIVE

logger = logging.getLogger(__name__)


def to_snake_case(column_name):
    """Преобразует название колонки в snake_case для нормализации"""
    # Убираем лишние пробелы и приводим к нижнему регистру
    normalized = column_name.strip().lower()
    # Заменяем пробелы и дефисы на подчеркивания
    normalized = re.sub(r"[\s-]+", "_", normalized)
    # Убираем все не-буквенно-цифровые символы, кроме подчеркиваний
    normalized = re.sub(r"[^a-z0-9_]", "", normalized)
    return normalized


class LoginWindow(QDialog):
    login_success = pyqtSignal(dict)
    login_failed = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Вход в систему")
        self.setWindowIcon(QIcon(self._resource_path("user_app/sberhealf.png")))
        self.setFixedSize(440, 360)
        self.user_data = None
        self.sheets_api = get_sheets_api()
        self.auth_in_progress = False
        self._success_emitted = False
        self._showing_error = False
        logger.debug("LoginWindow: инициализация окна входа")
        self._init_ui()
        self._setup_shortcuts()

    def _resource_path(self, relative_path):
        if hasattr(sys, "_MEIPASS"):
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
                target_width, target_height, Qt.KeepAspectRatio, Qt.SmoothTransformation
            )
            logo_label.setPixmap(pixmap)
            logo_label.setAlignment(Qt.AlignCenter)
            layout.addWidget(logo_label)
        except Exception as e:
            logger.warning(f"Не удалось загрузить логотип: {e}")

        title_label = QLabel("Вход в систему")
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setStyleSheet(
            """
            font-size: 22px;
            font-weight: bold;
            color: #222;
            margin-bottom: 15px;
        """
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
            """
            color: #666;
            font-size: 13px;
            margin-top: 12px;
            min-height: 18px;
        """
        )
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

    def _normalize_user_data(self, user_data, email):
        """Нормализует данные пользователя, используя snake_case для ключей"""
        normalized_data = {}

        # Создаем словарь с нормализованными ключами
        for key, value in user_data.items():
            normalized_key = to_snake_case(key)
            normalized_data[normalized_key] = value

        # Возвращаем данные с безопасным доступом через .get()
        return {
            "email": normalized_data.get("email", email),
            "name": normalized_data.get("name", ""),
            "role": normalized_data.get("role", "специалист"),
            "shift_hours": normalized_data.get("shift_hours", "8 часов"),
            "telegram_login": normalized_data.get("telegram_login", ""),
            "group": normalized_data.get("group", ""),
        }

    def _try_login(self):
        logger.info("LoginWindow: старт логина")
        if self.auth_in_progress or self._success_emitted:
            logger.debug(
                f"LoginWindow: пропуск попытки логина (auth_in_progress={self.auth_in_progress}, _success_emitted={self._success_emitted})"
            )
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

        # Быстрая проверка совместимости API (чтобы не ловить AttributeError)
        required_methods = [
            "get_user_by_email",
            "get_active_session",
            "set_active_session",
            "finish_active_session",
        ]
        for method_name in required_methods:
            if not hasattr(self.sheets_api, method_name):
                error_msg = f"Ошибка подключения: SheetsAPI object has no attribute '{method_name}'"
                logger.error(f"LoginWindow: {error_msg}")
                self._show_error_once(error_msg)
                self.login_failed.emit(error_msg)
                self.auth_in_progress = False
                return

        self._set_loading_state(True)

        try:
            logger.debug("LoginWindow: вызов validate_config")
            validate_config()

            # Проверяем доступность интернета (мягко)
            try:
                from sync.network import is_internet_available

                online = bool(is_internet_available())
            except Exception:
                online = True

            if online:
                logger.debug("LoginWindow: вызов get_user_by_email (online)")
                user_data = self.sheets_api.get_user_by_email(email)
            else:
                logger.debug("LoginWindow: офлайн — читаем кэш пользователя")
                db = LocalDB()
                cached = db.get_user_from_cache(email)
                if not cached:
                    raise RuntimeError(
                        "Нет подключения и пользователь не найден локально"
                    )
                user_data = {
                    "Email": cached["email"],
                    "Name": cached["name"],
                    "Role": cached["role"],
                    "ShiftHours": cached["shift_hours"],
                    "Telegram": cached["telegram_login"],
                    "Group": cached["group"],
                    "_offline": True,
                }

            # Опциональный автосайн-ап (включается ALLOW_SELF_SIGNUP в config.py)
            if not user_data:
                from config import ALLOW_SELF_SIGNUP

                if ALLOW_SELF_SIGNUP:
                    logger.info(
                        "LoginWindow: email не найден, пробуем автосоздать пользователя"
                    )
                    user_data = self.sheets_api.add_user_if_absent(email)

            if user_data:
                logger.info("LoginWindow: пользователь найден, продолжаем")

                # Нормализуем данные пользователя
                normalized_user_data = self._normalize_user_data(user_data, email)
                logger.debug(
                    f"LoginWindow: нормализованные данные пользователя: {normalized_user_data}"
                )

                # При онлайн-логине обновляем кэш
                if not user_data.get("_offline"):
                    try:
                        db = LocalDB()
                        db.update_user_cache(
                            {
                                "email": normalized_user_data["email"],
                                "name": normalized_user_data["name"],
                                "role": normalized_user_data["role"],
                                "group": normalized_user_data["group"],
                                "shift_hours": normalized_user_data["shift_hours"],
                                "telegram_login": normalized_user_data[
                                    "telegram_login"
                                ],
                            }
                        )
                    except Exception as e:
                        logger.warning(
                            f"Не удалось обновить локальный кэш пользователя: {e}"
                        )

                # --- ВСЕГДА ЗАВЕРШАЕМ СТАРУЮ СЕССИЮ И НАЧИНАЕМ НОВУЮ ---
                # 1. Находим активную сессию с безопасным фолбэком
                active_session = None
                if hasattr(self.sheets_api, "get_active_session"):
                    active_session = self.sheets_api.get_active_session(email)
                else:
                    # безопасный фолбэк: вручную фильтруем по email
                    try:
                        sessions = []
                        if hasattr(self.sheets_api, "get_all_active_sessions"):
                            sessions = self.sheets_api.get_all_active_sessions() or []
                        if not sessions and hasattr(self.sheets_api, "get_worksheet"):
                            # абсолютный фолбэк: прочитать лист ActiveSessions напрямую
                            from config import ACTIVE_SESSIONS_SHEET

                            ws = self.sheets_api.get_worksheet(ACTIVE_SESSIONS_SHEET)
                            sessions = self.sheets_api._read_table(
                                ws
                            )  # безопасно, уже используется выше
                        em = (email or "").strip().lower()
                        candidates = []
                        for i, raw in enumerate(sessions, start=2):
                            a = {k: v for k, v in raw.items()}
                            a.update(
                                {self.sheets_api._snake(k): v for k, v in raw.items()}
                            )
                            if (a.get("email", "") or "").strip().lower() != em:
                                continue
                            if (a.get("status", "") or "").strip().lower() != "active":
                                continue
                            candidates.append((i, raw))
                        if candidates:
                            candidates.sort(
                                key=lambda t: (
                                    (
                                        t[1].get("LoginTime")
                                        or t[1].get("login_time")
                                        or ""
                                    ),
                                    t[0],
                                )
                            )
                            active_session = candidates[-1][1]
                        logger.debug(
                            "LoginWindow: использован фолбэк для получения активной сессии"
                        )
                    except Exception as e:
                        logger.debug(f"fallback get_active_session failed: {e}")

                if active_session:
                    session_id = active_session.get("SessionID")
                    login_time = active_session.get("LoginTime")
                    # Автоматически завершаем старую сессию без вопроса
                    logger.info(
                        f"LoginWindow: Автоматически завершаем старую сессию от {login_time}"
                    )
                    logout_time = QDateTime.currentDateTime().toString(Qt.ISODate)
                    self.sheets_api.finish_active_session(
                        email, session_id, logout_time
                    )

                # 2. Создаем новую сессию
                login_dt = dt.datetime.now(dt.UTC)
                session_id = session_state.generate_session_id(email, login_dt)
                login_time_iso = login_dt.isoformat()
                self.sheets_api.set_active_session(
                    email,
                    normalized_user_data.get("name", ""),
                    session_id,
                    login_time_iso,
                )
                try:
                    db = LocalDB()
                    db.mark_session_active(
                        session_id,
                        email=normalized_user_data.get("email", email),
                        name=normalized_user_data.get("name", email),
                        status=STATUS_ACTIVE,
                        started_at=login_dt,
                        comment="Начало смены",
                        user_group=normalized_user_data.get("group") or None,
                    )
                except Exception as exc:
                    logger.warning(
                        "Не удалось зафиксировать локальную сессию %s: %s",
                        session_id,
                        exc,
                    )
                try:
                    session_state.set_session_id(session_id)
                except Exception:
                    logger.debug("Не удалось сохранить session_id в session_state")
                login_was_performed = True
                # --- КОНЕЦ ---

                # Формируем данные для передачи в GUI
                self.user_data = {
                    "email": normalized_user_data.get("email", email),
                    "name": normalized_user_data.get("name", ""),
                    "role": normalized_user_data.get("role", "специалист"),
                    "shift_hours": normalized_user_data.get("shift_hours", "8 часов"),
                    "telegram_login": normalized_user_data.get("telegram_login", ""),
                    "group": normalized_user_data.get("group", ""),
                    "login_was_performed": login_was_performed,
                    "session_id": session_id,
                    "session_started_at": login_time_iso,
                }

                # сохраняем email текущего пользователя для всех подсистем
                try:
                    email_value = (
                        self.email_input.text()
                        if hasattr(self, "email_input")
                        else email
                    ).strip()
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
            clean_error = str(e).replace("'", "")
            error_msg = f"Ошибка подключения: {clean_error}"
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
        logger.debug(
            f"LoginWindow: _show_error_once вызван с message='{message}', _showing_error={self._showing_error}"
        )
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
