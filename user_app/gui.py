import sys
import os
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Callable
import threading

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import STATUSES, STATUS_GROUPS, MAX_COMMENT_LENGTH
from sheets_api import get_sheets_api
from user_app.db_local import LocalDB, LocalDBError, write_tx

try:
    from sync.notifications import Notifier
except ImportError:
    try:
        from .sync.notifications import Notifier
    except ImportError:
        from notifications import Notifier

from PyQt5.QtWidgets import (
    QWidget, QLabel, QPushButton, QVBoxLayout,
    QHBoxLayout, QMessageBox, QTextEdit,
    QSizePolicy, QApplication
)
from PyQt5.QtCore import QTimer, Qt, pyqtSignal
from PyQt5.QtGui import QFont, QPixmap, QIcon

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
        login_was_performed: bool = True
    ):
        super().__init__()
        self.email = email
        self.name = name
        self.role = role
        self.group = group
        self.shift_hours = shift_hours
        self.telegram_login = telegram_login
        self.on_logout_callback = on_logout_callback

        self.current_status = "В работе"
        self.status_start_time = datetime.now()
        self.shift_start_time = datetime.now()
        self.last_sync_time = None
        self.shift_ended = False

        # Логика закрытия: None, "admin_logout", "user_close", "auto_logout"
        self._closing_reason = "user_close"  # по умолчанию

        if session_id is not None:
            self.session_id = session_id
            self._continue_existing_session = True
        else:
            self.session_id = self._generate_session_id()
            self._continue_existing_session = False
        self.status_buttons = {}

        self.login_was_performed = login_was_performed

        # Используем единый синглтон API
        self.sheets_api = get_sheets_api()

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

    def _generate_session_id(self) -> str:
        return f"{self.email[:8]}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

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

    def _send_action_to_sheets(self, record_id, user_group=None):
        threading.Thread(target=self._send_action_to_sheets_worker, args=(record_id, user_group), daemon=True).start()

    def _send_action_to_sheets_worker(self, record_id, user_group=None):
        try:
            row = self.db.get_action_by_id(record_id)
            if not row:
                logger.error(f"Не удалось найти запись с id={record_id} для отправки в Sheets")
                return

            action = self._make_action_payload_from_row(row)
            # ВАЖНО: сначала actions (список словарей), затем email
            ok = self.sheets_api.log_user_actions([action], action["email"], user_group=user_group or self.group)
            if ok:
                self.db.mark_actions_synced([record_id])
                self.last_sync_time = datetime.now()
            else:
                logger.warning("Sheets: log_user_actions вернул False — оставляю запись несинхронизированной")
        except Exception as e:
            logger.warning(f"Ошибка отправки действия в Google Sheets: {e}")
            Notifier.show("Оффлайн режим", "Данные будут отправлены при появлении интернета.")

    def _finish_and_send_previous_status(self):
        prev_id = self.db.finish_last_status(self.email, self.session_id)
        if prev_id:
            threading.Thread(target=self._finish_and_send_previous_status_worker, args=(prev_id,), daemon=True).start()

    def _finish_and_send_previous_status_worker(self, prev_id):
        row = self.db.get_action_by_id(prev_id)
        if not row:
            return
        try:
            action = self._make_action_payload_from_row(row)
            ok = self.sheets_api.log_user_actions([action], action["email"], user_group=self.group)
            if ok:
                self.db.mark_actions_synced([prev_id])
                self.last_sync_time = datetime.now()
            else:
                logger.warning("Sheets: log_user_actions вернул False — оставляю запись несинхронизированной")
        except Exception as e:
            logger.warning(f"Ошибка отправки завершённого статуса в Sheets: {e}")
            Notifier.show("Оффлайн режим", "Предыдущий статус будет синхронизирован позже.")

    def _init_db(self):
        try:
            self.db = LocalDB()
            if self.login_was_performed:
                now = datetime.now().isoformat()
                # Определяем тип действия: LOGIN только если это новая сессия
                has_session = bool(self._continue_existing_session)
                action_type = "STATUS_CHANGE" if has_session else "LOGIN"
                comment = "Начало смены" if action_type == "LOGIN" else "Смена статуса"
                
                # Используем сериализованную транзакцию для записи
                with write_tx() as conn:
                    record_id = self.db.log_action_tx(
                        conn=conn,
                        email=self.email,
                        name=self.name,
                        status=self.current_status,
                        action_type=action_type,
                        comment=comment,
                        session_id=self.session_id,
                        status_start_time=now,
                        status_end_time=None,
                        reason=None
                    )
                self.status_start_time = datetime.fromisoformat(now)
                self._send_action_to_sheets(record_id)
        except LocalDBError as e:
            logger.error(f"Ошибка инициализации БД: {e}")
            QMessageBox.critical(self, "Ошибка", "Не удалось инициализировать локальную базу данных")
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
        self.info_label.setStyleSheet("QLabel { background-color: #f5f5f5; border-radius: 5px; padding: 10px; }")
        self._update_info_text()
        main_layout.addWidget(self.info_label)

        self.comment_input = QTextEdit()
        self.comment_input.setPlaceholderText("Введите комментарий...")
        self.comment_input.setMaximumHeight(80)
        self.comment_input.setStyleSheet("QTextEdit { border: 1px solid #ddd; border-radius: 5px; padding: 5px; }")
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
        self.finish_btn.setStyleSheet("""
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
        """)
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

    def _is_session_finished_remote(self) -> bool:
        """
        True — если по (email, session_id) в ActiveSessions статус 'finished' или 'kicked'.
        """
        try:
            row = self.sheets_api.check_user_session_status(self.email, self.session_id)
            if isinstance(row, dict):
                st = (row.get("Status") or "").strip().lower()
                logger.debug(f"[ACTIVESESSIONS] status for {self.email}/{self.session_id}: {st}")
                return st in ("finished", "kicked")
        except Exception as e:
            logger.debug(f"_is_session_finished_remote error: {e}")
        return False

    def _auto_check_shift_ended(self):
        if self.shift_ended:
            return

        # 1) локальная проверка
        if self._is_shift_ended():
            self.shift_ended = True
            self.finish_btn.setEnabled(False)
            for btn in self.status_buttons.values():
                btn.setEnabled(False)
            Notifier.show("WorkLog", "Смена завершена (автоматически, по данным системы).")
            logger.info(f"[AUTO_LOGOUT_DETECT] Локально найден LOGOUT для {self.email}")
            return

        # 2) удалённая проверка ActiveSessions
        if self._is_session_finished_remote():
            logger.info(f"[AUTO_LOGOUT_DETECT] В ActiveSessions статус НЕ active для {self.email}, session={self.session_id}")
            self._closing_reason = "auto_logout"
            self.finish_btn.setEnabled(False)
            for btn in self.status_buttons.values():
                btn.setEnabled(False)
            Notifier.show("WorkLog", "Смена завершена администратором.")
            try:
                self._log_shift_end("Разлогинен администратором (удалённо)", reason="admin")
            except Exception as e:
                logger.error(f"Ошибка при автологаутах по сигналу из Sheets: {e}")
            self.shift_ended = True
            self.close()

    def force_logout_by_admin(self):
        """
        Слот под SyncSignals.force_logout.
        Вызывается без аргументов. Показываем предупреждение и закрываемся.
        """
        try:
            QMessageBox.warning(
                self,
                "Отключение",
                "Вы были отключены администратором. Сессия будет завершена."
            )
        except Exception:
            pass
        self._closing_reason = "admin_logout"
        # Передаём reason в on_logout_callback, если он его принимает
        if callable(self.on_logout_callback):
            try:
                self.on_logout_callback(self._closing_reason)
            except TypeError:
                self.on_logout_callback()
        self.close()

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
                btn.setStyleSheet("""
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
                """)
                btn.setEnabled(False)
            else:
                btn.setStyleSheet("""
                    QPushButton {
                        padding: 8px;
                        border-radius: 5px;
                        background-color: #e0e0e0;
                    }
                    QPushButton:hover {
                        background-color: #d0d0d0;
                    }
                """)
                btn.setEnabled(True)
            if self.shift_ended:
                btn.setEnabled(False)

    def _update_time_display(self):
        time_in_status = datetime.now() - self.status_start_time
        hours, remainder = divmod(time_in_status.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        self.time_label.setText(f"⏱ Время в статусе: {hours:02d}:{minutes:02d}:{seconds:02d}")

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
            QMessageBox.information(self, "Информация", "Вы уже находитесь в этом статусе")
            return

        comment = self.comment_input.toPlainText().strip()
        if len(comment) > MAX_COMMENT_LENGTH:
            QMessageBox.warning(
                self,
                "Ошибка",
                f"Комментарий слишком длинный (максимум {MAX_COMMENT_LENGTH} символов)"
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
                    reason=None
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

    def _log_shift_end(self, comment: str, reason: str = "user"):
        now = datetime.now().isoformat()
        try:
            with write_tx() as conn:
                # Завершаем последний статус
                self.db.finish_last_status_tx(conn, self.email, self.session_id, now)

                # Логируем завершение смены
                record_id = self.db.log_action_tx(
                    conn=conn,
                    email=self.email,
                    name=self.name,
                    status="LOGOUT",
                    action_type="LOGOUT",
                    comment=comment,
                    session_id=self.session_id,
                    status_start_time=now,
                    status_end_time=now,
                    reason=reason
                )
            self._send_action_to_sheets(record_id, user_group=self.group)
        except Exception as e:
            logger.error(f"Ошибка записи завершения смены: {e}")
            raise

    def finish_shift(self):
        if self.shift_ended:
            QMessageBox.information(self, "Информация", "Смена уже завершена")
            return

        comment = self.comment_input.toPlainText().strip()
        if not comment:
            comment = "Завершение смены"

        try:
            self._log_shift_end(comment)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", "Не удалось завершить смену")
            return

        self.shift_ended = True
        self.finish_btn.setEnabled(False)
        for btn in self.status_buttons.values():
            btn.setEnabled(False)

        # === Обновление ActiveSessions ===
        try:
            lt = datetime.now().isoformat()
            ok = self.sheets_api.finish_active_session(self.email, self.session_id, lt)
            logger.info(f"[LOGOUT/ActiveSessions] finish_active_session({self.email}, {self.session_id}) -> {ok}")
            if not ok:
                # Фоллбэк: обновим ПОСЛЕДНЮЮ активную строку по email (если есть) как 'finished'
                try:
                    # Универсальный путь через _update_session_status если доступен
                    if hasattr(self.sheets_api, "_update_session_status"):
                        ok2 = self.sheets_api._update_session_status(self.email, self.session_id, "finished", lt)
                        logger.warning(f"[LOGOUT/ActiveSessions] fallback _update_session_status -> {ok2}")
                        ok = ok or ok2
                    else:
                        ok2 = self.sheets_api.kick_active_session(self.email, self.session_id, lt)  # меняет статус, но приемлемо
                        logger.warning(f"[LOGOUT/ActiveSessions] fallback kick_active_session -> {ok2}")
                        ok = ok or ok2
                except Exception as e2:
                    logger.error(f"[LOGOUT/ActiveSessions] fallback error: {e2}")

            # Верификация статуса
            try:
                st = (self.sheets_api.check_user_session_status(self.email, self.session_id) or "").strip().lower()
                logger.info(f"[LOGOUT/ActiveSessions] post-check status: {st}")
                if st == "active":
                    # одна повторная попытка
                    ok3 = self.sheets_api.finish_active_session(self.email, self.session_id, lt)
                    logger.warning(f"[LOGOUT/ActiveSessions] repeat finish_active_session -> {ok3}")
            except Exception as e3:
                logger.warning(f"[LOGOUT/ActiveSessions] status verify error: {e3}")
        except Exception as e:
            logger.error(f"Ошибка завершения сессии в ActiveSessions: {e}")

        Notifier.show("WorkLog", "Смена завершена")
        QMessageBox.information(self, "Успех", "Смена завершена успешно")

    def closeEvent(self, event):
        if not self.shift_ended:
            reply = QMessageBox.question(
                self,
                "Подтверждение",
                "Вы уверены, что хотите закрыть приложение? Смена не завершена.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
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

if __name__ == "__main__":
    from logging_setup import setup_logging

    setup_logging(app_name="wtt-user-gui", force_console=True)
    app = QApplication(sys.argv)
    window = EmployeeApp(
        email="test@example.com",
        name="Тестовый Сотрудник",
        role="специалист",
        group="Тестовая группа",
        on_logout_callback=lambda reason: logger.info("Logout reason: %s", reason)
    )
    window.show()
    sys.exit(app.exec())