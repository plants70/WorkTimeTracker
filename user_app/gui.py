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
from sheets_api import sheets_api
from user_app.db_local import LocalDB, LocalDBError

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
    notification_requested = pyqtSignal(str, str)

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
        self._closing_reason = None

        if session_id is not None:
            self.session_id = session_id
            self._continue_existing_session = True
        else:
            self.session_id = self._generate_session_id()
            self._continue_existing_session = False
        self.status_buttons = {}

        self.login_was_performed = login_was_performed

        self.notification_requested.connect(self._show_notification)

        self._init_db()
        self._init_ui()
        self._init_timers()
        self._init_shift_check_timer()

    def _show_notification(self, title: str, message: str):
        Notifier.show(title, message)

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
            ok = sheets_api.log_user_actions([action], action["email"], user_group=user_group or self.group)
            if ok:
                self.db.mark_actions_synced([record_id])
            else:
                logger.warning("Sheets: log_user_actions вернул False — оставляю запись несинхронизированной")
        except Exception as e:
            logger.warning(f"Ошибка отправки действия в Google Sheets: {e}")
            self.notification_requested.emit(
                "Оффлайн режим",
                "Данные будут отправлены при появлении интернета."
            )

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
            ok = sheets_api.log_user_actions([action], action["email"], user_group=self.group)
            if ok:
                self.db.mark_actions_synced([prev_id])
            else:
                logger.warning("Sheets: log_user_actions вернул False — оставляю запись несинхронизированной")
        except Exception as e:
            logger.warning(f"Ошибка отправки завершённого статуса в Sheets: {e}")
            self.notification_requested.emit(
                "Оффлайн режим",
                "Предыдущий статус будет синхронизирован позже."
            )

    def _init_db(self):
        try:
            self.db = LocalDB()
            if self.login_was_performed:
                now = datetime.now().isoformat()
                record_id = self.db.log_action(
                    email=self.email,
                    name=self.name,
                    status=self.current_status,
                    action_type="LOGIN",
                    comment="Начало смены",
                    immediate_sync=False,
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
        True — если в ActiveSessions текущая (или последняя по email) сессия
        имеет статус 'finished' или 'kicked'.
        """
        try:
            if hasattr(sheets_api, "check_user_session_status"):
                st = str(sheets_api.check_user_session_status(self.email, self.session_id)).strip().lower()
                logger.debug(f"[ACTIVESESSIONS] status for {self.email}/{self.session_id}: {st}")
                if st in ("finished", "kicked"):
                    return True

            if hasattr(sheets_api, "get_all_active_sessions"):
                sessions = sheets_api.get_all_active_sessions() or []
                last_for_email = None
                for s in sessions:
                    if str(s.get("Email", "")).strip().lower() == self.email.lower():
                        last_for_email = s
                if last_for_email:
                    st2 = str(last_for_email.get("Status", "")).strip().lower()
                    logger.debug(f"[ACTIVESESSIONS] fallback status for {self.email}: {st2}")
                    return st2 in ("finished", "kicked")
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
            self._show_notification("WorkLog", "Смена завершена (автоматически, по данным системы).")
            logger.info(f"[AUTO_LOGOUT_DETECT] Локально найден LOGOUT для {self.email}")
            return

        # 2) удалённая проверка ActiveSessions
        if self._is_session_finished_remote():
            logger.info(f"[AUTO_LOGOUT_DETECT] В ActiveSessions статус НЕ active для {self.email}, session={self.session_id}")
            self._closing_reason = "auto_logout"
            self.finish_btn.setEnabled(False)
            for btn in self.status_buttons.values():
                btn.setEnabled(False)
            self._show_notification("WorkLog", "Смена завершена администратором.")
            try:
                self._log_shift_end("Разлогинен администратором (удалённо)", reason="admin")
            except Exception as e:
                logger.error(f"Ошибка при автологаутах по сигналу из Sheets: {e}")
            self.shift_ended = True
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
                self._show_notification("WorkLog", "Данные не синхронизировались более часа")

    def _is_shift_ended(self) -> bool:
        try:
            return self.db.check_existing_logout(self.email, session_id=self.session_id)
        except Exception as e:
            logger.error(f"Ошибка проверки завершения смены: {e}")
            return False

    def set_status(self, new_status: str):
        if self.shift_ended:
            QMessageBox.warning(self, "Ошибка", "Смена уже завершена")
            return

        if new_status == self.current_status:
            QMessageBox.information(self, "Информация", "Вы уже находитесь в этом статусе.")
            return

        comment = self.comment_input.toPlainText().strip()

        try:
            now = datetime.now().isoformat()
            
            # --- ШАГ 1: Явно завершаем ПОСЛЕДНИЙ статус, устанавливая end_time ---
            # Находим id последнего статуса (LOGIN или STATUS_CHANGE)
            with self.db._lock:
                cursor = self.db.conn.execute(
                    "SELECT id, status FROM logs WHERE email=? AND session_id=? "
                    "AND status_end_time IS NULL "
                    "AND action_type IN ('LOGIN', 'STATUS_CHANGE') "
                    "ORDER BY id DESC LIMIT 1",
                    (self.email, self.session_id)
                )
                row = cursor.fetchone()
                if row:
                    prev_id, prev_status = row
                    # Явно устанавливаем время окончания
                    self.db.conn.execute(
                        "UPDATE logs SET status_end_time=? WHERE id=?",
                        (now, prev_id)
                    )
                    self.db.conn.commit()
                    logger.info(f"Статус '{prev_status}' (id={prev_id}) завершен в {now}")
                    # персональные оповещения (частые переключения и т.п.)
                    try:
                        from user_app import session as session_state
                        from user_app.personal_rules import on_status_committed
                        current_email = session_state.get_user_email()
                        if current_email:
                            on_status_committed(email=current_email, status_name=prev_status, ts_iso=None)
                    except Exception:
                        logger.exception("on_status_committed failed")
                    # Отправляем старую запись в фоне
                    self._send_action_to_sheets(prev_id)
                else:
                    logger.warning("Не найден незавершенный статус для обновления end_time")

            # --- ШАГ 2: Логируем НОВЫЙ статус ---
            record_id = self.db.log_action(
                email=self.email,
                name=self.name,
                status=new_status,
                action_type="STATUS_CHANGE",
                comment=comment if comment else None,
                immediate_sync=False,
                session_id=self.session_id,
                status_start_time=now,
                status_end_time=None
            )
            
            # Отправляем новую запись в фоне
            self._send_action_to_sheets(record_id)
            
            # --- ШАГ 3: Обновляем состояние приложения ---
            self.current_status = new_status
            self.status_start_time = datetime.fromisoformat(now)
            self.comment_input.clear()
            self._update_info_text()
            self._show_notification("WorkLog", f"Статус изменен на: {new_status}")
            
        except Exception as e:
            logger.error(f"Ошибка при изменении статуса: {e}")
            QMessageBox.critical(self, "Ошибка", f"Не удалось изменить статус: {e}")

    def finish_shift(self):
        if self.shift_ended:
            QMessageBox.information(self, "Информация", "Смена уже завершена")
            return

        reply = QMessageBox.question(
            self,
            "Подтверждение",
            "Вы уверены, что хотите завершить смену?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )

        if reply == QMessageBox.Yes:
            try:
                result = self._log_shift_end("Завершение смены (нормальное)", reason="user", group=self.group, sync=False)
                if result:
                    logger.info(f"[LOGOUT] Запись LOGOUT успешно произведена для {self.email}")
                else:
                    logger.warning(f"[LOGOUT] LOGOUT уже был записан для {self.email}")
                self.shift_ended = True
                self.finish_btn.setEnabled(False)
                for btn in self.status_buttons.values():
                    btn.setEnabled(False)
                try:
                    sheets_api.finish_active_session(self.email, self.session_id, datetime.now().isoformat())
                except Exception as e:
                    logger.error(f"Ошибка завершения сессии в ActiveSessions: {e}")
                self.close()
            except LocalDBError as e:
                logger.error(f"Ошибка завершения смены: {e}")
                QMessageBox.critical(self, "Ошибка", f"Не удалось завершить смену: {e}")

    def force_logout_by_admin(self):
        """Принудительный выход по инициативе администратора с уведомлением пользователя"""
        if self.shift_ended:
            logger.info(f"[ADMIN_LOGOUT] Попытка принудительного выхода для уже завершенной смену: {self.email}")
            return
            
        self._closing_reason = "admin_logout"
        self.finish_btn.setEnabled(False)
        for btn in self.status_buttons.values():
            btn.setEnabled(False)
            
        try:
            # синхронно пишем статус+LOGOUT в WorkLog_Группа
            self._log_shift_end("Разлогинен администратором", reason="admin",
                                group=self.group, sync=True)
            # и сразу помечаем ActiveSessions как "kicked"
            try:
                sheets_api.kick_active_session(self.email, self.session_id, datetime.now().isoformat())
            except Exception as e:
                logger.error(f"kick_active_session error: {e}")
        except Exception as e:
            logger.error(f"Ошибка при админском выходе: {e}")
            if self.on_logout_callback:
                self.on_logout_callback()

        self.shift_ended = True

        # Показываем информационное окно (не критическое)
        QMessageBox.information(
            self,
            "Смена завершена администратором",
            "Ваша смена была завершена администратором.\n\nПриложение будет закрыто."
        )

        self.close()

    def _log_shift_end(self, comment: str, reason: str = "user", 
                       group: Optional[str] = None, sync: bool = False) -> bool:
        """
        Завершает смену и записывает LOGOUT.
        :param comment: Комментарий к выходу.
        :param reason: Причина выхода (user, admin, auto).
        :param group: Группа пользователя (для правильного выбора листа в Google Sheets).
        :param sync: Синхронная отправка данных (True для админского выхода).
        """
        try:
            if self._is_shift_ended():
                logger.warning(f"[LOGOUT] Повторная попытка LOGOUT для {self.email} — пропуск.")
                return False

            # 1) закрыть предыдущий статус
            prev_id = self.db.finish_last_status(self.email, self.session_id)
            if prev_id:
                if sync:
                    row = self.db.get_action_by_id(prev_id)
                    action = self._make_action_payload_from_row(row)
                    if sheets_api.log_user_actions([action], self.email, user_group=group or self.group):
                        self.db.mark_actions_synced([prev_id])
                else:
                    self._send_action_to_sheets(prev_id, user_group=group or self.group)

            # 2) записать LOGOUT
            now = datetime.now().isoformat()
            record_id = self.db.log_action(
                email=self.email,
                name=self.name,
                status="Завершено",
                action_type="LOGOUT",
                comment=comment,
                immediate_sync=False,
                session_id=self.session_id,
                status_start_time=now,
                status_end_time=now,
                reason=reason,
                user_group=group or self.group
            )

            if sync:
                row2 = self.db.get_action_by_id(record_id)
                action2 = self._make_action_payload_from_row(row2)
                if sheets_api.log_user_actions([action2], self.email, user_group=group or self.group):
                    self.db.mark_actions_synced([record_id])
            else:
                self._send_action_to_sheets(record_id, user_group=group or self.group)

            self.last_sync_time = datetime.now()
            self._check_sync_status()
            logger.info(f"[LOGOUT] Смена завершена: {self.email}. Причина: {comment}, reason={reason}")

            if self.on_logout_callback:
                self.on_logout_callback()
                
            return True
        except Exception as e:
            logger.error(f"Ошибка записи LOGOUT: {e}")
            return False

    def closeEvent(self, event):
        if self._closing_reason == "admin_logout":
            # Закрываем без подтверждения
            event.accept()
            self._closing_reason = None
            return

        if self._closing_reason == "auto_logout":
            # Можно закрыть без подтверждения или показать уведомление
            event.accept()
            self._closing_reason = None
            return

        # Если пользователь пытается закрыть окно вручную
        reply = QMessageBox.question(
            self,
            'Подтверждение закрытия',
            'Вы уверены, что хотите закрыть приложение? Смена будет автоматически завершена...',
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )

        if reply == QMessageBox.Yes:
            self._closing_reason = "user_close"
            try:
                self._log_shift_end("Приложение закрыто через крестик", reason="user", group=self.group, sync=False)
                sheets_api.finish_active_session(self.email, self.session_id, datetime.now().isoformat())
            except Exception as e:
                logger.error(f"Ошибка при закрытии приложения: {e}")
            self.shift_ended = True
            event.accept()
            self._closing_reason = None
        else:
            event.ignore()