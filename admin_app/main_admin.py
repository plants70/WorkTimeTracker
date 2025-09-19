# admin_app/main_admin.py
from __future__ import annotations

import logging
import sys
import time
from functools import partial

from PyQt5.QtCore import QObject, Qt, QThread, QTimer, pyqtSignal, pyqtSlot
from PyQt5.QtWidgets import (
    QAction,
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QGroupBox,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QAbstractItemView,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from admin_app.notifications_panel import open_panel as open_notifications_panel
from admin_app.repo import AdminRepo
from config import LOG_DIR

# --- Единое логирование для админки ---
from logging_setup import setup_logging

logger = logging.getLogger(__name__)

# =================== Константы UI ===================
FIELDS = ["Email", "Name", "Phone", "Role", "Telegram", "Group", "NotifyTelegram"]
ROLES = [
    "специалист",
    "старший специалист",
    "ведущий специалист",
    "руководитель группы",
]

# Загрузка GROUP_MAPPING с обработкой ошибок
try:
    # статическая карта групп, если определена в config.py
    from config import GROUP_MAPPING
except Exception:
    GROUP_MAPPING = {}

# =================== Диалог редактирования пользователя ===================


class UserDialog(QDialog):
    def __init__(
        self, parent=None, user: dict[str, str] | None = None, groups: list[str] = None
    ):
        super().__init__(parent)
        self.setWindowTitle("Карточка сотрудника")
        self.user = user or {}
        self.groups = groups or []
        self._build()

    def _build(self):
        layout = QVBoxLayout(self)

        self.email_input = QLineEdit(str(self.user.get("Email", "")))
        self.fio_input = QLineEdit(str(self.user.get("Name", "")))
        self.phone_input = QLineEdit(str(self.user.get("Phone", "")))
        self.tg_input = QLineEdit(str(self.user.get("Telegram", "")))
        # chat_id заполняется ботом, руками редактировать рискованно
        self.tg_input.setReadOnly(True)
        self.tg_input.setPlaceholderText(
            "Заполняется автоматически при привязке через Telegram-бота"
        )

        self.role_combo = QComboBox()
        self.role_combo.addItems(ROLES)
        role_val = str(self.user.get("Role", "")).strip()
        if role_val in ROLES:
            self.role_combo.setCurrentText(role_val)

        self.group_combo = QComboBox()
        self.group_combo.addItems(self.groups)
        group_val = str(self.user.get("Group", "")).strip()
        if group_val in self.groups:
            self.group_combo.setCurrentText(group_val)

        self.tg_notify_chk = QCheckBox("Отправлять уведомления в Telegram")
        chk = str(self.user.get("NotifyTelegram", "")).strip().lower()
        self.tg_notify_chk.setChecked(chk in ("yes", "true", "1", "да"))

        layout.addWidget(QLabel("Email:"))
        layout.addWidget(self.email_input)
        layout.addWidget(QLabel("ФИО:"))
        layout.addWidget(self.fio_input)
        layout.addWidget(QLabel("Телефон:"))
        layout.addWidget(self.phone_input)
        layout.addWidget(QLabel("Telegram:"))
        layout.addWidget(self.tg_input)
        layout.addWidget(QLabel("Должность:"))
        layout.addWidget(self.role_combo)
        layout.addWidget(QLabel("Группа:"))
        layout.addWidget(self.group_combo)
        layout.addWidget(self.tg_notify_chk)

        btns = QHBoxLayout()
        btn_save = QPushButton("Сохранить")
        btn_save.clicked.connect(self.accept)
        btn_cancel = QPushButton("Отмена")
        btn_cancel.clicked.connect(self.reject)
        btns.addWidget(btn_save)
        btns.addWidget(btn_cancel)
        layout.addLayout(btns)

    def get_user(self) -> dict[str, str]:
        return {
            "Email": self.email_input.text().strip().lower(),
            "Name": self.fio_input.text().strip(),
            "Phone": self.phone_input.text().strip(),
            "Role": self.role_combo.currentText().strip(),
            "Telegram": self.tg_input.text().strip(),
            "Group": self.group_combo.currentText().strip(),
            "NotifyTelegram": "Yes" if self.tg_notify_chk.isChecked() else "No",
        }


# =================== Главное окно ===================


class AdminWindow(QMainWindow):
    def __init__(self, groups: list[str]):
        super().__init__()
        self.setWindowTitle("Админка WorkTimeTracker")
        self.resize(1400, 780)

        # Группы
        self.groups = groups

        # Репозиторий
        self.repo = AdminRepo()

        # Кэш пользователей и активных e-mail
        self.users: list[dict[str, str]] = []
        self._active_cache: tuple[float, set[str]] = (0.0, set())  # (ts, {emails})
        self._active_ttl_sec = 30.0

        # Активные сессии
        self.active_sessions: list[dict[str, str]] = []
        self._sessions_thread: QThread | None = None
        self._sessions_worker: "_ListActiveSessionsWorker" | None = None
        self._kick_threads: dict[str, QThread] = {}
        self._kick_workers: dict[str, "_KickSessionWorker"] = {}
        self._kick_buttons: dict[str, QPushButton] = {}
        self._reap_thread: QThread | None = None
        self._reap_worker: "_ReapSessionsWorker" | None = None

        self._build_ui()

        self.tabs.currentChanged.connect(self._on_tab_changed)

        # Инициализируем загрузку списка пользователей в фоне
        self._load_users_async()
        self.load_shift_calendar()

        # Периодическое обновление кэша активных e-mail (каждые 30 сек)
        self._active_timer = QTimer(self)
        self._active_timer.setInterval(int(self._active_ttl_sec * 1000))
        self._active_timer.timeout.connect(self._refresh_active_cache)
        self._active_timer.start()

    # ---------- UI ----------
    def _build_ui(self):
        # Создаем тулбар с кнопками
        toolbar = QToolBar("Main Toolbar")
        self.addToolBar(toolbar)

        # Кнопка "Оповещения"
        btn_notifications = QAction("Оповещения", self)
        btn_notifications.triggered.connect(lambda: open_notifications_panel(self))
        toolbar.addAction(btn_notifications)

        self.tabs = QTabWidget(self)

        # --- Вкладка "Сотрудники" ---
        self.tab_users = QWidget()
        users_layout = QVBoxLayout(self.tab_users)

        # Фильтры
        filter_layout = QHBoxLayout()
        filter_layout.addWidget(QLabel("Группа:"))
        self.group_filter_combo = QComboBox()
        self.group_filter_combo.addItem("Все группы")
        self.group_filter_combo.addItems(self.groups)
        self.group_filter_combo.currentIndexChanged.connect(self.apply_user_search)
        filter_layout.addWidget(self.group_filter_combo)

        self.only_active_chk = QCheckBox("Только активные")
        self.only_active_chk.stateChanged.connect(self.apply_user_search)
        filter_layout.addWidget(self.only_active_chk)

        filter_layout.addStretch()
        users_layout.addLayout(filter_layout)

        # Поиск и кнопки
        top_layout = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Поиск по ФИО или email")
        self.search_input.textChanged.connect(self.apply_user_search)
        top_layout.addWidget(self.search_input)

        btn_add = QPushButton("Добавить")
        btn_add.clicked.connect(self.add_user)
        btn_edit = QPushButton("Редактировать")
        btn_edit.clicked.connect(self.edit_user)
        btn_delete = QPushButton("Удалить")
        btn_delete.clicked.connect(self.on_delete_user_clicked)
        btn_kick = QPushButton("Разлогинить")
        btn_kick.clicked.connect(self.on_force_logout_clicked)

        for b in (btn_add, btn_edit, btn_delete, btn_kick):
            top_layout.addWidget(b)
        users_layout.addLayout(top_layout)

        # Таблица пользователей
        self.users_table = QTableWidget(0, len(FIELDS))
        self.users_table.setHorizontalHeaderLabels(
            [
                "Email",
                "ФИО",
                "Телефон",
                "Должность",
                "Telegram",
                "Группа",
                "Telegram уведомления",
            ]
        )
        self.users_table.setSelectionBehavior(QTableWidget.SelectRows)
        users_layout.addWidget(self.users_table)

        self.tabs.addTab(self.tab_users, "Сотрудники")

        # --- Вкладка "График" ---
        self.tab_schedule = QWidget()
        schedule_layout = QVBoxLayout(self.tab_schedule)

        header_layout = QHBoxLayout()
        header_layout.addWidget(QLabel("Сотрудник:"))
        self.schedule_user_combo = QComboBox()
        self.schedule_user_combo.addItem("Выберите сотрудника")
        self.schedule_user_combo.currentIndexChanged.connect(
            self.on_schedule_user_change
        )
        header_layout.addWidget(self.schedule_user_combo)
        header_layout.addStretch()
        schedule_layout.addLayout(header_layout)

        self.info_group = QGroupBox("Информация о сотруднике")
        info_layout = QVBoxLayout()
        self.login_status_lbl = QLabel("Залогинен: Нет")
        self.btn_force_logout = QPushButton("Разлогинить")
        self.btn_force_logout.setEnabled(False)
        self.btn_force_logout.clicked.connect(self.force_logout_from_schedule)
        status_row = QHBoxLayout()
        status_row.addWidget(self.login_status_lbl)
        status_row.addWidget(self.btn_force_logout)
        status_row.addStretch()
        info_layout.addLayout(status_row)

        self.info_label = QLabel("")
        self.info_label.setWordWrap(True)
        info_layout.addWidget(self.info_label)
        self.info_group.setLayout(info_layout)
        schedule_layout.addWidget(self.info_group)

        self.schedule_table = QTableWidget()
        schedule_layout.addWidget(self.schedule_table)

        self.tabs.addTab(self.tab_schedule, "График")

        # --- Вкладка "Сессии" ---
        self.tab_sessions = QWidget()
        sessions_layout = QVBoxLayout(self.tab_sessions)

        sessions_controls = QHBoxLayout()
        self.btn_refresh_sessions = QPushButton("Обновить список")
        self.btn_refresh_sessions.clicked.connect(self.load_active_sessions)
        sessions_controls.addWidget(self.btn_refresh_sessions)

        self.btn_reap_sessions = QPushButton("Проверить неактивные")
        self.btn_reap_sessions.clicked.connect(self.reap_stale_sessions)
        sessions_controls.addWidget(self.btn_reap_sessions)

        sessions_controls.addStretch()
        sessions_layout.addLayout(sessions_controls)

        session_headers = [
            "Email",
            "ФИО",
            "Группа",
            "LoginTime",
            "LastPing",
            "Status",
            "SessionID",
            "Действия",
        ]
        self.sessions_table = QTableWidget(0, len(session_headers))
        self.sessions_table.setHorizontalHeaderLabels(session_headers)
        self.sessions_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.sessions_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        header = self.sessions_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeToContents)
        header.setStretchLastSection(True)
        sessions_layout.addWidget(self.sessions_table)

        self.tabs.addTab(self.tab_sessions, "Сессии")

        # --- Вкладка "Дополнительно" (плейсхолдер) ---
        self.tab_extra = QWidget()
        extra_layout = QVBoxLayout(self.tab_extra)
        extra_layout.addWidget(QLabel("Тут будет что-то ещё"))
        self.tabs.addTab(self.tab_extra, "Дополнительно")

        self.setCentralWidget(self.tabs)

    # ---------- Helpers ----------
    def _selected_email(self) -> str | None:
        items = self.users_table.selectedItems()
        if not items:
            return None
        val = items[0].text().strip()
        return val[2:] if val.startswith("🟢 ") else val

    def _confirm(self, msg: str) -> bool:
        return (
            QMessageBox.question(
                self,
                "Подтверждение",
                msg,
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            == QMessageBox.Yes
        )

    def _info(self, msg: str):
        QMessageBox.information(self, "Информация", msg)

    def _warn(self, msg: str):
        QMessageBox.warning(self, "Ошибка", msg)

    # ---------- Активные сессии (кэш) ----------
    def _on_tab_changed(self, index: int):
        try:
            widget = self.tabs.widget(index)
        except Exception:
            return
        if widget is getattr(self, "tab_sessions", None):
            self.load_active_sessions()

    def _get_active_emails_cached(self) -> set[str]:
        ts, emails = self._active_cache
        if time.monotonic() - ts < self._active_ttl_sec:
            return emails
        try:
            sessions = self.repo.get_active_sessions()
            emails = {
                str(s.get("Email", "")).strip().lower()
                for s in sessions
                if str(s.get("Status", "")).strip().lower() == "active"
            }
            self._active_cache = (time.monotonic(), emails)
            return emails
        except Exception as e:
            logger.warning("Не удалось получить активные сессии: %s", e)
            return set()

    def _refresh_active_cache(self):
        """Обновить кэш активных пользователей и при необходимости перерисовать таблицу."""
        try:
            sessions = self.repo.get_active_sessions()
            emails = {
                str(s.get("Email", "")).strip().lower()
                for s in sessions
                if str(s.get("Status", "")).strip().lower() == "active"
            }
            self._active_cache = (time.monotonic(), emails)
            if self.only_active_chk.isChecked():
                self.apply_user_search()
        except Exception as e:
            logger.warning("Не удалось обновить активные сессии: %s", e)

    def _set_sessions_controls_enabled(self, enabled: bool) -> None:
        if hasattr(self, "btn_refresh_sessions"):
            self.btn_refresh_sessions.setEnabled(enabled)
        if hasattr(self, "btn_reap_sessions"):
            self.btn_reap_sessions.setEnabled(enabled)

    def load_active_sessions(self):
        if self._sessions_thread and self._sessions_thread.isRunning():
            return
        self._set_sessions_controls_enabled(False)
        self.statusBar().showMessage("Загрузка активных сессий...")

        self._sessions_thread = QThread(self)
        self._sessions_worker = _ListActiveSessionsWorker(self.repo)
        self._sessions_worker.moveToThread(self._sessions_thread)
        self._sessions_worker.finished.connect(self._on_sessions_loaded)
        self._sessions_thread.started.connect(self._sessions_worker.run)
        self._sessions_worker.finished.connect(self._sessions_thread.quit)
        self._sessions_worker.finished.connect(self._sessions_worker.deleteLater)
        self._sessions_thread.finished.connect(self._sessions_thread.deleteLater)
        self._sessions_thread.finished.connect(
            lambda: setattr(self, "_sessions_thread", None)
        )
        self._sessions_thread.start()

    def _on_sessions_loaded(self, sessions: list[dict], error: str | None):
        self.statusBar().clearMessage()
        self._sessions_worker = None
        self._set_sessions_controls_enabled(True)

        if error:
            logger.warning("load_active_sessions error: %s", error)
            self._warn(f"Не удалось получить активные сессии: {error}")

        self.active_sessions = sessions or []
        emails = {
            str(s.get("Email", "")).strip().lower()
            for s in self.active_sessions
            if str(s.get("Status", "")).strip().lower() == "active"
        }
        self._active_cache = (time.monotonic(), emails)

        self._render_sessions_table()

    def _render_sessions_table(self):
        self.sessions_table.setRowCount(0)
        self._kick_buttons.clear()

        if not self.active_sessions:
            return

        self.sessions_table.setRowCount(len(self.active_sessions))
        for row_idx, session in enumerate(self.active_sessions):
            email = str(session.get("Email") or session.get("email") or "")
            name = str(session.get("Name") or session.get("name") or "")
            group = str(session.get("Group") or session.get("group") or "")
            login_time = str(
                session.get("LoginTime")
                or session.get("login_time")
                or session.get("Login")
                or ""
            )
            last_ping = str(
                session.get("LastPing")
                or session.get("lastping")
                or session.get("Last Ping")
                or ""
            )
            status = str(session.get("Status") or session.get("status") or "")
            session_id = str(session.get("SessionID") or session.get("sessionid") or "")

            values = [email, name, group, login_time, last_ping, status, session_id]
            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                item.setData(Qt.UserRole, value)
                self.sessions_table.setItem(row_idx, col, item)

            btn = QPushButton("Kick")
            btn.setEnabled(bool(session_id))
            btn.clicked.connect(partial(self._on_kick_clicked, session_id))
            self.sessions_table.setCellWidget(row_idx, len(values), btn)
            if session_id:
                self._kick_buttons[session_id] = btn

        self.sessions_table.resizeRowsToContents()

    def reap_stale_sessions(self):
        if self._reap_thread and self._reap_thread.isRunning():
            return
        self._set_sessions_controls_enabled(False)
        self.statusBar().showMessage("Проверка неактивных сессий...")

        self._reap_thread = QThread(self)
        self._reap_worker = _ReapSessionsWorker(self.repo)
        self._reap_worker.moveToThread(self._reap_thread)
        self._reap_worker.finished.connect(self._on_reap_finished)
        self._reap_thread.started.connect(self._reap_worker.run)
        self._reap_worker.finished.connect(self._reap_thread.quit)
        self._reap_worker.finished.connect(self._reap_worker.deleteLater)
        self._reap_thread.finished.connect(self._reap_thread.deleteLater)
        self._reap_thread.finished.connect(
            lambda: setattr(self, "_reap_thread", None)
        )
        self._reap_thread.start()

    def _on_reap_finished(self, count: int, error: str | None):
        self.statusBar().clearMessage()
        self._set_sessions_controls_enabled(True)
        self._reap_worker = None

        if error:
            logger.warning("reap_stale_sessions failed: %s", error)
            self._warn(f"Проверка неактивных сессий не удалась: {error}")
        else:
            msg = (
                f"Закрыто {count} сессий."
                if count
                else "Не найдено неактивных сессий."
            )
            self.statusBar().showMessage(msg, 5000)

        self._active_cache = (0.0, set())
        self.refresh_users()
        self.load_active_sessions()

    def _on_kick_clicked(self, session_id: str):
        sid = (session_id or "").strip()
        if not sid:
            self._warn("SessionID отсутствует")
            return

        if sid in self._kick_threads and self._kick_threads[sid].isRunning():
            return

        btn = self._kick_buttons.get(sid)
        if btn:
            btn.setEnabled(False)

        thread = QThread(self)
        worker = _KickSessionWorker(self.repo, sid)
        worker.moveToThread(thread)
        worker.finished.connect(self._on_kick_finished)
        worker.finished.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(lambda: self._kick_threads.pop(sid, None))
        self._kick_threads[sid] = thread
        self._kick_workers[sid] = worker
        self.statusBar().showMessage(f"Завершение сессии {sid}...")
        thread.started.connect(worker.run)
        thread.start()

    def _on_kick_finished(self, session_id: str, success: bool, error: str | None):
        self.statusBar().clearMessage()
        self._kick_workers.pop(session_id, None)
        self._kick_threads.pop(session_id, None)

        btn = self._kick_buttons.get(session_id)
        if success:
            if btn:
                btn.setEnabled(False)
            self._active_cache = (0.0, set())
            self.refresh_users()
            self.load_active_sessions()
            self.statusBar().showMessage(
                f"Сессия {session_id} завершена.", 5000
            )
        else:
            if btn:
                btn.setEnabled(True)
            if error:
                self._warn(f"Не удалось завершить сессию: {error}")
            else:
                self._warn("Не удалось завершить сессию")

    # =================== Таб "Сотрудники" ===================

    def refresh_users(self):
        """Явное обновление списка пользователей (перезагрузка с сервера)."""
        self._load_users_async()

    def _load_users_async(self):
        """Фоновая загрузка списка пользователей с отображением статуса."""
        self.statusBar().showMessage("Загрузка списка пользователей...")
        self._set_ui_enabled(False)
        # Запускаем воркер в отдельном потоке
        self._users_thread = QThread(self)
        self._users_worker = _ListUsersWorker(self.repo)
        self._users_worker.moveToThread(self._users_thread)
        self._users_worker.finished.connect(self._on_users_loaded)
        self._users_thread.started.connect(self._users_worker.run)
        # Автоочистка потока по завершению
        self._users_worker.finished.connect(self._users_thread.quit)
        self._users_worker.finished.connect(self._users_worker.deleteLater)
        self._users_thread.finished.connect(self._users_thread.deleteLater)
        self._users_thread.start()

    @pyqtSlot(list)
    def _on_users_loaded(self, users: list):
        self.statusBar().clearMessage()
        self._set_ui_enabled(True)

        if not users:
            QMessageBox.critical(
                self,
                "Ошибка",
                "Не удалось загрузить список пользователей (проверьте подключение).",
            )

        self.users = users or []
        self.apply_user_search()

        # и выпадающий список на вкладке "График"
        self.schedule_user_combo.blockSignals(True)
        self.schedule_user_combo.clear()
        self.schedule_user_combo.addItem("Выберите сотрудника")
        for u in self.users:
            fio = u.get("Name", "")
            if fio:
                self.schedule_user_combo.addItem(fio)
        self.schedule_user_combo.blockSignals(False)

    def _set_ui_enabled(self, enabled: bool):
        """Включить/отключить элементы управления UI."""
        self.search_input.setEnabled(enabled)
        self.group_filter_combo.setEnabled(enabled)
        self.only_active_chk.setEnabled(enabled)
        self.users_table.setEnabled(enabled)

        # Находим кнопки в layout
        for i in range(self.tab_users.layout().count()):
            item = self.tab_users.layout().itemAt(i)
            if isinstance(item, QHBoxLayout):
                for j in range(item.count()):
                    widget = item.itemAt(j).widget()
                    if isinstance(widget, QPushButton):
                        widget.setEnabled(enabled)

    def apply_user_search(self):
        self.refresh_users_table(self.search_input.text())

    def refresh_users_table(self, filter_text: str = ""):
        self.users_table.setRowCount(0)
        selected_group = self.group_filter_combo.currentText()
        only_active = self.only_active_chk.isChecked()
        active_emails = self._get_active_emails_cached() if only_active else set()

        for u in self.users:
            email = u.get("Email", "").strip().lower()
            group = u.get("Group", "").strip()
            is_active = email in active_emails

            # поиск
            if filter_text:
                q = filter_text.lower()
                if q not in email and q not in u.get("Name", "").lower():
                    continue
            # фильтр по группе
            if selected_group != "Все группы" and group != selected_group:
                continue
            # фильтр активности
            if only_active and not is_active:
                continue

            row = self.users_table.rowCount()
            self.users_table.insertRow(row)
            for col, key in enumerate(FIELDS):
                val = u.get(key, "")
                if key == "Email" and is_active:
                    val = f"🟢 {val}"
                item = QTableWidgetItem(str(val))
                item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                self.users_table.setItem(row, col, item)

    # --- CRUD/Actions ---

    def add_user(self):
        dlg = UserDialog(self, groups=self.groups)
        if dlg.exec_():
            data = dlg.get_user()
            if self.repo.add_or_update_user(data):
                self._info("Пользователь добавлен")
                self.refresh_users()
            else:
                self._warn("Ошибка при добавлении пользователя")

    def edit_user(self):
        row = self.users_table.currentRow()
        if row < 0 or row >= len(self.users):
            self._warn("Сначала выберите строку для редактирования.")
            return
        user = self.users[row]
        dlg = UserDialog(self, user=user, groups=self.groups)
        if dlg.exec_():
            data = dlg.get_user()
            if self.repo.add_or_update_user(data):
                self._info("Пользователь обновлён")
                self.refresh_users()
            else:
                self._warn("Ошибка при обновлении пользователя")

    def on_delete_user_clicked(self):
        email = self._selected_email()
        if not email:
            self._warn("Выберите пользователя")
            return
        if not self._confirm(f"Удалить пользователя {email}?"):
            return
        if self.repo.delete_user(email):
            self._info("Пользователь удалён")
            self.refresh_users()
        else:
            self._warn("Пользователь не найден или не удалён")

    def on_force_logout_clicked(self):
        email = self._selected_email()
        if not email:
            self._warn("Выберите пользователя из списка.")
            return

        # отображаем ФИО для красоты
        fio = ""
        sel = self.users_table.selectedItems()
        if sel and len(sel) > 1:
            fio = sel[1].text()

        if not self._confirm(f"Разлогинить {fio or email}?"):
            return

        if self.repo.force_logout(email=email):
            self._info(f"Пользователь {fio or email} был разлогинен.")
            # сбрасываем кэш активностей, чтобы таблица обновилась корректно
            self._active_cache = (0.0, set())
            self.refresh_users()
        else:
            self._warn("Активная сессия не найдена")

    # =================== Таб "График" ===================

    def load_shift_calendar(self):
        """Подтягиваем таблицу графика. Если её нет — отключаем элементы."""
        try:
            data = self.repo.get_shift_calendar()
        except Exception as e:
            logger.exception("Ошибка при загрузке графика: %s", e)
            data = []

        self.shift_calendar_data: list[list[str]] = data
        self.shift_headers: list[str] = data[0] if data else []

        if not data:
            self.info_label.setText("Лист графика не найден или пуст.")
            self.login_status_lbl.setText("Залогинен: Нет")
            self.btn_force_logout.setEnabled(False)
            self.schedule_table.setRowCount(0)
            self.schedule_table.setColumnCount(0)
            self.schedule_user_combo.setEnabled(bool(self.users))
            return

        self.schedule_user_combo.setEnabled(True)

    def on_schedule_user_change(self):
        idx = self.schedule_user_combo.currentIndex()
        if idx <= 0 or not self.shift_calendar_data:
            self.schedule_table.setRowCount(0)
            self.schedule_table.setColumnCount(0)
            self.info_label.setText("")
            self.login_status_lbl.setText("Залогинен: Нет")
            self.btn_force_logout.setEnabled(False)
            return

        fio = self.schedule_user_combo.currentText()
        email = ""
        for u in self.users:
            if u.get("Name", "") == fio:
                email = u.get("Email", "")
                break

        # статус логина
        active = self._get_active_emails_cached()
        is_logged_in = email.strip().lower() in active
        self.login_status_lbl.setText(f"Залогинен: {'Да' if is_logged_in else 'Нет'}")
        self.btn_force_logout.setEnabled(is_logged_in)
        self.btn_force_logout.setProperty("user_email", email)
        self.btn_force_logout.setProperty("user_fio", fio)

        # инфо по сотруднику
        info_parts = [f"<b>ФИО:</b> {fio}", f"<b>Email:</b> {email}"]
        self.info_label.setText("<br>".join(info_parts))

        # табель по дням (ищем первые числовые заголовки как дни месяца)
        headers = self.shift_headers
        row_for_user: list[str] | None = None
        for r in self.shift_calendar_data[1:]:
            if r and r[0].strip() == fio:
                row_for_user = r
                break

        day_indices = [(i, h) for i, h in enumerate(headers) if str(h).isdigit()]
        self.schedule_table.setRowCount(0)
        self.schedule_table.setColumnCount(len(day_indices))
        self.schedule_table.setHorizontalHeaderLabels([str(h) for _, h in day_indices])

        if row_for_user:
            self.schedule_table.setRowCount(1)
            for col, (i, _) in enumerate(day_indices):
                val = row_for_user[i] if i < len(row_for_user) else ""
                self.schedule_table.setItem(0, col, QTableWidgetItem(str(val)))
            self.schedule_table.resizeColumnsToContents()

    def force_logout_from_schedule(self):
        email = self.btn_force_logout.property("user_email")
        fio = self.btn_force_logout.property("user_fio")
        if not email:
            self._warn("Не удалось определить Email пользователя.")
            return
        if not self._confirm(f"Разлогинить {fio or email}?"):
            return

        if self.repo.force_logout(email=email):
            self._info(f"Пользователь {fio or email} разлогинен.")
            self.btn_force_logout.setEnabled(False)
            self.login_status_lbl.setText("Залогинен: Нет")
            # сбрасываем кэш активностей
            self._active_cache = (0.0, set())
            self.refresh_users()
        else:
            self._warn("Активная сессия не найдена")


# =================== Вспомогательные функции ===================


def get_available_groups(repo: AdminRepo) -> list[str]:
    """Получение списка доступных групп"""
    if GROUP_MAPPING:
        return sorted(set(GROUP_MAPPING.values()))
    return repo.list_groups_from_sheet()


# =================== Entrypoint ===================


def main():
    # Единое логирование для админки
    log_path = setup_logging(app_name="wtt-admin", log_dir=LOG_DIR)
    logger = logging.getLogger(__name__)
    logger.info("Admin app logging initialized (path=%s)", log_path)

    # Получение списка групп
    repo = AdminRepo()
    groups = get_available_groups(repo)
    logger.info("Groups: %s", ", ".join(groups) if groups else "<none>")

    # Запуск GUI с передачей списка групп
    app = QApplication(sys.argv)
    win = AdminWindow(groups=groups)
    win.show()
    sys.exit(app.exec_())


# ----------------- Фоновый воркер для загрузки пользователей -----------------
class _ListUsersWorker(QObject):
    finished = pyqtSignal(list)

    def __init__(self, repo: AdminRepo):
        super().__init__()
        self.repo = repo

    def run(self):
        try:
            users = self.repo.list_users()
        except Exception as e:
            logging.getLogger(__name__).exception(
                "Ошибка при загрузке пользователей: %s", e
            )
            users = []
        # вернёмся в GUI-поток через сигнал
        self.finished.emit(users)


class _ListActiveSessionsWorker(QObject):
    finished = pyqtSignal(list, str)

    def __init__(self, repo: AdminRepo):
        super().__init__()
        self.repo = repo

    def run(self):
        error_msg = ""
        sessions: list[dict[str, str]] = []
        try:
            sessions = self.repo.get_active_sessions()
        except Exception as e:
            logging.getLogger(__name__).exception(
                "Ошибка при загрузке активных сессий: %s", e
            )
            error_msg = str(e)
        self.finished.emit(sessions or [], error_msg)


class _ReapSessionsWorker(QObject):
    finished = pyqtSignal(int, str)

    def __init__(self, repo: AdminRepo, max_idle_minutes: int | None = None):
        super().__init__()
        self.repo = repo
        self.max_idle_minutes = max_idle_minutes

    def run(self):
        error_msg = ""
        count = 0
        try:
            count = int(self.repo.reap_stale_sessions(self.max_idle_minutes))
        except Exception as e:
            logging.getLogger(__name__).exception(
                "Ошибка при запуске reaper: %s", e
            )
            error_msg = str(e)
        self.finished.emit(count, error_msg)


class _KickSessionWorker(QObject):
    finished = pyqtSignal(str, bool, str)

    def __init__(self, repo: AdminRepo, session_id: str):
        super().__init__()
        self.repo = repo
        self.session_id = session_id

    def run(self):
        error_msg = ""
        success = False
        try:
            success = bool(self.repo.kick_session(self.session_id))
        except Exception as e:
            logging.getLogger(__name__).exception(
                "Ошибка при завершении сессии %s: %s", self.session_id, e
            )
            error_msg = str(e)
        if not success and not error_msg:
            error_msg = "Сессия не найдена или уже завершена"
        self.finished.emit(self.session_id, success, error_msg)


if __name__ == "__main__":
    main()
