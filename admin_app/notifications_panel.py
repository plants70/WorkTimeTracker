# admin_app/notifications_panel.py
from __future__ import annotations

import logging

from PyQt5 import QtWidgets

from notifications.rules_manager import HEADER, load_rules, save_rules

log = logging.getLogger(__name__)

COLS = HEADER


class NotificationsPanel(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Настройки оповещений")
        self.resize(1000, 600)

        v = QtWidgets.QVBoxLayout(self)
        self.table = QtWidgets.QTableWidget(self)
        self.table.setColumnCount(len(COLS))
        self.table.setHorizontalHeaderLabels(COLS)
        self.table.horizontalHeader().setSectionResizeMode(
            QtWidgets.QHeaderView.Stretch
        )

        btns = QtWidgets.QHBoxLayout()
        self.btn_add = QtWidgets.QPushButton("Добавить")
        self.btn_del = QtWidgets.QPushButton("Удалить")
        self.btn_reload = QtWidgets.QPushButton("Обновить из Sheets")
        self.btn_save = QtWidgets.QPushButton("Сохранить в Sheets")
        btns.addWidget(self.btn_add)
        btns.addWidget(self.btn_del)
        btns.addStretch(1)
        btns.addWidget(self.btn_reload)
        btns.addWidget(self.btn_save)

        v.addWidget(self.table)
        v.addLayout(btns)

        self.btn_add.clicked.connect(self.on_add)
        self.btn_del.clicked.connect(self.on_del)
        self.btn_reload.clicked.connect(self.on_reload)
        self.btn_save.clicked.connect(self.on_save)

        self.on_reload()

    def on_reload(self):
        self.table.setRowCount(0)
        rules = load_rules()
        # Преобразуем в «строки» (не забудем ID)
        rows = []
        for r in rules:
            rows.append(
                [
                    str(r.id),
                    "TRUE" if r.enabled else "FALSE",
                    r.kind,
                    r.scope,
                    r.group_tag or "",
                    ",".join(r.statuses or []),
                    str(r.min_duration_min or ""),
                    str(r.window_min or ""),
                    str(r.limit or ""),
                    str(r.rate_limit_sec or ""),
                    "TRUE" if r.silent else "FALSE",
                    r.template or "",
                ]
            )
        for row in rows:
            self._append_row(row)

    def _append_row(self, data):
        r = self.table.rowCount()
        self.table.insertRow(r)
        for c, val in enumerate(data):
            item = QtWidgets.QTableWidgetItem(val)
            self.table.setItem(r, c, item)

    def on_add(self):
        # Найдём макс ID и создадим новый
        max_id = 0
        for r in range(self.table.rowCount()):
            try:
                max_id = max(max_id, int(self.table.item(r, 0).text()))
            except Exception:
                pass
        new_id = max_id + 1
        self._append_row(
            [
                str(new_id),
                "TRUE",
                "long_status",
                "personal",
                "",
                "",
                "30",
                "",
                "",
                "1800",
                "FALSE",
                "⏰ Длительный статус: <b>{status}</b> уже <b>{duration_min} мин</b> (порог {min_duration_min} мин).",
            ]
        )

    def on_del(self):
        rows = sorted({i.row() for i in self.table.selectedIndexes()}, reverse=True)
        for r in rows:
            self.table.removeRow(r)

    def on_save(self):
        # Соберём все строки и валидацию по минимуму
        out = []
        for r in range(self.table.rowCount()):
            row = []
            for c in range(self.table.columnCount()):
                it = self.table.item(r, c)
                row.append(it.text().strip() if it else "")
            out.append(row)
        save_rules(out)
        QtWidgets.QMessageBox.information(
            self, "Сохранено", "Таблица правил обновлена в Sheets."
        )


def open_panel(parent=None):
    dlg = NotificationsPanel(parent)
    dlg.exec_()
