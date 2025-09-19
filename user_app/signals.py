from __future__ import annotations

# user_app/signals.py
from PyQt5.QtCore import QObject, pyqtSignal


class SyncSignals(QObject):
    """
    Общие сигналы синка/управления для прокидывания в GUI и фоновый менеджер.
    """

    # Администратор принудительно завершил сессию
    force_logout = pyqtSignal()
    # Телеметрия синхронизации (обновляется после каждого цикла)
    sync_status_updated = pyqtSignal(dict)


class SessionSignals(QObject):
    """Сигналы жизненного цикла пользовательской сессии."""

    # Причина завершения передаётся строкой (local_logout, remote_force_logout, ...)
    sessionFinished = pyqtSignal(str)
