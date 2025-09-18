# user_app/personal_rules.py
from __future__ import annotations

import datetime as dt
import logging
import sqlite3

from config import LOCAL_DB_PATH  # путь к вашей локальной БД, как в user_app.db_local
from config import PERSONAL_RULES_ENABLED
from notifications.engine import long_status_check, record_status_event

# Импортируем модуль для работы с общим подключением к БД
from user_app import db_local

log = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS status_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    status TEXT NOT NULL,
    ts_utc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_status_events_email_ts ON status_events(email, ts_utc);
"""


def _utcnow_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()


def _open_db() -> sqlite3.Connection:
    con = sqlite3.connect(LOCAL_DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.executescript(DDL)
    return con


def on_status_committed(
    email: str, status_name: str, ts_iso: str | None = None
) -> None:
    """Фиксирует событие статуса и даёт движку шанс сработать по правилам status_window."""
    if not PERSONAL_RULES_ENABLED:
        return

    email = (email or "").strip().lower()
    if not email:
        return

    ts_iso = ts_iso or _utcnow_iso()
    try:
        record_status_event(email=email, status_name=status_name, ts_iso=ts_iso)
    except Exception as e:
        log.exception("personal_rules.on_status_committed error: %s", e)


def check_long_status(
    email: str, status_name: str, started_iso: str, elapsed_min: int
) -> None:
    """
    Проверяет длительные статусы и передаёт информацию в движок правил.
    """
    if not PERSONAL_RULES_ENABLED:
        return

    email = (email or "").strip().lower()
    if not email:
        return

    try:
        # Преобразуем строку в datetime с учетом таймзоны
        started_dt = dt.datetime.fromisoformat(started_iso)

        # Если тайзона отсутствует — считаем это ЛОКАЛЬНЫМ временем машины
        local_tz = dt.datetime.now().astimezone().tzinfo
        if started_dt.tzinfo is None:
            started_local = started_dt.replace(tzinfo=local_tz)
        else:
            started_local = started_dt.astimezone(local_tz)

        # Нормализуем в UTC для расчётов
        started_utc = started_local.astimezone(dt.UTC)

        # Добавляем отладочную информацию
        log.debug(
            "long-status poll: status=%s started_local=%s started_utc=%s elapsed_min=%d",
            status_name,
            started_local.isoformat(),
            started_utc.isoformat(),
            elapsed_min,
        )

        # Передаём в движок правил: он сам решит, какие long_status правила совпадают
        long_status_check(
            email=email,
            status_name=status_name,
            started_dt=started_utc,
            elapsed_min=elapsed_min,
        )
    except Exception as e:
        log.exception("personal_rules.check_long_status error: %s", e)


def poll_long_running_local() -> None:
    """
    Опрос длительных статусов с использованием общего подключения к БД.
    БД инициализируется один раз при старте приложения (в main).
    Здесь просто читаем через общий коннект, без закрытия.
    """
    if not PERSONAL_RULES_ENABLED:
        return

    try:
        with db_local.read_cursor() as cur:
            # Читаем нужные записи о длительных статусах
            cur.execute(
                """
                SELECT email, status_name, started_iso, elapsed_min
                FROM long_running_statuses
                WHERE elapsed_min > 0
            """
            )

            for row in cur.fetchall():
                email, status_name, started_iso, elapsed_min = row
                check_long_status(email, status_name, started_iso, elapsed_min)

    except Exception as e:
        log.exception("personal_rules.poll_long_running_local error: %s", e)
