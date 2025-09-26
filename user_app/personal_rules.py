# user_app/personal_rules.py
from __future__ import annotations
import sqlite3
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from config import (
    PERSONAL_RULES_ENABLED,
    PERSONAL_WINDOW_MIN,
    PERSONAL_STATUS_LIMIT_PER_WINDOW,
    LOCAL_DB_PATH,   # путь к вашей локальной БД, как в user_app.db_local
)
from telegram_bot.notifier import TelegramNotifier

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
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _open_db() -> sqlite3.Connection:
    con = sqlite3.connect(LOCAL_DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.executescript(DDL)
    return con

def on_status_committed(email: str, status_name: str, ts_iso: Optional[str] = None) -> None:
    """
    Вызывайте ЭТУ функцию в месте, где статус успешно зафиксирован (после записи в Sheets/БД).
    Если превышен порог частоты за окно, отправит личное уведомление сотруднику.
    """
    if not PERSONAL_RULES_ENABLED:
        return

    email = (email or "").strip().lower()
    if not email:
        return

    ts_iso = ts_iso or _utcnow_iso()
    try:
        con = _open_db()
        with con:
            con.execute(
                "INSERT INTO status_events(email, status, ts_utc) VALUES (?, ?, ?)",
                (email, status_name or "", ts_iso)
            )

            # окно
            window_min = int(PERSONAL_WINDOW_MIN)
            limit = int(PERSONAL_STATUS_LIMIT_PER_WINDOW)
            start_ts = (datetime.now(timezone.utc) - timedelta(minutes=window_min)).replace(microsecond=0).isoformat()

            cur = con.execute(
                "SELECT COUNT(*) FROM status_events WHERE email=? AND ts_utc>=?",
                (email, start_ts)
            )
            cnt = int(cur.fetchone()[0])

        if cnt > limit:
            # триггерим персональный алерт
            n = TelegramNotifier()
            text = (
                f"⚠️ Частые изменения статусов: <b>{cnt}</b> за последние "
                f"{window_min} мин. Порог: {limit}.\n"
                f"Последний статус: <b>{status_name or '—'}</b>."
            )
            ok = n.send_personal(email, text)
            log.info("Personal alert for %s: sent=%s (count=%s>limit=%s)", email, ok, cnt, limit)
    except Exception as e:
        log.exception("personal_rules.on_status_committed error: %s", e)
