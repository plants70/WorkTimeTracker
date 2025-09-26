# user_app/db_migrations.py
from __future__ import annotations
import sqlite3
from typing import Iterable

DDL: Iterable[str] = [
    # ActiveSessions: ускоряем поиск по e-mail/сессии/статусу
    "CREATE INDEX IF NOT EXISTS idx_active_email_session ON ActiveSessions(Email, SessionID);",
    "CREATE INDEX IF NOT EXISTS idx_active_status ON ActiveSessions(Status);",

    # WorkLog: по опыту — фильтры по Email/Timestamp/SessionID
    "CREATE INDEX IF NOT EXISTS idx_worklog_email_ts ON WorkLog(Email, Timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_worklog_session ON WorkLog(SessionID);",

    # ActionLogs/Queue (если у вас есть очередь или флаг synced)
    "CREATE INDEX IF NOT EXISTS idx_actions_synced ON ActionLogs(Synced, CreatedAt);",
]

def apply_migrations(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    for sql in DDL:
        try:
            cur.execute(sql)
        except Exception as e:
            # не валим миграцию, просто пишем в лог через pragma user_version позже при развитии схемы
            pass
    conn.commit()
