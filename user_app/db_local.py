# user_app/db_local.py
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Iterable, Tuple, List
import logging

from config import LOCAL_DB_PATH, MAX_COMMENT_LENGTH, MAX_HISTORY_DAYS
from user_app.db_migrations import apply_migrations

logger = logging.getLogger(__name__)


class LocalDBError(Exception):
    """Ошибки локальной БД."""


class LocalDB:
    """
    Локальная БД с полной совместимостью со старым кодом.
    Авто-открытие, самовосстановление, безопасное закрытие.
    """

    def __init__(self, db_path: Optional[str] = None) -> None:
        self.conn: Optional[sqlite3.Connection] = None
        self.db_path: Optional[Path] = None
        self._lock = threading.RLock()
        self._opened_path: Optional[Path] = None

        # автозагрузка как раньше
        self._bootstrap_open(db_path or str(LOCAL_DB_PATH))

    # ------------------------------------------------------------------ #
    # Bootstrap & lifecycle
    # ------------------------------------------------------------------ #
    def _bootstrap_open(self, primary_path: str) -> None:
        """Пробуем основной путь, затем домашний, затем ':memory:'."""
        try:
            self.open(primary_path)
            return
        except Exception as e:
            logger.error("Не удалось открыть БД по основному пути '%s': %s", primary_path, e)

        home_fallback = Path.home() / "WorkTimeTracker" / "local_backup.db"
        try:
            self.open(str(home_fallback))
            logger.warning("Используется резервный путь локальной БД: %s", home_fallback)
            return
        except Exception as e:
            logger.error("Не удалось открыть резервную БД '%s': %s", home_fallback, e)

        # крайний случай — in-memory (чтобы UI не падал)
        with self._lock:
            self.db_path = None
            self.conn = sqlite3.connect(":memory:", timeout=10, check_same_thread=False)
            self.conn.execute("PRAGMA journal_mode=MEMORY;")
            self.conn.execute("PRAGMA synchronous=OFF;")
            self.conn.execute("PRAGMA foreign_keys=ON;")
            self._ensure_schema()
            self._opened_path = None
            logger.warning("Локальная БД запущена в режиме ':memory:' (без записи на диск).")

    def open(self, db_path: str) -> None:
        with self._lock:
            self.db_path = Path(db_path).resolve()
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            logger.debug("Инициализация LocalDB по пути: %s", self.db_path)
            try:
                self.conn = sqlite3.connect(str(self.db_path), timeout=10, check_same_thread=False)
                self.conn.execute("PRAGMA journal_mode=WAL;")
                self.conn.execute("PRAGMA synchronous=NORMAL;")
                self.conn.execute("PRAGMA foreign_keys=ON;")
                self._ensure_schema()
                
                # миграции индексов (быстро и безопасно)
                try:
                    apply_migrations(self.conn)
                    logger.info("DB migrations (indexes) applied")
                except Exception as e:
                    logger.warning("DB migrations failed: %s", e)
                
                self._opened_path = self.db_path
                # профилактика
                self.cleanup_old_action_logs(days=MAX_HISTORY_DAYS)
                logger.info("Локальная БД успешно инициализирована: %s", self.db_path)
            except sqlite3.Error as e:
                self.conn = None
                raise LocalDBError(f"Ошибка инициализации БД: {e}")

    def _ensure_open(self) -> None:
        if self.conn is not None:
            return
        base = str(self._opened_path or self.db_path or LOCAL_DB_PATH)
        try:
            self.open(base)
        except Exception as e:
            logger.error("Повторное открытие БД по '%s' не удалось: %s", base, e)
            self._bootstrap_open(base)

    def close(self) -> None:
        with self._lock:
            conn = getattr(self, "conn", None)
            if conn is not None:
                try:
                    conn.commit()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
            self.conn = None
            logger.info("Соединение с локальной БД закрыто")

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Schema & migration
    # ------------------------------------------------------------------ #
    def _ensure_schema(self) -> None:
        assert self.conn is not None, "База не открыта"
        cur = self.conn.cursor()

        # Если есть старая таблица logs (без нужных колонок) — переименуем
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='logs';")
        if cur.fetchone():
            cur.execute("PRAGMA table_info(logs);")
            cols = [r[1] for r in cur.fetchall()]
            required = {'session_id', 'email', 'name', 'action_type', 'timestamp'}
            if not required.issubset(set(cols)):
                legacy_name = f"app_logs_legacy_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                cur.execute(f"ALTER TABLE logs RENAME TO {legacy_name};")
                logger.warning("Обнаружена старая схема 'logs' — переименована в %s", legacy_name)

        # Основная таблица действий
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                email TEXT NOT NULL,
                name TEXT NOT NULL,
                status TEXT,
                action_type TEXT NOT NULL,
                comment TEXT,
                timestamp TEXT NOT NULL,
                synced INTEGER DEFAULT 0,
                sync_attempts INTEGER DEFAULT 0,
                last_sync_attempt TEXT,
                priority INTEGER DEFAULT 1,
                status_start_time TEXT,
                status_end_time TEXT,
                reason TEXT,
                user_group TEXT
            );
            """
        )

        # Индексы (безопасно: проверяем наличие колонок)
        cur.execute("PRAGMA table_info(logs);")
        cols = {r[1] for r in cur.fetchall()}
        if 'email' in cols:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_email ON logs(email);")
        if 'synced' in cols:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_synced ON logs(synced);")
        if 'timestamp' in cols:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);")
        if 'session_id' in cols:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_session ON logs(session_id);")

        # Триггеры
        cur.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS check_comment_length
            BEFORE INSERT ON logs
            FOR EACH ROW
            WHEN length(NEW.comment) > {int(MAX_COMMENT_LENGTH)}
            BEGIN
                SELECT RAISE(ABORT, 'Comment too long');
            END;
            """
        )
        cur.execute(
            """
            CREATE TRIGGER IF NOT EXISTS prevent_duplicate_logout
            BEFORE INSERT ON logs
            FOR EACH ROW
            WHEN LOWER(NEW.action_type) = 'logout' AND EXISTS (
                SELECT 1 FROM logs
                WHERE session_id = NEW.session_id
                  AND LOWER(action_type) = 'logout'
                  AND timestamp > datetime('now', '-5 minutes')
            )
            BEGIN
                SELECT RAISE(ABORT, 'Duplicate LOGOUT action');
            END;
            """
        )

        # Диагностические логи приложения
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS app_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_app_logs_ts ON app_logs(ts);")

        self.conn.commit()

    # ------------------------------------------------------------------ #
    # App logs (диагностика)
    # ------------------------------------------------------------------ #
    def add_log(self, level: str, message: str) -> int:
        self._ensure_open()
        if self.conn is None:
            return -1
        ts = datetime.now(timezone.utc).isoformat()
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO app_logs (ts, level, message) VALUES (?, ?, ?)", (ts, level, message))
            self.conn.commit()
            return int(cur.lastrowid)

    def cleanup_old_logs(self, days: int = 30) -> int:
        """Очистка app_logs старше N дней (совм. со старым вызовом)."""
        self._ensure_open()
        if self.conn is None:
            return 0
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM app_logs WHERE ts < ?", (cutoff,))
            cnt = int(cur.fetchone()[0] or 0)
            cur.execute("DELETE FROM app_logs WHERE ts < ?", (cutoff,))
            self.conn.commit()
            return cnt

    def cleanup_old_action_logs(self, days: int = 30) -> int:
        self._ensure_open()
        if self.conn is None:
            return 0
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM logs WHERE timestamp < ?", (cutoff,))
            cnt = int(cur.fetchone()[0] or 0)
            cur.execute("DELETE FROM logs WHERE timestamp < ?", (cutoff,))
            self.conn.commit()
            return cnt

    # ------------------------------------------------------------------ #
    # Action logs (то, что синхронизируется)
    # ------------------------------------------------------------------ #
    def _gen_session_id(self, email: str) -> str:
        return f"{(email or '')[:8]}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    def log_action(
        self,
        email: str,
        name: str,
        status: Optional[str],
        action_type: str,
        comment: Optional[str] = None,
        immediate_sync: bool = False,
        priority: int = 1,
        session_id: Optional[str] = None,
        status_start_time: Optional[str] = None,
        status_end_time: Optional[str] = None,
        reason: Optional[str] = None,
        user_group: Optional[str] = None,
    ) -> int:
        if not email or not name or not action_type:
            raise LocalDBError("Обязательные поля не заполнены (email/name/action_type)")

        if comment and len(comment) > MAX_COMMENT_LENGTH:
            comment = comment[:MAX_COMMENT_LENGTH]

        ts = datetime.now(timezone.utc).isoformat()
        session_id = session_id or self._gen_session_id(email)
        prio = max(1, min(3, int(priority or 1)))

        self._ensure_open()
        if self.conn is None:
            raise LocalDBError("Не удалось открыть локальную БД")

        try:
            with self._lock:
                cur = self.conn.cursor()
                cur.execute(
                    """
                    INSERT INTO logs
                    (email, name, status, action_type, comment, timestamp, priority,
                     session_id, status_start_time, status_end_time, reason, user_group)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        email.strip(),
                        name.strip(),
                        status,
                        action_type,
                        comment,
                        ts,
                        prio,
                        session_id,
                        status_start_time,
                        status_end_time,
                        reason,
                        user_group,
                    ),
                )
                self.conn.commit()
                return int(cur.lastrowid)
        except sqlite3.Error as e:
            if "Duplicate LOGOUT action" in str(e):
                logger.warning("Попытка дублирования LOGOUT (session_id=%s)", session_id)
                return -1
            raise LocalDBError(f"Ошибка записи в лог: {e}")

    def get_action_by_id(self, action_id: int) -> Optional[Tuple]:
        """Нужен GUI для немедленной отправки одной записи."""
        self._ensure_open()
        if self.conn is None:
            return None
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM logs WHERE id = ?", (int(action_id),))
            return cur.fetchone()

    def get_unsynced_actions(self, limit: int = 100) -> List[Tuple]:
        self._ensure_open()
        if self.conn is None:
            return []
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT id, email, name, status, action_type, comment, timestamp,
                       session_id, status_start_time, status_end_time, reason, user_group
                  FROM logs
                 WHERE synced = 0
              ORDER BY priority DESC, timestamp ASC
                 LIMIT ?
                """,
                (int(limit),),
            )
            return list(cur.fetchall())

    def get_unsynced_count(self) -> int:
        """Нужен авто-синху для статистики очереди."""
        self._ensure_open()
        if self.conn is None:
            return 0
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM logs WHERE synced = 0;")
            row = cur.fetchone()
            return int(row[0] or 0)

    def mark_actions_synced(self, ids: List[int]) -> None:
        if not ids:
            return
        self._ensure_open()
        if self.conn is None:
            return
        with self._lock:
            cur = self.conn.cursor()
            placeholders = ",".join(["?"] * len(ids))
            cur.execute(
                f"""
                UPDATE logs
                   SET synced = 1,
                       sync_attempts = sync_attempts + 1,
                       last_sync_attempt = ?
                 WHERE id IN ({placeholders})
                """,
                [datetime.now(timezone.utc).isoformat(), *ids],
            )
            self.conn.commit()

    def check_existing_logout(self, email: str, session_id: Optional[str] = None) -> bool:
        self._ensure_open()
        if self.conn is None:
            return False
        with self._lock:
            cur = self.conn.cursor()
            if session_id:
                cur.execute(
                    "SELECT COUNT(*) FROM logs WHERE email=? AND session_id=? AND LOWER(action_type)='logout'",
                    (email, session_id),
                )
            else:
                cur.execute(
                    "SELECT COUNT(*) FROM logs WHERE email=? AND LOWER(action_type)='logout'",
                    (email,),
                )
            return (cur.fetchone()[0] or 0) > 0

    def finish_last_status(self, email: str, session_id: str) -> Optional[int]:
        self._ensure_open()
        if self.conn is None:
            return None
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT id FROM logs
                 WHERE email=? AND session_id=? AND status_end_time IS NULL
                   AND (action_type='STATUS_CHANGE' OR action_type='LOGIN')
              ORDER BY id DESC LIMIT 1
                """,
                (email, session_id),
            )
            row = cur.fetchone()
            if not row:
                return None
            rid = int(row[0])
            cur.execute(
                "UPDATE logs SET status_end_time=? WHERE id=?",
                (datetime.now(timezone.utc).isoformat(), rid),
            )
            self.conn.commit()
            return rid

    def get_last_unfinished_session(self, email: str) -> Optional[Dict[str, Any]]:
        self._ensure_open()
        if self.conn is None:
            return None
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT session_id, timestamp
                  FROM logs
                 WHERE email=? AND action_type='LOGIN'
                   AND session_id NOT IN (
                        SELECT session_id FROM logs
                         WHERE email=? AND LOWER(action_type)='logout'
                   )
              ORDER BY timestamp DESC
                 LIMIT 1
                """,
                (email, email),
            )
            row = cur.fetchone()
            return {"session_id": row[0], "timestamp": row[1]} if row else None

    def get_active_session(self, email: str) -> Optional[Dict[str, Any]]:
        return self.get_last_unfinished_session(email)

    def get_current_user_email(self) -> Optional[str]:
        self._ensure_open()
        if self.conn is None:
            return None
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT email
                  FROM logs
                 WHERE status_end_time IS NULL
                   AND action_type IN ('LOGIN','STATUS_CHANGE')
              ORDER BY id DESC
                 LIMIT 1
                """
            )
            row = cur.fetchone()
            return row[0] if row else None


# Синглтон (при необходимости)
_DB_SINGLETON: Optional[LocalDB] = None
_SINGLETON_LOCK = threading.Lock()

def get_db() -> LocalDB:
    global _DB_SINGLETON
    if _DB_SINGLETON is None:
        with _SINGLETON_LOCK:
            if _DB_SINGLETON is None:
                _DB_SINGLETON = LocalDB(str(LOCAL_DB_PATH))
    return _DB_SINGLETON


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="LocalDB helper")
    ap.add_argument("--path", type=str, default=str(LOCAL_DB_PATH))
    ap.add_argument("--add-log", type=str, default=None, help="Добавить app_log (level:msg)")
    ap.add_argument("--cleanup-days", type=int, default=None, help="Удалить app_logs старше N дней")
    args = ap.parse_args()

    db = LocalDB(args.path)
    if args.add_log:
        try:
            level, msg = args.add_log.split(":", 1)
        except Exception:
            level, msg = "INFO", args.add_log
        rid = db.add_log(level, msg)
        print(f"Inserted app_log id={rid}")

    if args.cleanup_days is not None:
        cnt = db.cleanup_old_logs(days=args.cleanup_days)
        print(f"Deleted {cnt} old app_log rows")

    db.close()