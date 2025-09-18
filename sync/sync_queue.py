from __future__ import annotations

import datetime as dt
import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from config import LOCAL_DB_PATH, MAX_COMMENT_LENGTH, MAX_HISTORY_DAYS
from user_app.db_migrations import apply_migrations
from sync.threading_utils import guard_gui_long_operation

logger = logging.getLogger(__name__)

# --- глобальное состояние одной БД на процесс ---
_MIGRATIONS_DONE = False
_LOCK = threading.RLock()
_CONN: sqlite3.Connection | None = None
_DB_PATH: str | None = None
_BUSY_MS = 60_000           # ждать блокировку до 60с
_WRITE_MAX_RETRIES = 5
_WRITE_RETRY_MAX_DELAY = 1.0


class LocalDBError(Exception):
    """Ошибки локальной БД."""


def _connect(path: str) -> sqlite3.Connection:
    """Создаём соединение с настройками стабильности (WAL, busy_timeout)."""
    conn = sqlite3.connect(
        path,
        timeout=60,
        isolation_level=None,      # autocommit; явные BEGIN/COMMIT
        check_same_thread=False,
        uri=False,
    )
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute(f"PRAGMA busy_timeout={_BUSY_MS};")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA foreign_keys=ON;")
    cur.close()
    return conn


def _apply_migrations_once(conn: sqlite3.Connection) -> None:
    global _MIGRATIONS_DONE
    if _MIGRATIONS_DONE:
        return
    try:
        apply_migrations(conn)
        _MIGRATIONS_DONE = True
        logger.info("DB migrations (indexes) applied")
    except Exception as e:
        logger.warning("DB migrations failed: %s", e)


def init_db(path_main: str, path_fallback: str) -> tuple[sqlite3.Connection, str]:
    """Инициализируем РОВНО ОДИН коннект на процесс и запоминаем путь."""
    global _CONN, _DB_PATH
    if _CONN:
        return _CONN, _DB_PATH or path_main

    last_err: Exception | None = None
    for i in range(5):
        try:
            conn = _connect(path_main)
            _apply_migrations_once(conn)
            _CONN, _DB_PATH = conn, path_main
            logger.info("Локальная БД успешно инициализирована: %s", path_main)
            return _CONN, _DB_PATH
        except Exception as e:
            last_err = e
            if "database is locked" in str(e).lower():
                time.sleep(1.5 * (i + 1))
                continue
            break

    try:
        conn = _connect(path_fallback)
        _apply_migrations_once(conn)
        _CONN, _DB_PATH = conn, path_fallback
        logger.info("Локальная БД успешно инициализирована: %s (fallback)", path_fallback)
        return _CONN, _DB_PATH
    except Exception as e2:
        logger.critical("Не удалось открыть БД: %s; fallback: %s", last_err, e2)
        raise


def get_conn() -> sqlite3.Connection:
    if not _CONN:
        raise RuntimeError("DB не инициализирована. Вызови init_db() при старте.")
    return _CONN


def _ensure_conn_alive() -> None:
    global _CONN, _DB_PATH
    if _CONN is None or _DB_PATH is None:
        return
    try:
        _CONN.execute("PRAGMA user_version")
    except sqlite3.ProgrammingError:
        _CONN = _connect(_DB_PATH)
        _apply_migrations_once(_CONN)


@contextmanager
def read_cursor():
    """Короткие чтения под общим RLock."""
    if not _CONN:
        raise RuntimeError("DB не инициализирована")
    with _LOCK:
        _ensure_conn_alive()
        cur = _CONN.cursor()
        with guard_gui_long_operation("db.read_cursor", threshold=0.4):
            try:
                yield cur
            finally:
                cur.close()


@contextmanager
def write_tx():
    """Сериализованная запись: BEGIN IMMEDIATE → COMMIT/ROLLBACK с ретраями."""
    if not _CONN:
        raise RuntimeError("DB не инициализирована")
    last_err: Exception | None = None
    for attempt in range(1, _WRITE_MAX_RETRIES + 1):
        with _LOCK:
            _ensure_conn_alive()
            try:
                _CONN.execute("BEGIN IMMEDIATE;")
            except sqlite3.OperationalError as exc:
                if "database is locked" in str(exc).lower():
                    last_err = exc
                else:
                    raise
            else:
                last_err = None
                try:
                    with guard_gui_long_operation("db.write_tx"):
                        yield _CONN
                    _CONN.commit()
                    return
                except Exception:
                    _CONN.rollback()
                    raise

        if last_err is not None:
            if attempt >= _WRITE_MAX_RETRIES:
                break
            delay = min(0.2 * attempt, _WRITE_RETRY_MAX_DELAY)
            logger.warning(
                "database is locked (attempt %d/%d), retrying in %.1fs",
                attempt, _WRITE_MAX_RETRIES, delay,
            )
            time.sleep(delay)

    if last_err is not None:
        raise LocalDBError(
            f"Не удалось начать транзакцию после {_WRITE_MAX_RETRIES} попыток"
        ) from last_err
    raise LocalDBError("Не удалось начать транзакцию")


def close_connection(_conn=None):
    """Закрывать соединение следует при завершении приложения."""
    global _CONN, _DB_PATH
    if not _CONN:
        return
    try:
        _CONN.close()
        logger.info("Соединение с локальной БД закрыто")
    finally:
        _CONN, _DB_PATH = None, None


class LocalDB:
    """Локальная БД с полной совместимостью со старым кодом."""

    def __init__(self, db_path: str | None = None) -> None:
        self.conn: sqlite3.Connection | None = None
        self.db_path: Path | None = None
        self._lock = threading.RLock()
        self._opened_path: Path | None = None
        self._bootstrap_open(db_path or str(LOCAL_DB_PATH))

    # ---- lifecycle ----
    def _bootstrap_open(self, primary_path: str) -> None:
        home_fallback = Path.home() / "WorkTimeTracker" / "local_backup.db"
        try:
            self.conn, self.db_path = init_db(primary_path, str(home_fallback))
            self._opened_path = Path(self.db_path)
            self.cleanup_old_action_logs(days=MAX_HISTORY_DAYS)
            logger.info("Локальная БД успешно инициализирована: %s", self.db_path)
            return
        except Exception as e:
            logger.error("Не удалось открыть БД по основному и резервному путям: %s", e)

        # крайний случай — in-memory (чтобы UI не падал)
        with self._lock:
            self.db_path = None
            self.conn = sqlite3.connect(":memory:", timeout=10, check_same_thread=False)
            self.conn.execute("PRAGMA journal_mode=MEMORY;")
            self.conn.execute(f"PRAGMA busy_timeout={_BUSY_MS};")
            self.conn.execute("PRAGMA synchronous=OFF;")
            self.conn.execute("PRAGMA foreign_keys=ON;")
            self._ensure_schema()
            self._opened_path = None
