# user_app/db_local.py
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Iterable, Tuple, List
import logging
from contextlib import contextmanager

from config import LOCAL_DB_PATH, MAX_COMMENT_LENGTH, MAX_HISTORY_DAYS
from user_app.db_migrations import apply_migrations

logger = logging.getLogger(__name__)

# Глобальные флаги для управления миграциями
_MIGRATIONS_DONE = False
_LOCK = threading.RLock()
_CONN = None
_DB_PATH = None
_BUSY_MS = 60000  # до 60с ждём блокировку


class LocalDBError(Exception):
    """Ошибки локальной БД."""


def _connect(path: str) -> sqlite3.Connection:
    """Создаем соединение с настройками стабильности."""
    # Один коннект на процесс: WAL + autocommit + shared cache.
    conn = sqlite3.connect(
        path,
        timeout=60,
        isolation_level=None,        # autocommit; транзакции начинаем руками
        check_same_thread=False,     # используем общий коннект из разных потоков
        uri=False
    )
    cur = conn.cursor()
    # стабильность под нагрузкой
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute(f"PRAGMA busy_timeout={_BUSY_MS};")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA foreign_keys=ON;")
    cur.close()
    return conn


def _apply_migrations_once(conn: sqlite3.Connection) -> None:
    """Применяем миграции только один раз на процесс."""
    global _MIGRATIONS_DONE
    if _MIGRATIONS_DONE:
        return
    
    try:
        apply_migrations(conn)
        _MIGRATIONS_DONE = True
        logger.info("DB migrations (indexes) applied")
    except Exception as e:
        logger.warning("DB migrations failed: %s", e)


def init_db(path_main: str, path_fallback: str) -> Tuple[sqlite3.Connection, str]:
    """
    Инициализируем РОВНО ОДИН коннект на процесс и храним его в модуле.
    Без переходов в :memory:.
    """
    global _CONN, _DB_PATH
    if _CONN:
        return _CONN, _DB_PATH
    
    last_err = None
    # 5 попыток на основной путь с экспоненциальной задержкой
    for i in range(5):
        try:
            conn = _connect(path_main)
            _apply_migrations_once(conn)
            _CONN, _DB_PATH = conn, path_main
            logger.info("Локальная БД успешно инициализирована: %s", path_main)
            return _CONN, _DB_PATH
        except Exception as e:
            last_err = e
            if "database is locked" in str(e):
                time.sleep(1.5 * (i + 1))
                continue
            break
    
    # пробуем резервный (без :memory:)
    try:
        conn = _connect(path_fallback)
        _apply_migrations_once(conn)
        _CONN, _DB_PATH = conn, path_fallback
        logger.info("Локальная БД успешно инициализирована: %s", path_fallback)
        logger.warning("Используется резервный путь локальной БД: %s", path_fallback)
        return _CONN, _DB_PATH
    except Exception as e2:
        logger.critical("Не удалось открыть БД ни по основному, ни по резервному пути: %s; %s", last_err, e2)
        raise


def get_conn() -> sqlite3.Connection:
    """Получить глобальное соединение с БД."""
    if not _CONN:
        raise RuntimeError("DB не инициализирована. Вызови init_db() при старте приложения.")
    return _CONN


def _ensure_conn_alive() -> None:
    """Если ссылка на _CONN есть, но соединение кем-то закрыто — переоткроем."""
    global _CONN, _DB_PATH
    if _CONN is None or _DB_PATH is None:
        return
    try:
        _CONN.execute("PRAGMA user_version")
    except sqlite3.ProgrammingError:
        # Соединение закрыто, создаём заново
        _CONN = _connect(_DB_PATH)
        _apply_migrations_once(_CONN)


@contextmanager
def read_cursor():
    """
    Короткие чтения. Под общим RLock, чтобы не столкнуться с записью.
    """
    if not _CONN:
        raise RuntimeError("DB не инициализирована")
    with _LOCK:
        _ensure_conn_alive()
        cur = _CONN.cursor()
        try:
            yield cur
        finally:
            cur.close()


@contextmanager
def write_tx():
    """
    Короткая сериализованная запись: BEGIN IMMEDIATE → COMMIT / ROLLBACK.
    """
    if not _CONN:
        raise RuntimeError("DB не инициализирована")
    with _LOCK:
        _ensure_conn_alive()
        try:
            _CONN.execute("BEGIN IMMEDIATE;")
            yield _CONN
            _CONN.commit()
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                logger.warning("database is locked -> retrying in 200ms")
                time.sleep(0.2)
                _CONN.rollback()
                _CONN.execute("BEGIN IMMEDIATE")
                yield _CONN
                _CONN.commit()
            else:
                _CONN.rollback()
                raise
        except Exception:
            _CONN.rollback()
            raise


def close_connection(_conn=None):
    """
    В длительно живущем приложении не закрываем коннект каждую минуту.
    Закрывать — только при завершении процесса.
    """
    global _CONN, _DB_PATH
    if not _CONN:
        return
    try:
        _CONN.close()
        logger.info("Соединение с локальной БД закрыто")
    finally:
        _CONN, _DB_PATH = None, None


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
        home_fallback = Path.home() / "WorkTimeTracker" / "local_backup.db"
        
        try:
            self.conn, self.db_path = init_db(primary_path, str(home_fallback))
            self._opened_path = Path(self.db_path)
            
            # профилактика
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
                # Более устойчивые настройки под параллельные операции
                self.conn = sqlite3.connect(
                    str(self.db_path),
                    timeout=30,             # ждать до 30с при блокировке
                    check_same_thread=False # доступ из разных потоков GUI/таймеров
                )
                try:
                    self.conn.execute("PRAGMA journal_mode=WAL;")      # лучше для конкуренции
                    self.conn.execute("PRAGMA busy_timeout=5000;")     # 5с на блокировки внутри sqlite
                    self.conn.execute("PRAGMA synchronous=NORMAL;")    # быстрее, достаточно надёжно
                    self.conn.execute("PRAGMA foreign_keys=ON;")
                except Exception as e:
                    logger.warning("PRAGMA setup failed: %s", e)
                
                self._ensure_schema()
                _apply_migrations_once(self.conn)
                
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
        """Закрывает ГЛОБАЛЬНОЕ соединение корректно (через close_connection)."""
        with self._lock:
            close_connection()  # обнуляет _CONN и закрывает соединение атомарно
            self.conn = None

    def __del__(self):
        # Не закрываем БД из деструктора: синглтон может понадобиться другим частям.
        # Закрытие выполняется в main при завершении приложения.
        pass

    # ------------------------------------------------------------------ #
    # Schema & migration
    # ------------------------------------------------------------------ #
    def _ensure_schema(self) -> None:
        assert self.conn is not None, "База не открыта"
        with write_tx() as conn:
            cur = conn.cursor()

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

            # Кэш пользователей для офлайн-логина
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users_cache (
                    email TEXT PRIMARY KEY,
                    name TEXT,
                    role TEXT,
                    "group" TEXT,
                    shift_hours TEXT,
                    telegram_login TEXT,
                    updated_at TEXT
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

            # Новые индексы под частые запросы
            if 'synced' in cols and 'priority' in cols and 'id' in cols:
                cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_synced_priority_id ON logs(synced, priority, id)")
            if 'email' in cols and 'session_id' in cols and 'status_end_time' in cols:
                cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_email_session_end ON logs(email, session_id, status_end_time)")
            if 'action_type' in cols:
                cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_action_type ON logs(action_type)")

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
            # Ровно один LOGOUT на session_id (без окна "5 минут")
            cur.execute(
                """
                CREATE TRIGGER IF NOT EXISTS prevent_duplicate_logout
                BEFORE INSERT ON logs
                FOR EACH ROW
                WHEN LOWER(NEW.action_type) = 'logout' AND EXISTS (
                    SELECT 1 FROM logs
                     WHERE session_id = NEW.session_id
                       AND LOWER(action_type) = 'logout'
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

    def _ensure_users_cache(self, conn: sqlite3.Connection) -> None:
        """Гарантирует создание таблицы users_cache (идемпотентно)."""
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users_cache (
                email TEXT PRIMARY KEY,
                name TEXT,
                role TEXT,
                "group" TEXT,
                shift_hours TEXT,
                telegram_login TEXT,
                updated_at TEXT
            );
            """
        )

    # ------------------------------------------------------------------ #
    # App logs (диагностика)
    # ------------------------------------------------------------------ #
    def add_log(self, level: str, message: str) -> int:
        self._ensure_open()
        if self.conn is None:
            return -1
        ts = datetime.now(timezone.utc).isoformat()
        with self._lock:
            with write_tx() as conn:
                cur = conn.cursor()
                cur.execute("INSERT INTO app_logs (ts, level, message) VALUES (?, ?, ?)", (ts, level, message))
                return int(cur.lastrowid)

    def cleanup_old_logs(self, days: int = 30) -> int:
        """Очистка app_logs старше N дней (совм. со старым вызовом)."""
        self._ensure_open()
        if self.conn is None:
            return 0
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with self._lock:
            with write_tx() as conn:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM app_logs WHERE ts < ?", (cutoff,))
                cnt = int(cur.fetchone()[0] or 0)
                cur.execute("DELETE FROM app_logs WHERE ts < ?", (cutoff,))
                return cnt

    def cleanup_old_action_logs(self, days: int = 30) -> int:
        self._ensure_open()
        if self.conn is None:
            return 0
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with self._lock:
            with write_tx() as conn:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM logs WHERE timestamp < ?", (cutoff,))
                cnt = int(cur.fetchone()[0] or 0)
                cur.execute("DELETE FROM logs WHERE timestamp < ?", (cutoff,))
                return cnt

    # ------------------------------------------------------------------ #
    # Helpers for sync/anti-dup
    # ------------------------------------------------------------------ #
    def is_unsynced(self, action_id: int) -> bool:
        """Проверяет, не синхронизирована ли запись с указанным ID."""
        self._ensure_open()
        if self.conn is None:
            return False
        with self._lock:
            with read_cursor() as cur:
                cur.execute("SELECT synced FROM logs WHERE id = ?", (action_id,))
                row = cur.fetchone()
                return bool(row and int(row[0]) == 0)

    def get_action_by_id(self, action_id: int) -> Optional[Tuple]:
        """Получает запись действия по ID."""
        self._ensure_open()
        if self.conn is None:
            return None
        with self._lock:
            with read_cursor() as cur:
                cur.execute("""
                    SELECT id, session_id, email, name, status, action_type, comment,
                           timestamp, synced, sync_attempts, last_sync_attempt,
                           priority, status_start_time, status_end_time, reason, user_group
                    FROM logs WHERE id = ?""", (action_id,))
                return cur.fetchone()

    # ------------------------------------------------------------------ #
    # Users offline cache
    # ------------------------------------------------------------------ #
    def update_user_cache(self, user: dict) -> None:
        """Обновляет кэш пользователей для офлайн-работы."""
        self._ensure_open()
        if self.conn is None:
            return
        with self._lock:
            with write_tx() as conn:
                self._ensure_users_cache(conn)
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO users_cache (email, name, role, "group", shift_hours, telegram_login, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(email) DO UPDATE SET
                        name=excluded.name, role=excluded.role, "group"=excluded."group",
                        shift_hours=excluded.shift_hours, telegram_login=excluded.telegram_login,
                        updated_at=datetime('now')
                """, (
                    (user.get("email") or "").strip().lower(),
                    user.get("name") or "",
                    user.get("role") or "специалист",
                    user.get("group") or "",
                    user.get("shift_hours") or "8 часов",
                    user.get("telegram_login") or "",
                ))

    def get_user_from_cache(self, email: str) -> Optional[dict]:
        """Получает пользователя из кэша по email."""
        self._ensure_open()
        if self.conn is None:
            return None
        with self._lock:
            with read_cursor() as cur:
                cur.execute("""SELECT email, name, role, "group", shift_hours, telegram_login
                               FROM users_cache WHERE email = ?""", ((email or "").strip().lower(),))
                row = cur.fetchone()
                if not row:
                    return None
                return {
                    "email": row[0], "name": row[1], "role": row[2],
                    "group": row[3], "shift_hours": row[4], "telegram_login": row[5],
                }

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
        """Обычная запись (самостоятельная транзакция)."""
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
                with write_tx() as conn:
                    cur = conn.cursor()
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
                    return int(cur.lastrowid)
        except sqlite3.Error as e:
            if "Duplicate LOGOUT action" in str(e):
                logger.warning("Попытка дублирования LOGOUT (session_id=%s)", session_id)
                return -1
            raise LocalDBError(f"Ошибка записи в лог: {e}")

    def log_action_tx(
        self,
        conn: sqlite3.Connection,
        *,
        email: str,
        name: str,
        status: Optional[str],
        action_type: str,
        comment: Optional[str] = None,
        priority: int = 1,
        session_id: Optional[str] = None,
        status_start_time: Optional[str] = None,
        status_end_time: Optional[str] = None,
        reason: Optional[str] = None,
        user_group: Optional[str] = None,
        **_ignore: object,
    ) -> int:
        """
        Тот же insert, но в ПЕРЕДАННУЮ транзакцию (используется из GUI, когда
        надо зафиксировать несколько шагов атомарно).
        """
        if not email or not name or not action_type:
            raise LocalDBError("Обязательные поля не заполнены (email/name/action_type)")

        if comment and len(comment) > MAX_COMMENT_LENGTH:
            comment = comment[:MAX_COMMENT_LENGTH]

        ts = datetime.now(timezone.utc).isoformat()
        session_id = session_id or self._gen_session_id(email)
        prio = max(1, min(3, int(priority or 1)))

        try:
            cur = conn.cursor()
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
            return int(cur.lastrowid)
        except sqlite3.Error as e:
            if "Duplicate LOGOUT action" in str(e):
                logger.warning("Попытка дублирования LOGOUT (session_id=%s)", session_id)
                return -1
            raise LocalDBError(f"Ошибка записи в лог: {e}")

    def get_unsynced_actions(self, limit: int = 100) -> List[Tuple]:
        self._ensure_open()
        if self.conn is None:
            return []
        with self._lock:
            with read_cursor() as cur:
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
            with read_cursor() as cur:
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
            with write_tx() as conn:
                cur = conn.cursor()
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

    def check_existing_logout(self, email: str, session_id: Optional[str] = None) -> bool:
        self._ensure_open()
        if self.conn is None:
            return False
        with self._lock:
            with read_cursor() as cur:
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

    def finish_last_status_tx(
        self,
        conn: sqlite3.Connection,
        email: str,
        session_id: str,
        end_time: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> Optional[int]:
        """
        Идемпотентно закрывает последнюю «открытую» запись статуса (status_end_time IS NULL)
        для заданных (email, session_id). Работает внутри переданной транзакции `conn`.
        Возвращает id обновлённой записи или None, если открытой записи нет.
        """
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id
                  FROM logs
                 WHERE email = ?
                   AND session_id = ?
                   AND status_end_time IS NULL
                   AND (action_type='STATUS_CHANGE' OR action_type='LOGIN')
              ORDER BY id DESC
                 LIMIT 1
                """,
                (email.strip(), str(session_id).strip()),
            )
            row = cur.fetchone()
            if not row:
                return None

            rid = int(row[0])
            # Нормализуем время окончания
            if end_time is None:
                ts_end = datetime.now(timezone.utc).isoformat()
            elif isinstance(end_time, datetime):
                ts_end = end_time.astimezone(timezone.utc).isoformat()
            else:
                ts_end = str(end_time)

            if reason is None:
                cur.execute(
                    "UPDATE logs SET status_end_time=? WHERE id=?", 
                    (ts_end, rid),
                )
            else:
                cur.execute(
                    "UPDATE logs SET status_end_time=?, reason=? WHERE id=?",
                    (ts_end, reason, rid),
                )
            return rid
        except sqlite3.Error as e:
            raise LocalDBError(f"Ошибка закрытия последнего статуса: {e}")

    def finish_last_status(
        self,
        email: str,
        session_id: str,
    ) -> Optional[int]:
        """
        Бэк-компат: та же операция, но с автосозданием соединения.
        Используется старым кодом; новые вызовы должны идти через finish_last_status_tx(...).
        """
        with write_tx() as conn:
            return self.finish_last_status_tx(conn, email, session_id)

    # --- совместимость с GUI: ожидается суффикс _tx ---
    def finish_last_status_tx_compat(self, email: str, session_id: str) -> Optional[int]:
        """
        Совместимость: GUI вызывает метод с _tx, здесь просто делегируем на фактический finish_last_status.
        Логика finish_last_status уже использует транзакцию (write_tx), поэтому вторичную обертку не делаем.
        """
        try:
            return self.finish_last_status(email, session_id)
        except Exception as e:
            logger.error(f"finish_last_status_tx failed: {e}")
            return None

    def get_last_unfinished_session(self, email: str) -> Optional[Dict[str, Any]]:
        self._ensure_open()
        if self.conn is None:
            return None
        with self._lock:
            with read_cursor() as cur:
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
            with read_cursor() as cur:
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


# Вспомогательная транзакция для внешнего кода (GUI/сервисы)
@contextmanager
def write_tx_external(db_path: Optional[str] = None):
    """Вспомогательная транзакция для внешнего кода (GUI/сервисы)."""
    path = db_path or str(LOCAL_DB_PATH)
    conn = sqlite3.connect(
        path, timeout=30, check_same_thread=False
    )
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
    except Exception:
        pass
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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