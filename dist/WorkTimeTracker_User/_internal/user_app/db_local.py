import sqlite3
import logging
import threading
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict
from config import LOCAL_DB_PATH, MAX_COMMENT_LENGTH, MAX_HISTORY_DAYS  # Добавлен импорт

logger = logging.getLogger(__name__)

class LocalDBError(Exception):
    """Класс для ошибок работы с локальной БД"""
    pass

class LocalDB:
    def __init__(self):
        self.db_path = LOCAL_DB_PATH
        self.conn = None
        self._lock = threading.RLock()
        logger.debug(f"Инициализация LocalDB с путем: {self.db_path}")
        self._init_db()

    def _init_db(self):
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.conn.execute("PRAGMA journal_mode = WAL")
            self.conn.execute("PRAGMA synchronous = NORMAL")
            self._create_tables()
            self._run_migrations()
            self._add_synced_column_if_not_exists()
            self._add_status_time_columns_if_not_exists()
            self._add_reason_column_if_not_exists()
            self._clean_old_data()
            logger.info("Локальная БД успешно инициализирована")
        except sqlite3.Error as e:
            logger.error(f"Ошибка инициализации БД: {e}")
            raise LocalDBError(f"Ошибка инициализации БД: {e}")

    def _add_synced_column_if_not_exists(self):
        try:
            with self._lock:
                cursor = self.conn.execute("PRAGMA table_info(logs)")
                columns = [col[1] for col in cursor.fetchall()]
                if 'synced' not in columns:
                    self.conn.execute("ALTER TABLE logs ADD COLUMN synced INTEGER DEFAULT 0")
                    self.conn.execute("ALTER TABLE logs ADD COLUMN sync_attempts INTEGER DEFAULT 0")
                    self.conn.execute("ALTER TABLE logs ADD COLUMN last_sync_attempt TEXT")
                    self.conn.commit()
                    logger.info("Добавлены столбцы для синхронизации в таблицу logs")
        except sqlite3.Error as e:
            logger.error(f"Ошибка добавления столбцов синхронизации: {e}")

    def _add_status_time_columns_if_not_exists(self):
        try:
            with self._lock:
                cursor = self.conn.execute("PRAGMA table_info(logs)")
                columns = [col[1] for col in cursor.fetchall()]
                added = False
                if 'status_start_time' not in columns:
                    self.conn.execute("ALTER TABLE logs ADD COLUMN status_start_time TEXT")
                    added = True
                if 'status_end_time' not in columns:
                    self.conn.execute("ALTER TABLE logs ADD COLUMN status_end_time TEXT")
                    added = True
                if added:
                    self.conn.commit()
                    logger.info("Добавлены столбцы времени статусов в таблицу logs")
        except sqlite3.Error as e:
            logger.error(f"Ошибка добавления столбцов времени статусов: {e}")

    def _add_reason_column_if_not_exists(self):
        try:
            with self._lock:
                cursor = self.conn.execute("PRAGMA table_info(logs)")
                columns = [col[1] for col in cursor.fetchall()]
                if 'reason' not in columns:
                    self.conn.execute("ALTER TABLE logs ADD COLUMN reason TEXT")
                    self.conn.commit()
                    logger.info("Добавлен столбец reason в таблицу logs")
        except sqlite3.Error as e:
            logger.error(f"Ошибка добавления столбца reason: {e}")

    def _create_tables(self):
        tables_sql = [
            '''
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
            )
            ''',
            'CREATE INDEX IF NOT EXISTS idx_logs_email ON logs(email)',
            'CREATE INDEX IF NOT EXISTS idx_logs_synced ON logs(synced)',
            'CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_logs_session ON logs(session_id)',
            '''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            ''',
            f'''
            CREATE TRIGGER IF NOT EXISTS check_comment_length
            BEFORE INSERT ON logs
            FOR EACH ROW
            WHEN length(NEW.comment) > {MAX_COMMENT_LENGTH}
            BEGIN
                SELECT RAISE(ABORT, 'Comment too long');
            END
            ''',
            '''
            CREATE TRIGGER IF NOT EXISTS prevent_duplicate_logout
            BEFORE INSERT ON logs
            FOR EACH ROW
            WHEN NEW.action_type = 'LOGOUT' AND EXISTS (
                SELECT 1 FROM logs 
                WHERE session_id = NEW.session_id 
                AND action_type = 'LOGOUT'
                AND timestamp > datetime('now', '-5 minutes')
            )
            BEGIN
                SELECT RAISE(ABORT, 'Duplicate LOGOUT action');
            END
            '''
        ]
        try:
            with self._lock:
                for sql in tables_sql:
                    self.conn.execute(sql)
                self.conn.commit()
                logger.debug("Таблицы и триггеры успешно созданы/проверены")
        except sqlite3.Error as e:
            logger.error(f"Ошибка создания таблиц: {e}")
            raise LocalDBError(f"Ошибка создания таблиц: {e}")

    def _run_migrations(self):
        migrations = [
            ('session_id', "ALTER TABLE logs ADD COLUMN session_id TEXT NOT NULL DEFAULT ''"),
            ('priority', "ALTER TABLE logs ADD COLUMN priority INTEGER DEFAULT 1"),
            ('user_group', "ALTER TABLE logs ADD COLUMN user_group TEXT"),
        ]
        try:
            with self._lock:
                cursor = self.conn.execute("PRAGMA table_info(logs)")
                columns = [col[1] for col in cursor.fetchall()]
                for column_name, sql in migrations:
                    if column_name not in columns:
                        self.conn.execute(sql)
                        logger.info(f"Выполнена миграция - добавлен столбец {column_name}")
                if 'session_id' not in columns:
                    self.conn.execute('UPDATE logs SET session_id = substr(email, 1, 8) || substr(timestamp, 1, 10)')
                self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Ошибка миграции БД: {e}")
            raise LocalDBError(f"Ошибка миграции БД: {e}")

    def _clean_old_data(self):
        try:
            with self._lock:
                cutoff = (datetime.now() - timedelta(days=MAX_HISTORY_DAYS)).isoformat()
                deleted = self.conn.execute('DELETE FROM logs WHERE timestamp < ?', (cutoff,)).rowcount
                self.conn.commit()
                logger.info(f"Удалено старых записей из logs: {deleted}")
        except sqlite3.Error as e:
            logger.error(f"Ошибка очистки старых данных: {e}")

    def get_unsynced_count(self) -> int:
        try:
            with self._lock:
                cursor = self.conn.execute(
                    "SELECT COUNT(*) FROM logs WHERE synced = 0"
                )
                count = cursor.fetchone()[0]
                logger.debug(f"Количество несинхронизированных записей: {count}")
                return count
        except sqlite3.Error as e:
            logger.error(f"Ошибка получения количества несинхронизированных записей: {e}")
            return 0

    def finish_last_status(self, email: str, session_id: str) -> Optional[int]:
        logger.debug(f"Попытка закрыть последний статус для email={email}, session_id={session_id}")
        try:
            with self._lock:
                cursor = self.conn.execute(
                    "SELECT id FROM logs WHERE email=? AND session_id=? "
                    "AND status_end_time IS NULL "
                    "AND (action_type='STATUS_CHANGE' OR action_type='LOGIN') "
                    "ORDER BY id DESC LIMIT 1",
                    (email, session_id)
                )
                row = cursor.fetchone()
                if row:
                    self.conn.execute(
                        "UPDATE logs SET status_end_time=? WHERE id=?",
                        (datetime.now().isoformat(), row[0])
                    )
                    self.conn.commit()
                    logger.info(f"Закрыт последний статус с id={row[0]} для session_id={session_id}")
                    return row[0]
                else:
                    logger.debug(f"Не найден открытый статус для закрытия email={email}, session_id={session_id}")
            return None
        except sqlite3.Error as e:
            logger.error(f"Ошибка при завершении предыдущего статуса: {e}")
            return None

    def update_status_end_time(self, action_id: int, end_time_iso: str) -> bool:
        """Установить время завершения для записи статуса."""
        try:
            with self._lock:
                self.conn.execute(
                    "UPDATE logs SET status_end_time=? WHERE id=?",
                    (end_time_iso, action_id)
                )
                self.conn.commit()
            return True
        except sqlite3.Error as e:
            logger.error(f"Ошибка update_status_end_time: {e}")
            return False

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
            logger.error(f"Попытка записи действия с пустыми обязательными полями: email={email}, name={name}, action_type={action_type}")
            raise ValueError("Обязательные поля не заполнены")
        if comment and len(comment) > MAX_COMMENT_LENGTH:
            comment = comment[:MAX_COMMENT_LENGTH]
        timestamp = datetime.now().isoformat()
        session_id = session_id or self._generate_session_id(email)
        logger.debug(f"Запись действия: email={email}, action_type={action_type}, session_id={session_id}, timestamp={timestamp}")
        try:
            with self._lock:
                cursor = self.conn.execute(
                    """INSERT INTO logs 
                    (email, name, status, action_type, comment, timestamp, priority, session_id, status_start_time, status_end_time, reason, user_group) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        email.strip(), 
                        name.strip(), 
                        status, 
                        action_type, 
                        comment, 
                        timestamp, 
                        max(1, min(3, priority)),
                        session_id,
                        status_start_time,
                        status_end_time,
                        reason,
                        user_group
                    )
                )
                self.conn.commit()
                action_id = cursor.lastrowid
                logger.debug(f"Действие записано с id={action_id}")
                
                # --- КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Сразу синхронизируем, если immediate_sync=True ---
                if immediate_sync:
                    if self._try_immediate_sync(action_id):
                        logger.info(f"Немедленная синхронизация выполнена для id={action_id}")
                    else:
                        logger.warning(f"Не удалось немедленно синхронизировать id={action_id}")
                # ---
                
                logger.info(f"Действие записано с id={action_id}: {action_type} для {email}")
                return action_id
        except sqlite3.Error as e:
            if "Duplicate LOGOUT action" in str(e):
                logger.warning(f"Попытка дублирования записи LOGOUT для session_id={session_id}")
                return -1
            logger.error(f"Ошибка записи в лог: {e}")
            raise LocalDBError(f"Ошибка записи в лог: {e}")

    def _try_immediate_sync(self, record_id: int) -> bool:
        try:
            record = self.get_action_by_id(record_id)
            if not record:
                logger.warning(f"Невозможно немедленно синхронизировать: запись id={record_id} не найдена")
                return False
            self.mark_actions_synced([record_id])
            logger.info(f"Немедленная синхронизация выполнена для id={record_id}")
            return True
        except Exception as e:
            logger.debug(f"Ошибка немедленной синхронизации: {str(e)}")
            return False

    def _generate_session_id(self, email: str) -> str:
        sid = f"{email[:8]}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        logger.debug(f"Сгенерирован session_id: {sid}")
        return sid

    def get_action_by_id(self, action_id: int) -> Optional[Tuple]:
        try:
            with self._lock:
                cursor = self.conn.execute("SELECT * FROM logs WHERE id = ?", (action_id,))
                record = cursor.fetchone()
                logger.debug(f"Получение действия по id={action_id}: найдено={record is not None}")
                return record
        except sqlite3.Error as e:
            logger.error(f"Ошибка получения действия: {e}")
            raise LocalDBError(f"Ошибка получения действия: {e}")

    def get_unsynced_actions(self, limit: int = 100) -> List[Tuple]:
        try:
            with self._lock:
                cursor = self.conn.execute(
                    """SELECT id, email, name, status, action_type, comment, timestamp, session_id, status_start_time, status_end_time, reason, user_group
                    FROM logs 
                    WHERE synced = 0 
                    ORDER BY priority DESC, timestamp ASC 
                    LIMIT ?""",
                    (limit,)
                )
                records = cursor.fetchall()
                logger.debug(f"Получено несинхронизированных действий: {len(records)}")
                return records
        except sqlite3.Error as e:
            logger.error(f"Ошибка получения действий: {e}")
            raise LocalDBError(f"Ошибка получения действий: {e}")

    def mark_actions_synced(self, ids: List[int]):
        if not ids:
            logger.debug("mark_actions_synced вызван с пустым списком")
            return
        try:
            with self._lock:
                placeholders = ",".join(["?"] * len(ids))
                self.conn.execute(
                    f"""UPDATE logs 
                    SET synced = 1, 
                        sync_attempts = sync_attempts + 1, 
                        last_sync_attempt = ? 
                    WHERE id IN ({placeholders})""",
                    [datetime.now().isoformat()] + ids
                )
                self.conn.commit()
                logger.info(f"Отмечены как синхронизированные записи: {ids}")
        except sqlite3.Error as e:
            logger.error(f"Ошибка обновления статуса: {e}")
            raise LocalDBError(f"Ошибка обновления статуса: {e}")

    def check_existing_logout(self, email: str, session_id: Optional[str] = None) -> bool:
        logger.debug(f"Проверка существования LOGOUT для email={email}, session_id={session_id}")
        try:
            with self._lock:
                if session_id:
                    cursor = self.conn.execute(
                        "SELECT COUNT(*) FROM logs WHERE email = ? AND session_id = ? AND action_type = 'LOGOUT'",
                        (email, session_id)
                    )
                else:
                    cursor = self.conn.execute(
                        "SELECT COUNT(*) FROM logs WHERE email = ? AND action_type = 'LOGOUT'",
                        (email,)
                    )
                count = cursor.fetchone()[0]
                logger.debug(f"Найдено LOGOUT записей: {count}")
                return count > 0
        except sqlite3.Error as e:
            logger.error(f"Ошибка проверки LOGOUT: {e}")
            return False

    def get_last_unfinished_session(self, email: str) -> Optional[Dict]:
        logger.debug(f"Поиск незавершённой сессии для email={email}")
        try:
            with self._lock:
                cursor = self.conn.execute(
                    """
                    SELECT session_id, timestamp FROM logs
                    WHERE email=? AND action_type='LOGIN'
                    AND session_id NOT IN (
                        SELECT session_id FROM logs WHERE email=? AND action_type='LOGOUT'
                    )
                    ORDER BY timestamp DESC LIMIT 1
                    """,
                    (email, email)
                )
                row = cursor.fetchone()
                if row:
                    logger.debug(f"Найдена незавершённая сессия: {row[0]}, {row[1]}")
                    return {"session_id": row[0], "timestamp": row[1]}
                logger.debug("Незавершённых сессий не найдено")
                return None
        except Exception as e:
            logger.error(f"Ошибка поиска незавершённой сессии: {e}")
            return None

    def get_active_session(self, email: str) -> Optional[Dict]:
        return self.get_last_unfinished_session(email)

    def get_current_user_email(self) -> Optional[str]:
        """Получить email текущего активного пользователя."""
        try:
            with self._lock:
                cursor = self.conn.execute("""SELECT email FROM logs
                                              WHERE status_end_time IS NULL
                                              AND action_type IN ('LOGIN', 'STATUS_CHANGE')
                                              ORDER BY id DESC
                                              LIMIT 1""")
                row = cursor.fetchone()
                if row:
                    return row[0]
                return None
        except sqlite3.Error as e:
            logger.error(f"Ошибка получения текущего пользователя: {e}")
            return None

    def get_logout_history(self, email: str, limit: int = 20) -> List[Tuple]:
        """Получить истории разлогиниваний пользователя"""
        try:
            with self._lock:
                cursor = self.conn.execute(
                    "SELECT * FROM logs WHERE email=? AND action_type='LOGOUT' ORDER BY timestamp DESC LIMIT ?",
                    (email, limit)
                )
                return cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Ошибка получения истории разлогиниваний: {e}")
            return []

    def close(self):
        if self.conn:
            try:
                with self._lock:
                    self.conn.close()
                logger.info("Соединение с локальной БД закрыто")
            except sqlite3.Error as e:
                logger.error(f"Ошибка при закрытии соединения: {e}")
            finally:
                self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

# Ниже — обёртки для удобного импорта из модуля

_db_instance = LocalDB()

def get_active_session(email: str) -> Optional[Dict]:
    return _db_instance.get_active_session(email)

def get_last_unfinished_session(email: str) -> Optional[Dict]:
    return _db_instance.get_last_unfinished_session(email)

def get_current_user_email() -> Optional[str]:
    return _db_instance.get_current_user_email()