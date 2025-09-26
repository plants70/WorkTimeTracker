import sys
import logging
import time
import signal
from datetime import datetime
from threading import Event, RLock, Thread
from pathlib import Path
from typing import Dict, List, Optional
import socket

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from PyQt5.QtCore import QObject, pyqtSignal
except ImportError:
    logging.warning("PyQt5 не найден. Сигналы GUI не будут работать. Запуск в режиме CLI.")
    class QObject: pass
    class pyqtSignal:
        def __init__(self): pass
        def emit(self, *args, **kwargs): pass

try:
    from config import (
        SYNC_INTERVAL,
        API_MAX_RETRIES,
        SYNC_BATCH_SIZE,
        SYNC_RETRY_STRATEGY,
        SYNC_INTERVAL_ONLINE,
        SYNC_INTERVAL_OFFLINE_RECOVERY
    )
    from user_app.db_local import LocalDB
    from sheets_api import sheets_api
    from sync.network import is_internet_available
except ImportError as e:
    logging.error(f"Ошибка импорта модулей: {e}")
    raise

logger = logging.getLogger(__name__)

PING_PORT = 43333
PING_TIMEOUT = 3600  # 1 час

class SyncSignals(QObject):
    force_logout = pyqtSignal()
    sync_status_updated = pyqtSignal(dict)

class SyncManager(QObject):
    def __init__(self, signals: Optional[SyncSignals] = None, background_mode: bool = True):
        super().__init__()
        logger.info(f"Инициализация SyncManager: background_mode={background_mode}")
        self._db = LocalDB()
        self._db_lock = RLock()
        self._stop_event = Event()
        self.signals = signals
        self._background_mode = background_mode
        self._sync_interval = SYNC_INTERVAL if background_mode else 0
        self._last_sync_time = None
        self._is_offline_recovery = False  # Флаг для режима восстановления
        self._stats = {
            'total_synced': 0,
            'last_sync': None,
            'last_duration': 0,
            'success_rate': 1.0,
            'queue_size': 0
        }
        self._last_ping = time.time()
        if background_mode:
            self._ping_thread = Thread(target=self._ping_listener, daemon=True)
            self._ping_thread.start()
            logger.debug("Ping listener поток запущен")

    def _check_remote_commands(self):
        logger.info("=== ПРОВЕРКА КОМАНД ===")
        if not is_internet_available():
            logger.debug("Проверка удаленных команд невозможна: нет интернета.")
            return

        with self._db_lock:
            email = self._db.get_current_user_email()
            logger.debug(f"Текущий email пользователя: {email}")
            session = self._db.get_active_session(email) if email else None
            session_id = session["session_id"] if session else None
            logger.debug(f"Активная сессия: session_id={session_id}")

        if not email or not session_id:
            logger.debug("Нет активной сессии для проверки удаленных команд.")
            return

        try:
            logger.info(f"Проверка статуса сессии для пользователя {email}, session_id: {session_id}")
            remote_status = self._check_user_session_status(email, session_id)
            logger.debug(f"Получен удаленный статус: {remote_status}")
            
            if remote_status == "kicked":
                logger.info(f"[ADMIN_LOGOUT] Обнаружен статус 'kicked' для пользователя {email}. Испускаем force_logout.")
                if self.signals:
                    self.signals.force_logout.emit()
                return
            elif remote_status == "finished":
                logger.warning(f"Получена команда 'finished' для пользователя {email}. Отправка сигнала в GUI.")
                if self.signals:
                    logger.info("Emit force_logout signal to GUI")
                    self.signals.force_logout.emit()
                # НЕ вызываем self.stop() здесь!
            else:
                logger.debug(f"Статус сессии в норме: {remote_status}")
                
        except Exception as e:
            logger.error(f"Ошибка при проверке удаленных команд для {email}: {e}", exc_info=True)

    def _check_user_session_status(self, email: str, session_id: str) -> str:
        """
        Проверяет статус указанной сессии пользователя в Google Sheets.
        Возвращает: 'active', 'kicked', 'finished', 'expired', 'unknown'
        """
        try:
            return sheets_api.check_user_session_status(email, session_id)
        except Exception as e:
            logger.error(f"Ошибка при проверке статуса сессии: {e}")
            return "unknown"

    def _ping_listener(self):
        logger.info(f"Запуск ping listener на UDP порту {PING_PORT}")
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(("127.0.0.1", PING_PORT))
        s.settimeout(2)
        logger.info(f"Ping listener запущен на UDP порту {PING_PORT}")
        while not self._stop_event.is_set():
            try:
                data, addr = s.recvfrom(1024)
                logger.debug(f"Получен UDP пакет от {addr}: {data}")
                if data == b"ping":
                    self._last_ping = time.time()
                    logger.debug("Получен ping, обновлено время последнего ping")
            except socket.timeout:
                continue
            except Exception as e:
                logger.warning(f"Ошибка в ping listener: {e}", exc_info=True)
        s.close()
        logger.info("Ping listener завершен")

    def _prepare_batch(self) -> Optional[Dict[str, List[Dict]]]:
        logger.debug("Подготовка пакета данных для синхронизации")
        with self._db_lock:
            try:
                unsynced = self._db.get_unsynced_actions(SYNC_BATCH_SIZE)
                logger.debug(f"Найдено {len(unsynced)} несинхронизированных действий")
                
                if not unsynced:
                    logger.debug("Нет данных для подготовки пакета")
                    return None
                
                batch = {}
                for action in unsynced:
                    email = action[1]
                    if email not in batch:
                        batch[email] = []
                    batch[email].append({
                        'id': action[0],
                        'email': action[1],
                        'name': action[2],
                        'status': action[3],
                        'action_type': action[4],
                        'comment': action[5],
                        'timestamp': action[6],
                        'session_id': action[7],
                        'status_start_time': action[8],
                        'status_end_time': action[9],
                        'reason': action[10],        # NEW
                        'user_group': action[11],    # NEW
                    })
                
                logger.info(f"Подготовлен пакет для {len(batch)} пользователей, всего действий: {sum(len(actions) for actions in batch.values())}")
                return batch
                
            except Exception as e:
                logger.error(f"Ошибка подготовки пакета: {e}", exc_info=True)
                return None

    def _sync_batch(self, batch: Dict[str, List[Dict]]) -> bool:
        if not batch:
            logger.debug("Пустой пакет, пропускаем синхронизацию")
            return True
            
        start_time = time.time()
        total_actions = sum(len(actions) for actions in batch.values())
        success_count = 0
        synced_ids = []
        
        logger.info(f"Начало синхронизации пакета из {total_actions} действий для {len(batch)} пользователей")
        
        for email, actions in batch.items():
            logger.debug(f"Синхронизация для пользователя {email}: {len(actions)} действий")
            
            for attempt in range(API_MAX_RETRIES):
                try:
                    logger.debug(f"Попытка {attempt + 1}/{API_MAX_RETRIES} для пользователя {email}")
                    
                    if not is_internet_available():
                        logger.warning("Интернет недоступен, пропускаем синхронизацию.")
                        return False
                    
                    # Готовим список словарей — то, что ждёт sheets_api.log_user_actions
                    actions_payload = []
                    for a in actions:
                        actions_payload.append({
                            "session_id": a['session_id'],
                            "email": a['email'],
                            "name": a['name'],
                            "status": a['status'],
                            "action_type": a['action_type'],
                            "comment": a['comment'],
                            "timestamp": a['timestamp'],
                            "status_start_time": a['status_start_time'],
                            "status_end_time": a['status_end_time'],
                            "reason": a.get('reason'),
                        })

                    user_group = actions[0].get('user_group')  # пробуем передать группу сразу
                    if sheets_api.log_user_actions(actions_payload, email, user_group=user_group):
                        success_count += len(actions)
                        synced_ids.extend([a['id'] for a in actions])
                        logger.info(f"Успешно синхронизировано {len(actions)} действий для {email}")
                        break
                    else:
                        logger.warning(f"Не удалось синхронизировать действия для {email}, попытка {attempt + 1}")
                        
                except Exception as e:
                    logger.error(f"Ошибка синхронизации для {email} (попытка {attempt + 1}): {e}", exc_info=True)
                
                if attempt < API_MAX_RETRIES - 1:
                    delay = SYNC_RETRY_STRATEGY[min(attempt, len(SYNC_RETRY_STRATEGY) - 1)]
                    logger.info(f"Повторная попытка через {delay} сек...")
                    time.sleep(delay)
        
        if synced_ids:
            with self._db_lock:
                try:
                    logger.debug(f"Помечаем как синхронизированные {len(synced_ids)} записей")
                    self._db.mark_actions_synced(synced_ids)
                    logger.info(f"Успешно синхронизировано и отмечено {len(synced_ids)} записей.")
                except Exception as e:
                    logger.error(f"Ошибка обновления статуса записей в локальной БД: {e}", exc_info=True)
        
        duration = time.time() - start_time
        logger.info(f"Синхронизация завершена за {duration:.2f} сек. Успешно: {success_count}/{total_actions}")
        
        self._update_stats(success_count, total_actions, duration)
        return success_count == total_actions

    def _update_stats(self, success_count: int, total_actions: int, duration: float):
        logger.debug(f"Обновление статистики: success={success_count}, total={total_actions}, duration={duration:.2f}")
        with self._db_lock:
            self._stats['total_synced'] += success_count
            self._stats['last_sync'] = datetime.now().isoformat()
            self._stats['last_duration'] = duration
            if total_actions > 0:
                rate = success_count / total_actions
                self._stats['success_rate'] = 0.9 * self._stats['success_rate'] + 0.1 * rate
            self._stats['queue_size'] = self._db.get_unsynced_count()
            
        logger.debug(f"Обновленная статистика: {self._stats}")
        if self.signals:
            self.signals.sync_status_updated.emit(self._stats.copy())
            logger.debug("Сигнал sync_status_updated отправлен")

    def sync_once(self) -> bool:
        logger.info("=== ЗАПУСК РАЗОВОЙ СИНХРОНИЗАЦИИ ===")
        batch = self._prepare_batch()
        if not batch:
            logger.debug("Нет данных для синхронизации.")
            return True

        total_actions = sum(len(actions) for actions in batch.values())
        logger.info(f"Начало синхронизации пакета из {total_actions} записей.")

        # Если очередь очень большая, активируем режим восстановления
        if total_actions > 100 and not self._is_offline_recovery:
            self._is_offline_recovery = True
            self._sync_interval = SYNC_INTERVAL_OFFLINE_RECOVERY
            logger.info(f"Обнаружено большое количество действий ({total_actions}). Активирован режим восстановления.")

        result = self._sync_batch(batch)
        logger.info(f"Результат разовой синхронизации: {'УСПЕХ' if result else 'НЕУДАЧА'}")
        return result

    def run_service(self):
        logger.info(f"Сервис синхронизации запущен. Интервал: {self._sync_interval} сек.")
        cycle_count = 0
        
        while not self._stop_event.is_set():
            cycle_count += 1
            logger.debug(f"=== ЦИКЛ СИНХРОНИЗАЦИИ #{cycle_count} ===")
            
            now = time.time()
            if (now - self._last_ping) > PING_TIMEOUT:
                logger.warning("Ping не получен более часа — завершаем работу сервиса.")
                break
            
            start_time = time.time()
            try:
                # Проверяем, есть ли интернет
                internet_available = is_internet_available()
                logger.debug(f"Доступность интернета: {internet_available}")
                
                if internet_available:
                    # Если интернет есть, проверяем, в каком режиме мы находимся
                    if self._is_offline_recovery:
                        # Если мы в режиме восстановления, проверяем, сколько записей осталось
                        queue_size = self._db.get_unsynced_count()
                        logger.debug(f"Режим восстановления. Размер очереди: {queue_size}")
                        
                        if queue_size < 50:  # Если осталось меньше 50 записей, считаем, что восстановление завершено
                            self._is_offline_recovery = False
                            self._sync_interval = SYNC_INTERVAL  # Возвращаемся к нормальному интервалу
                            logger.info("Режим восстановления завершен. Возвращаемся к нормальному интервалу синхронизации.")
                        else:
                            self._sync_interval = SYNC_INTERVAL_OFFLINE_RECOVERY
                    else:
                        # Нормальный режим
                        self._sync_interval = SYNC_INTERVAL_ONLINE
                else:
                    # Нет интернета — используем минимальный интервал для быстрого обнаружения его появления
                    self._sync_interval = 10
                    logger.debug("Нет интернета, установлен интервал 10 сек")

                logger.debug(f"Текущий интервал синхронизации: {self._sync_interval} сек")
                
                # Выполняем синхронизацию
                self.sync_once()
                self._check_remote_commands()
                
            except Exception as e:
                logger.critical(f"Критическая ошибка в цикле синхронизации: {e}", exc_info=True)
            
            elapsed = time.time() - start_time
            sleep_time = max(1, self._sync_interval - elapsed)
            logger.debug(f"Цикл завершен за {elapsed:.2f} сек. Ожидание {sleep_time:.2f} сек")
            
            self._stop_event.wait(sleep_time)

        logger.info("Сервис синхронизации завершён.")

    def stop(self):
        logger.info("Остановка SyncManager...")
        self._stop_event.set()
        try:
            self._db.close()
            logger.debug("База данных закрыта")
        except Exception as e:
            logger.error(f"Ошибка при закрытии БД: {e}", exc_info=True)
        logger.info("Сервис синхронизации остановлен.")

def configure_logging(background_mode: bool):
    log_file = 'auto_sync.log' if background_mode else None
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
    
    # Увеличиваем уровень логирования до DEBUG для более детальной информации
    logging.basicConfig(
        level=logging.DEBUG,  # Изменено с INFO на DEBUG
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    
    # Для некоторых библиотеки устанавливаем более высокий уровень, чтобы избежать слишком много логов
    logging.getLogger('urllib3').setLevel(logging.INFO)
    logging.getLogger('googleapiclient').setLevel(logging.INFO)

def handle_shutdown(signum, frame):
    logger.info("Получен сигнал завершения работы (SIGTERM/SIGINT)")
    raise SystemExit("Завершение по сигналу.")

def main(background_mode: bool = True):
    configure_logging(background_mode)
    manager = None
    try:
        signal.signal(signal.SIGINT, handle_shutdown)
        signal.signal(signal.SIGTERM, handle_shutdown)

        demo_signals = SyncSignals()
        def on_force_logout():
            logger.info("--- Демонстрация: получен сигнал force_logout! Приложение должно выйти. ---")
        demo_signals.force_logout.connect(on_force_logout)

        manager = SyncManager(signals=demo_signals, background_mode=background_mode)

        if background_mode:
            logger.info("Запуск в режиме сервиса (демо)")
            manager.run_service()
        else:
            logger.info("Выполнение разовой синхронизации (демо)")
            manager.sync_once()
            manager._check_remote_commands()

    except SystemExit as e:
        logger.info(f"Завершение работы: {e}")
    except Exception as e:
        logger.critical(f"Фатальная ошибка в main: {e}", exc_info=True)
    finally:
        if manager:
            manager.stop()

if __name__ == "__main__":
    main(background_mode=True)