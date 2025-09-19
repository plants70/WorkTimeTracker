from __future__ import annotations

import logging
import signal
import socket
import sys
import time
from datetime import datetime
from pathlib import Path
from threading import Event, Lock, RLock, Thread
from time import monotonic
from typing import Callable, Dict, List, Optional

from telemetry import trace_time

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from PyQt5.QtCore import QObject, pyqtSignal
except ImportError:
    logging.warning(
        "PyQt5 не найден. Сигналы GUI не будут работать. Запуск в режиме CLI."
    )

    class QObject:
        """Заглушка QObject для headless-режима."""

        pass

    class pyqtSignal:
        """Минимальная заглушка pyqtSignal."""

        def __init__(self):
            pass

        def emit(self, *args, **kwargs):
            pass


try:
    from config import (
        API_MAX_RETRIES,
        SYNC_BATCH_SIZE,
        SYNC_INTERVAL,
        SYNC_INTERVAL_OFFLINE_RECOVERY,
        SYNC_INTERVAL_ONLINE,
        SYNC_RETRY_STRATEGY,
    )
    from consts import STATUS_ACTIVE, normalize_session_status
    from sheets_api import SheetsAPIError, get_sheets_api
    from user_app.db_local import LocalDB
    from user_app.signals import SessionSignals

    # сохраняем прежнее имя переменной для кода ниже
    sheets_api = get_sheets_api()
    from sync.network import is_internet_available
    from sync.session_inspector import SessionInspector
except ImportError as e:
    logging.error(f"Ошибка импорта модулей: {e}")
    raise

# Пулинг персональных правила перенесён в notifications.engine; безопасный импорт с фолбэком
try:
    from notifications.engine import poll_long_running_remote
except Exception:

    def poll_long_running_remote():
        return


# Персональные правила теперь обрабатываются через движок уведомлений, прямой импорт не нужен.

logger = logging.getLogger(__name__)
PING_PORT = 43333
PING_TIMEOUT = 3600  # 1 час
HEARTBEAT_INTERVAL = 300  # 5 минут


class SyncSignals(QObject):
    force_logout = pyqtSignal()
    sync_status_updated = pyqtSignal(dict)


class SyncManager(QObject):
    def __init__(
        self,
        signals: Optional[SyncSignals] = None,
        background_mode: bool = True,
        session_signals: Optional[SessionSignals] = None,
        *,
        db: LocalDB | None = None,
        remote_force_logout_callback: Callable[[str], None] | None = None,
    ):
        super().__init__()
        logger.info(f"Инициализация SyncManager: background_mode={background_mode}")
        self._db = db or LocalDB()
        self._db_lock = RLock()
        self._stop_event = Event()
        self.signals = signals
        self._session_signals = session_signals
        self._background_mode = background_mode
        self._sync_interval = SYNC_INTERVAL if background_mode else 0
        self._last_sync_time = None
        self._is_offline_recovery = False  # Флаг для режима восстановления
        self._stats = {
            "total_synced": 0,
            "last_sync": None,
            "last_duration": 0,
            "success_rate": 1.0,
            "queue_size": 0,
        }
        self._last_ping = time.time()
        self._last_heartbeat = time.time()
        self._last_loop_started = monotonic()
        self._tick_lock = Lock()  # Защита от перекрытия циклов синхронизации
        self._remote_callback = remote_force_logout_callback
        self._remote_emitted = False
        if background_mode:
            self._ping_thread = Thread(target=self._ping_listener, daemon=True)
            self._ping_thread.start()
            logger.debug("Ping listener поток запущен")
        # Централизованный «санитар» для ActiveSessions
        self._inspector = SessionInspector(self._db)

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
            status = self._check_user_session_status(email, session_id)
            logger.info(
                "Проверка статуса сессии для пользователя %s, session_id=%s -> %s",
                email,
                session_id,
                status or "<unknown>",
            )

            if status and status != STATUS_ACTIVE:
                self._emit_remote_logout(email, session_id, status)
            else:
                logger.debug(f"Статус сессии в норме: {status}")

        except Exception as e:
            logger.error(
                f"Ошибка при проверке удаленных команд для {email}: {e}", exc_info=True
            )

    def _emit_remote_logout(self, email: str, session_id: str, status: str) -> None:
        if self._remote_emitted:
            return
        self._remote_emitted = True

        logger.info(
            "[ADMIN_LOGOUT] Обнаружен статус '%s' для пользователя %s. Запускаем завершение сессии.",
            status,
            email,
        )

        handled = False
        if self._remote_callback:
            try:
                self._remote_callback("remote_force_logout")
                handled = True
            except Exception as exc:
                logger.debug("remote logout callback failed: %s", exc)

        if not handled:
            if self._session_signals:
                try:
                    self._session_signals.sessionFinished.emit("remote_force_logout")
                except Exception as exc:
                    logger.debug("sessionFinished emit failed: %s", exc)
            elif self.signals:
                try:
                    self.signals.force_logout.emit()
                except Exception as exc:
                    logger.debug("force_logout emit failed: %s", exc)

        self._ack_remote_command(email, session_id)

    def _ack_remote_command(self, email: str | None, session_id: str | None) -> None:
        if not hasattr(sheets_api, "ack_remote_command"):
            return

        email_value = (email or "").strip()
        session_value = str(session_id or "").strip()
        if not email_value or not session_value:
            return

        try:
            ok = sheets_api.ack_remote_command(
                email=email_value, session_id=session_value
            )
            logger.info(
                "Remote command ACK (sync manager) email=%s session=%s -> %s",
                email_value,
                session_value,
                ok,
            )
        except Exception as exc:
            logger.warning(
                "Failed to ACK remote command (email=%s, session=%s): %s",
                email_value,
                session_value,
                exc,
            )

    def _check_user_session_status(self, email: str, session_id: str) -> str | None:
        """
        Возвращает нормализованный статус из ActiveSessions:
        'В работе' | 'LOGOUT' | 'FORCE_LOGOUT'
        """
        try:
            with trace_time("check_user_session_status"):
                status = sheets_api.check_user_session_status(email, session_id)
            return normalize_session_status(status)
        except Exception as e:
            logger.error(f"Ошибка при проверке статуса сессии: {e}")
            return None

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
                    batch[email].append(
                        {
                            "id": action[0],
                            "email": action[1],
                            "name": action[2],
                            "status": action[3],
                            "action_type": action[4],
                            "comment": action[5],
                            "timestamp": action[6],
                            "session_id": action[7],
                            "status_start_time": action[8],
                            "status_end_time": action[9],
                            "reason": action[10],  # NEW
                            "user_group": action[11],  # NEW
                        }
                    )

                logger.info(
                    f"Подготовлен пакет для {len(batch)} пользователей, всего действий: {sum(len(actions) for actions in batch.values())}"
                )
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

        logger.info(
            f"Начало синхронизации пакета из {total_actions} действий для {len(batch)} пользователей"
        )

        for email, actions in batch.items():
            logger.debug(
                f"Синхронизация для пользователя {email}: {len(actions)} действий"
            )

            for attempt in range(API_MAX_RETRIES):
                try:
                    logger.debug(
                        f"Попытка {attempt + 1}/{API_MAX_RETRIES} для пользователя {email}"
                    )

                    if not is_internet_available():
                        logger.warning("Интернет недоступен, пропускаем синхронизацию.")
                        return False

                    # Получаем группу пользователя из листа Users
                    user = sheets_api.get_user_by_email(email)
                    user_group = user.get("group") if user else None

                    # Готовим список словарей для отправки
                    actions_payload = []
                    filtered = []
                    for a in actions:
                        # анти-дубли: перепроверяем, не помечена ли уже запись синхронизированной
                        if not self._db.is_unsynced(a["id"]):
                            logger.info(
                                f"Пропуск действия id={a['id']} — уже синхронизировано другим потоком"
                            )
                            continue
                        actions_payload.append(
                            {
                                "id": a["id"],
                                "session_id": a["session_id"],
                                "email": a["email"],
                                "name": a["name"],
                                "status": a["status"],
                                "action_type": a["action_type"],
                                "comment": a["comment"],
                                "timestamp": a["timestamp"],
                                "status_start_time": a["status_start_time"],
                                "status_end_time": a["status_end_time"],
                                "reason": a.get("reason"),
                            }
                        )
                        filtered.append(a["id"])

                    if not actions_payload:
                        logger.info(
                            f"Для {email} нет действий после фильтрации — пропуск отправки"
                        )
                        break

                    success = True
                    for payload in actions_payload:
                        try:
                            sheets_api.log_user_actions(
                                email=payload["email"],
                                action=payload.get("action_type", ""),
                                status=payload.get("status", ""),
                                group=user_group,
                                timestamp_utc=payload.get("timestamp"),
                                start_utc=payload.get("status_start_time"),
                                end_utc=payload.get("status_end_time"),
                                session_id=payload.get("session_id"),
                                group_at_start=user_group,
                            )
                        except SheetsAPIError as exc:
                            logger.warning(
                                "Не удалось синхронизировать действие id=%s для %s: %s",
                                payload.get("id"),
                                email,
                                exc,
                            )
                            success = False
                            break

                    if success:
                        success_count += len(actions_payload)
                        synced_ids.extend(filtered)
                        logger.info(
                            f"Успешно синхронизировано {len(actions_payload)} действий для {email}"
                        )
                        break
                    else:
                        logger.warning(
                            f"Не удалось синхронизировать действия для {email}, попытка {attempt + 1}"
                        )

                except Exception as e:
                    logger.error(
                        f"Ошибка синхронизации для {email} (попытка {attempt + 1}): {e}",
                        exc_info=True,
                    )
                    # различаем: сеть vs. прочие ошибки API
                    if not is_internet_available():
                        logger.warning(
                            "Нет интернета. Данные будут отправлены при восстановлении соединения"
                        )
                    else:
                        logger.warning("Ошибка синхронизации. Повторим позже")

                if attempt < API_MAX_RETRIES - 1:
                    delay = SYNC_RETRY_STRATEGY[
                        min(attempt, len(SYNC_RETRY_STRATEGY) - 1)
                    ]
                    logger.info(f"Повторная попытка через {delay} сек...")
                    time.sleep(delay)

        if synced_ids:
            with self._db_lock:
                try:
                    logger.debug(
                        f"Помечаем как синхронизированные {len(synced_ids)} записей"
                    )
                    self._db.mark_actions_synced(synced_ids)
                    logger.info(
                        f"Успешно синхронизировано и отмечено {len(synced_ids)} записей."
                    )
                except Exception as e:
                    logger.error(
                        f"Ошибка обновления статуса записей в локальной БД: {e}",
                        exc_info=True,
                    )

        duration = time.time() - start_time
        logger.info(
            f"Синхронизация завершена за {duration:.2f} сек. Успешно: {success_count}/{total_actions}"
        )

        self._update_stats(success_count, total_actions, duration)
        return success_count == total_actions

    def _send_heartbeat(self):
        """Отправка heartbeat для активной сессии"""
        try:
            with self._db_lock:
                email = self._db.get_current_user_email()
                if not email:
                    logger.debug("Нет активного пользователя для heartbeat")
                    return

                session = self._db.get_active_session(email)
                if not session:
                    logger.debug(f"Нет активной сессии для пользователя {email}")
                    return

                session_id = session["session_id"]

            # Проверяем, есть ли интернет
            if not is_internet_available():
                logger.debug("Интернет недоступен, пропускаем heartbeat")
                return

            # Отправляем heartbeat
            if hasattr(sheets_api, "update_heartbeat"):
                logger.debug(
                    f"Отправка heartbeat для {email}, session_id: {session_id}"
                )
                sheets_api.update_heartbeat(email, session_id)
                logger.info(f"Heartbeat отправлен для {email}")
            else:
                logger.debug("Метод update_heartbeat не доступен в sheets_api")

        except Exception as e:
            logger.error(f"Ошибка отправки heartbeat: {e}", exc_info=True)

    def _update_stats(self, success_count: int, total_actions: int, duration: float):
        logger.debug(
            f"Обновление статистики: success={success_count}, total={total_actions}, duration={duration:.2f}"
        )
        with self._db_lock:
            self._stats["total_synced"] += success_count
            self._stats["last_sync"] = datetime.now().isoformat(timespec="seconds")
            self._stats["last_duration"] = round(duration, 3)
            if total_actions > 0:
                rate = success_count / total_actions
                self._stats["success_rate"] = (
                    0.9 * self._stats["success_rate"] + 0.1 * rate
                )
            self._stats["queue_size"] = self._db.get_unsynced_count()

        logger.debug(f"Обновленная статистика: {self._stats}")
        if self.signals:
            self.signals.sync_status_updated.emit(self._stats.copy())
            logger.debug("Сигнал sync_status_updated отправлен")

    def sync_once(self) -> bool:
        logger.info("=== ЗАПУСК РАЗОВОЙ СИНХРОНИЗАЦИИ ===")
        start = time.time()
        ok = False
        try:
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
                logger.info(
                    f"Обнаружено большое количество действий ({total_actions}). Активирован режим восстановления."
                )

            ok = self._sync_batch(batch)
            logger.info(
                f"Результат разовой синхронизации: {'УСПЕХ' if ok else 'НЕУДАЧА'}"
            )
        finally:
            elapsed = time.time() - start
            self._stats["last_sync"] = datetime.now().isoformat(timespec="seconds")
            self._stats["last_duration"] = round(elapsed, 3)
            self._stats["queue_size"] = self._db.get_unsynced_count()
            if ok:
                self._stats["total_synced"] += 1
            if self.signals:
                self.signals.sync_status_updated.emit(dict(self._stats))
        return ok

    def _sync_cycle(self):
        """Один цикл синхронизации с защитой от перекрытия"""
        # Не пускаем второй тик, пока идёт текущий
        if not self._tick_lock.acquire(blocking=False):
            logger.debug("Skip sync tick: previous tick still running")
            return

        try:
            logger.debug("=== НАЧАЛО ЦИКЛА СИНХРОНИЗАЦИИ ===")

            now = time.time()
            if (now - self._last_ping) > PING_TIMEOUT:
                logger.warning("Ping не получен более часа — завершаем работу сервиса.")
                self._stop_event.set()
                return

            # Проверяем и отправляем heartbeat если нужно
            if (now - self._last_heartbeat) > HEARTBEAT_INTERVAL:
                self._send_heartbeat()
                self._last_heartbeat = now

            start_time = time.time()

            # Проверяем, есть ли интернет
            internet_available = is_internet_available()
            logger.debug(f"Доступность интернета: {internet_available}")

            if internet_available:
                # Если интернет есть, проверяем, в каком режиме мы находимся
                if self._is_offline_recovery:
                    # Если мы в режиме восстановления, проверяем, сколько записей осталось
                    queue_size = self._db.get_unsynced_count()
                    logger.debug(f"Режим восстановления. Размер очереди: {queue_size}")

                    if (
                        queue_size < 50
                    ):  # Если осталось меньше 50 записей, считаем, что восстановление завершено
                        self._is_offline_recovery = False
                        self._sync_interval = (
                            SYNC_INTERVAL  # Возвращаемся к нормальному интервалу
                        )
                        logger.info(
                            "Режим восстановления завершен. Возвращаемся к нормальному интервалу синхронизации."
                        )
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

            # Централизованный контроль ActiveSessions (онлайн-подчистка зависших active)
            try:
                self._inspector.tick()
            except Exception as e:
                logger.error(f"SessionInspector tick failed: {e}")

            # Проверяем персональные правила (если есть интернет)
            if internet_available:
                try:
                    poll_long_running_remote()
                except Exception as e:
                    logger.error(f"Ошибка при проверке персональных правил: {e}")

            duration = time.time() - start_time
            logger.debug(f"=== ЦИКЛ СИНХРОНИЗАЦИИ ЗАВЕРШЕН за {duration:.2f} сек ===")

        except Exception as e:
            logger.error(
                f"Критическая ошибка в цикле синхронизации: {e}", exc_info=True
            )
        finally:
            self._tick_lock.release()

    def run(self):
        logger.info("=== ЗАПУСК СЕРВИСА СИНХРОНИЗАЦИИ ===")
        self._stop_event.clear()
        self._last_loop_started = monotonic()

        while not self._stop_event.is_set():
            try:
                self._sync_cycle()
            except Exception as e:
                logger.error(
                    f"Непредвиденная ошибка в главном цикле: {e}", exc_info=True
                )

            # Ждем до следующего цикла
            if not self._stop_event.is_set():
                logger.debug(
                    f"Ожидание {self._sync_interval} сек до следующего цикла..."
                )
                self._stop_event.wait(self._sync_interval)

    def stop(self):
        logger.info("=== ОСТАНОВКА СЕРВИСА СИНХРОНИЗАЦИИ ===")
        self._stop_event.set()

    def get_stats(self) -> dict:
        with self._db_lock:
            return self._stats.copy()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                PROJECT_ROOT / "logs" / "sync_service.log", encoding="utf-8"
            ),
        ],
    )

    logger.info("=== ЗАПУСК СЕРВИСА СИНХРОНИЗАЦИИ В КОНСОЛЬНОМ РЕЖИМЕ ===")

    manager = SyncManager(background_mode=True)

    def signal_handler(sig, frame):
        logger.info("Получен сигнал завершения, останавливаем сервис...")
        manager.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        manager.run()
    except KeyboardInterrupt:
        logger.info("Прервано пользователем")
        manager.stop()
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        manager.stop()


if __name__ == "__main__":
    main()
