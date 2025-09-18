import logging
import json
from pathlib import Path
from datetime import datetime, timedelta
from threading import Lock
from typing import List, Dict, Optional
import uuid
from config import MAX_COMMENT_LENGTH

logger = logging.getLogger(__name__)

class SyncQueue:
    """
    Очередь для хранения несинхронизированных действий с поддержкой:
    - Приоритезации запросов
    - Экспоненциального backoff
    - Группировки по пользователям
    - Сохранения состояния в файл
    """

    def __init__(self, queue_file: Path = Path("sync_queue.json")):
        self.queue_file = queue_file
        self.lock = Lock()
        logger.debug(f"Инициализация SyncQueue с файлом {self.queue_file}")
        self._load_queue()

    def _load_queue(self):
        """Загружает очередь из файла (если есть)"""
        try:
            if self.queue_file.exists():
                with open(self.queue_file, "r", encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self.queue = data
                        logger.info(f"Очередь загружена из {self.queue_file} с {len(self.queue)} записями")
                    else:
                        self.queue = []
                        logger.warning("Неверный формат файла очереди, инициализация пустой очереди")
            else:
                self.queue = []
                logger.info("Файл очереди не найден, инициализация пустой очереди")
        except Exception as e:
            logger.error(f"Ошибка загрузки очереди: {e}")
            self.queue = []
            self._save_queue()

    def _save_queue(self):
        """Сохраняет очередь в файл"""
        try:
            with self.lock:
                with open(self.queue_file, "w", encoding='utf-8') as f:
                    json.dump(self.queue, f, ensure_ascii=False, indent=2, default=str)
            logger.debug("Очередь сохранена в файл")
        except Exception as e:
            logger.error(f"Ошибка сохранения очереди: {e}")

    def add_actions(self, actions: List[Dict]):
        """
        Добавляет действия в очередь
        Args:
            actions: Список словарей с действиями:
                {
                    'email': str,
                    'name': str,
                    'status': str,
                    'action_type': str,
                    'comment': str,
                    'timestamp': str (ISO format)
                }
        """
        if not actions:
            logger.debug("add_actions вызван с пустым списком")
            return

        with self.lock:
            for action in actions:
                # Генерируем уникальный ID для действия
                action_id = str(uuid.uuid4())
                
                # Проверяем и обрезаем комментарий
                comment = action.get('comment', '')
                if len(comment) > MAX_COMMENT_LENGTH:
                    comment = comment[:MAX_COMMENT_LENGTH]
                    logger.warning(f"Обрезан комментарий для действия {action_id}")

                # Определяем приоритет
                priority = self._determine_priority(action['action_type'])

                self.queue.append({
                    'id': action_id,
                    'email': action['email'],
                    'name': action['name'],
                    'status': action['status'],
                    'action_type': action['action_type'],
                    'comment': comment,
                    'timestamp': action['timestamp'],
                    'next_retry': datetime.now().isoformat(),
                    'retry_count': 0,
                    'priority': priority,
                    'last_attempt': None,
                    'attempts': []
                })
                logger.info(f"Добавлено действие в очередь: id={action_id}, action_type={action['action_type']}, email={action['email']}")
            self._save_queue()

    def _determine_priority(self, action_type: str) -> int:
        """Определяет приоритет действия"""
        priority_map = {
            'LOGIN': 3,      # Высокий приоритет для входов
            'LOGOUT': 3,     # Высокий приоритет для выходов
            'STATUS_CHANGE': 1  # Обычный приоритет для смен статусов
        }
        return priority_map.get(action_type, 1)

    def get_pending_actions(self, limit: int = 50) -> List[Dict]:
        """
        Возвращает готовые к отправке действия с учетом:
        - Времени следующей попытки
        - Приоритета
        - Даты создания
        """
        with self.lock:
            now = datetime.now()
            ready_actions = [
                a for a in self.queue
                if datetime.fromisoformat(a['next_retry']) <= now
            ]

            # Сортируем по приоритету (по убыванию) и времени создания (по возрастанию)
            sorted_actions = sorted(
                ready_actions,
                key=lambda x: (-x['priority'], x['timestamp'])
            )
            logger.debug(f"Получено {len(sorted_actions[:limit])} готовых к отправке действий (limit={limit})")
            return sorted_actions[:limit]

    def mark_as_attempted(self, action_ids: List[str], success: bool):
        """Обновляет статус действий после попытки синхронизации"""
        if not action_ids:
            logger.debug("mark_as_attempted вызван с пустым списком")
            return

        with self.lock:
            now = datetime.now().isoformat()
            for action in self.queue[:]:
                if action['id'] in action_ids:
                    action['last_attempt'] = now
                    action['attempts'].append({
                        'time': now,
                        'success': success
                    })

                    if success:
                        # Удаляем успешные действия из очереди
                        self.queue.remove(action)
                        logger.info(f"Удалено успешно синхронизированное действие id={action['id']}")
                    else:
                        # Увеличиваем счетчик попыток
                        action['retry_count'] += 1
                        # Устанавливаем время следующей попытки
                        action['next_retry'] = self._calculate_next_retry(
                            action['retry_count']
                        ).isoformat()
                        logger.info(f"Отмечено неудачное действие id={action['id']}, retry_count={action['retry_count']}")
            self._save_queue()

    def _calculate_next_retry(self, retry_count: int) -> datetime:
        """Вычисляет время следующей попытки с экспоненциальным backoff"""
        base_delay = min(60 * (2 ** retry_count), 86400)  # Максимум 1 день (86400 секунд)
        jitter = base_delay * 0.1  # Добавляем 10% случайности
        next_retry_time = datetime.now() + timedelta(seconds=base_delay + jitter)
        logger.debug(f"Расчет времени следующей попытки: retry_count={retry_count}, delay={base_delay}s, next_retry={next_retry_time.isoformat()}")
        return next_retry_time

    def clear_processed(self, action_ids: List[str]):
        """Удаляет обработанные действия из очереди"""
        if not action_ids:
            logger.debug("clear_processed вызван с пустым списком")
            return

        with self.lock:
            before_count = len(self.queue)
            self.queue = [a for a in self.queue if a['id'] not in action_ids]
            removed = before_count - len(self.queue)
            if removed > 0:
                logger.info(f"Удалено {removed} обработанных действий из очереди")
                self._save_queue()

    def retry_failed_actions(self, max_retries: int = 5):
        """Обновляет время повторных попыток для неудачных действий"""
        with self.lock:
            updated = 0
            for action in self.queue:
                if action['retry_count'] >= max_retries:
                    continue

                if not action['attempts'] or not action['attempts'][-1]['success']:
                    action['next_retry'] = self._calculate_next_retry(
                        action['retry_count']
                    ).isoformat()
                    updated += 1
            if updated > 0:
                logger.info(f"Обновлено время повторных попыток для {updated} действий")
                self._save_queue()

    def get_stats(self) -> Dict:
        """Возвращает статистику очереди"""
        with self.lock:
            now = datetime.now()
            pending = [
                a for a in self.queue
                if datetime.fromisoformat(a['next_retry']) <= now
            ]
            
            stats = {
                'total': len(self.queue),
                'pending': len(pending),
                'oldest': min(
                    [datetime.fromisoformat(a['timestamp']) for a in self.queue],
                    default=None
                ),
                'by_status': self._count_by_status()
            }
            logger.debug(f"Статистика очереди: {stats}")
            return stats

    def _count_by_status(self) -> Dict:
        """Считает действия по типам"""
        counts = {}
        for action in self.queue:
            typ = action['action_type']
            counts[typ] = counts.get(typ, 0) + 1
        return counts

    def clean_old_entries(self, days: int = 7):
        """Очищает старые записи старше указанного количества дней"""
        with self.lock:
            cutoff = datetime.now() - timedelta(days=days)
            initial_count = len(self.queue)
            
            self.queue = [
                a for a in self.queue
                if datetime.fromisoformat(a['timestamp']) >= cutoff
            ]
            
            removed = initial_count - len(self.queue)
            if removed > 0:
                logger.info(f"Удалено {removed} старых записей из очереди")
                self._save_queue()

    def __len__(self):
        """Возвращает количество элементов в очереди"""
        with self.lock:
            length = len(self.queue)
            logger.debug(f"Текущий размер очереди: {length}")
            return length
