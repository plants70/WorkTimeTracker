# sync/service_alerts.py
from __future__ import annotations
import logging
from telegram_bot.notifier import TelegramNotifier
from config import SERVICE_ALERTS_ENABLED, SERVICE_ALERT_MIN_SECONDS

log = logging.getLogger(__name__)

_last: dict[str, float] = {}


def _should_skip(key: str, now_ts: float, min_gap: int) -> bool:
    last = _last.get(key, 0.0)
    if now_ts - last < min_gap:
        return True
    _last[key] = now_ts
    return False


def alert_sync_error(err_text: str, now_ts: float) -> None:
    """Позовите при фатальной/повторяющейся ошибке цикла синхронизации."""
    if not SERVICE_ALERTS_ENABLED:
        return
    key = "sync_error"
    if _should_skip(key, now_ts, SERVICE_ALERT_MIN_SECONDS):
        return
    n = TelegramNotifier()
    n.send_service(
        f"🛠️ Ошибка синхронизации:\n<code>{(err_text or '').strip()[:500]}</code>"
    )


def alert_queue_size(queue_len: int, threshold: int, now_ts: float) -> None:
    """
    Позовите, если очередь несинхронизированных записей > threshold.
    Например, считайте её как количество локальных непушнутых действий.
    """
    if not SERVICE_ALERTS_ENABLED:
        return
    if queue_len <= threshold:
        return
    key = "sync_queue_over"
    if _should_skip(key, now_ts, SERVICE_ALERT_MIN_SECONDS):
        return
    n = TelegramNotifier()
    n.send_service(
        f"🛠️ Очередь синка выросла: {queue_len} (порог {threshold}). Проверьте соединение/квоты."
    )
