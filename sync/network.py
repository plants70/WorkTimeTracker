"""Networking helpers with GUI-thread protection."""
from __future__ import annotations

import logging
import socket
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, TimeoutError

from .threading_utils import guard_gui_long_operation, is_gui_thread

logger = logging.getLogger(__name__)

_PROBE_URL = "https://www.google.com"
# Один воркер, чтобы не плодить потоки из GUI; при зависании пересоздаём.
_EXECUTOR: ThreadPoolExecutor | None = ThreadPoolExecutor(max_workers=1)


def _reset_executor() -> None:
    """Безопасно пересоздать пул, чтобы не зависал на одном «залипшем» таске."""
    global _EXECUTOR
    try:
        if _EXECUTOR is not None:
            _EXECUTOR.shutdown(cancel_futures=True)
    except Exception:
        pass
    _EXECUTOR = ThreadPoolExecutor(max_workers=1)


def _probe_once(timeout: float) -> bool:
    with urllib.request.urlopen(_PROBE_URL, timeout=timeout) as response:
        status = getattr(response, "status", None)
        if status == 200:
            logger.debug("Интернет доступен")
            return True
        logger.warning("Ответ сервера %s: %s", _PROBE_URL, status)
        return False


def is_internet_available(timeout: int = 3) -> bool:
    """Проверить доступность интернета без подвешивания GUI."""
    probe_timeout = max(1.0, float(timeout))
    guard_threshold = min(probe_timeout, 0.8)

    with guard_gui_long_operation("network.is_internet_available",
                                  threshold=guard_threshold):
        try:
            if is_gui_thread():
                # Отправляем проверку в фон, чтобы не блокировать GUI.
                future = _EXECUTOR.submit(_probe_once, probe_timeout)  # type: ignore[arg-type]
                try:
                    return future.result(timeout=probe_timeout)
                except TimeoutError:
                    # Важно: отменяем зависший таск и пересоздаём пул,
                    # иначе единственный воркер навсегда занят.
                    future.cancel()
                    _reset_executor()
                    logger.warning(
                        "Интернет недоступен: проверка превысила %.1fs",
                        probe_timeout,
                    )
                    return False
            else:
                return _probe_once(probe_timeout)
        except (urllib.error.URLError, socket.timeout) as exc:
            logger.warning("Интернет недоступен: %s", exc)
            return False
        except Exception as exc:
            logger.error("Неожиданная ошибка при проверке интернета: %s", exc)
            return False
