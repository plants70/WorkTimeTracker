# sync/network.py
from __future__ import annotations

import logging
import socket
import urllib.request

from telemetry import record_network_call


logger = logging.getLogger(__name__)


def is_internet_available(timeout: int = 3) -> bool:
    """Проверить доступность интернета."""

    try:
        with record_network_call("network.internet_check"):
            logger.debug("Проверка доступности интернета...")
            response = urllib.request.urlopen("https://www.google.com", timeout=timeout)
            if response.status == 200:
                logger.debug("Интернет доступен")
                return True
            logger.warning("Ответ сервера Google: %s", response.status)
            return False
    except (urllib.error.URLError, socket.timeout) as exc:
        logger.warning("Интернет недоступен", exc_info=exc)
        return False
    except Exception as exc:  # pragma: no cover - unexpected path
        logger.error("Неожиданная ошибка при проверке интернета", exc_info=exc)
        return False
