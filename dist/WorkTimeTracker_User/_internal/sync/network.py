# sync/network.py
import urllib.request
import socket
import logging

logger = logging.getLogger(__name__)

def is_internet_available(timeout: int = 3) -> bool:
    """Проверить доступность интернета."""
    try:
        logger.debug("Проверка доступности интернета...")
        # Используем google.com или любой стабильный сайт
        response = urllib.request.urlopen("https://www.google.com", timeout=timeout)
        if response.status == 200:
            logger.debug("Интернет доступен")
            return True
        else:
            logger.warning(f"Ответ сервера Google: {response.status}")
            return False
    except (urllib.error.URLError, socket.timeout) as e:
        logger.warning(f"Интернет недоступен: {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при проверке интернета: {e}")
        return False