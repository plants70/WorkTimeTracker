# user_app/ui_helpers.py
import re, time
from typing import Optional

EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.IGNORECASE)

class MessageDebouncer:
    """
    Блокирует повтор одного и того же сообщения в течение N секунд,
    чтобы не было дубля "e-mail не найден".
    """
    def __init__(self, cooldown_sec: float = 1.0):
        self.cooldown = cooldown_sec
        self._last_key: Optional[str] = None
        self._last_ts: float = 0.0

    def should_show(self, key: str) -> bool:
        now = time.monotonic()
        if self._last_key == key and (now - self._last_ts) < self.cooldown:
            return False
        self._last_key, self._last_ts = key, now
        return True

def is_valid_email(email: str) -> bool:
    return bool(EMAIL_RE.match((email or "").strip()))
