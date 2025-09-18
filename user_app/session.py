# user_app/session.py
from __future__ import annotations
import threading
from typing import Optional

from logging_setup import (
    get_session_id as _get_log_session_id,
    set_session_id as _set_log_session_id,
)

# Простой потокобезопасный storage для текущих реквизитов сессии
_lock = threading.RLock()
_current_email: Optional[str] = None
_current_session_id: Optional[str] = None

def set_user_email(email: str) -> None:
    global _current_email
    with _lock:
        _current_email = (email or "").strip().lower()

def get_user_email() -> Optional[str]:
    with _lock:
        return _current_email

def set_session_id(session_id: str) -> None:
    global _current_session_id
    with _lock:
        normalized = (session_id or "").strip()
        _current_session_id = normalized
        _set_log_session_id(normalized)

def get_session_id() -> Optional[str]:
    with _lock:
        session_id = _current_session_id or _get_log_session_id()
        return session_id if session_id and session_id != "-" else None
