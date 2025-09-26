# user_app/session.py
from __future__ import annotations
from typing import Optional
import threading

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
        _current_session_id = (session_id or "").strip()

def get_session_id() -> Optional[str]:
    with _lock:
        return _current_session_id
