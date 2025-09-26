# user_app/api.py
from __future__ import annotations
from typing import Optional, Dict, List
from sheets_api import SheetsAPI
from datetime import datetime, timezone
import uuid

class UserNotFound(Exception):
    pass

class UserAPI:
    """
    Сервис-слой user_app: вся работа с Google Sheets только через SheetsAPI.
    """
    def __init__(self, sheets: Optional[SheetsAPI] = None):
        self.sheets = sheets or SheetsAPI()

    # ---- Users ----
    def find_user(self, email: str) -> Dict:
        email = (email or "").strip().lower()
        user = self.sheets.get_user_by_email(email)
        if not user:
            raise UserNotFound(email)
        return user

    # ---- Sessions ----
    def start_session(self, email: str, name: str) -> str:
        """
        Создаёт запись в ActiveSessions (Status=active).
        Возвращает session_id.
        """
        session_id = str(uuid.uuid4())
        self.sheets.set_active_session(
            email=email,
            name=name,
            session_id=session_id,
            login_time=datetime.now(timezone.utc).isoformat()
        )
        return session_id

    def finish_session(self, email: str, session_id: str) -> bool:
        return self.sheets.finish_active_session(email=email, session_id=session_id)

    def force_logout_if_needed(self, email: str, session_id: str) -> bool:
        """
        Пулинг статуса: если админ принудительно разлогинил (Status=kicked) — вернём True.
        """
        st = self.sheets.check_user_session_status(email=email, session_id=session_id)
        return st in ("kicked", "finished")

    # ---- WorkLog ----
    def log_actions(self, actions: List[Dict], email: str, user_group: Optional[str] = None) -> bool:
        return self.sheets.log_user_actions(actions, email=email, user_group=user_group)
