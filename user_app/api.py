# user_app/api.py
from __future__ import annotations

import datetime as dt
import uuid

from sheets_api import SheetsAPI, SheetsAPIError


class UserNotFound(Exception):
    pass


class UserAPI:
    """
    Сервис-слой user_app: вся работа с Google Sheets только через SheetsAPI.
    """

    def __init__(self, sheets: SheetsAPI | None = None):
        self.sheets = sheets or SheetsAPI()

    # ---- Users ----
    def find_user(self, email: str) -> dict:
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
            login_time=dt.datetime.now(dt.UTC).isoformat(),
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
    def log_actions(
        self, actions: list[dict], email: str, user_group: str | None = None
    ) -> bool:
        try:
            for action in actions:
                self.sheets.log_user_actions(
                    email=action.get("email", email),
                    action=action.get("action_type", ""),
                    status=action.get("status", ""),
                    group=user_group,
                    timestamp_utc=action.get("timestamp"),
                    start_utc=action.get("status_start_time"),
                    end_utc=action.get("status_end_time"),
                    session_id=action.get("session_id"),
                    group_at_start=user_group,
                )
            return True
        except SheetsAPIError:
            return False
