# sync/session_inspector.py
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, List

from sheets_api import get_sheets_api
sheets_api = get_sheets_api()
from sync.network import is_internet_available

logger = logging.getLogger(__name__)

class SessionInspector:
    """
    Лёгкий «санитар»: не меняет поведение клиента, а только подлечивает зависшие active-сессии.
    Вызывается из вашего SyncManager в каждом тике при наличии интернета.
    """
    def __init__(self, db, max_age_hours: int = 12):
        self.db = db
        self.max_age_hours = max_age_hours

    def _too_old(self, login_time: Optional[str]) -> bool:
        if not login_time:
            return False
        try:
            dt = datetime.fromisoformat(login_time.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_h = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 3600.0
            return age_h > self.max_age_hours
        except Exception:
            return False

    def _is_finished_locally(self, email: str, sid: str) -> bool:
        try:
            # локальная проверка: есть ли LOGOUT для пары email+sid
            return self.db.check_existing_logout(email, session_id=sid)
        except Exception as e:
            logger.debug(f"Local check failed for {email}/{sid}: {e}")
            return False

    def tick(self) -> None:
        if not is_internet_available():
            logger.debug("[Inspector] offline -> skip")
            return
        try:
            sessions: List[Dict[str, str]] = sheets_api.get_all_active_sessions() or []
        except Exception as e:
            logger.error(f"[Inspector] cannot read ActiveSessions: {e}")
            return

        for r in sessions:
            try:
                status = (r.get("Status") or "").strip().lower()
                if status != "active":
                    continue

                email = (r.get("Email") or "").strip()
                sid   = (r.get("SessionID") or "").strip()
                ltime = (r.get("LoginTime") or "").strip()

                # 1) Уже завершена локально -> доводим таблицу
                if self._is_finished_locally(email, sid):
                    ok = False
                    try:
                        ok = sheets_api.finish_active_session(email, sid)
                    except Exception as e:
                        logger.debug(f"[Inspector] finish failed, will fallback: {e}")
                    if not ok:
                        try:
                            sheets_api.kick_active_session(email, sid, status="finished")
                        except Exception as e:
                            logger.error(f"[Inspector] fallback failed for {email}/{sid}: {e}")
                            continue
                    logger.info(f"[Inspector] healed {email}/{sid} -> finished")
                    continue

                # 2) Слишком старая «active» -> завершаем мягко
                if self._too_old(ltime):
                    ok = False
                    try:
                        ok = sheets_api.finish_active_session(email, sid)
                    except Exception as e:
                        logger.debug(f"[Inspector] finish(old) failed, fallback: {e}")
                    if not ok:
                        try:
                            sheets_api.kick_active_session(email, sid, status="finished")
                        except Exception as e:
                            logger.error(f"[Inspector] fallback(old) failed for {email}/{sid}: {e}")
                            continue
                    logger.info(f"[Inspector] expired {email}/{sid} -> finished")
            except Exception as e:
                logger.error(f"[Inspector] error on row: {e}")