# admin_app/repo.py
from __future__ import annotations

import logging
from typing import List, Dict, Optional
from datetime import datetime, timezone

from sheets_api import SheetsAPI, SheetsAPIError
from config import (
    GOOGLE_SHEET_NAME,
    USERS_SHEET,
    ACTIVE_SESSIONS_SHEET,
)

logger = logging.getLogger(__name__)

# Возможные названия листа с графиком (по приоритету)
CANDIDATE_SCHEDULE_TITLES = ["ShiftCalendar", "Schedule", "График"]


class AdminRepo:
    """
    Репозиторий административных операций.
    Все операции идут через централизованный SheetsAPI (ретраи/квоты/логирование).
    """

    def __init__(self, sheets: Optional[SheetsAPI] = None):
        self.sheets = sheets or SheetsAPI()

    # -------------------------------------------------------------------------
    # Users
    # -------------------------------------------------------------------------
    def list_users(self) -> List[Dict[str, str]]:
        """
        Возвращает список пользователей как список словарей (колонки по заголовку листа Users).
        """
        try:
            # Используем высокоуровневый метод SheetsAPI, чтобы не дублировать логику
            users = self.sheets.get_users()  # type: ignore[attr-defined]
            return users or []
        except AttributeError:
            # Фолбэк, если вдруг нет get_users() (старый SheetsAPI)
            ws = self.sheets.get_worksheet(USERS_SHEET)
            values = self.sheets._request_with_retry(ws.get_all_values)
            if not values:
                return []
            header = values[0]
            out: List[Dict[str, str]] = []
            for row in values[1:]:
                if any((c or "").strip() for c in row):
                    out.append({header[i]: (row[i] if i < len(header) else "") for i in range(len(header))})
            return out
        except Exception as e:
            logger.exception("Не удалось получить список пользователей: %s", e)
            return []

    def add_or_update_user(self, user: Dict[str, str]) -> bool:
        """
        Добавляет или обновляет пользователя (по Email).
        """
        try:
            self.sheets.upsert_user(user)  # type: ignore[attr-defined]
            return True
        except AttributeError:
            # Фолбэк на старый интерфейс — пробуем обновить набор полей
            try:
                email = user.get("Email") or user.get("email")
                if not email:
                    raise ValueError("user.Email is required")
                fields = {k: v for k, v in user.items() if k != "Email"}
                self.sheets.update_user_fields(email=email, fields=fields)  # type: ignore[attr-defined]
                return True
            except Exception as e:
                logger.exception("Fallback upsert_user failed: %s", e)
                return False
        except Exception as e:
            logger.exception("add_or_update_user error: %s", e)
            return False

    def delete_user(self, email: str) -> bool:
        """
        Удаляет пользователя по Email.
        """
        try:
            return bool(self.sheets.delete_user(email))  # type: ignore[attr-defined]
        except Exception as e:
            logger.exception("delete_user error for %s: %s", email, e)
            return False

    # -------------------------------------------------------------------------
    # Groups
    # -------------------------------------------------------------------------
    def list_groups_from_sheet(self) -> list[str]:
        """
        Возвращает список доступных групп из листа 'Groups' (колонка 'Group').
        Пустые/дубликаты фильтруются.
        """
        try:
            ws = self.sheets.get_worksheet("Groups")
            values = self.sheets._request_with_retry(ws.get_all_values)
            groups = []
            for row in values[1:]:  # пропускаем заголовок
                if not row:
                    continue
                g = (row[0] or "").strip()
                if g:
                    groups.append(g)
            return sorted(set(groups))
        except Exception as e:
            logger.warning("list_groups_from_sheet failed: %s", e)
            return []

    # -------------------------------------------------------------------------
    # Active sessions
    # -------------------------------------------------------------------------
    def get_active_sessions(self) -> List[Dict]:
        """
        Возвращает все записи листа ActiveSessions (словари колонок).
        """
        try:
            sessions = self.sheets.get_all_active_sessions()  # type: ignore[attr-defined]
            return sessions or []
        except Exception as e:
            logger.exception("get_active_sessions error: %s", e)
            return []

    def force_logout(self, email: str) -> bool:
        """
        Принудительно завершает ПОСЛЕДНЮЮ активную сессию пользователя.
        Возвращает True, если удалось обновить строку.
        """
        try:
            ok = self.sheets.kick_active_session(email=email)  # type: ignore[attr-defined]
            if ok:
                logger.info("Force logout success for %s", email)
            else:
                logger.info("Force logout: активная сессия не найдена для %s", email)
            return bool(ok)
        except Exception as e:
            logger.exception("force_logout error for %s: %s", email, e)
            return False

    # -------------------------------------------------------------------------
    # Schedule (Shift calendar)
    # -------------------------------------------------------------------------
    def _list_titles(self) -> List[str]:
        """
        Возвращает список названий листов книги.
        """
        try:
            if hasattr(self.sheets, "list_worksheet_titles"):
                return list(self.sheets.list_worksheet_titles())  # type: ignore[attr-defined]
        except Exception:
            pass

        # Фолбэк через открытую книгу
        try:
            spreadsheet = self.sheets._request_with_retry(self.sheets.client.open, GOOGLE_SHEET_NAME)
            worksheets = self.sheets._request_with_retry(spreadsheet.worksheets)
            return [ws.title for ws in worksheets]
        except Exception as e:
            logger.warning("Не удалось получить список листов: %s", e)
            return []

    def _pick_schedule_title(self, titles: List[str]) -> Optional[str]:
        """
        Выбирает название листа графика из известных вариантов.
        """
        available = set(titles)
        for cand in CANDIDATE_SCHEDULE_TITLES:
            if cand in available:
                return cand
        return None

    def get_shift_calendar(self) -> List[List[str]]:
        """
        Возвращает таблицу графика как список списков:
        [ [header...], [row1...], ... ]. Если лист отсутствует — [].
        """
        try:
            titles = self._list_titles()
            if not titles:
                logger.info("В книге '%s' не найдено листов.", GOOGLE_SHEET_NAME)
                return []

            name = self._pick_schedule_title(titles)
            if not name:
                logger.info(
                    "Лист графика не найден. Ожидались: %s; есть: %s",
                    ", ".join(CANDIDATE_SCHEDULE_TITLES),
                    ", ".join(titles),
                )
                return []

            ws = self.sheets.get_worksheet(name)
            values = self.sheets._request_with_retry(ws.get_all_values)
            return values or []
        except SheetsAPIError as e:
            logger.warning("Ошибка доступа к листу графика: %s", e)
            return []
        except Exception as e:
            logger.exception("get_shift_calendar error: %s", e)
            return []