# admin_app/repo.py
from __future__ import annotations

import logging
from datetime import datetime

from sheets_api import SheetsAPIError, get_sheets_api
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

    def __init__(self, sheets=None):
        # используем общий ленивый синглтон, чтобы не плодить клиентов
        self.sheets = sheets or get_sheets_api()

    # -------------------------------------------------------------------------
    # Users
    # -------------------------------------------------------------------------
    def list_users(self) -> list[dict[str, str]]:
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
            out: list[dict[str, str]] = []
            for row in values[1:]:
                if any((c or "").strip() for c in row):
                    out.append(
                        {
                            header[i]: (row[i] if i < len(header) else "")
                            for i in range(len(header))
                        }
                    )
            return out
        except Exception as e:
            logger.exception("Не удалось получить список пользователей: %s", e)
            return []

    def add_or_update_user(self, user: dict[str, str]) -> bool:
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
    def get_active_sessions(self) -> list[dict]:
        """
        Возвращает все записи листа ActiveSessions (словари колонок).
        """
        try:
            # Предпочитаем публичный метод SheetsAPI, если он есть
            if hasattr(self.sheets, "get_all_active_sessions"):
                sessions = self.sheets.get_all_active_sessions()  # type: ignore[attr-defined]
            else:
                # Фолбэк: читаем лист напрямую и фильтруем active
                ws = self.sheets._get_ws(ACTIVE_SESSIONS_SHEET)  # type: ignore[attr-defined]
                table = self.sheets._read_table(ws)  # type: ignore[attr-defined]
                sessions = [
                    r
                    for r in table
                    if str(r.get("Status", "")).strip().lower() == "active"
                ]
            return sessions or []
        except Exception as e:
            logger.exception("get_active_sessions error: %s", e)
            return []

    def force_logout(self, email: str) -> bool:
        """
        Принудительно завершает ПОСЛЕДНЮЮ активную сессию пользователя:
        1) ищет среди активных записей по Email,
        2) берёт последнюю по LoginTime,
        3) вызывает finish_active_session(email, session_id, now).
        Возвращает True, если строка обновлена.
        """
        try:
            em = (email or "").strip().lower()
            sessions = self.get_active_sessions()  # уже фильтрует Status=='active'
            # отбираем только по данному email
            same_user = [
                r for r in sessions if (str(r.get("Email", "")).strip().lower() == em)
            ]
            if not same_user:
                logger.info("Force logout: активная сессия не найдена для %s", email)
                return False

            # выбираем последнюю по LoginTime (если формат строки стабилен) или по порядку
            def _key(row: dict):
                return (
                    str(row.get("LoginTime", "")).strip(),
                    str(row.get("SessionID", "")).strip(),
                )

            row = sorted(same_user, key=_key)[-1]
            sid = str(row.get("SessionID", "")).strip()
            if not sid:
                logger.info(
                    "Force logout: нет SessionID у последней активной строки для %s",
                    email,
                )
                return False
            now_iso = (
                datetime.now(datetime.UTC).astimezone().isoformat(timespec="seconds")
            )
            # корректное завершение активной строки
            ok = False
            if hasattr(self.sheets, "finish_active_session"):
                ok = bool(self.sheets.finish_active_session(email=em, session_id=sid, logout_time=now_iso))  # type: ignore[attr-defined]
            else:
                # на очень старых версиях можно попробовать «универсальный» апдейт, если он есть
                if hasattr(self.sheets, "_update_session_status"):
                    ok = bool(self.sheets._update_session_status(email=em, session_id=sid, status="finished", logout_time=now_iso))  # type: ignore[attr-defined]
            if ok:
                logger.info("Force logout success for %s (session %s)", email, sid)
            else:
                logger.warning(
                    "Force logout: не удалось завершить сессию %s для %s", sid, email
                )
            return ok
        except Exception as e:
            logger.exception("force_logout error for %s: %s", email, e)
            return False

    # -------------------------------------------------------------------------
    # Schedule (Shift calendar)
    # -------------------------------------------------------------------------
    def _list_titles(self) -> list[str]:
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
            spreadsheet = self.sheets._request_with_retry(
                self.sheets.client.open, GOOGLE_SHEET_NAME
            )
            worksheets = self.sheets._request_with_retry(spreadsheet.worksheets)
            return [ws.title for ws in worksheets]
        except Exception as e:
            logger.warning("Не удалось получить список листов: %s", e)
            return []

    def _pick_schedule_title(self, titles: list[str]) -> str | None:
        """
        Выбирает название листа графика из известных вариантов.
        """
        available = set(titles)
        for cand in CANDIDATE_SCHEDULE_TITLES:
            if cand in available:
                return cand
        return None

    def get_shift_calendar(self) -> list[list[str]]:
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
