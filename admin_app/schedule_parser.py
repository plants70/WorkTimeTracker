# admin_app/schedule_parser.py
"""
DEPRECATED: помогает сохранить совместимость, но не должен использоваться в новом коде.
Теперь график читается централизованно через SheetsAPI / AdminRepo.get_shift_calendar().

Если модуль всё ещё импортируется старыми участками кода, функции ниже делегируют чтение
в Google Sheets через SheetsAPI (без pandas/requests и без прямых gspread-вызовов).
"""

from __future__ import annotations
from typing import List, Optional
import logging
from pathlib import Path
import sys

# Добавляем корень проекта в sys.path, чтобы были доступны config и sheets_api при прямом запуске
ROOT_PATH = str(Path(__file__).parent.parent.resolve())
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from sheets_api import SheetsAPI, SheetsAPIError  # централизованный слой
from config import GOOGLE_SHEET_NAME  # только для сообщений

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO)

# Возможные названия листа с графиком (приоритет по порядку)
CANDIDATE_SCHEDULE_TITLES = ["ShiftCalendar", "Schedule", "График"]


def _list_titles(sheets: SheetsAPI) -> List[str]:
    """Пытаемся получить список названий листов через SheetsAPI, с фолбэком."""
    try:
        # Новая версия SheetsAPI может иметь list_worksheet_titles()
        if hasattr(sheets, "list_worksheet_titles"):
            return list(sheets.list_worksheet_titles())  # type: ignore
    except Exception:
        pass

    # Фолбэк: напрямую через открытую книгу (всё равно через _request_with_retry)
    try:
        spreadsheet = sheets._request_with_retry(sheets.client.open, GOOGLE_SHEET_NAME)
        worksheets = sheets._request_with_retry(spreadsheet.worksheets)
        return [ws.title for ws in worksheets]
    except Exception as e:
        logger.warning("Не удалось получить список листов: %s", e)
        return []


def _pick_schedule_sheet_title(titles: List[str]) -> Optional[str]:
    """Выбираем первый подходящий лист из известных вариантов."""
    tset = set(titles)
    for cand in CANDIDATE_SCHEDULE_TITLES:
        if cand in tset:
            return cand
    return None


def get_shift_calendar() -> List[List[str]]:
    """
    Возвращает «таблицу графика» как список списков: [ [header...], [row1...], ... ].
    Если лист графика отсутствует — вернёт [].
    В новом коде используйте AdminRepo.get_shift_calendar().
    """
    sheets = SheetsAPI()

    titles = _list_titles(sheets)
    if not titles:
        logger.info("В книге '%s' не найдено ни одного листа.", GOOGLE_SHEET_NAME)
        return []

    sheet_name = _pick_schedule_sheet_title(titles)
    if not sheet_name:
        logger.info(
            "Лист графика не найден. Ожидались один из: %s; есть: %s",
            ", ".join(CANDIDATE_SCHEDULE_TITLES), ", ".join(titles),
        )
        return []

    try:
        ws = sheets.get_worksheet(sheet_name)
        values = sheets._request_with_retry(ws.get_all_values)
        if not values:
            logger.info("Лист '%s' пустой.", sheet_name)
            return []
        return values
    except SheetsAPIError as e:
        logger.warning("Ошибка доступа к листу '%s': %s", sheet_name, e)
        return []
    except Exception as e:
        logger.exception("Не удалось прочитать лист '%s': %s", sheet_name, e)
        return []


# Для обратной совместимости со старым именем
def get_shift_info() -> List[List[str]]:
    """Старое имя функции, оставлено для совместимости."""
    logger.warning("schedule_parser.get_shift_info() устарела — используйте AdminRepo.get_shift_calendar().")
    return get_shift_calendar()
