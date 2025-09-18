import logging

from config import DEFAULT_WORKLOG_GROUP, normalize_group_name
from logging_setup import setup_logging
from sheets_api import get_sheets_api


logger = logging.getLogger(__name__)


def print_headers(ws) -> None:
    try:
        headers = get_sheets_api()._request_with_retry(ws.row_values, 1)
    except Exception as exc:
        logger.error("Не удалось прочитать шапку", exc_info=exc)
        return
    logger.info("Шапка: %s", headers)


def main() -> None:
    api = get_sheets_api()
    try:
        titles = api._list_titles()
        logger.info("Доступные вкладки:")
        for title in titles:
            logger.info(" - %s", title)
    except Exception as exc:
        logger.error("Не удалось получить список вкладок", exc_info=exc)
        return

    logger.info("")
    for group in [None, DEFAULT_WORKLOG_GROUP, "Стоматология", "Тест"]:
        normalized = normalize_group_name(group or "")
        try:
            worksheet = api._resolve_worklog_ws(normalized)
            logger.info("group=%r -> %s", group, worksheet.title)
            print_headers(worksheet)
        except Exception as exc:
            logger.error("group=%r: ошибка", group, exc_info=exc)


if __name__ == "__main__":
    setup_logging(app_name="wtt-debug-worklogs", force_console=True)
    main()
