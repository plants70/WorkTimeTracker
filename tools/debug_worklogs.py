import logging

from config import DEFAULT_WORKLOG_GROUP, normalize_group_name
from sheets_api import get_sheets_api

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def print_headers(ws):
    try:
        headers = get_sheets_api()._request_with_retry(ws.row_values, 1)
    except Exception as e:
        print("   [ERR] не удалось прочитать шапку:", e)
        return
    print("   Шапка:", headers)


def main():
    api = get_sheets_api()
    # Список вкладок
    try:
        titles = api._list_titles()
        print("Доступные вкладки:")
        for t in titles:
            print(" -", t)
    except Exception as e:
        print("Не удалось получить список вкладок:", e)
        return
    print()

    # Проверим резолвинг по нескольким группам
    for grp in [None, DEFAULT_WORKLOG_GROUP, "Стоматология", "Тест"]:
        g = normalize_group_name(grp or "")
        try:
            ws = api._resolve_worklog_ws(g)
            print(f"[OK] group={grp!r} -> {ws.title}")
            print_headers(ws)
        except Exception as e:
            print(f"[ERR] group={grp!r}: {e}")


if __name__ == "__main__":
    main()
