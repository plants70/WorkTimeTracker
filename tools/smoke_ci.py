# tools/smoke_ci.py
import os
import sys
import time
from datetime import datetime
from typing import List

import gspread
import config

SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "WorkTimeTracker")
REQUIRED_SHEETS: List[str] = [
    os.getenv("USERS_SHEET", "Users"),
    os.getenv("WORKLOG_SHEET", "WorkLog"),
    os.getenv("ACTIVE_SESSIONS_SHEET", "ActiveSessions"),
]
ARCHIVE_SHEET = os.getenv("ARCHIVE_SHEET", "Archive") or ""
CI_SHEET = "CI_Smoke"  # отдельный лист для теста, реальные данные не трогаем

def get_client() -> gspread.Client:
    with config.credentials_path() as p:
        return gspread.service_account(filename=str(p))

def ensure_sheet(spread, title: str, rows: int = 100, cols: int = 10):
    try:
        return spread.worksheet(title)
    except gspread.WorksheetNotFound:
        return spread.add_worksheet(title=title, rows=str(rows), cols=str(cols))

def main() -> int:
    # импорт конфига уже валидирует окружение
    print("✓ config imported, base:", config.BASE_DIR)

    # подключение к Sheets
    client = get_client()
    print("✓ gspread client OK")

    spread = client.open(SHEET_NAME)
    print(f"✓ spreadsheet opened: {SHEET_NAME}")

    # проверим обязательные листы
    for ws_title in REQUIRED_SHEETS:
        if not ws_title:
            continue
        ensure_sheet(spread, ws_title)
        print(f"✓ worksheet exists: {ws_title}")

    # архивный — опционально
    if ARCHIVE_SHEET:
        ensure_sheet(spread, ARCHIVE_SHEET)
        print(f"✓ worksheet exists: {ARCHIVE_SHEET}")

    # безопасная запись/чтение на служебный лист
    ws = ensure_sheet(spread, CI_SHEET, rows=50, cols=6)
    ts = datetime.utcnow().isoformat(timespec="seconds")
    marker = f"smoke-{int(time.time())}"
    ws.append_row(["marker", marker, ts], value_input_option="RAW")
    print("✓ row appended on CI_Smoke")

    # убедимся, что запись есть и удалим её
    found = False
    cell = ws.find(marker)
    if cell:
        found = True
        ws.delete_rows(cell.row)
        print("✓ row deleted from CI_Smoke")
    assert found, "smoke row not found back in CI_Smoke"

    print("ALL GOOD ✅")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)
