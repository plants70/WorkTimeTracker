# archiver.py (reworked to use centralized SheetsAPI only)
from __future__ import annotations

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Dict
import logging
import argparse

# Ensure project root is importable (so we can import config and sheets_api when run directly)
ROOT_PATH = str(Path(__file__).parent.resolve())
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from config import GOOGLE_SHEET_NAME, WORKLOG_SHEET, ARCHIVE_SHEET  # type: ignore
from sheets_api import SheetsAPI, SheetsAPIError  # type: ignore

logger = logging.getLogger(__name__)


# ---- helpers ----

TS_HEADER_CANDIDATES = ("timestamp", "Timestamp", "time", "Time", "Дата", "Время", "DateTime", "datetime")

def _parse_ts(s: str) -> Optional[datetime]:
    """
    Parse timestamp in flexible formats. Prefer ISO-8601 with timezone.
    Returns timezone-aware datetime in local timezone for date comparison.
    """
    s = (s or "").strip()
    if not s:
        return None
    fmts = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y",
        "%Y-%m-%d",
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone()
        except Exception:
            continue
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone()
    except Exception:
        return None


def _yesterday_local(base: Optional[datetime] = None) -> datetime.date:
    now_local = (base or datetime.now().astimezone())
    return (now_local.date() - timedelta(days=1))


def _find_timestamp_index(header: List[str]) -> Optional[int]:
    idx_map = { (h or "").strip(): i for i, h in enumerate(header) }
    for key in TS_HEADER_CANDIDATES:
        for h,i in idx_map.items():
            if h.lower() == key.lower():
                return i
    if len(header) >= 6 and header[5].lower().startswith("time"):
        return 5
    return None


def _ensure_archive_sheet(sheets: SheetsAPI, header: List[str]) -> object:
    """
    Get archive worksheet; if missing — create and put header.
    """
    try:
        ws = sheets.get_worksheet(ARCHIVE_SHEET)
        values = sheets._request_with_retry(ws.get_all_values)
        if not values:
            sheets._request_with_retry(ws.update, "A1", [header])
        elif values and values[0] != header:
            pass
        return ws
    except SheetsAPIError:
        from config import GOOGLE_SHEET_NAME  # lazy import
        spreadsheet = sheets._request_with_retry(sheets.client.open, GOOGLE_SHEET_NAME)
        ws = sheets._request_with_retry(spreadsheet.add_worksheet, title=ARCHIVE_SHEET, rows=1, cols=max(1, len(header)))
        sheets._request_with_retry(ws.update, "A1", [header])
        return ws


def _collect_rows_for_date(values: List[List[str]], day: datetime.date) -> Tuple[List[List[str]], List[List[str]], List[str]]:
    """
    Split table rows to (to_archive, to_keep).
    Returns (to_archive_rows, keep_rows, header)
    """
    if not values:
        return [], [], []

    header = values[0]
    body = values[1:]
    ts_idx = _find_timestamp_index(header)
    if ts_idx is None:
        logger.warning("Timestamp column not found in header: %s", header)
        return [], values[1:], header

    to_archive: List[List[str]] = []
    keep_rows: List[List[str]] = []

    for row in body:
        ts_raw = row[ts_idx] if ts_idx < len(row) else ""
        dt = _parse_ts(ts_raw)
        if dt and dt.date() == day:
            to_archive.append(row)
        else:
            keep_rows.append(row)

    return to_archive, keep_rows, header


def _process_sheet(sheets: SheetsAPI, sheet_name: str, day: datetime.date, dry_run: bool = False) -> Tuple[int,int]:
    """
    Process one sheet: move rows for `day` to ARCHIVE_SHEET.
    Returns (archived_count, kept_count)
    """
    try:
        ws = sheets.get_worksheet(sheet_name)
        values = sheets._request_with_retry(ws.get_all_values)
        to_move, keep, header = _collect_rows_for_date(values, day)
        if not header:
            logger.info("[%s] empty or no header — skipping", sheet_name)
            return 0, len(keep)

        archived = len(to_move)
        if archived == 0:
            logger.info("[%s] no rows for %s", sheet_name, day.isoformat())
            return 0, len(keep)

        logger.info("[%s] archiving %d rows for %s", sheet_name, archived, day.isoformat())

        if dry_run:
            return archived, len(keep)

        arch = _ensure_archive_sheet(sheets, header)
        sheets._request_with_retry(arch.append_rows, to_move, value_input_option="USER_ENTERED")

        new_data = [header] + keep if keep else [header]
        sheets._request_with_retry(ws.clear)
        sheets._request_with_retry(ws.update, "A1", new_data)

        return archived, len(keep)
    except Exception as e:
        logger.exception("Failed to process sheet %s: %s", sheet_name, e)
        return 0, 0


def run_archive(target_date: Optional[str] = None, dry_run: bool = False, only_sheet: Optional[str] = None) -> None:
    """
    Archive rows for yesterday (or for specific date YYYY-MM-DD) from WorkLog sheets to Archive.
    - If `only_sheet` is provided, process only that sheet.
    - Otherwise process WORKLOG_SHEET and all 'WorkLog_*' sheets that exist.
    """
    sheets = SheetsAPI()

    if target_date:
        try:
            day = datetime.strptime(target_date, "%Y-%m-%d").date()
        except Exception:
            raise SystemExit("Invalid --date format. Use YYYY-MM-DD")
    else:
        day = _yesterday_local()

    titles: List[str] = []
    try:
        titles = sheets.list_worksheet_titles()
    except Exception:
        pass

    candidates: List[str] = []
    if only_sheet:
        if only_sheet not in titles:
            raise SystemExit(f"Sheet '{only_sheet}' not found in {GOOGLE_SHEET_NAME}")
        candidates = [only_sheet]
    else:
        for t in titles:
            if t == WORKLOG_SHEET or t.startswith(f"{WORKLOG_SHEET}_"):
                candidates.append(t)

    if not candidates:
        logger.warning("No WorkLog-like sheets found. Nothing to do.")
        return

    total_archived = 0
    for name in candidates:
        a, _k = _process_sheet(sheets, name, day, dry_run=dry_run)
        total_archived += a

    if dry_run:
        logger.info("DRY-RUN complete. Would archive %d rows total for %s.", total_archived, day.isoformat())
    else:
        logger.info("Archive complete. Archived %d rows total for %s.", total_archived, day.isoformat())


def main():
    ap = argparse.ArgumentParser(description="Archive yesterday's rows from WorkLog sheets to Archive via SheetsAPI.")
    ap.add_argument("--date", help="Target date YYYY-MM-DD (default: yesterday in local tz)", default=None)
    ap.add_argument("--dry-run", action="store_true", help="Do not modify sheets, just report.")
    ap.add_argument("--only-sheet", help="Process only given sheet name", default=None)
    args = ap.parse_args()
    run_archive(target_date=args.date, dry_run=args.dry_run, only_sheet=args.only_sheet)

if __name__ == "__main__":
    main()