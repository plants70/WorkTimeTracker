# tools/sheets_bootstrap.py
from __future__ import annotations

import logging
import sys

from logging_setup import setup_logging
from config import GOOGLE_SHEET_NAME
from sheets_api import SheetsAPI


logger = logging.getLogger(__name__)


_USERS_HEADER = ["Email", "Name", "Group", "TelegramChatId"]
_GROUPS_HEADER = ["Group", "TelegramTopicId"]
_ACCESS_HEADER = ["Email", "IsAdmin"]
_RULES_HEADER = [
    "RuleId",
    "Type",
    "Target",
    "ThresholdMinutes",
    "CooldownMinutes",
    "Enabled",
    "Comment",
]
_LOG_HEADER = [
    "TsUtc",
    "Type",
    "Scope",
    "Target",
    "Message",
    "Sent",
    "Error",
]


def ensure_header(api: SheetsAPI, title: str, header: list[str]) -> None:
    _ = api.worksheet(title)
    rows = api.values_get(f"'{title}'!1:1") or []
    have = rows[0] if rows else []
    if have != header:
        api.values_update(f"'{title}'!1:1", header)


def main() -> int:
    setup_logging(app_name="wtt-sheets-bootstrap", force_console=True)
    api = SheetsAPI(GOOGLE_SHEET_NAME)
    for title, hdr in [
        ("Users", _USERS_HEADER),
        ("Groups", _GROUPS_HEADER),
        ("AccessControl", _ACCESS_HEADER),
        ("NotificationRules", _RULES_HEADER),
        ("NotificationsLog", _LOG_HEADER),
    ]:
        ensure_header(api, title, hdr)
    logger.info("Bootstrap completed for %s", GOOGLE_SHEET_NAME)
    return 0


if __name__ == "__main__":
    sys.exit(main())
