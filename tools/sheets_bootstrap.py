# tools/sheets_bootstrap.py
from __future__ import annotations

import sys

from sheets_api import SheetsAPI
from config import GOOGLE_SHEET_NAME

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
_LOG_HEADER = ["TsUtc", "Type", "Scope", "Target", "Message", "Sent", "Error"]


def ensure_header(api: SheetsAPI, title: str, header: list[str]) -> None:
    api.worksheet(title)
    rows = api.values_get(f"'{title}'!1:1") or []
    have = rows[0] if rows else []
    if have != header:
        api.values_update(f"'{title}'!1:1", header)


def main() -> None:
    api = SheetsAPI(GOOGLE_SHEET_NAME)
    for title, hdr in [
        ("Users", _USERS_HEADER),
        ("Groups", _GROUPS_HEADER),
        ("AccessControl", _ACCESS_HEADER),
        ("NotificationRules", _RULES_HEADER),
        ("NotificationsLog", _LOG_HEADER),
    ]:
        ensure_header(api, title, hdr)
    print("OK: bootstrap done")


if __name__ == "__main__":
    sys.exit(main())
