# notifications/rules_manager.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Any
import logging

from sheets_api import SheetsAPI
from config import GOOGLE_SHEET_NAME

log = logging.getLogger(__name__)

RULES_SHEET = "NotificationRules"
HEADER = [
    "ID",  # int, уникальный
    "Enabled",  # TRUE/FALSE
    "Kind",  # long_status | status_window | late_login (зарезервировано)
    "Scope",  # service | personal | group
    "GroupTag",  # для Scope=group (иначе пусто)
    "Statuses",  # через запятую (для long_status), можно пусто
    "MinDurationMin",  # для long_status
    "WindowMin",  # для status_window
    "Limit",  # для status_window (кол-во событий за окно)
    "RateLimitSec",  # антиспам для этого правила
    "Silent",  # TRUE/FALSE (тихое уведомление)
    "MessageTemplate",  # HTML, допускает {email}, {status}, {duration_min}, {limit}, {window_min}, {group}
]


@dataclass
class Rule:
    id: int
    enabled: bool
    kind: str
    scope: str
    group_tag: str
    statuses: List[str]
    min_duration_min: Optional[int]
    window_min: Optional[int]
    limit: Optional[int]
    rate_limit_sec: int
    silent: bool
    template: str


def _to_bool(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "да")


def _to_int(v: Any) -> Optional[int]:
    s = str(v or "").strip()
    if s == "" or s.lower() == "none":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def ensure_sheet_exists(api: SheetsAPI):
    ss = api.client.open(GOOGLE_SHEET_NAME)
    titles = [w.title for w in ss.worksheets()]
    if RULES_SHEET not in titles:
        ws = ss.add_worksheet(title=RULES_SHEET, rows=1000, cols=len(HEADER))
        api._request_with_retry(ws.update, "A1", [HEADER])
        # пример: долгий «Обед» > 30 минут, персонально
        api._request_with_retry(
            ws.append_rows,
            [
                [
                    1,
                    "TRUE",
                    "long_status",
                    "personal",
                    "",
                    "Обед",
                    30,
                    "",
                    "",
                    1800,
                    "FALSE",
                    "⏰ Длительный статус: <b>{status}</b> уже <b>{duration_min} мин</b> (порог {min_duration_min} мин).",
                ]
            ],
            value_input_option="USER_ENTERED",
        )


def load_rules() -> List[Rule]:
    api = SheetsAPI()
    ensure_sheet_exists(api)
    ws = api.client.open(GOOGLE_SHEET_NAME).worksheet(RULES_SHEET)
    rows = api._request_with_retry(ws.get_all_values) or []
    if not rows:
        return []
    hdr = [h.strip() for h in rows[0]]
    col = {name: hdr.index(name) for name in HEADER if name in hdr}
    rules: List[Rule] = []
    for r in rows[1:]:
        if not any(r):
            continue

        def at(name: str) -> str:
            i = col.get(name, -1)
            return r[i].strip() if 0 <= i < len(r) else ""

        try:
            rid = _to_int(at("ID")) or 0
            if rid <= 0:
                continue
            sts = [s.strip() for s in at("Statuses").split(",") if s.strip()]

            # --- normalize template ---
            tmpl = (at("MessageTemplate") or "").strip()
            # Sheets иногда превращает пустой шаблон в TRUE/FALSE,
            # а также админы могут случайно поставить "FALSE".
            if tmpl.upper() in ("TRUE", "FALSE"):
                tmpl = ""

            rules.append(
                Rule(
                    id=rid,
                    enabled=_to_bool(at("Enabled")),
                    kind=at("Kind").lower(),
                    scope=at("Scope").lower(),
                    group_tag=at("GroupTag"),
                    statuses=sts,
                    min_duration_min=_to_int(at("MinDurationMin")),
                    window_min=_to_int(at("WindowMin")),
                    limit=_to_int(at("Limit")),
                    rate_limit_sec=_to_int(at("RateLimitSec")) or 900,
                    silent=_to_bool(at("Silent")),
                    template=tmpl,
                )
            )
        except Exception as e:
            log.warning("Bad rule row: %s (%s)", r, e)
    return [r for r in rules if r.enabled]


def save_rules(rows: List[List[Any]]) -> None:
    """Сохранить полный набор правил (с заголовком) — используем из админ-панели."""
    api = SheetsAPI()
    ensure_sheet_exists(api)
    ws = api.client.open(GOOGLE_SHEET_NAME).worksheet(RULES_SHEET)
    # Очистим и запишем заново
    api._request_with_retry(ws.clear)
    out = [HEADER] + rows
    api._request_with_retry(ws.update, "A1", out)
