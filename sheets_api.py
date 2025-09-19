from __future__ import annotations

# sheets_api.py
import datetime as dt
import json
import logging
import os
import random
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo  # stdlib (Python 3.9+)

import gspread
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials

from config import GOOGLE_API_TIMEOUT
from consts import normalize_session_status
from telemetry import trace_time

logger = logging.getLogger(
    "sheets_api"
)  # никаких handlers здесь — конфиг только в приложении

__all__ = [
    "SheetsAPI",
    "SheetsAPIError",
    "get_sheets_api",
    "_LazySheetsAPI",
    "sheets_api",
]


@dataclass
class QuotaInfo:
    remaining: int
    reset_time: int
    daily_used: float


@dataclass
class WorklogWorksheetInfo:
    worksheet: Any
    header_to_col_index: dict[str, int]


class _TimeoutAuthorizedSession(AuthorizedSession):
    def __init__(self, credentials, *, timeout: float):
        super().__init__(credentials)
        self._default_timeout = max(1.0, float(timeout))

    def request(self, method, url, **kwargs):  # type: ignore[override]
        if "timeout" not in kwargs or kwargs["timeout"] is None:
            kwargs["timeout"] = self._default_timeout
        return super().request(method, url, **kwargs)


class SheetsAPIError(Exception):
    def __init__(self, message: str, is_retryable: bool = False, details: str = None):
        super().__init__(message)
        self.is_retryable = is_retryable
        self.details = details
        logger.error(
            f"SheetsAPIError: {message}\n"
            f"Retryable: {is_retryable}\n"
            f"Details: {details if details else 'None'}"
        )


def _normalize_group_name(group: str | None) -> str:
    """
    Приводит имя группы к аккуратному виду:
    - тримминг
    - убираем ведущие подчёркивания
    - если пусто — 'General'
    """
    g = (group or "").strip()
    if g.startswith("_"):
        g = g.lstrip("_")
    return g or "General"


def _compose_worklog_title(group: str | None) -> str:
    """
    Составляет корректное название вкладки WorkLog_<Group>
    с учётом того, что префикс уже содержит подчёркивание.
    """
    from config import WORKLOG_SHEET_PREFIX  # например, "WorkLog_"

    prefix = WORKLOG_SHEET_PREFIX.rstrip("_")  # "WorkLog"
    g = _normalize_group_name(group)
    return f"{prefix}_{g}"


def _safe_lower(s: str | None) -> str:
    return str(s or "").strip().lower()


def _fmt_local() -> str:
    import time

    return time.strftime("%Y-%m-%d %H:%M:%S")


# Заголовки WorkLog_* (как на скриншоте)
def _worklog_headers() -> list[str]:
    return [
        "Timestamp",
        "Email",
        "Action",
        "Status",
        "Group",
        "Start",
        "End",
        "Duration",
        "SessionID",
        "EventID",
        "GroupAtStart",
    ]


class SheetsAPI:
    """Обёртка над gspread с ретраями, кэшем и batch-операциями."""

    def __init__(self):
        with trace_time("sheets_api_init"):
            self._initialize()

    def _initialize(self):
        from config import get_credentials_file

        self._last_request_time = None
        self._sheet_cache: dict[str, Any] = {}
        self._session: AuthorizedSession | None = None
        self._quota_info = QuotaInfo(remaining=100, reset_time=60, daily_used=0.0)
        self._quota_lock = threading.Lock()
        try:
            logger.debug("=== SheetsAPI Initialization Debug ===")
            logger.debug(f"sys.frozen: {getattr(sys, 'frozen', False)}")
            logger.debug(f"sys._MEIPASS: {getattr(sys, '_MEIPASS', 'N/A')}")
            logger.debug(f"cwd: {os.getcwd()}")
            logger.debug(f"sys.path ok, len={len(sys.path)}")

            self.credentials_path = Path(get_credentials_file()).resolve()
            logger.info(f"Initializing with credentials: {self.credentials_path}")
            logger.debug(f"Credentials exists: {os.path.exists(self.credentials_path)}")
            if not self.credentials_path.exists():
                if getattr(sys, "frozen", False):
                    logger.error("Running in frozen mode but credentials not found!")
                raise FileNotFoundError(
                    f"Credentials file missing at: {self.credentials_path}"
                )
            self._init_client()
        except Exception as e:
            logger.critical("Initialization failed", exc_info=True)
            raise SheetsAPIError(
                "Google Sheets API initialization failed",
                is_retryable=False,
                details=str(e),
            ) from e

    # ---------- low-level client/bootstrap ----------

    def _init_client(self, max_retries: int = 3) -> None:
        for attempt in range(max_retries):
            try:
                logger.info(f"Client init attempt {attempt + 1}/{max_retries}")
                with open(self.credentials_path, encoding="utf-8") as f:
                    data = json.load(f)
                    required = {
                        "type",
                        "project_id",
                        "private_key_id",
                        "private_key",
                        "client_email",
                        "client_id",
                    }
                    if not required.issubset(data.keys()):
                        missing = required - set(data.keys())
                        raise ValueError(f"Missing fields in credentials: {missing}")

                scopes = [
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                ]
                credentials = Credentials.from_service_account_file(
                    str(self.credentials_path), scopes=scopes
                )
                timeout = GOOGLE_API_TIMEOUT
                self.client = gspread.client.Client(auth=credentials)
                session = _TimeoutAuthorizedSession(credentials, timeout=timeout)
                self.client.session = session
                if hasattr(self.client, "http_client") and hasattr(
                    self.client.http_client, "timeout"
                ):
                    self.client.http_client.timeout = timeout

                self._session = _TimeoutAuthorizedSession(credentials, timeout=timeout)

                self._test_connection()
                self._update_quota_info()
                logger.info("Google Sheets client initialized successfully")
                return
            except Exception as e:
                logger.error(f"Init attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    logger.critical("Client init failed after max attempts")
                    raise SheetsAPIError(
                        "Failed to initialize Google Sheets client",
                        is_retryable=True,
                        details=str(e),
                    ) from e
                wait = 2**attempt + 5
                logger.warning(f"Retrying in {wait} seconds...")
                time.sleep(wait)

    def _test_connection(self) -> None:
        try:
            logger.info("Testing API connection...")
            start = time.time()
            _ = list(self.client.list_spreadsheet_files())
            elapsed = time.time() - start
            logger.debug(f"API test OK in {elapsed:.2f}s")
            self._update_quota_info()
        except Exception as e:
            logger.error(f"API connection test failed: {e}")
            try:
                import urllib.request

                urllib.request.urlopen("https://www.google.com", timeout=5)
                logger.debug("Internet connection is available")
            except Exception:
                logger.error("No internet connection detected")
            raise SheetsAPIError(
                "Google Sheets API connection test failed",
                is_retryable=True,
                details=str(e),
            ) from e

    def _update_quota_info(self) -> None:
        try:
            resp = self._session.get(  # type: ignore[union-attr]
                "https://www.googleapis.com/drive/v3/about",
                params={"fields": "user,storageQuota"},
                timeout=10,
            )
            resp.raise_for_status()
            with self._quota_lock:
                self._quota_info.remaining = int(
                    resp.headers.get("x-ratelimit-remaining", 100)
                )
                self._quota_info.reset_time = int(
                    resp.headers.get("x-ratelimit-reset", 60)
                )
                self._quota_info.daily_used = float(
                    resp.json().get("storageQuota", {}).get("usage", 0) or 0.0
                )
            logger.debug(f"Quota updated: {self._quota_info}")
        except Exception as e:
            logger.warning(f"Failed to update quota info: {e}")
            with self._quota_lock:
                self._quota_info.remaining = max(1, self._quota_info.remaining)
                self._quota_info.reset_time = 60

    def _check_quota(self, required: int = 1) -> bool:
        with self._quota_lock:
            if self._quota_info.remaining >= required:
                return True
            wait_time = max(
                1,
                self._quota_info.reset_time - time.time() % self._quota_info.reset_time,
            )
            logger.warning(f"Quota low. Waiting {wait_time:.1f}s")
        time.sleep(wait_time + 1)
        self._update_quota_info()
        with self._quota_lock:
            return self._quota_info.remaining >= required

    def _check_rate_limit(self, delay: float) -> None:
        if self._last_request_time:
            elapsed = time.time() - self._last_request_time
            if elapsed < delay:
                wait = delay - elapsed
                logger.debug(f"Rate limit: waiting {wait:.2f}s")
                time.sleep(wait)
        self._last_request_time = time.time()

    def _coerce_values(self, values):
        """
        Нормализует значения для передачи в Google Sheets API.
        Превращает одиночную строку/число в [[...]], список строк в [list], список списков в [list[list]].
        """
        if values is None:
            return [[]]
        if not isinstance(values, (list, tuple)):
            return [[values]]
        # list -> может быть список-строк (одна строка) или список-списков
        if values and not isinstance(values[0], (list, tuple)):
            return [list(values)]
        return [list(row) for row in values]

    def _request_with_retry(self, func, *args, **kwargs):
        from config import API_DELAY_SECONDS, API_MAX_RETRIES, GOOGLE_API_LIMITS

        last_exc: Exception | None = None
        for attempt in range(API_MAX_RETRIES):
            try:
                if not self._check_quota(required=1):
                    raise SheetsAPIError("Insufficient API quota", is_retryable=True)
                self._check_rate_limit(API_DELAY_SECONDS)
                name = getattr(func, "__name__", "<callable>")
                logger.debug(f"Attempt {attempt + 1}: {name}")
                result = func(*args, **kwargs)
                with self._quota_lock:
                    self._quota_info.remaining = max(0, self._quota_info.remaining - 1)
                return result
            except Exception as e:
                last_exc = e
                msg = str(e).lower()

                # Классификация ошибок
                is_format_error = any(
                    x in msg
                    for x in (
                        "invalid value at 'data.values'",
                        "invalid value at 'values'",
                        "invalid json payload",
                        "bad request",
                    )
                )

                # 429/5xx/сетевые — повторимые, ошибки формата — нет
                retryable = not is_format_error and any(
                    x in msg
                    for x in (
                        "rate limit",
                        "quota",
                        "429",
                        "timeout",
                        "temporarily",
                        "unavailable",
                        "socket",
                        "503",
                        "500",
                        "502",
                    )
                )

                if is_format_error:
                    logger.error(f"Invalid payload format for Sheets API: {e}")
                    raise SheetsAPIError(
                        f"Invalid data format for Google Sheets API: {e}",
                        is_retryable=False,
                        details="Check that all values are properly formatted strings/numbers",
                    ) from e

                if attempt == API_MAX_RETRIES - 1 or not retryable:
                    logger.error(f"Request failed after {API_MAX_RETRIES} attempts")
                    if isinstance(e, SheetsAPIError):
                        raise
                    raise SheetsAPIError(
                        f"API request failed: {e}",
                        is_retryable=retryable,
                        details=str(e),
                    ) from e

                # Full jitter: base * 2^n + random(0..base)
                base = max(1.0, float(API_DELAY_SECONDS))
                wait = base * (2**attempt)
                wait = wait + random.uniform(0, base)
                # мягкая нормализация под минутный лимит
                per_min = max(1, GOOGLE_API_LIMITS.get("max_requests_per_minute", 60))
                min_gap = 60.0 / per_min
                wait = max(wait, min_gap)
                logger.warning(
                    f"Retry {attempt + 1}/{API_MAX_RETRIES} in {wait:.2f}s (error: {e})"
                )
                time.sleep(wait)
        raise last_exc or Exception("Unknown request error")

    # ---------- compatibility health-checks ----------

    def check_credentials(self) -> bool:
        """
        Backward-compatible health check, used at app startup.
        1) Проверяем наличие и структуру локального service_account.json.
        2) Если интернет доступен — делаем быстрый API-пинг.
        Возвращаем True/False, не выбрасываем наружу исключений.
        """
        try:
            if not getattr(self, "credentials_path", None):
                logger.error("credentials_path is not set")
                return False
            if not self.credentials_path.exists():
                logger.error(f"Credentials file not found: {self.credentials_path}")
                return False
            with open(self.credentials_path, encoding="utf-8") as f:
                data = json.load(f)
            required = {
                "type",
                "project_id",
                "private_key_id",
                "private_key",
                "client_email",
                "client_id",
            }
            missing = required - set(data.keys())
            if missing:
                logger.error(f"Missing fields in credentials: {missing}")
                return False

            # Онлайновая проверка — только если сеть есть
            try:
                from sync.network import is_internet_available

                online = bool(is_internet_available())
            except Exception:
                online = True

            if online:
                try:
                    self._test_connection()  # бросит SheetsAPIError при проблемах
                except Exception as e:
                    logger.error(f"API connection test failed: {e}")
                    return False
            return True
        except Exception as e:
            logger.error(f"Credentials check failed: {e}", exc_info=True)
            return False

    def test_connection(self) -> bool:
        """Булева обёртка над _test_connection()."""
        try:
            self._test_connection()
            return True
        except Exception as e:
            logger.error(f"test_connection failed: {e}")
            return False

    # ---------- timezone helpers ----------

    def _get_tz(self):
        """
        Возвращает часовой пояс:
        1) config.APP_TIMEZONE или переменная окружения APP_TIMEZONE (например, 'Europe/Moscow')
        2) при ошибке — системный локальный TZ (datetime.now().astimezone().tzinfo)
        3) при отсутствии — UTC
        """
        try:
            try:
                from config import APP_TIMEZONE  # опционально

                tz_name = APP_TIMEZONE or os.getenv("APP_TIMEZONE", "Europe/Moscow")
            except Exception:
                tz_name = os.getenv("APP_TIMEZONE", "Europe/Moscow")
            try:
                return ZoneInfo(tz_name)
            except Exception:
                local_tz = dt.datetime.now().astimezone().tzinfo
                if local_tz:
                    logger.warning(
                        f"ZoneInfo('{tz_name}') unavailable; using system local TZ"
                    )
                    return local_tz
                logger.warning(f"ZoneInfo('{tz_name}') unavailable; fallback to UTC")
                return dt.UTC
        except Exception:
            return dt.UTC

    def _fmt_local(self, moment: dt.datetime | None = None) -> str:
        """
        Возвращает строку 'YYYY-MM-DD HH:MM:SS' в локальном TZ (для корректного парсинга в Google Sheets).
        """
        tz = self._get_tz()
        if moment is None:
            moment = dt.datetime.now(tz)
        else:
            if moment.tzinfo is None:
                # считаем вход как UTC-метку без tzinfo
                moment = moment.replace(tzinfo=dt.UTC)
            moment = moment.astimezone(tz)
        return moment.strftime("%Y-%m-%d %H:%M:%S")

    def _ensure_local_str(self, ts: str | None) -> str:
        """
        Принимает ISO-строку (в т.ч. ...Z или +00:00), возвращает локальную строку для Sheets.
        Если не удаётся распарсить — возвращает исходное значение.
        """
        if not ts:
            return self._fmt_local()
        try:
            moment = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return self._fmt_local(moment)
        except Exception:
            return ts

    # ---------- worksheet cache + discovery ----------

    def get_worksheet(self, sheet_name: str):
        from config import GOOGLE_SHEET_NAME

        if sheet_name not in self._sheet_cache:
            try:
                logger.debug(f"Opening spreadsheet: {GOOGLE_SHEET_NAME}")
                spreadsheet = self._request_with_retry(
                    self.client.open, GOOGLE_SHEET_NAME
                )
                logger.debug(f"Caching worksheet: {sheet_name}")
                self._sheet_cache[sheet_name] = self._request_with_retry(
                    spreadsheet.worksheet, sheet_name
                )
                logger.info(f"Worksheet '{sheet_name}' cached")
            except Exception as e:
                logger.error(f"Failed to access worksheet '{sheet_name}': {e}")
                try:
                    sheets = [ws.title for ws in spreadsheet.worksheets()]  # type: ignore[UnboundLocalVariable]
                    logger.debug(f"Available worksheets: {sheets}")
                except Exception:
                    pass
                raise SheetsAPIError(
                    f"Worksheet access error: {sheet_name}",
                    is_retryable=True,
                    details=str(e),
                ) from e
        return self._sheet_cache[sheet_name]

    def _get_ws(self, name: str):
        """Единая точка доступа к листам (через кэш)."""
        return self.get_worksheet(name)

    def list_worksheet_titles(self) -> list[str]:
        """Список названий листов книги без лишних ошибков в логах."""
        from config import GOOGLE_SHEET_NAME

        spreadsheet = self._request_with_retry(self.client.open, GOOGLE_SHEET_NAME)
        sheets = self._request_with_retry(spreadsheet.worksheets)
        return [ws.title for ws in sheets]

    def has_worksheet(self, name: str) -> bool:
        """Проверяет, существует ли лист в книге."""
        try:
            titles = self.list_worksheet_titles()
            return name in titles
        except Exception as e:
            logger.error(f"has_worksheet failed for '{name}': {e}")
            return False

    # ---------- helpers for WorkLog sheets ----------
    def _resolve_worklog_sheet_name(self, group: str | None) -> str:
        """
        Возвращает корректное имя WorkLog-вкладки по группе:
        - убирает ведущие '_' у группы
        - формирует 'WorkLog_<Group>'
        - без двойных подчёркиваний
        """
        from config import WORKLOG_SHEET_PREFIX

        g = _normalize_group_name(group)  # e.g. "Почта"
        prefix = WORKLOG_SHEET_PREFIX.rstrip("_")  # "WorkLog"
        return f"{prefix}_{g}"

    def get_or_create_worklog_ws(self, group: str | None) -> WorklogWorksheetInfo:
        """Возвращает WorkLog worksheet c гарантированным набором заголовков."""
        from config import AUTOCREATE_WORKLOG_SHEET, GOOGLE_SHEET_NAME, WORKLOG_HEADERS

        desired_headers = list(WORKLOG_HEADERS)
        name = self._resolve_worklog_sheet_name(group)
        created = False

        if self.has_worksheet(name):
            ws = self._get_ws(name)
        else:
            if not AUTOCREATE_WORKLOG_SHEET:
                raise SheetsAPIError(
                    f"WorkLog worksheet '{name}' not found and autocreate disabled",
                    is_retryable=False,
                )
            spreadsheet = self._request_with_retry(self.client.open, GOOGLE_SHEET_NAME)
            ws = self._request_with_retry(
                spreadsheet.add_worksheet,
                title=name,
                rows=1000,
                cols=max(len(desired_headers), 20),
            )
            header_range = f"A1:{self._num_to_a1_col(len(desired_headers))}1"
            self._request_with_retry(ws.update, header_range, [desired_headers])
            self._sheet_cache[name] = ws
            created = True

        try:
            raw_header = self._request_with_retry(ws.row_values, 1)
        except Exception:
            raw_header = []

        header_row = [str(cell).strip() for cell in raw_header]
        header_changed = False

        if not header_row:
            header_row = desired_headers.copy()
            header_changed = True
        else:
            existing_map = {h: idx for idx, h in enumerate(header_row, start=1) if h}
            for header in desired_headers:
                if header not in existing_map:
                    header_row.append(header)
                    existing_map[header] = len(header_row)
                    header_changed = True

        if header_changed and header_row:
            header_range = f"A1:{self._num_to_a1_col(len(header_row))}1"
            self._request_with_retry(ws.update, header_range, [header_row])
            if created:
                logger.info(f"WorkLog headers created for {name}")
            else:
                logger.info(f"WorkLog headers updated for {name}")
        else:
            logger.info(f"WorkLog headers verified for {name}")

        header_to_col = {
            str(header).strip(): idx
            for idx, header in enumerate(header_row, start=1)
            if str(header).strip()
        }
        return WorklogWorksheetInfo(worksheet=ws, header_to_col_index=header_to_col)

    def _ensure_worklog_worksheet(self, group: str | None):
        info = self.get_or_create_worklog_ws(group)
        return info.worksheet

    # ---------- header/table parsing ----------

    def _header_map(self, ws) -> dict[str, int]:
        """
        Возвращает словарь {lower(header): column_index (1-based)}.
        Если заголовков нет — пустой словарь.
        """
        try:
            headers = self._request_with_retry(ws.row_values, 1)
            return {str(h).strip().lower(): i + 1 for i, h in enumerate(headers) if h}
        except Exception as e:
            logger.warning(f"Failed to get headers: {e}")
            return {}

    def _read_table(self, ws) -> list[dict[str, str]]:
        """
        Читает всю таблицу (кроме заголовков) и возвращает список словарей.
        Если таблица пустая — возвращает пустой список.
        """
        try:
            data = self._request_with_retry(ws.get_all_values)
            if not data:
                return []
            headers = [str(h).strip() for h in data[0]]
            rows = []
            for row in data[1:]:
                if not any(row):  # пропускаем пустые строки
                    continue
                row_dict = {}
                for i, val in enumerate(row):
                    if i < len(headers) and headers[i]:
                        row_dict[headers[i]] = val
                rows.append(row_dict)
            return rows
        except Exception as e:
            logger.error(f"Failed to read table: {e}")
            raise SheetsAPIError(
                "Failed to read table data", is_retryable=True, details=str(e)
            ) from e

    # ---------- domain helpers ----------

    def get_user_by_email(self, email: str) -> dict[str, str] | None:
        """
        Ищет пользователя в листе Users (регистронезависимо по колонке Email).
        Возвращает словарь со значениями + удобные алиасы ('name', 'group').
        """
        from config import USERS_SHEET

        try:
            ws = self._get_ws(USERS_SHEET)
            table = self._read_table(ws)
            em = (email or "").strip().lower()
            for row in table:
                if (row.get("Email", "") or "").strip().lower() == em:
                    out: dict[str, str] = {}
                    for k, v in row.items():
                        out[k] = v
                        out[k.strip().lower()] = v
                    out.setdefault("name", row.get("Name") or row.get("ФИО") or "")
                    out.setdefault("group", row.get("Group") or row.get("Группа") or "")
                    return out
            return None
        except Exception as e:
            logger.error(f"get_user_by_email failed: {e}")
            return None

    def _find_rows_by_email(self, ws, email: str) -> list[tuple[int, dict[str, str]]]:
        """
        Возвращает [(row_index, row_dict), ...] для заданного email.
        """
        table = self._read_table(ws)
        em = (email or "").strip().lower()
        out: list[tuple[int, dict[str, str]]] = []
        for idx, row in enumerate(table, start=2):
            row_email = (row.get("Email", "") or "").strip().lower()
            if row_email == em:
                out.append((idx, row))
        return out

    def get_active_session(self, email: str) -> dict[str, str] | None:
        """Возвращает последнюю активную сессию пользователя из ActiveSessions."""
        from config import ACTIVE_SESSIONS_SHEET

        ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
        rows = self._find_rows_by_email(ws, email)
        if not rows:
            return None

        # по возможности выбираем по максимальному LoginTime (как строка в одинаковом формате)
        def sort_key(t):  # (row_idx, row_dict)
            idx, row = t
            return ((row.get("LoginTime") or "").strip(), idx)

        row_idx, row = sorted(rows, key=sort_key)[-1]
        row["__row__"] = str(row_idx)
        return row

    def set_active_session(
        self,
        email: str,
        name: str,
        session_id: str,
        login_time: str | None = None,
        group: str | None = None,
    ) -> bool:
        """Добавляет запись в ActiveSessions со статусом 'active'."""
        from config import ACTIVE_SESSIONS_SHEET

        try:
            ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
            hmap = self._header_map(ws)  # lower-name -> 1-based index
            inv = {v: k for k, v in hmap.items()}
            payload = {
                "email": email,
                "name": name or "",
                "sessionid": session_id,
                "logintime": self._ensure_local_str(login_time),
                "logouttime": "",
                "status": "active",
                "remotecommand": "",
                "group": group or "",
            }
            maxcol = max(hmap.values()) if hmap else 0
            row = []
            for i in range(1, maxcol + 1):
                key = inv.get(i, "")
                row.append(payload.get(key, ""))
            self._request_with_retry(
                ws.append_row, row, value_input_option="USER_ENTERED"
            )
            logger.info("Active session appended")
            return True
        except Exception as e:
            logger.error(f"set_active_session failed: {e}")
            return False

    def heartbeat_session(
        self, session_id: str, ts_utc: dt.datetime | None = None
    ) -> None:
        """Обновляет поле LastPing для активной сессии."""

        from config import ACTIVE_SESSIONS_SHEET

        sid = str(session_id or "").strip()
        if not sid:
            return

        try:
            ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
            header_map = self._header_map(ws)

            session_col = header_map.get("sessionid")
            status_col = header_map.get("status")
            last_ping_col = header_map.get("lastping")

            if not session_col:
                logger.warning("heartbeat: SessionID column not found")
                return

            if not last_ping_col:
                new_col = (max(header_map.values()) if header_map else 0) + 1
                self._request_with_retry(ws.update_cell, 1, new_col, "LastPing")
                header_map = self._header_map(ws)
                last_ping_col = header_map.get("lastping")

            if not last_ping_col:
                logger.warning("heartbeat: cannot create LastPing column")
                return

            data = self._request_with_retry(ws.get_all_values)
            if not data or len(data) < 2:
                return

            active_statuses = {"active", "в работе"}
            timestamp = self._fmt_iso_utc(
                self._as_utc_datetime(ts_utc) or dt.datetime.now(dt.UTC)
            )

            for row_idx, row in enumerate(data[1:], start=2):
                if session_col > len(row):
                    continue
                row_sid = (row[session_col - 1] or "").strip()
                if row_sid != sid:
                    continue

                if status_col and status_col <= len(row):
                    status_value = (row[status_col - 1] or "").strip().lower()
                    if status_value and status_value not in active_statuses:
                        return

                self._request_with_retry(
                    ws.update_cell, row_idx, last_ping_col, timestamp
                )
                logger.info("heartbeat ok (session=%s)", sid)
                return

            logger.debug("heartbeat: session %s not found", sid)
        except Exception as e:
            logger.warning("heartbeat failed for session %s: %s", sid, e)

    def reap_stale_sessions(self, max_idle_minutes: int | None = None) -> int:
        """Завершает сессии без пинга дольше порога."""

        from config import ACTIVE_SESSIONS_SHEET, STALE_SESSION_MINUTES

        try:
            if max_idle_minutes is None:
                idle_minutes = int(STALE_SESSION_MINUTES)
            else:
                idle_minutes = int(max_idle_minutes)
        except (TypeError, ValueError):
            idle_minutes = int(STALE_SESSION_MINUTES)

        idle_minutes = max(0, idle_minutes)
        cutoff = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=idle_minutes)

        try:
            ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
            data = self._request_with_retry(ws.get_all_values)
        except Exception as e:
            logger.error("reap_stale_sessions failed to read sheet: %s", e)
            return 0

        if not data or len(data) < 2:
            return 0

        headers = [str(h).strip() for h in data[0]]
        header_map = {
            header.lower(): idx + 1 for idx, header in enumerate(headers) if header
        }

        session_col = header_map.get("sessionid")
        email_col = header_map.get("email")
        status_col = header_map.get("status")
        login_col = header_map.get("logintime") or header_map.get("login time")
        last_ping_col = header_map.get("lastping")

        if not session_col or not email_col:
            logger.warning("reap_stale_sessions: required columns missing")
            return 0

        active_statuses = {"active", "в работе"}
        closed = 0

        for row in data[1:]:
            if not row:
                continue

            status_ok = True
            if status_col and status_col <= len(row):
                status_value = (row[status_col - 1] or "").strip().lower()
                if status_value and status_value not in active_statuses:
                    status_ok = False
            if not status_ok:
                continue

            email_value = ""
            if email_col <= len(row):
                email_value = (row[email_col - 1] or "").strip()
            session_value = ""
            if session_col <= len(row):
                session_value = (row[session_col - 1] or "").strip()

            if not email_value or not session_value:
                continue

            last_ping_value = ""
            if last_ping_col and last_ping_col <= len(row):
                last_ping_value = (row[last_ping_col - 1] or "").strip()

            login_value = ""
            if login_col and login_col <= len(row):
                login_value = (row[login_col - 1] or "").strip()

            last_ping_dt = self._as_utc_datetime(last_ping_value)
            login_dt = self._as_utc_datetime(login_value)

            is_stale = False
            if last_ping_dt:
                is_stale = last_ping_dt < cutoff
            elif login_dt:
                is_stale = login_dt < cutoff
            else:
                is_stale = True

            if not is_stale:
                continue

            try:
                if self.finish_active_session(
                    email=email_value,
                    session_id=session_value,
                    reason="FORCE_LOGOUT (stale)",
                ):
                    closed += 1
            except Exception as e:
                logger.warning(
                    "reap: failed to close session %s: %s",
                    session_value,
                    e,
                )

        logger.info("reap: force-closed %s stale sessions.", closed)
        return closed

    def finish_active_session(
        self,
        email: str,
        session_id: str,
        logout_time: str | None = None,
        *,
        reason: str | None = None,
    ) -> bool:
        with trace_time("finish_active_session"):
            return self._finish_active_session_impl(
                email=email,
                session_id=session_id,
                logout_time=logout_time,
                reason=reason,
            )

    def _finish_active_session_impl(
        self,
        *,
        email: str,
        session_id: str,
        logout_time: str | None = None,
        reason: str | None = None,
    ) -> bool:
        """
        Надёжно завершает активную сессию:
        1) Пытается найти точный матч по Email+SessionID (безусловно обновляет статус/время).
        2) Если не найден — берёт последнюю строку по email со Status=='active' и завершает её.
        """
        from config import ACTIVE_SESSIONS_SHEET

        try:
            ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
            table = self._read_table(ws)
            em = (email or "").strip().lower()
            sid = str(session_id or "").strip()
            hmap = self._header_map(ws)  # lower -> index

            def col(name: str) -> int | None:
                return hmap.get(name.lower())

            c_status = col("Status") or col("status")
            c_logout = col("LogoutTime") or col("logouttime") or col("logout time")
            if not (c_status and c_logout):
                raise SheetsAPIError("ActiveSessions headers missing Status/LogoutTime")

            logout_dt_obj = self._as_utc_datetime(logout_time)
            if logout_dt_obj is None:
                logout_dt_obj = dt.datetime.now(dt.UTC)
                lt_source = self._fmt_iso_utc(logout_dt_obj)
            else:
                lt_source = logout_time or self._fmt_iso_utc(logout_dt_obj)

            lt = self._ensure_local_str(lt_source)

            status_value = "finished"
            worklog_status = "LOGOUT"
            action_note = None
            if reason:
                status_value = "FORCE_LOGOUT"
                worklog_status = "FORCE_LOGOUT"
                action_note = reason

            def apply_update(row_idx: int, row_dict: dict[str, str]) -> bool:
                cols = sorted([c_status, c_logout])
                left = self._num_to_a1_col(cols[0])
                right = self._num_to_a1_col(cols[-1])
                rng = f"{left}{row_idx}:{right}{row_idx}"
                buf = [""] * (cols[-1] - cols[0] + 1)
                buf[c_status - cols[0]] = status_value
                buf[c_logout - cols[0]] = lt
                self._request_with_retry(lambda: ws.update(rng, [buf]))
                self._update_worklog_logout(
                    email=email,
                    session_id=sid,
                    logout_dt=logout_dt_obj,
                    active_row=row_dict,
                    status_value=worklog_status,
                    action_note=action_note,
                )
                return True

            exact_match = next(
                (
                    (i, r)
                    for i, r in enumerate(table, start=2)
                    if (r.get("Email", "") or "").strip().lower() == em
                    and str(r.get("SessionID", "")).strip() == sid
                ),
                None,
            )
            if exact_match:
                row_idx, row_dict = exact_match
                return apply_update(row_idx, row_dict)

            candidates = [
                (i, r)
                for i, r in enumerate(table, start=2)
                if (r.get("Email", "") or "").strip().lower() == em
                and (r.get("Status", "") or "").strip().lower() == "active"
            ]
            if not candidates:
                logger.warning(
                    "finish_active_session: no active rows for email=%s, sid=%s",
                    email,
                    session_id,
                )
                return False

            def sort_key(item):
                idx, row = item
                return ((row.get("LoginTime") or "").strip(), idx)

            row_idx, row_dict = sorted(candidates, key=sort_key)[-1]
            return apply_update(row_idx, row_dict)
        except Exception as e:
            logger.error(f"finish_active_session failed: {e}")
            return False

    def kick_active_session(self, *args, **kwargs) -> bool:
        """Принудительно завершает активную сессию."""

        reason = kwargs.pop("reason", None)
        status_override = kwargs.pop("status", None)
        logout_time = kwargs.pop("logout_time", None)
        email_kw = kwargs.pop("email", None)
        session_kw = kwargs.pop("session_id", None)

        if kwargs:
            raise TypeError("kick_active_session received unexpected keyword arguments")

        email = email_kw
        session_id = session_kw

        if len(args) == 1 and session_id is None and email is None:
            session_id = args[0]
        elif len(args) == 2 and session_id is None and email is None:
            email, session_id = args
        elif len(args) == 3 and session_id is None and email is None:
            email, session_id, logout_time = args
        elif len(args) > 3:
            raise TypeError(
                "kick_active_session expects at most 3 positional arguments"
            )

        sid = str(session_id or "").strip()
        if email:
            return self._update_session_status(
                email,
                sid,
                status_override or "kicked",
                logout_time,
            )

        if not sid:
            return False

        reason_value = reason or "FORCE_LOGOUT (admin)"

        try:
            from config import ACTIVE_SESSIONS_SHEET

            ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
            data = self._request_with_retry(ws.get_all_values)
            if not data or len(data) < 2:
                return False

            headers = [str(h).strip() for h in data[0]]
            header_map = {
                header.lower(): idx + 1 for idx, header in enumerate(headers) if header
            }

            session_col = header_map.get("sessionid")
            email_col = header_map.get("email")
            status_col = header_map.get("status")

            if not session_col or not email_col:
                return False

            email_value = ""
            for row in data[1:]:
                if session_col > len(row):
                    continue
                row_sid = (row[session_col - 1] or "").strip()
                if row_sid != sid:
                    continue
                if status_col and status_col <= len(row):
                    status_value = (row[status_col - 1] or "").strip().lower()
                    if status_value and status_value not in {"active", "в работе"}:
                        return False
                if email_col <= len(row):
                    email_value = (row[email_col - 1] or "").strip()
                break

            if not email_value:
                logger.warning("kick: session %s not found", sid)
                return False

            ok = self.finish_active_session(
                email=email_value,
                session_id=sid,
                logout_time=logout_time,
                reason=reason_value,
            )
            if ok:
                logger.info("kick: session %s closed by admin.", sid)
            return ok
        except Exception as e:
            logger.error("kick_active_session failed for %s: %s", sid, e)
            return False

    def _update_session_status(
        self, email: str, session_id: str, status: str, logout_time: str | None
    ) -> bool:
        from config import ACTIVE_SESSIONS_SHEET

        try:
            ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
            table = self._read_table(ws)
            em = (email or "").strip().lower()
            sid = str(session_id or "").strip()
            hmap = self._header_map(ws)
            for row_idx, row in enumerate(table, start=2):
                if (row.get("Email", "") or "").strip().lower() == em and str(
                    row.get("SessionID", "")
                ).strip() == sid:
                    updates = {
                        "status": status,
                        "logouttime": self._ensure_local_str(logout_time),
                    }
                    for k, v in updates.items():
                        col = hmap.get(k)
                        if col:
                            self._request_with_retry(ws.update_cell, row_idx, col, v)
                    return True
            logger.warning(
                f"_update_session_status: not found email={email} sid={session_id}"
            )
            return False
        except Exception as e:
            logger.error(f"_update_session_status failed: {e}")
            return False

    def check_user_session_status(self, email: str, session_id: str) -> str | None:
        """Возвращает нормализованный статус сессии или None."""

        from config import ACTIVE_SESSIONS_SHEET

        with trace_time("check_user_session_status"):
            sid = str(session_id or "").strip()
            if not sid:
                return None

            ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
            table = self._read_table(ws)
            em = (email or "").strip().lower()
            fallback_row: dict[str, Any] | None = None
            fallback_row_idx: int | None = None

            for row_idx, row in enumerate(table, start=2):
                row_sid = str(row.get("SessionID", "") or "").strip()
                if row_sid == sid:
                    status_value = normalize_session_status(row.get("Status"))
                    logger.info(
                        "ActiveSessions status for session=%s -> %s",
                        sid,
                        status_value or "<unknown>",
                    )
                    return status_value
                if em and (row.get("Email", "") or "").strip().lower() == em:
                    fallback_row = row
                    fallback_row_idx = row_idx

            if fallback_row:
                status_value = normalize_session_status(fallback_row.get("Status"))
                logger.info(
                    "ActiveSessions status for session=%s (email=%s row=%s) -> %s",
                    sid,
                    em or "<unknown>",
                    fallback_row_idx if fallback_row_idx is not None else "<unknown>",
                    status_value or "<unknown>",
                )
                return status_value

            logger.info("ActiveSessions status for session=%s -> not found", sid)
            return None

    def ack_remote_command(self, email: str, session_id: str) -> bool:
        """Очищает RemoteCommand в ActiveSessions по (email, session_id)."""
        from config import ACTIVE_SESSIONS_SHEET

        try:
            ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
            table = self._read_table(ws)
            em = (email or "").strip().lower()
            sid = str(session_id or "").strip()
            hmap = self._header_map(ws)
            col = hmap.get("remotecommand")
            if not col:
                return True
            for row_idx, row in enumerate(table, start=2):
                if (row.get("Email", "") or "").strip().lower() == em and str(
                    row.get("SessionID", "")
                ).strip() == sid:
                    self._request_with_retry(ws.update_cell, row_idx, col, "")
                    return True
            return False
        except Exception as e:
            logger.error(f"ack_remote_command failed: {e}")
            return False

    # ---------- ActiveSessions — helpers (унифицированный контракт) ----------

    def get_all_active_sessions(self) -> list[dict[str, str]]:
        """
        Вернуть все строки из ActiveSessions со статусом 'active' (в нижнем регистре).
        Без побочных эффектов, только чтение.
        """
        from config import ACTIVE_SESSIONS_SHEET

        try:
            ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
            table = self._read_table(ws)
        except Exception as e:
            logger.error("get_all_active_sessions failed: %s", e)
            return []

        out = []
        for r in table:
            st = str(r.get("Status", "")).strip().lower()
            if st == "active":
                out.append(r)
        return out

    def get_active_session_by_email(self, email: str) -> dict[str, str] | None:
        """
        Вернуть ПОСЛЕДНЮЮ активную сессию пользователя по email, если есть.
        Считаем "последней" — с максимальным LoginTime (как строка).
        """
        from config import ACTIVE_SESSIONS_SHEET

        ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
        table = self._read_table(ws)
        em = (email or "").strip().lower()
        candidates = [
            r
            for r in table
            if (r.get("Email", "") or "").strip().lower() == em
            and (r.get("Status", "") or "").strip().lower() == "active"
        ]
        if not candidates:
            return None
        # сортируем по LoginTime (строка в формате YYYY-MM-DD HH:MM:SS)
        return sorted(candidates, key=lambda r: (r.get("LoginTime") or "").strip())[-1]

    def get_remote_command(self, email: str, session_id: str) -> str | None:
        """
        Возвращает RemoteCommand для (email, session_id) или None.
        """
        from config import ACTIVE_SESSIONS_SHEET

        ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
        table = self._read_table(ws)
        em = (email or "").strip().lower()
        sid = str(session_id or "").strip()
        for r in table:
            if (r.get("Email", "") or "").strip().lower() == em and str(
                r.get("SessionID", "")
            ).strip() == sid:
                return r.get("RemoteCommand") or r.get("remotecommand")
        return None

    # ---------- WorkLog logging ----------
    def log_user_actions(
        self,
        email: str,
        action: str,
        status: str,
        group: str | None,
        *,
        timestamp_utc: dt.datetime | str,
        start_utc: dt.datetime | str | None = None,
        end_utc: dt.datetime | str | None = None,
        session_id: str | None = None,
        event_id: str | None = None,
        group_at_start: str | None = None,
    ) -> str:
        """Добавляет запись в WorkLog_<Group> с гарантированными колонками."""

        try:
            event_id = event_id or str(uuid4())
            timestamp_dt = self._as_utc_datetime(timestamp_utc) or dt.datetime.now(
                dt.UTC
            )
            start_dt = self._as_utc_datetime(start_utc)
            end_dt = self._as_utc_datetime(end_utc)

            duration = ""
            if start_dt and end_dt:
                delta = end_dt - start_dt
                minutes = max(0, int(delta.total_seconds() // 60))
                duration = str(minutes)

            payload = {
                "Timestamp": self._fmt_iso_utc(timestamp_dt),
                "Email": (email or "").strip(),
                "Action": action or "",
                "Status": status or "",
                "Group": group or "",
                "Start": self._fmt_iso_utc(start_dt),
                "End": self._fmt_iso_utc(end_dt),
                "Duration": duration,
                "SessionID": session_id or "",
                "EventID": event_id,
                "GroupAtStart": group_at_start or group or "",
            }

            target_group = group or "Default"
            info = self.get_or_create_worklog_ws(target_group)
            headers_map = info.header_to_col_index
            max_col = max(headers_map.values()) if headers_map else len(payload)
            row = [""] * max_col

            for key, value in payload.items():
                col = headers_map.get(key)
                if not col:
                    continue
                if col > len(row):
                    row.extend([""] * (col - len(row)))
                row[col - 1] = value

            with trace_time("append_worklog"):
                self._request_with_retry(
                    info.worksheet.append_row, row, value_input_option="USER_ENTERED"
                )
            logger.info(
                f"WorkLog append: {event_id} (session={session_id or '-'}, group={target_group})"
            )
            return event_id
        except SheetsAPIError:
            raise
        except Exception as e:
            logger.error(f"log_user_actions failed: {e}")
            raise SheetsAPIError(
                "Failed to append WorkLog row", is_retryable=True, details=str(e)
            ) from e

    def _update_worklog_logout(
        self,
        *,
        email: str,
        session_id: str,
        logout_dt: dt.datetime | None,
        active_row: dict[str, str] | None,
        status_value: str = "LOGOUT",
        action_note: str | None = None,
    ) -> bool:
        """Обновляет End/Duration в WorkLog по session_id или создаёт LOGOUT-запись."""

        logout_dt = logout_dt or dt.datetime.now(dt.UTC)

        candidates: list[str | None] = []
        seen: set[str | None] = set()

        def add_candidate(value: str | None) -> None:
            key = (value or "").strip() or None
            if key in seen:
                return
            seen.add(key)
            candidates.append(key)

        if active_row:
            add_candidate(active_row.get("Group"))
            add_candidate(active_row.get("group"))

        # Попробуем выяснить группу пользователя через Users
        try:
            user = self.get_user_by_email(email)
            if user:
                add_candidate(user.get("group") or user.get("Group"))
        except Exception:
            pass

        add_candidate(None)  # fallback на Default

        best_match: tuple[WorklogWorksheetInfo, int, list[str]] | None = None

        for group in candidates:
            try:
                info = self.get_or_create_worklog_ws(group)
            except SheetsAPIError:
                continue

            header_map = info.header_to_col_index
            session_col = header_map.get("SessionID")
            if not session_col:
                continue

            end_col = header_map.get("End")
            action_col = header_map.get("Action")

            try:
                data = self._request_with_retry(info.worksheet.get_all_values)
            except Exception:
                continue

            if not data or len(data) < 2:
                continue

            fallback_login: tuple[int, list[str]] | None = None
            for idx, row in enumerate(data[1:], start=2):
                if session_col > len(row):
                    continue
                row_sid = (row[session_col - 1] or "").strip()
                if row_sid != session_id:
                    continue

                end_value = ""
                if end_col and end_col <= len(row):
                    end_value = (row[end_col - 1] or "").strip()

                action_value = ""
                if action_col and action_col <= len(row):
                    action_value = (row[action_col - 1] or "").strip().upper()

                if not end_value:
                    best_match = (info, idx, row)
                elif action_value == "LOGIN" and not best_match:
                    fallback_login = (idx, row)

            if best_match:
                break
            if fallback_login:
                best_match = (info, fallback_login[0], fallback_login[1])

        if best_match:
            info, row_idx, row_values = best_match
            header_map = info.header_to_col_index
            start_col = header_map.get("Start")
            duration_col = header_map.get("Duration")
            status_col = header_map.get("Status")

            start_value = ""
            if start_col and start_col <= len(row_values):
                start_value = row_values[start_col - 1]

            start_dt = self._as_utc_datetime(start_value)
            duration = ""
            if start_dt:
                delta = logout_dt - start_dt
                minutes = max(0, int(delta.total_seconds() // 60))
                duration = str(minutes)

            updates: dict[int, str] = {}
            end_col = header_map.get("End")
            if end_col:
                updates[end_col] = self._fmt_iso_utc(logout_dt)
            if duration_col:
                updates[duration_col] = duration
            if status_col:
                current_status = ""
                if status_col <= len(row_values):
                    current_status = (row_values[status_col - 1] or "").strip()
                if current_status.upper() != status_value.upper():
                    updates[status_col] = status_value

            if action_note and action_col:
                current_action = ""
                if action_col <= len(row_values):
                    current_action = (row_values[action_col - 1] or "").strip()
                base_action = current_action or "LOGOUT"
                note = action_note.strip()
                if note and note.lower() not in base_action.lower():
                    updates[action_col] = f"{base_action} ({note})"

            if not updates:
                return True

            full_row = list(row_values)
            max_index = max(updates)
            if len(full_row) < max_index:
                full_row.extend([""] * (max_index - len(full_row)))

            for col, value in updates.items():
                full_row[col - 1] = value

            left = min(updates)
            right = max(updates)
            segment = full_row[left - 1 : right]
            rng = f"{self._num_to_a1_col(left)}{row_idx}:{self._num_to_a1_col(right)}{row_idx}"
            self._request_with_retry(lambda: info.worksheet.update(rng, [segment]))
            logger.info(f"WorkLog update: End/Duration set (session={session_id})")
            return True

        # Если не нашли LOGIN-запись — создаём LOGOUT строку
        fallback_group = None
        for g in candidates:
            if g:
                fallback_group = g
                break

        login_start = None
        if active_row:
            login_start = active_row.get("LoginTime") or active_row.get("logintime")

        try:
            action_text = "LOGOUT"
            if action_note:
                note = action_note.strip()
                if note:
                    action_text = f"{action_text} ({note})"

            self.log_user_actions(
                email=email,
                action=action_text,
                status=status_value,
                group=fallback_group,
                timestamp_utc=logout_dt,
                start_utc=login_start,
                end_utc=logout_dt,
                session_id=session_id,
                group_at_start=fallback_group,
            )
            return True
        except SheetsAPIError:
            raise
        except Exception as e:
            logger.error(
                f"Failed to append fallback LOGOUT row for session={session_id}: {e}"
            )
            raise SheetsAPIError(
                "Failed to append fallback WorkLog logout row",
                is_retryable=True,
                details=str(e),
            ) from e

    def sort_worklog(
        self,
        group: str,
        *,
        scope: str = "today",
        by: list[str] | None = None,
        last_hours: int | None = None,
    ) -> None:
        info = self.get_or_create_worklog_ws(group)
        header_map = info.header_to_col_index
        if not header_map:
            logger.debug("sort_worklog skipped: no headers for group=%s", group)
            return

        columns = by or ["Start", "Timestamp"]
        sort_specs = []
        for column in columns:
            idx = header_map.get(column)
            if not idx:
                logger.debug(
                    "sort_worklog: column %s missing for group=%s", column, group
                )
                continue
            sort_specs.append({"dimensionIndex": idx - 1, "sortOrder": "ASCENDING"})
        if not sort_specs:
            logger.debug("sort_worklog skipped: no valid columns for group=%s", group)
            return

        range_indices = self._resolve_sort_range(
            info,
            scope=scope,
            last_hours=last_hours,
        )
        if not range_indices:
            return
        start_row_index, end_row_index = range_indices
        if end_row_index <= start_row_index:
            logger.debug(
                "sort_worklog skipped: empty range for group=%s (scope=%s)",
                group,
                scope,
            )
            return

        max_col = max(header_map.values())
        body = {
            "requests": [
                {
                    "sortRange": {
                        "range": {
                            "sheetId": info.worksheet.id,
                            "startRowIndex": start_row_index,
                            "endRowIndex": end_row_index,
                            "startColumnIndex": 0,
                            "endColumnIndex": max_col,
                        },
                        "sortSpecs": sort_specs,
                    }
                }
            ]
        }
        spreadsheet = info.worksheet.spreadsheet
        with trace_time("sort_worklog"):
            self._request_with_retry(spreadsheet.batch_update, body)

    def _resolve_sort_range(
        self,
        info: WorklogWorksheetInfo,
        *,
        scope: str,
        last_hours: int | None = None,
    ) -> tuple[int, int] | None:
        header_map = info.header_to_col_index
        if not header_map:
            return None
        max_col = max(header_map.values())
        data_range = f"A2:{self._num_to_a1_col(max_col)}"
        try:
            values = self._request_with_retry(info.worksheet.get_values, data_range)
        except Exception as exc:
            logger.debug(
                "sort_worklog: failed to fetch range for %s: %s",
                info.worksheet.title,
                exc,
            )
            return None
        if not values:
            return None

        start_row_index = 1  # skip header
        end_row_index = 1 + len(values)

        scope_value = (scope or "").strip().lower()
        if scope_value == "all":
            return start_row_index, end_row_index

        cutoff: dt.datetime | None = None
        if scope_value == "today":
            cutoff = dt.datetime.now(dt.UTC).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        elif scope_value == "lastnhours" and last_hours:
            cutoff = dt.datetime.now(dt.UTC) - dt.timedelta(
                hours=max(1, int(last_hours))
            )
        if cutoff is None:
            return start_row_index, end_row_index

        start_col = header_map.get("Start")
        timestamp_col = header_map.get("Timestamp")
        for offset, row in enumerate(values, start=2):
            row_dt = self._extract_row_datetime(row, start_col, timestamp_col)
            if row_dt and row_dt >= cutoff:
                return offset - 1, end_row_index

        # nothing to sort within the requested window
        return end_row_index, end_row_index

    def _extract_row_datetime(
        self,
        row: list[Any],
        start_col: int | None,
        timestamp_col: int | None,
    ) -> dt.datetime | None:
        candidates: list[dt.datetime] = []
        if start_col and start_col - 1 < len(row):
            candidate = self._as_utc_datetime(row[start_col - 1])
            if candidate:
                candidates.append(candidate)
        if timestamp_col and timestamp_col - 1 < len(row):
            candidate = self._as_utc_datetime(row[timestamp_col - 1])
            if candidate:
                candidates.append(candidate)
        if not candidates:
            return None
        return min(candidates)

    # ---------- utils ----------

    def _as_utc_datetime(self, value: dt.datetime | str | None) -> dt.datetime | None:
        """Преобразует ISO/локальную строку или datetime в UTC datetime."""

        if value is None:
            return None

        dt_obj: dt.datetime
        if isinstance(value, dt.datetime):
            dt_obj = value
        else:
            text = str(value).strip()
            if not text:
                return None
            try:
                dt_obj = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                try:
                    dt_obj = dt.datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return None

        if dt_obj.tzinfo is None:
            tz = self._get_tz()
            try:
                dt_obj = dt_obj.replace(tzinfo=tz)
            except Exception:
                dt_obj = dt_obj.replace(tzinfo=dt.UTC)
        return dt_obj.astimezone(dt.UTC)

    def _fmt_iso_utc(self, value: dt.datetime | None) -> str:
        if not value:
            return ""
        return value.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _num_to_a1_col(self, n: int) -> str:
        """Конвертирует номер колонки (1-based) в A1-нотацию (A, B, ..., Z, AA, ...)."""
        s = ""
        while n > 0:
            n, rem = divmod(n - 1, 26)
            s = chr(65 + rem) + s
        return s

    def clear_cache(self) -> None:
        """Очищает кэш листов (например, при смене книги)."""
        self._sheet_cache.clear()

    def __del__(self):
        try:
            if hasattr(self, "client"):
                self.client.session.close()  # type: ignore[attr-defined]
        except Exception:
            pass


# ---------- global singleton (lazy) ----------


class _LazySheetsAPI:
    _instance: SheetsAPI | None = None
    _lock = threading.Lock()

    def __init__(self):
        if self._instance is None:
            with self._lock:
                if self._instance is None:
                    self._instance = SheetsAPI()

    def __getattr__(self, name):
        return getattr(self._instance, name)

    def get_instance(self) -> SheetsAPI:
        return self._instance  # type: ignore[return-value]


sheets_api = _LazySheetsAPI()


def get_sheets_api() -> SheetsAPI:
    return sheets_api.get_instance()
