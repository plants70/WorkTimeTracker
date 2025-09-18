# sheets_api.py
import gspread
import time
import json
import sys
import os
import random
import logging
from datetime import datetime
from typing import Any
from pathlib import Path
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials
from dataclasses import dataclass
import threading
from zoneinfo import ZoneInfo  # stdlib (Python 3.9+)

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
        "Reason",
        "Comment",
        "Name",
    ]


class SheetsAPI:
    """Обёртка над gspread с ретраями, кэшем и batch-операциями."""

    def __init__(self):
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
                self.client = gspread.client.Client(auth=credentials)
                # gspread >=5
                self.client.session = AuthorizedSession(credentials)
                # На некоторых версиях http_client может отсутствовать — оставляем, как было у тебя
                if hasattr(self.client, "http_client") and hasattr(
                    self.client.http_client, "timeout"
                ):
                    self.client.http_client.timeout = 30

                self._session = AuthorizedSession(credentials)
                # У объекта AuthorizedSession нет атрибута timeout во всех версиях,
                # но если есть — выставим.
                try:
                    self._session.timeout = 30  # type: ignore[attr-defined]
                except Exception:
                    pass

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
        from config import API_MAX_RETRIES, API_DELAY_SECONDS, GOOGLE_API_LIMITS

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
                local_tz = datetime.now().astimezone().tzinfo
                if local_tz:
                    logger.warning(
                        f"ZoneInfo('{tz_name}') unavailable; using system local TZ"
                    )
                    return local_tz
                logger.warning(f"ZoneInfo('{tz_name}') unavailable; fallback to UTC")
                return datetime.UTC
        except Exception:
            return datetime.UTC

    def _fmt_local(self, dt: datetime | None = None) -> str:
        """
        Возвращает строку 'YYYY-MM-DD HH:MM:SS' в локальном TZ (для корректного парсинга в Google Sheets).
        """
        tz = self._get_tz()
        if dt is None:
            dt = datetime.now(tz)
        else:
            if dt.tzinfo is None:
                # считаем вход как UTC-метку без tzinfo
                dt = dt.replace(tzinfo=datetime.UTC)
            dt = dt.astimezone(tz)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def _ensure_local_str(self, ts: str | None) -> str:
        """
        Принимает ISO-строку (в т.ч. ...Z или +00:00), возвращает локальную строку для Sheets.
        Если не удаётся распарсить — возвращает исходное значение.
        """
        if not ts:
            return self._fmt_local()
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return self._fmt_local(dt)
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

    def _ensure_worklog_worksheet(self, group: str | None):
        """
        Возвращает открытую вкладку WorkLog_<Group>. Создаёт при необходимости (если включено в конфиге).
        """
        from config import AUTOCREATE_WORKLOG_SHEET, WORKLOG_HEADERS

        name = self._resolve_worklog_sheet_name(group)
        if self.has_worksheet(name):
            return self._get_ws(name)
        if not AUTOCREATE_WORKLOG_SHEET:
            raise SheetsAPIError(
                f"WorkLog worksheet '{name}' not found and autocreate disabled",
                is_retryable=False,
            )
        # создаём лист и пишем заголовок
        from config import GOOGLE_SHEET_NAME

        spreadsheet = self._request_with_retry(self.client.open, GOOGLE_SHEET_NAME)
        ws = self._request_with_retry(
            spreadsheet.add_worksheet, title=name, rows=1000, cols=len(WORKLOG_HEADERS)
        )
        # заголовок
        self._request_with_retry(
            ws.update, f"A1:{chr(ord('A')+len(WORKLOG_HEADERS)-1)}1", [WORKLOG_HEADERS]
        )
        # кешируем
        self._sheet_cache[name] = ws
        logger.info(f"Worksheet '{name}' created and cached")
        return ws

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

    def finish_active_session(
        self, email: str, session_id: str, logout_time: str | None = None
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

            # индексы с защитой разных регистров
            def col(name):
                return hmap.get(name.lower())

            c_status = col("Status") or col("status")
            c_logout = col("LogoutTime") or col("logouttime") or col("logout time")
            if not (c_status and c_logout):
                raise SheetsAPIError("ActiveSessions headers missing Status/LogoutTime")

            # 1) точный матч
            exact_idx = None
            for i, r in enumerate(table, start=2):
                if (r.get("Email", "") or "").strip().lower() == em and str(
                    r.get("SessionID", "")
                ).strip() == sid:
                    exact_idx = i
                    break
                else:
                    # краткий лог для дебага совпадений
                    if (r.get("Email", "") or "").strip().lower() == em:
                        logger.debug(
                            f"[finish_active_session] mismatch sid: row_sid={str(r.get('SessionID','')).strip()} wanted={sid}, row_status={(r.get('Status','') or '').strip()}"
                        )

            lt = self._ensure_local_str(logout_time)

            def apply_update(row_idx: int) -> None:
                cols = sorted([c_status, c_logout])
                left = self._num_to_a1_col(cols[0])
                right = self._num_to_a1_col(cols[-1])
                rng = f"{left}{row_idx}:{right}{row_idx}"
                buf = [""] * (cols[-1] - cols[0] + 1)
                buf[c_status - cols[0]] = "finished"
                buf[c_logout - cols[0]] = lt
                self._request_with_retry(lambda: ws.update(rng, [buf]))

            if exact_idx:
                apply_update(exact_idx)
                return True

            # 2) фоллбэк: последняя активная по email
            candidates = [
                (i, r)
                for i, r in enumerate(table, start=2)
                if (r.get("Email", "") or "").strip().lower() == em
                and (r.get("Status", "") or "").strip().lower() == "active"
            ]
            if not candidates:
                logger.warning(
                    f"finish_active_session: no active rows for email={email}, sid={session_id}"
                )
                return False

            # выбираем по максимальному LoginTime (как строка, формат единообразный)
            def sort_key(t):
                idx, row = t
                return ((row.get("LoginTime") or "").strip(), idx)

            row_idx, _ = sorted(candidates, key=sort_key)[-1]
            apply_update(row_idx)
            return True
        except Exception as e:
            logger.error(f"finish_active_session failed: {e}")
            return False

    def kick_active_session(
        self, email: str, session_id: str, logout_time: str | None = None
    ) -> bool:
        """Устанавливает Status='kicked' и LogoutTime по (email, session_id)."""
        return self._update_session_status(email, session_id, "kicked", logout_time)

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
        """Возвращает статус ('active'/'finished'/'kicked'/...) или None."""
        from config import ACTIVE_SESSIONS_SHEET

        ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
        table = self._read_table(ws)
        em = (email or "").strip().lower()
        sid = str(session_id or "").strip()
        for r in table:
            if (r.get("Email", "") or "").strip().lower() == em and str(
                r.get("SessionID", "")
            ).strip() == sid:
                return (r.get("Status") or "").strip().lower()
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

    # ---------- WorkLog append (batch) ----------
    def log_user_actions(
        self,
        actions: list[dict[str, Any]],
        email: str | None = None,
        group: str | None = None,
        **kwargs,
    ) -> bool:
        """
        Записывает список действий пользователя в соответствующую WorkLog-вкладку по группе.
        Совместим со старыми вызовами: принимает и user_group=... в kwargs.

        actions: [{'timestamp': iso, 'email', 'name', 'status', 'action_type', 'comment', 'session_id', 'event_id', ...}, ...]
        """
        try:
            # backward-compat: user_group alias
            user_group = kwargs.pop("user_group", None)
            if group is None and user_group is not None:
                group = user_group

            # если в actions у каждой записи есть своя группа, используем первую; иначе — аргумент
            if not group:
                for a in actions:
                    g = a.get("group") or a.get("Group")
                    if g:
                        group = g
                        break

            ws = self._ensure_worklog_worksheet(group)
            headers_map = self._header_map(ws)  # lower->1-based

            # Соберём строки согласно текущему заголовку листа
            # Нормализуем ключи к lower для сопоставления
            rows: list[list[Any]] = []
            for a in actions:
                norm = {str(k).strip().lower(): v for k, v in a.items()}
                # Унифицируем основные поля
                # timestamp -> локальная строка
                ts = norm.get("timestamp") or norm.get("time") or norm.get("date")
                ts = self._ensure_local_str(ts)
                # session/event ids — опционально
                # собираем в порядке реального header’а листа
                maxcol = max(headers_map.values()) if headers_map else 0
                inv = {v: k for k, v in headers_map.items()}  # 1-based -> lower
                row = []
                for col in range(1, maxcol + 1):
                    key = inv.get(col, "")
                    if not key:
                        row.append("")
                        continue
                    if key == "timestamp":
                        row.append(ts)
                    else:
                        row.append(norm.get(key, ""))
                rows.append(row)

            if not rows:
                return True

            # batch update — по 200 строк (gspread limit-friendly)
            BATCH = 200
            for i in range(0, len(rows), BATCH):
                chunk = rows[i : i + BATCH]
                self._request_with_retry(
                    ws.append_rows, chunk, value_input_option="USER_ENTERED"
                )
            logger.info(f"log_user_actions: appended {len(rows)} rows to {ws.title}")
            return True
        except Exception as e:
            logger.error(f"log_user_actions failed: {e}")
            return False

    # ---------- utils ----------

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
