# sheets_api.py
import gspread
import time
import json
import sys
import os
import random
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from pathlib import Path
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials
from dataclasses import dataclass
import threading
from zoneinfo import ZoneInfo  # stdlib (Python 3.9+)

logger = logging.getLogger("sheets_api")  # никаких handlers здесь — конфиг только в приложении


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


class SheetsAPI:
    """Синглтон-обёртка над gspread с ретраями, кэшем и batch-операциями."""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        from config import get_credentials_file, GOOGLE_SHEET_NAME
        self._last_request_time = None
        self._sheet_cache: Dict[str, Any] = {}
        self._session: Optional[AuthorizedSession] = None
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
                if getattr(sys, 'frozen', False):
                    logger.error("Running in frozen mode but credentials not found!")
                raise FileNotFoundError(f"Credentials file missing at: {self.credentials_path}")
            self._init_client()
        except Exception as e:
            logger.critical("Initialization failed", exc_info=True)
            raise SheetsAPIError(
                "Google Sheets API initialization failed",
                is_retryable=False,
                details=str(e)
            )

    # ---------- low-level client/bootstrap ----------

    def _init_client(self, max_retries: int = 3) -> None:
        for attempt in range(max_retries):
            try:
                logger.info(f"Client init attempt {attempt + 1}/{max_retries}")
                with open(self.credentials_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    required = {'type', 'project_id', 'private_key_id', 'private_key', 'client_email', 'client_id'}
                    if not required.issubset(data.keys()):
                        missing = required - set(data.keys())
                        raise ValueError(f"Missing fields in credentials: {missing}")

                scopes = [
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                ]
                credentials = Credentials.from_service_account_file(str(self.credentials_path), scopes=scopes)
                self.client = gspread.client.Client(auth=credentials)
                # gspread >=5
                self.client.session = AuthorizedSession(credentials)
                # На некоторых версиях http_client может отсутствовать — оставляем, как было у тебя
                if hasattr(self.client, "http_client") and hasattr(self.client.http_client, "timeout"):
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
                        details=str(e)
                    )
                wait = 2 ** attempt + 5
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
                urllib.request.urlopen('https://www.google.com', timeout=5)
                logger.debug("Internet connection is available")
            except Exception:
                logger.error("No internet connection detected")
            raise SheetsAPIError(
                "Google Sheets API connection test failed",
                is_retryable=True,
                details=str(e)
            )

    def _update_quota_info(self) -> None:
        try:
            resp = self._session.get(  # type: ignore[union-attr]
                "https://www.googleapis.com/drive/v3/about",
                params={'fields': 'user,storageQuota'},
                timeout=10
            )
            resp.raise_for_status()
            with self._quota_lock:
                self._quota_info.remaining = int(resp.headers.get('x-ratelimit-remaining', 100))
                self._quota_info.reset_time = int(resp.headers.get('x-ratelimit-reset', 60))
                self._quota_info.daily_used = float(resp.json().get('storageQuota', {}).get('usage', 0) or 0.0)
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
            wait_time = max(1, self._quota_info.reset_time - time.time() % self._quota_info.reset_time)
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

    def _request_with_retry(self, func, *args, **kwargs):
        from config import API_MAX_RETRIES, API_DELAY_SECONDS, GOOGLE_API_LIMITS
        last_exc: Optional[Exception] = None
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
                # Классификация: 429/5xx/сетевые — повторимые
                msg = str(e).lower()
                retryable = any(x in msg for x in ("rate limit", "quota", "429", "timeout", "temporarily", "unavailable", "socket"))
                if attempt == API_MAX_RETRIES - 1 or not retryable:
                    logger.error(f"Request failed after {API_MAX_RETRIES} attempts")
                    if isinstance(e, SheetsAPIError):
                        raise
                    raise SheetsAPIError(
                        f"API request failed: {e}",
                        is_retryable=True,
                        details=str(e)
                    )
                # Full jitter: base * 2^n + random(0..base)
                base = max(1.0, float(API_DELAY_SECONDS))
                wait = base * (2 ** attempt)
                wait = wait + random.uniform(0, base)
                # мягкая нормализация под минутный лимит
                per_min = max(1, GOOGLE_API_LIMITS.get("max_requests_per_minute", 60))
                min_gap = 60.0 / per_min
                wait = max(wait, min_gap)
                logger.warning(f"Retry {attempt + 1}/{API_MAX_RETRIES} in {wait:.2f}s (error: {e})")
                time.sleep(wait)
        raise last_exc or Exception("Unknown request error")

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
                    logger.warning(f"ZoneInfo('{tz_name}') unavailable; using system local TZ")
                    return local_tz
                logger.warning(f"ZoneInfo('{tz_name}') unavailable; fallback to UTC")
                return timezone.utc
        except Exception:
            return timezone.utc

    def _fmt_local(self, dt: Optional[datetime] = None) -> str:
        """
        Возвращает строку 'YYYY-MM-DD HH:MM:SS' в локальном TZ (для корректного парсинга в Google Sheets).
        """
        tz = self._get_tz()
        if dt is None:
            dt = datetime.now(tz)
        else:
            if dt.tzinfo is None:
                # считаем вход как UTC-метку без tzinfo
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(tz)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def _ensure_local_str(self, ts: Optional[str]) -> str:
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
                spreadsheet = self._request_with_retry(self.client.open, GOOGLE_SHEET_NAME)
                logger.debug(f"Caching worksheet: {sheet_name}")
                self._sheet_cache[sheet_name] = self._request_with_retry(spreadsheet.worksheet, sheet_name)
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
                    details=str(e)
                )
        return self._sheet_cache[sheet_name]

    def _get_ws(self, name: str):
        """Единая точка доступа к листам (через кэш)."""
        return self.get_worksheet(name)

    def list_worksheet_titles(self) -> List[str]:
        """Список названий листов книги без лишних ошибок в логах."""
        from config import GOOGLE_SHEET_NAME
        spreadsheet = self._request_with_retry(self.client.open, GOOGLE_SHEET_NAME)
        sheets = self._request_with_retry(spreadsheet.worksheets)
        return [ws.title for ws in sheets]

    def has_worksheet(self, name: str) -> bool:
        """Проверяем существование листа по имени."""
        try:
            return name in self.list_worksheet_titles()
        except Exception:
            return False

    # ---------- helpers for tables ----------

    @staticmethod
    def _num_to_a1_col(n: int) -> str:
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    def _read_table(self, ws) -> List[Dict[str, str]]:
        rows = self._request_with_retry(lambda: ws.get_all_values())
        if not rows:
            return []
        header = rows[0]
        out: List[Dict[str, str]] = []
        for r in rows[1:]:
            if any((c or "").strip() for c in r):
                out.append({header[i]: (r[i] if i < len(header) else "") for i in range(len(header))})
        return out

    def _header_map(self, ws) -> Dict[str, int]:
        header = self._request_with_retry(lambda: ws.row_values(1))
        return {name: i + 1 for i, name in enumerate(header)}  # 1-based

    def _find_row_by(self, ws, col_name: str, value: str) -> Optional[int]:
        table = self._read_table(ws)
        val = (value or "").strip().lower()
        for idx, row in enumerate(table, start=2):  # +1 header, 1-based
            if (row.get(col_name, "") or "").strip().lower() == val:
                return idx
        return None

    # ---------- generic batch append ----------

    def batch_update(self, sheet_name: str, data: List[List[str]]) -> bool:
        if not data:
            logger.debug("No data to update - skipping")
            return True
        try:
            logger.info(f"Batch append -> '{sheet_name}' ({len(data)} rows)")
            ws = self._get_ws(sheet_name)
            chunk = 50
            for i in range(0, len(data), chunk):
                part = data[i:i + chunk]
                required_quota = max(1, len(part) // 10)
                if not self._check_quota(required=required_quota):
                    raise SheetsAPIError("Insufficient quota", is_retryable=True)
                self._request_with_retry(ws.append_rows, part, value_input_option='USER_ENTERED')
            logger.info(f"Batch append for '{sheet_name}' completed")
            return True
        except Exception as e:
            logger.error(f"Batch update failed for '{sheet_name}': {e}")
            raise SheetsAPIError(
                f"Failed to update worksheet: {sheet_name}",
                is_retryable=True,
                details=str(e)
            )

    # ========= USERS =========

    def get_users(self) -> List[Dict[str, str]]:
        from config import USERS_SHEET
        ws = self._get_ws(USERS_SHEET)
        return self._read_table(ws)

    def upsert_user(self, user: Dict[str, str]) -> None:
        from config import USERS_SHEET
        if not user.get("Email"):
            raise ValueError("user.Email is required")
        ws = self._get_ws(USERS_SHEET)
        hmap = self._header_map(ws)
        row_idx = self._find_row_by(ws, "Email", user["Email"])

        values = [[""] * len(hmap)]
        for k, v in user.items():
            if k in hmap:
                values[0][hmap[k] - 1] = str(v)

        if row_idx:
            left = self._num_to_a1_col(1)
            right = self._num_to_a1_col(len(hmap))
            rng = f"{left}{row_idx}:{right}{row_idx}"
            self._request_with_retry(lambda: ws.update(rng, values))
        else:
            self._request_with_retry(ws.append_rows, values, value_input_option='USER_ENTERED')

    def update_user_fields(self, email: str, fields: Dict[str, str]) -> None:
        from config import USERS_SHEET
        ws = self._get_ws(USERS_SHEET)
        hmap = self._header_map(ws)
        row_idx = self._find_row_by(ws, "Email", email)
        if not row_idx:
            raise ValueError(f"User {email} not found")

        row_vals = self._request_with_retry(lambda: ws.row_values(row_idx))
        row_vals = (row_vals + [""] * (len(hmap) - len(row_vals)))[:len(hmap)]
        for k, v in fields.items():
            if k in hmap:
                row_vals[hmap[k] - 1] = str(v)

        left = self._num_to_a1_col(1)
        right = self._num_to_a1_col(len(hmap))
        rng = f"{left}{row_idx}:{right}{row_idx}"
        self._request_with_retry(lambda: ws.update(rng, [row_vals]))

    def delete_user(self, email: str) -> bool:
        from config import USERS_SHEET
        ws = self._get_ws(USERS_SHEET)
        row_idx = self._find_row_by(ws, "Email", email)
        if not row_idx:
            return False
        self._request_with_retry(lambda: ws.delete_rows(row_idx))
        return True

    def get_user_by_email(self, email: str) -> Optional[Dict[str, str]]:
        """Быстрый поиск пользователя по email в листе Users."""
        from config import USERS_SHEET
        try:
            ws = self._get_ws(USERS_SHEET)
            table = self._read_table(ws)
            em = (email or "").strip().lower()
            for row in table:
                if (row.get("Email", "") or "").strip().lower() == em:
                    return {
                        "email": em,
                        "name": row.get("Name", ""),
                        "role": row.get("Role", "специалист"),
                        "shift_hours": row.get("ShiftHours", "8 часов"),
                        "telegram_login": row.get("Telegram", ""),
                        "group": row.get("Group", ""),
                    }
            return None
        except Exception as e:
            logger.error(f"User lookup failed for '{email}': {e}")
            raise SheetsAPIError("Failed to lookup user", is_retryable=True, details=str(e))

    # ========= ACTIVE SESSIONS =========

    def get_all_active_sessions(self) -> List[Dict[str, str]]:
        from config import ACTIVE_SESSIONS_SHEET
        ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
        return self._read_table(ws)

    def get_active_session(self, email: str) -> Optional[Dict[str, str]]:
        email_lower = (email or "").strip().lower()
        for row in self.get_all_active_sessions():
            if (row.get("Email", "") or "").strip().lower() == email_lower and \
               (row.get("Status", "") or "").strip().lower() == "active":
                return row
        return None

    def set_active_session(self, email: str, name: str, session_id: str, login_time: Optional[str] = None) -> bool:
        from config import ACTIVE_SESSIONS_SHEET
        ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
        lt = self._ensure_local_str(login_time)
        values = [[email, name, session_id, lt, "active", ""]]
        self._request_with_retry(ws.append_rows, values, value_input_option='USER_ENTERED')
        return True

    def check_user_session_status(self, email: str, session_id: str) -> str:
        """Статус по точному email+session_id, иначе — по последней записи email."""
        from config import ACTIVE_SESSIONS_SHEET
        ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
        table = self._read_table(ws)

        em = (email or "").strip().lower()
        sid = str(session_id).strip()

        def key_fn(t):
            idx, r = t
            ts = (r.get("LoginTime") or "").strip()
            return (ts, idx)

        exact = [(i, r) for i, r in enumerate(table, start=2)
                 if (r.get("Email", "") or "").strip().lower() == em
                 and str(r.get("SessionID", "")).strip() == sid]

        if exact:
            _, row = sorted(exact, key=key_fn)[-1]
        else:
            same_email = [(i, r) for i, r in enumerate(table, start=2)
                          if (r.get("Email", "") or "").strip().lower() == em]
            if not same_email:
                return "unknown"
            _, row = sorted(same_email, key=key_fn)[-1]

        status = (row.get("Status", "") or "").strip().lower()
        return status or "unknown"

    def finish_active_session(self, email: str, session_id: str, logout_time: Optional[str] = None) -> bool:
        """Status=finished, LogoutTime=..., batch-обновление одной командой."""
        from config import ACTIVE_SESSIONS_SHEET
        ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
        table = self._read_table(ws)
        em = (email or "").strip().lower()
        sid = str(session_id).strip()

        row_idx: Optional[int] = None
        for i, r in enumerate(table, start=2):
            if (r.get("Email", "") or "").strip().lower() == em and \
               str(r.get("SessionID", "")).strip() == sid and \
               (r.get("Status", "") or "").strip().lower() == "active":
                row_idx = i
                break
        if not row_idx:
            return False

        hmap = self._header_map(ws)
        lt = self._ensure_local_str(logout_time)

        cols = sorted([hmap["Status"], hmap["LogoutTime"]])
        left = self._num_to_a1_col(cols[0]); right = self._num_to_a1_col(cols[-1])
        rng = f"{left}{row_idx}:{right}{row_idx}"
        buf = [""] * (cols[-1] - cols[0] + 1)
        buf[hmap["Status"] - cols[0]] = "finished"
        buf[hmap["LogoutTime"] - cols[0]] = lt

        self._request_with_retry(lambda: ws.update(rng, [buf]))
        return True

    def kick_active_session(
        self,
        email: str,
        session_id: Optional[str] = None,
        status: str = "kicked",
        remote_cmd: str = "FORCE_LOGOUT",
        logout_time: Optional[datetime] = None
    ) -> bool:
        """
        Находит ПОСЛЕДНЮЮ активную сессию пользователя (опционально по SessionID) и
        batch-обновлением выставляет: Status, LogoutTime (локальное время), RemoteCommand.
        """
        from config import ACTIVE_SESSIONS_SHEET
        ws = self._get_ws(ACTIVE_SESSIONS_SHEET)
        table = self._read_table(ws)
        em = (email or "").strip().lower()

        candidates = [
            (i, r) for i, r in enumerate(table, start=2)
            if (r.get("Email", "") or "").strip().lower() == em
            and (r.get("Status", "") or "").strip().lower() == "active"
            and (session_id is None or str(r.get("SessionID", "")).strip() == str(session_id).strip())
        ]
        if not candidates:
            return False

        def key_fn(t):
            idx, r = t
            ts = (r.get("LoginTime") or "").strip()
            return (ts, idx)

        row_idx, _ = sorted(candidates, key=key_fn)[-1]

        hmap = self._header_map(ws)
        need = ["Status", "LogoutTime", "RemoteCommand"]
        if not all(k in hmap for k in need):
            raise RuntimeError("ActiveSessions headers missing one of: " + ", ".join(need))

        if isinstance(logout_time, datetime):
            lt = self._fmt_local(logout_time)
        else:
            lt = self._ensure_local_str(logout_time)

        ordered_cols = sorted([hmap["Status"], hmap["LogoutTime"], hmap["RemoteCommand"]])
        left = self._num_to_a1_col(ordered_cols[0])
        right = self._num_to_a1_col(ordered_cols[-1])
        rng = f"{left}{row_idx}:{right}{row_idx}"

        width = ordered_cols[-1] - ordered_cols[0] + 1
        buf = [""] * width
        buf[hmap["Status"] - ordered_cols[0]] = status
        buf[hmap["LogoutTime"] - ordered_cols[0]] = lt
        buf[hmap["RemoteCommand"] - ordered_cols[0]] = remote_cmd

        self._request_with_retry(lambda: ws.update(rng, [buf]))
        return True

    # ---------- remote command ACK helpers ----------
    def ack_remote_command(self, email: str, session_id: str) -> bool:
        """
        Помечает обработку команды на листе ActiveSessions:
        - если есть колонка RemoteCommandAck — ставим метку времени туда,
        - иначе мягко очищаем RemoteCommand (чтобы команда не срабатывала повторно).
        """
        SHEET = "ActiveSessions"
        try:
            ws = self._get_ws(SHEET)
            header = [h.strip() for h in self._request_with_retry(ws.row_values, 1)]
            # индексы нужных колонок (1-based для update_cell)
            def idx(col: str) -> int | None:
                return header.index(col) + 1 if col in header else None
            c_email = idx("Email")
            c_sess  = idx("SessionID")
            c_cmd   = idx("RemoteCommand")
            c_ack   = idx("RemoteCommandAck")  # может не быть — это нормально
            if not (c_email and c_sess and (c_cmd or c_ack)):
                logger.info("ACK: required columns are not present on %s", SHEET)
                return False
            values = self._request_with_retry(ws.get_all_values)
            # Поиск строки снизу вверх (чаще новые внизу)
            for i in range(len(values)-1, 0, -1):
                row = values[i]
                if len(row) >= max(c_email, c_sess):
                    if row[c_email-1] == email and row[c_sess-1] == session_id:
                        ts = time.strftime("%Y-%m-%d %H:%M:%S")
                        if c_ack:
                            self._request_with_retry(ws.update_cell, i+1, c_ack, ts)
                            logger.info("ACK set on %s for %s (%s)", SHEET, email, session_id)
                            return True
                        elif c_cmd:
                            # fallback: очищаем команду
                            self._request_with_retry(ws.update_cell, i+1, c_cmd, "")
                            logger.info("RemoteCommand cleared on %s for %s (%s)", SHEET, email, session_id)
                            return True
            logger.info("ACK: row not found for %s (%s)", email, session_id)
        except Exception as e:
            logger.warning("ACK failed: %s", e)
        return False

    # ========= LOGGING =========

    def _determine_user_group(self, email: str) -> str:
        """Сначала Users.Group, затем по префиксу GROUP_MAPPING, иначе 'Входящие'."""
        try:
            user = self.get_user_by_email(email)
            grp = str((user or {}).get("group", "")).strip()
            if grp:
                return grp
        except Exception as e:
            logger.warning(f"Users lookup failed while determining group for {email}: {e}")

        try:
            from config import GROUP_MAPPING
            email_prefix = str(email).split("@")[0].lower()
            for k, v in GROUP_MAPPING.items():
                if k and k.lower() in email_prefix:
                    return str(v).title()
        except Exception as e:
            logger.warning(f"Failed to determine group from GROUP_MAPPING for {email}: {e}")

        return "Входящие"

    def log_user_actions(self, actions: List[Dict[str, Any]], email: str, user_group: Optional[str] = None) -> bool:
        """
        Синхронно логирует действия пользователя в WorkLog_*.
        Формат строки: email, name, status, action_type, comment, timestamp, session_id,
                       status_start_time, status_end_time, reason
        """
        try:
            if not isinstance(email, str):
                guessed = actions[0].get("email") if actions and isinstance(actions[0], dict) else None
                email = guessed or str(email)
            email = (email or "").strip().lower()

            group = (user_group or "").strip() or self._determine_user_group(email)
            sheet_name = f"WorkLog_{group}"

            try:
                ws = self._get_ws(sheet_name)
            except SheetsAPIError:
                user = self.get_user_by_email(email) or {}
                grp2 = str(user.get("group", "")).strip()
                sheet_name = f"WorkLog_{grp2 or 'Входящие'}"
                ws = self._get_ws(sheet_name)

            values = []
            for a in actions:
                values.append([
                    a.get("email", ""),
                    a.get("name", ""),
                    a.get("status", ""),
                    a.get("action_type", ""),
                    a.get("comment", ""),
                    self._ensure_local_str(a.get("timestamp")),
                    a.get("session_id", ""),
                    self._ensure_local_str(a.get("status_start_time")),
                    self._ensure_local_str(a.get("status_end_time")),
                    a.get("reason", "")
                ])

            if values:
                self._request_with_retry(ws.append_rows, values, value_input_option='USER_ENTERED')
                logger.info(f"WorkLog appended: {sheet_name} (+{len(values)})")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to log actions to sheets: {e}")
            return False

    # ---------- back-compat for user_app ----------

    def check_credentials(self) -> bool:
        """
        Back-compat для user_app: проверяем, что есть файл creds и клиент инициализирован.
        """
        try:
            return (
                hasattr(self, "credentials_path")
                and self.credentials_path
                and os.path.exists(self.credentials_path)
                and hasattr(self, "client")
                and self.client is not None
            )
        except Exception as e:
            logger.error(f"Credentials validation error: {e}")
            return False

    # ---------- debug ----------

    def print_debug_info(self):
        print("\n=== SheetsAPI Debug Info ===")
        print(f"Credentials path: {self.credentials_path}")
        print(f"Credentials exists: {os.path.exists(self.credentials_path)}")
        print(f"Client initialized: {hasattr(self, 'client') and self.client is not None}")
        if hasattr(self, '_quota_info'):
            print(f"API Quota: {self._quota_info}")
        print("===========================\n")


# --- Lazy proxy for SheetsAPI (инициализация при первом обращении) ---

class _LazySheetsAPI:
    """
    Лёгкий прокси, который создаёт реальный экземпляр SheetsAPI при первом доступе
    к любому его атрибуту/методу. Это убирает сайд-эффекты при импорте модуля.
    """
    __slots__ = ("_inst",)

    def __init__(self):
        self._inst: Optional["SheetsAPI"] = None

    def _ensure(self) -> "SheetsAPI":
        if self._inst is None:
            # Важно: здесь используется текущая логика конструктора SheetsAPI — 
            # она сама подтянет пароль из .env и распакует зашифрованный ZIP.
            self._inst = SheetsAPI()
        return self._inst

    def __getattr__(self, name: str):
        # Проксируем любые обращения к реальному инстансу
        return getattr(self._ensure(), name)

    def __repr__(self) -> str:  # чтобы в логах было понятно, что это прокси
        return "<SheetsAPI (lazy proxy)>"


# Публичная точка входа, совместимая с существующим кодом:
sheets_api = _LazySheetsAPI()


def get_sheets_api() -> "SheetsAPI":
    """
    Опциональная фабрика для явного получения API.
    """
    return sheets_api._ensure()