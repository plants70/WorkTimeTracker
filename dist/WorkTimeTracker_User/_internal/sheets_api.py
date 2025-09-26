import gspread
import time
import json
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any, Tuple
import logging
from pathlib import Path
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials
from urllib.parse import urlparse
from dataclasses import dataclass
import threading

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.hasHandlers():
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


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


def get_resource_path(relative_path: Union[str, Path]) -> Path:
    try:
        if getattr(sys, 'frozen', False):
            base_path = Path(sys._MEIPASS) if hasattr(sys, '_MEIPASS') else Path(sys.executable).parent
            possible_locations = [
                base_path / relative_path,
                base_path / Path(relative_path).name,
                Path.cwd() / relative_path,
                Path.cwd() / Path(relative_path).name
            ]
            for path in possible_locations:
                if path.exists():
                    logger.debug(f"Resource found at: {path}")
                    return path.resolve()
            raise FileNotFoundError(f"Resource not found in any location: {relative_path}")
        base_path = Path(__file__).parent.parent
        path = base_path / relative_path
        if not path.exists():
            raise FileNotFoundError(f"Resource not found: {path}")
        return path.resolve()
    except Exception as e:
        logger.critical(f"Resource path resolution failed: {str(e)}")
        raise


class SheetsAPI:
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
        self._last_request_time = None
        self._sheet_cache = {}
        self._session = None
        self._quota_info = QuotaInfo(remaining=100, reset_time=60, daily_used=0)
        self._quota_lock = threading.Lock()
        try:
            logger.debug("=== SheetsAPI Initialization Debug ===")
            logger.debug(f"sys.frozen: {getattr(sys, 'frozen', False)}")
            logger.debug(f"sys._MEIPASS: {getattr(sys, '_MEIPASS', 'N/A')}")
            logger.debug(f"Current working dir: {os.getcwd()}")
            logger.debug(f"sys.path: {sys.path}")
            from config import CREDENTIALS_FILE
            self.credentials_path = get_resource_path(CREDENTIALS_FILE)
            logger.info(f"Initializing with credentials: {self.credentials_path}")
            logger.debug(f"Credentials exists: {os.path.exists(self.credentials_path)}")
            if not self.credentials_path.exists():
                if getattr(sys, 'frozen', False):
                    logger.error("Running in frozen mode but credentials not found!")
                raise FileNotFoundError(f"Credentials file missing at: {self.credentials_path}")
            self._init_client()
        except Exception as e:
            logger.critical(
                f"Initialization failed: {type(e).__name__}: {str(e)}",
                exc_info=True
            )
            raise SheetsAPIError(
                "Google Sheets API initialization failed",
                is_retryable=False,
                details=str(e)
            )

    def _init_client(self, max_retries: int = 3) -> None:
        for attempt in range(max_retries):
            try:
                logger.info(f"Client init attempt {attempt + 1}/{max_retries}")
                with open(self.credentials_path, 'r', encoding='utf-8') as f:
                    creds_data = json.load(f)
                    required_fields = {
                        'type', 'project_id', 'private_key_id',
                        'private_key', 'client_email', 'client_id'
                    }
                    if not required_fields.issubset(creds_data.keys()):
                        missing = required_fields - set(creds_data.keys())
                        raise ValueError(f"Missing fields in credentials: {missing}")
                scope = [
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"
                ]
                credentials = Credentials.from_service_account_file(
                    str(self.credentials_path), scopes=scope
                )
                self.client = gspread.Client(auth=credentials)
                self.client.http_client.timeout = 30
                self._session = AuthorizedSession(credentials)
                self._session.timeout = 30
                self._test_connection()
                self._update_quota_info()
                logger.info("Google Sheets client initialized successfully")
                return
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    logger.critical(f"Client init failed after {max_retries} attempts")
                    raise SheetsAPIError(
                        "Failed to initialize Google Sheets client",
                        is_retryable=True,
                        details=str(e)
                    )
                wait_time = 2 ** attempt + 5
                logger.warning(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

    def _update_quota_info(self) -> None:
        try:
            response = self._session.get(
                "https://www.googleapis.com/drive/v3/about",
                params={'fields': 'user,storageQuota'},
                timeout=10
            )
            response.raise_for_status()
            with self._quota_lock:
                self._quota_info.remaining = int(response.headers.get('x-ratelimit-remaining', 100))
                self._quota_info.reset_time = int(response.headers.get('x-ratelimit-reset', 60))
                self._quota_info.daily_used = float(response.json().get('storageQuota', {}).get('usage', 0))
            logger.debug(f"Quota updated: {self._quota_info}")
        except Exception as e:
            logger.warning(f"Failed to update quota info: {str(e)}")
            with self._quota_lock:
                self._quota_info.remaining = 50
                self._quota_info.reset_time = 60

    def _check_quota(self, required: int = 1) -> bool:
        with self._quota_lock:
            if self._quota_info.remaining >= required:
                return True
            wait_time = max(1, self._quota_info.reset_time - time.time() % self._quota_info.reset_time)
            logger.warning(f"Quota exceeded. Waiting {wait_time:.1f} seconds")
            time.sleep(wait_time + 1)
            self._update_quota_info()
            return self._quota_info.remaining >= required

    def _test_connection(self) -> None:
        try:
            logger.info("Testing API connection...")
            start_time = time.time()
            spreadsheets = list(self.client.list_spreadsheet_files())
            elapsed = time.time() - start_time
            logger.debug(f"API test successful. Found {len(spreadsheets)} spreadsheets in {elapsed:.2f}s")
            self._update_quota_info()
        except Exception as e:
            logger.error(f"API connection test failed: {str(e)}")
            try:
                import urllib.request
                urllib.request.urlopen('https://www.google.com', timeout=5)
                logger.debug("Internet connection is available")
            except:
                logger.error("No internet connection detected")
            raise SheetsAPIError(
                "Google Sheets API connection test failed",
                is_retryable=True,
                details=str(e)
            )

    def check_credentials(self) -> bool:
        try:
            creds_valid = (
                os.path.exists(self.credentials_path) and
                hasattr(self, 'client') and
                self.client is not None
            )
            logger.debug(f"Credentials check: {'valid' if creds_valid else 'invalid'}")
            return creds_valid
        except Exception as e:
            logger.error(f"Credentials validation error: {str(e)}")
            return False

    def _request_with_retry(self, func, *args, **kwargs):
        from config import API_MAX_RETRIES, API_DELAY_SECONDS
        last_exception = None
        for attempt in range(API_MAX_RETRIES):
            try:
                if not self._check_quota(required=1):
                    raise SheetsAPIError("Insufficient API quota", is_retryable=True)
                self._check_rate_limit(API_DELAY_SECONDS)
                logger.debug(f"Attempt {attempt + 1}: {func.__name__}")
                result = func(*args, **kwargs)
                with self._quota_lock:
                    self._quota_info.remaining -= 1
                return result
            except Exception as e:
                last_exception = e
                if attempt == API_MAX_RETRIES - 1:
                    logger.error(f"Request failed after {API_MAX_RETRIES} attempts")
                    if isinstance(e, SheetsAPIError):
                        raise
                    raise SheetsAPIError(
                        f"API request failed: {str(e)}",
                        is_retryable=True,
                        details=str(e)
                    )
                wait_time = 2 ** attempt + 1
                logger.warning(f"Retry {attempt + 1}/{API_MAX_RETRIES} in {wait_time}s")
                time.sleep(wait_time)
        raise last_exception if last_exception else Exception("Unknown request error")

    def _check_rate_limit(self, delay: float) -> None:
        if self._last_request_time:
            elapsed = time.time() - self._last_request_time
            if elapsed < delay:
                wait_time = delay - elapsed
                logger.debug(f"Rate limit: waiting {wait_time:.2f}s")
                time.sleep(wait_time)
        self._last_request_time = time.time()

    def get_worksheet(self, sheet_name: str):
        from config import GOOGLE_SHEET_NAME
        if sheet_name not in self._sheet_cache:
            try:
                logger.debug(f"Accessing worksheet '{sheet_name}'")
                logger.debug(f"Opening spreadsheet: {GOOGLE_SHEET_NAME}")
                spreadsheet = self._request_with_retry(
                    self.client.open, GOOGLE_SHEET_NAME
                )
                logger.debug(f"Getting worksheet: {sheet_name}")
                self._sheet_cache[sheet_name] = self._request_with_retry(
                    spreadsheet.worksheet, sheet_name
                )
                logger.info(f"Worksheet '{sheet_name}' cached successfully")
            except Exception as e:
                logger.error(f"Failed to access worksheet '{sheet_name}': {str(e)}")
                try:
                    sheets = [ws.title for ws in spreadsheet.worksheets()]
                    logger.debug(f"Available worksheets: {sheets}")
                except:
                    pass
                raise SheetsAPIError(
                    f"Worksheet access error: {sheet_name}",
                    is_retryable=True,
                    details=str(e)
                )
        return self._sheet_cache[sheet_name]

    def batch_update(self, sheet_name: str, data: List[List[str]]) -> bool:
        if not data:
            logger.debug("No data to update - skipping")
            return True
        try:
            logger.info(f"Starting batch update for '{sheet_name}' ({len(data)} rows)")
            worksheet = self.get_worksheet(sheet_name)
            chunk_size = 50
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                logger.debug(f"Processing chunk {i // chunk_size + 1} ({len(chunk)} rows)")
                required_quota = max(1, len(chunk) // 10)
                if not self._check_quota(required=required_quota):
                    raise SheetsAPIError("Insufficient quota", is_retryable=True)
                self._request_with_retry(
                    worksheet.append_rows,
                    chunk,
                    value_input_option='USER_ENTERED'
                )
            logger.info(f"Batch update for '{sheet_name}' completed successfully")
            return True
        except Exception as e:
            logger.error(f"Batch update failed for '{sheet_name}': {str(e)}")
            raise SheetsAPIError(
                f"Failed to update worksheet: {sheet_name}",
                is_retryable=True,
                details=str(e)
            )

    # ====== АКТИВНЫЕ СЕССИИ ======

    def get_all_active_sessions(self) -> List[Dict]:
        """
        Возвращает ВСЕ записи из листа ActiveSessions.
        Это необходимо для проверки статуса сессии, которая могла быть изменена на 'kicked'.
        """
        try:
            logger.debug("Accessing worksheet 'ActiveSessions'")
            worksheet = self.get_worksheet("ActiveSessions")
            records = self._request_with_retry(worksheet.get_all_records)
            logger.debug(f"Получено {len(records)} записей из 'ActiveSessions'")
            return records
        except Exception as e:
            logger.error(f"Ошибка при получении всех активных сессий: {e}")
            return []

    def get_active_session(self, email: str) -> Optional[Dict]:
        """
        Возвращает активную сессию пользователя из листа ActiveSessions.
        """
        try:
            sessions = self.get_all_active_sessions()
            email_lower = email.strip().lower()
            for session in sessions:
                if (str(session.get("Email", "")).strip().lower() == email_lower and 
                    str(session.get("Status", "")).strip().lower() == "active"):
                    return session
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении активной сессии для {email}: {e}")
            return None

    def set_active_session(self, email: str, name: str, session_id: str, login_time: str):
        """
        Добавляет запись об активной сессии в лист ActiveSessions.
        """
        worksheet = self.get_worksheet("ActiveSessions")
        worksheet.append_row([email, name, session_id, login_time, "active", ""])

    def finish_active_session(self, email: str, session_id: str, logout_time: str):
        """
        Помечает активную сессию как завершённую (Status=finished, LogoutTime=logout_time).
        """
        worksheet = self.get_worksheet("ActiveSessions")
        records = worksheet.get_all_records()
        email_lower = email.strip().lower()
        for i, row in enumerate(records, start=2):  # 2 потому что первая строка — заголовок
            if str(row.get("Email", "")).strip().lower() == email_lower and \
               str(row.get("SessionID", "")) == session_id and \
               str(row.get("Status", "")).strip() == "active":
                worksheet.update(f"E{i}", [["finished"]])
                worksheet.update(f"F{i}", [[logout_time]])
                break

    def check_user_session_status(self, email: str, session_id: str) -> str:
        """
        Пытаемся найти точное совпадение по email+session_id.
        Если нет — возвращаем статус последней строки по email.
        """
        worksheet = self.get_worksheet("ActiveSessions")
        records = self._request_with_retry(worksheet.get_all_records)
        email_lower = email.strip().lower()

        exact = None
        last_for_email = None
        for row in records:
            if str(row.get("Email", "")).strip().lower() == email_lower:
                last_for_email = row
                if str(row.get("SessionID", "")).strip() == str(session_id).strip():
                    exact = row

        row = exact or last_for_email
        if not row:
            return "unknown"

        status = str(row.get("Status", "")).strip().lower()
        return status if status else "unknown"

    def kick_active_session(self, email: str, session_id: str, logout_time: str):
        """
        Помечает активную сессию как 'kicked' (админ-принудительный выход).
        """
        worksheet = self.get_worksheet("ActiveSessions")
        records = worksheet.get_all_records()
        email_lower = email.strip().lower()
        for i, row in enumerate(records, start=2):  # 2 потому что первая строка — заголовок
            if str(row.get("Email", "")).strip().lower() == email_lower and \
               str(row.get("SessionID", "")) == session_id and \
               str(row.get("Status", "")).strip() == "active":
                worksheet.update(f"E{i}", [["kicked"]])
                worksheet.update(f"F{i}", [[logout_time]])
                break

    # ====== /АКТИВНЫЕ СЕССИИ ======

    def get_user_by_email(self, email: str) -> Optional[Dict]:
        from config import USERS_SHEET
        try:
            logger.info(f"Looking up user: {email}")
            worksheet = self.get_worksheet(USERS_SHEET)
            records = self._request_with_retry(worksheet.get_all_records)
            email_lower = email.strip().lower()
            for row in records:
                if str(row.get("Email", "")).strip().lower() == email_lower:
                    user_data = {
                        "email": email_lower,
                        "name": row.get("Name", ""),
                        "role": row.get("Role", "специалист"),
                        "shift_hours": row.get("ShiftHours", "8 часов"),
                        "telegram_login": row.get("Telegram", ""),
                        "group": row.get("Group", "")  # Добавляем группу
                    }
                    logger.debug(f"User found: {user_data}")
                    return user_data
            logger.warning(f"User not found: {email_lower}")
            return None
        except Exception as e:
            logger.error(f"User lookup failed for '{email}': {str(e)}")
            raise SheetsAPIError(
                "Failed to lookup user",
                is_retryable=True,
                details=str(e)
            )

    def _determine_user_group(self, email: str) -> str:
        """
        Сначала берём группу из Users, затем из GROUP_MAPPING по префиксу email.
        В конце — безопасный дефолт 'Входящие'.
        """
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

    def log_user_actions(self, actions: List[Dict], email: str, user_group: Optional[str] = None) -> bool:
        """
        Логирует действия пользователя в соответствующий лист Google Sheets.
        Этот метод работает СИНХРОННО.
        """
        try:
            # Нормализация email (иногда прилетает не строка)
            if not isinstance(email, str):
                guessed = None
                if actions and isinstance(actions[0], dict):
                    guessed = actions[0].get("email")
                email = guessed or str(email)
            email = (email or "").strip().lower()

            # Определяем группу
            group = (user_group or "").strip()
            if not group:
                group = self._determine_user_group(email)

            sheet_name = f"WorkLog_{group}"
            # Получаем worksheet с фолбэками
            try:
                worksheet = self.get_worksheet(sheet_name)
            except SheetsAPIError:
                user = self.get_user_by_email(email) or {}
                grp2 = str(user.get("group", "")).strip()
                if grp2 and grp2 != group:
                    sheet_name = f"WorkLog_{grp2}"
                    worksheet = self.get_worksheet(sheet_name)
                else:
                    sheet_name = "WorkLog_Входящие"
                    worksheet = self.get_worksheet(sheet_name)

            # Подготовка и запись
            values = []
            for a in actions:
                values.append([
                    a.get("session_id", ""),
                    a.get("email", ""),
                    a.get("name", ""),
                    a.get("status", ""),
                    a.get("action_type", ""),
                    a.get("comment", ""),
                    a.get("timestamp", ""),
                    a.get("status_start_time", ""),
                    a.get("status_end_time", ""),
                    a.get("reason", "")
                ])

            if values:
                self._request_with_retry(worksheet.append_rows, values, value_input_option='USER_ENTERED')
                logger.info(f"Batch update for '{sheet_name}' completed successfully")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to log actions to sheets: {e}")
            return False

    def print_debug_info(self):
        print("\n=== SheetsAPI Debug Info ===")
        print(f"Credentials path: {self.credentials_path}")
        print(f"Credentials exists: {os.path.exists(self.credentials_path)}")
        print(f"Client initialized: {hasattr(self, 'client') and self.client is not None}")
        if hasattr(self, '_quota_info'):
            print(f"API Quota: {self._quota_info}")
        print("===========================\n")


try:
    sheets_api = SheetsAPI()
    logger.info("SheetsAPI instance initialized successfully")
    sheets_api.print_debug_info()
except Exception as e:
    logger.critical(
        f"Failed to create SheetsAPI instance: {type(e).__name__}: {str(e)}",
        exc_info=True
    )
    raise