# config.py
import os
import sys
import platform
from pathlib import Path
from typing import Dict, List, Set, Optional
from contextlib import contextmanager
import atexit

# ==================== Загрузка переменных окружения из .env ====================
from dotenv import load_dotenv
load_dotenv()

# ==================== Импорт для работы с зашифрованным credentials ====================
import pyzipper
import tempfile

# ==================== Базовые настройки ====================
if getattr(sys, 'frozen', False):
    # Режим сборки (PyInstaller)
    BASE_DIR = Path(sys.executable).parent
else:
    # Режим разработки
    BASE_DIR = Path(__file__).parent.absolute()

# --- Исправлено: Создаем LOG_DIR сразу ---
if platform.system() == "Windows":
    LOG_DIR = Path(os.getenv('APPDATA')) / "WorkTimeTracker" / "logs"
else:
    LOG_DIR = Path.home() / ".local" / "share" / "WorkTimeTracker" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True) # Создаем при импорте модуля
# ---

# ==================== Пути к файлам ====================
# Настройки для зашифрованного архива с credentials
CREDENTIALS_ZIP = BASE_DIR / 'secret_creds.zip'  # архив должен лежать рядом с exe

# Пароль берётся из переменной окружения
CREDENTIALS_ZIP_PASSWORD = os.getenv("CREDENTIALS_ZIP_PASSWORD")
if CREDENTIALS_ZIP_PASSWORD is None:
    raise RuntimeError("CREDENTIALS_ZIP_PASSWORD не найден в .env файле!")
CREDENTIALS_ZIP_PASSWORD = CREDENTIALS_ZIP_PASSWORD.encode('utf-8')

# --- Ленивая загрузка credentials ---
_CREDS_TMP_DIR = Path(tempfile.gettempdir()) / "wtt_creds"
_CREDS_TMP_DIR.mkdir(parents=True, exist_ok=True)
_CREDENTIALS_FILE: Optional[Path] = None

def _cleanup_credentials():
    """Удаляет временный файл с учетными данными при выходе из процесса."""
    try:
        if _CREDENTIALS_FILE and _CREDENTIALS_FILE.exists():
            _CREDENTIALS_FILE.unlink()
    except Exception:
        pass

# Регистрируем очистку при выходе
atexit.register(_cleanup_credentials)

@contextmanager
def credentials_path() -> Path:
    """
    Лениво и временно извлекает service_account.json из зашифрованного ZIP.
    Используйте: with credentials_path() as p: ...
    """
    global _CREDENTIALS_FILE
    
    # Если файл уже извлечен и существует, используем его
    if _CREDENTIALS_FILE and _CREDENTIALS_FILE.exists():
        yield _CREDENTIALS_FILE
        return
    
    # Проверяем существование ZIP-архива
    if not CREDENTIALS_ZIP.exists():
        raise FileNotFoundError(f"Zip с credentials не найден: {CREDENTIALS_ZIP}")
    
    # Извлекаем файл из зашифрованного архива
    with pyzipper.AESZipFile(CREDENTIALS_ZIP) as zf:
        zf.pwd = CREDENTIALS_ZIP_PASSWORD
        try:
            data = zf.read('service_account.json')
        except KeyError:
            raise FileNotFoundError("Файл 'service_account.json' не найден в архиве")
        
        # Сохраняем во временный файл
        temp_file = _CREDS_TMP_DIR / 'service_account.json'
        with open(temp_file, 'wb') as f:
            f.write(data)
        
        _CREDENTIALS_FILE = temp_file
        yield _CREDENTIALS_FILE

def get_credentials_file() -> Path:
    """Обратная совместимость: получить путь к JSON (извлечёт при первом вызове)."""
    with credentials_path() as p:
        return Path(p)

LOCAL_DB_PATH = BASE_DIR / 'local_backup.db'
ERROR_LOG_FILE = LOG_DIR / 'error.log'
SYNC_LOG_FILE = LOG_DIR / 'sync.log'  # Добавлен лог для синхронизации

# ==================== Настройки Google Sheets ====================
GOOGLE_SHEET_NAME = "WorkLog"
USERS_SHEET = "Users"
WORKLOG_SHEET = "WorkLog"
ARCHIVE_SHEET = "Archive"
ACTIVE_SESSIONS_SHEET = "ActiveSessions"
SHIFT_CALENDAR_SHEET = ""  # опционально: 'ShiftCalendar' / 'График' если появится лист графика

# ==================== Лимиты API ====================
GOOGLE_API_LIMITS: Dict[str, int] = {
    'max_requests_per_minute': 60,
    'max_rows_per_request': 50,
    'max_cells_per_request': 10000,
    'daily_limit': 100000
}

# ==================== Настройки синхронизации ====================
SYNC_INTERVAL: int = 100
SYNC_BATCH_SIZE: int = 35
API_MAX_RETRIES: int = 5  # Увеличено количество ретраев
API_DELAY_SECONDS: float = 1.5  # Увеличен базовый интервал
SYNC_RETRY_STRATEGY: List[int] = [60, 300, 900, 1800, 3600]  # 1, 5, 15, 30, 60 минут - увеличенная стратегия

# Интервалы синхронизации для разных режимов работы
SYNC_INTERVAL_ONLINE: int = 60  # 60 секунд при нормальной работе
SYNC_INTERVAL_OFFLINE_RECOVERY: int = 300  # 300 секунд (5 минут) при восстановлении после оффлайна

# ==================== Группы обработки ====================
GROUP_MAPPING: Dict[str, str] = {
    "call": "Входящие",
    "appointment": "Запись",
    "mail": "Почта",
    "dental": "Стоматология",
    "default": "Входящие"
}

# ==================== Статусы системы ====================
STATUSES: List[str] = [
    "В работе",
    "Чат",
    "Аудио",
    "Запись",
    "Анкеты",
    "Перерыв",
    "Обед",
    "ЦИТО",
    "Обучение"
]

# Группы для интерфейса (раскладка кнопок)
STATUS_GROUPS: List[List[str]] = [
    ["В работе", "Чат", "Аудио", "Запись", "Анкеты"],   # Основная работа
    ["Перерыв", "Обед"],                                # Перерывы
    ["ЦИТО", "Обучение"]                                # Специальные
]

CONFIRMATION_STATUSES: Set[str] = {"Перерыв", "Обед", "ЦИТО"}
RESTRICTED_STATUSES_FIRST_2H: Set[str] = {"Перерыв", "Обед"}
MAX_COMMENT_LENGTH: int = 500
MAX_HISTORY_DAYS: int = 30

# ==================== Настройки безопасности ====================
PASSWORD_MIN_LENGTH: int = 8
SESSION_TIMEOUT: int = 3600  # секунды
ALLOWED_DOMAINS: List[str] = ["company.com", "sberhealth.ru"]

# ==================== Telegram уведомления ====================
TELEGRAM_BOT_TOKEN: str | None = os.getenv("8318266102:AAESpe4TIQpkTEAFuFD_ECZKWBkc5Tk32LU") or None
# Личный чат админа:
TELEGRAM_ADMIN_CHAT_ID: str | None = os.getenv("1053909260") or None
# Общий канал для групповых объявлений (может быть отрицательный id):
TELEGRAM_BROADCAST_CHAT_ID: str | None = os.getenv("TELEGRAM_BROADCAST_CHAT_ID") or None
# Анти-спам ключей (минут между одинаковыми событиями)
TELEGRAM_MIN_INTERVAL_SEC: int = int(os.getenv("TELEGRAM_MIN_INTERVAL_SEC", "600"))
# Тихие уведомления по умолчанию
TELEGRAM_SILENT: bool = os.getenv("TELEGRAM_SILENT", "0") == "1"
TELEGRAM_ALERTS_ENABLED: bool = bool(TELEGRAM_BOT_TOKEN and (TELEGRAM_ADMIN_CHAT_ID or TELEGRAM_BROADCAST_CHAT_ID))

# ==================== Архивирование ====================
ARCHIVE_DELETE_SOURCE_ROWS: bool = os.getenv("ARCHIVE_DELETE_SOURCE_ROWS", "1") == "1"

# ==================== Пороги правил уведомлений ====================
# опоздание на логин, минут
LATE_LOGIN_MINUTES: int = int(os.getenv("LATE_LOGIN_MINUTES", "15"))
# слишком частая смена статусов, штук за час
OVER_STATUS_MAX_PER_HOUR: int = int(os.getenv("OVER_STATUS_MAX_PER_HOUR", "10"))
# порог очереди несинхрона
NOTIFY_QUEUE_THRESHOLD: int = int(os.getenv("NOTIFY_QUEUE_THRESHOLD", "50"))

# ==================== Настройки мониторинга и логирования ====================
LOG_LEVEL: str = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_ROTATION_SIZE: int = 10 * 1024 * 1024  # 10MB
LOG_BACKUP_COUNT: int = 5  # Количество резервных копий логов

# ==================== Утилиты для работы с переменными окружения ====================
def _bool_env(name: str, default: bool) -> bool:
    """Безопасно преобразует переменную окружения в булево значение."""
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "да")

def _int_env(name: str, default: int) -> int:
    """Безопасно преобразует переменную окружения в целое число."""
    try:
        return int(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        return default

# ==================== Telegram rules toggles & thresholds ====================
# Персональные оповещения сотрудникам
PERSONAL_RULES_ENABLED: bool = _bool_env("PERSONAL_RULES_ENABLED", True)
PERSONAL_WINDOW_MIN: int = _int_env("PERSONAL_WINDOW_MIN", 60)                  # окно в минутах
PERSONAL_STATUS_LIMIT_PER_WINDOW: int = _int_env("PERSONAL_STATUS_LIMIT", 12)   # порог событий/окно

# Служебные оповещения админу
SERVICE_ALERTS_ENABLED: bool = _bool_env("SERVICE_ALERTS_ENABLED", True)
SERVICE_ALERT_MIN_SECONDS: int = _int_env("SERVICE_ALERT_MIN_SECONDS", 900)     # антиспам: не чаще, чем раз в 15 минут

# ==================== Валидация конфигурации ====================
def validate_config() -> None:
    """Проверяет корректность конфигурации при запуске."""
    errors = []
    
    # Ленивая проверка учетных данных
    try:
        with credentials_path() as creds_file:
            if not creds_file.exists():
                errors.append(f"Файл учетных данных не найден: {creds_file}")
    except Exception as e:
        errors.append(f"Ошибка доступа к учетным данным: {e}")
    
    if not LOG_DIR.exists():
        try:
            LOG_DIR.mkdir(parents=True)
        except Exception as e:
            errors.append(f"Не удалось создать директорию логов: {e}")
    
    if not GROUP_MAPPING.get("default"):
        errors.append("Не определена группы по умолчанию в GROUP_MAPPING")
    
    # Проверяем наличие критически важных файлов
    if not CREDENTIALS_ZIP.exists():
        errors.append(f"Файл secret_creds.zip не найден: {CREDENTIALS_ZIP}")
    
    # Проверяем стратегию ретраев
    if len(SYNC_RETRY_STRATEGY) < 3:
        errors.append("Стратегия повторных попыток синхронизации должна содержать минимум 3 интервала")
    
    if max(SYNC_RETRY_STRATEGY) < 1800:
        errors.append("Максимальный интервал повторных попыток должен быть не менее 1800 секунд (30 минут)")
    
    if errors:
        raise ValueError("Ошибки конфигурации:\n- " + "\n- ".join(errors))

# ==================== Утилиты для работы с конфигурации ====================
def get_sync_retry_delay(attempt: int) -> int:
    """
    Возвращает задержку для повторной попытки синхронизации.
    
    Args:
        attempt: Номер попытки (начиная с 0)
    
    Returns:
        Задержка в секундах
    """
    if attempt < len(SYNC_RETRY_STRATEGY):
        return SYNC_RETRY_STRATEGY[attempt]
    return SYNC_RETRY_STRATEGY[-1]  # Последний интервал для всех последующих попыток

def should_retry_sync(error: Exception) -> bool:
    """
    Определяет, следует ли повторять попытку синхронизации при данной ошибке.
    
    Args:
        error: Исключение, которое произошло
        
    Returns:
        True если следует повторить, False если нет
    """
    # Ошибки, при которых стоит повторять попытку
    retryable_errors = [
        "ConnectionError",
        "TimeoutError",
        "HttpError",
        "ServiceUnavailable",
        "RateLimitExceeded"
    ]
    
    error_name = type(error).__name__
    return any(retryable in error_name for retryable in retryable_errors)

# ==================== Инициализация конфигурации ====================
try:
    validate_config()
    print("✓ Конфигурация успешно проверена")
    print(f"✓ Стратегия повторных попыток: {SYNC_RETRY_STRATEGY}")
except Exception as e:
    print(f"✗ Ошибка конфигурации: {e}")
    raise

# ==================== Утилиты для PyInstaller ====================
def get_resource_path(relative_path: str) -> str:
    """Возвращает абсолютный путь к ресурсу, учитывая PyInstaller."""
    if hasattr(sys, '_MEIPASS'):
        base_path = Path(sys._MEIPASS)
    else:
        base_path = BASE_DIR
    return str(base_path / relative_path)

# ==================== Константы для тестирования ====================
if __name__ == "__main__":
    print(f"BASE_DIR: {BASE_DIR}")
    print(f"LOG_DIR: {LOG_DIR}")
    print(f"CREDENTIALS_ZIP: {CREDENTIALS_ZIP}")
    print(f"SYNC_RETRY_STRATEGY: {SYNC_RETRY_STRATEGY}")
    print(f"Максимальная задержка: {max(SYNC_RETRY_STRATEGY)} секунд ({max(SYNC_RETRY_STRATEGY)/60} минут)")
    
    # Тестируем ленивую загрузку credentials
    try:
        with credentials_path() as creds:
            print(f"✓ Credentials file: {creds}")
            print(f"✓ File exists: {creds.exists()}")
    except Exception as e:
        print(f"✗ Error accessing credentials: {e}")
    
    # Тестируем новые настройки правил
    print(f"PERSONAL_RULES_ENABLED: {PERSONAL_RULES_ENABLED}")
    print(f"PERSONAL_WINDOW_MIN: {PERSONAL_WINDOW_MIN}")
    print(f"PERSONAL_STATUS_LIMIT_PER_WINDOW: {PERSONAL_STATUS_LIMIT_PER_WINDOW}")
    print(f"SERVICE_ALERTS_ENABLED: {SERVICE_ALERTS_ENABLED}")
    print(f"SERVICE_ALERT_MIN_SECONDS: {SERVICE_ALERT_MIN_SECONDS}")