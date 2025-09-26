# config.py
import os
import sys
import platform
from pathlib import Path
from typing import Dict, List, Set, Optional

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

def extract_creds_from_zip() -> Path:
    """Извлекает service_account.json из зашифрованного zip в временный файл и возвращает путь к нему."""
    if not CREDENTIALS_ZIP.exists():
        raise FileNotFoundError(f"Zip с credentials не найден: {CREDENTIALS_ZIP}")
    with pyzipper.AESZipFile(CREDENTIALS_ZIP) as zf:
        zf.pwd = CREDENTIALS_ZIP_PASSWORD
        try:
            data = zf.read('service_account.json')
        except KeyError:
            raise FileNotFoundError("Файл 'service_account.json' не найден в архиве")
        # Создаем временный файл, который будет автоматически удален
        temp = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
        temp.write(data)
        temp.close()
        return Path(temp.name)

# Извлекаем credentials при импорте модуля
try:
    CREDENTIALS_FILE = extract_creds_from_zip()
except Exception as e:
    print(f"Ошибка при извлечении credentials: {e}")
    raise

LOCAL_DB_PATH = BASE_DIR / 'local_backup.db'
ERROR_LOG_FILE = LOG_DIR / 'error.log'
SYNC_LOG_FILE = LOG_DIR / 'sync.log'  # Добавлен лог для синхронизации

# ==================== Настройки Google Sheets ====================
GOOGLE_SHEET_NAME = "WorkLog"
USERS_SHEET = "Users"
WORKLOG_SHEET = "WorkLog"
ARCHIVE_SHEET = "Archive"
ACTIVE_SESSIONS_SHEET = "ActiveSessions"

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

# ==================== Настройки мониторинга и логирования ====================
LOG_LEVEL: str = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_ROTATION_SIZE: int = 10 * 1024 * 1024  # 10MB
LOG_BACKUP_COUNT: int = 5  # Количество резервных копий логов

# ==================== Валидация конфигурации ====================
def validate_config() -> None:
    """Проверяет корректность конфигурации при запуске."""
    errors = []
    
    if not CREDENTIALS_FILE.exists():
        errors.append(f"Файл учетных данных не найден: {CREDENTIALS_FILE}")
    
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

# ==================== Утилиты для работы с конфигурацией ====================
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
    print(f"CREDENTIALS_FILE: {CREDENTIALS_FILE}")
    print(f"SYNC_RETRY_STRATEGY: {SYNC_RETRY_STRATEGY}")
    print(f"Максимальная задержка: {max(SYNC_RETRY_STRATEGY)} секунд ({max(SYNC_RETRY_STRATEGY)/60} минут)")