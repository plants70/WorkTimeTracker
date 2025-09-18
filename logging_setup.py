# logging_setup.py
from __future__ import annotations

import logging
import os
import re
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, Union

# Внутренние флаги, чтобы не плодить хендлеры
_LOGGING_INITIALIZED = False
_ROOT_LOGGER_CONFIGURED = False


# ----------------------------- PII masking -----------------------------------
def _mask_pii(msg: str) -> str:
    """Грубое маскирование email и телефонов в логах."""
    msg = re.sub(r"([A-Za-z0-9._%+-]+)@([A-Za-z0-9.-]+\.[A-Za-z]{2,})", r"***@\2", msg)
    msg = re.sub(r"\+?\d[\d\s\-()]{6,}\d", "***PHONE***", msg)
    return msg


class PIIFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = _mask_pii(record.msg)
        return True


# --------------------------- helpers / internals ------------------------------
def _parse_level_from_env(default_level: int = logging.INFO) -> int:
    """Берём уровень логирования из WTT_LOG_LEVEL, иначе default_level."""
    raw = os.getenv("WTT_LOG_LEVEL")
    if not raw:
        return default_level
    raw = raw.strip().upper()
    # Разрешаем как имена уровней, так и числа
    if raw.isdigit():
        try:
            val = int(raw)
            if val in (10, 20, 30, 40, 50):
                return val
        except Exception:
            pass
    return getattr(logging, raw, default_level)


def _console_enabled(force_console: Optional[bool]) -> bool:
    """
    Возвращает True/False для консольного вывода:
      - если force_console is not None → используем его,
      - иначе WTT_LOG_CONSOLE (1/true/yes),
      - для b/c поддерживаем DEBUG_CONSOLE=1.
    """
    if force_console is not None:
        return bool(force_console)
    env = os.getenv("WTT_LOG_CONSOLE", "")
    if env.strip().lower() in {"1", "true", "yes", "on"}:
        return True
    # Back-compat: старая переменная
    return os.getenv("DEBUG_CONSOLE") == "1"


def _configure_root_logger(level: int, handler: logging.Handler) -> None:
    """Ставит уровень и хендлер на root-логгер, убирая предыдущие при повторной инициализации."""
    global _ROOT_LOGGER_CONFIGURED
    root = logging.getLogger()
    if _ROOT_LOGGER_CONFIGURED:
        for h in list(root.handlers):
            root.removeHandler(h)
    root.setLevel(level)
    root.addHandler(handler)
    _ROOT_LOGGER_CONFIGURED = True


def init_app_log_path(app_name: str, log_dir: Optional[str] = None) -> str:
    """
    Возвращает путь к файлу лога wtt-<app_name>.log.
    Если log_dir не задан, используем %APPDATA%/WorkTimeTracker/logs (или ~).
    """
    if log_dir:
        base = Path(log_dir)
    else:
        appdata = os.getenv("APPDATA") or os.path.expanduser("~")
        base = Path(appdata) / "WorkTimeTracker" / "logs"
    base.mkdir(parents=True, exist_ok=True)
    return str(base / f"wtt-{app_name}.log")


def _setup_logging_impl(
    app_name: str,
    log_dir: Union[str, Path],
    level: int,
    rotate_mb: int = 5,
    backup_count: int = 5,
    force_console: Optional[bool] = None,
) -> Path:
    """
    Реальная настройка: ротация, PII-маскирование, подавление болтливых библиотек.
    """
    global _LOGGING_INITIALIZED

    dir_path = Path(log_dir)
    dir_path.mkdir(parents=True, exist_ok=True)
    log_path = dir_path / f"{app_name}.log"

    # Одноразовые действия при первом вызове
    if not _LOGGING_INITIALIZED:
        root = logging.getLogger()
        root.setLevel(
            logging.DEBUG
        )  # root на DEBUG; фактический уровень задают хендлеры
        for h in list(root.handlers):
            root.removeHandler(h)

        # Менее шумные сторонние логгеры
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("google").setLevel(logging.WARNING)
        logging.getLogger("gspread").setLevel(logging.INFO)
        logging.captureWarnings(True)

        _LOGGING_INITIALIZED = True

    fmt = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Файловый хендлер с ротацией
    fh = RotatingFileHandler(
        log_path,
        maxBytes=rotate_mb * 1024 * 1024,
        backupCount=backup_count,
        encoding="utf-8",
    )
    fh.setLevel(level)
    fh.setFormatter(fmt)
    fh.addFilter(PIIFilter())

    _configure_root_logger(level, fh)

    # Консоль — опционально (force_console / WTT_LOG_CONSOLE / DEBUG_CONSOLE)
    if _console_enabled(force_console):
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        ch.setFormatter(fmt)
        ch.addFilter(PIIFilter())
        logging.getLogger().addHandler(ch)

    logging.getLogger(__name__).info(
        "Logging initialized (path=%s, level=%s, console=%s)",
        log_path,
        logging.getLevelName(level),
        _console_enabled(force_console),
    )
    return log_path


# ------------------------------ public APIs ----------------------------------
def setup_logging(
    *,
    app_name: str = "app",
    log_dir: Optional[Union[str, Path]] = None,
    level: Optional[int] = None,
    rotate_mb: int = 5,
    backup_count: int = 5,
    force_console: Optional[bool] = None,
) -> Path:
    """
    Основной публичный метод.
    Параметры:
      - app_name: имя приложения (например, "wtt-admin", "wtt-user", "wtt-doctor")
      - log_dir: каталог логов (по умолчанию ./logs, либо внешний путь)
      - level: уровень логов (если None — читаем WTT_LOG_LEVEL, по умолчанию INFO)
      - rotate_mb: размер файла лога до ротации (MiB)
      - backup_count: число файлов-историй
      - force_console: True/False для вывода в консоль; если None — берём из WTT_LOG_CONSOLE/DEBUG_CONSOLE
    Возвращает Path к лог-файлу.
    """
    # Определяем уровень из параметра или из окружения
    eff_level = level if level is not None else _parse_level_from_env(logging.INFO)

    # Каталог логов
    if log_dir is None:
        # По умолчанию локальная папка ./logs
        log_dir = Path("logs")
    else:
        log_dir = Path(log_dir)

    return _setup_logging_impl(
        app_name=app_name,
        log_dir=log_dir,
        level=eff_level,
        rotate_mb=rotate_mb,
        backup_count=backup_count,
        force_console=force_console,
    )


def setup_logging_compat(*args, **kwargs) -> Path:
    """
    Совместимый враппер для старых вызовов.
    Поддерживаем:
      - setup_logging_compat("wtt-admin", LOG_DIR)
      - setup_logging_compat(app_name="wtt-admin", log_dir=LOG_DIR)
      - setup_logging_compat(app_name="wtt-admin", log_dir=LOG_DIR, level=logging.DEBUG, force_console=True)
    """
    if kwargs:
        return setup_logging(**kwargs)

    # Позиционные аргументы: (app_name, log_dir, level?)
    name = args[0] if len(args) > 0 else "app"
    log_dir = args[1] if len(args) > 1 else None
    lvl = args[2] if len(args) > 2 else None

    return setup_logging(app_name=str(name), log_dir=log_dir, level=lvl)


__all__ = ["setup_logging", "setup_logging_compat", "init_app_log_path"]
