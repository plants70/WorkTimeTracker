# logging_setup.py
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import sys
import re

def _mask_pii(msg: str) -> str:
    # простое маскирование email и телефонов
    msg = re.sub(r'([A-Za-z0-9._%+-]+)@([A-Za-z0-9.-]+\.[A-Za-z]{2,})', r'***@\2', msg)
    msg = re.sub(r'\+?\d[\d\s\-()]{6,}\d', '***PHONE***', msg)
    return msg

class PIIFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = _mask_pii(record.msg)
        return True

def setup_logging(app_name: str, log_dir: Path, level_console: int = logging.INFO, level_file: int = logging.DEBUG, reset: bool = True):
    log_dir.mkdir(parents=True, exist_ok=True)
    logfile = log_dir / f"{app_name}.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    # ВАЖНО: убираем ранее навешанные хендлеры (basicConfig и т.д.), чтобы не было дублей
    if reset and root.handlers:
        for h in list(root.handlers):
            root.removeHandler(h)

    fmt = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # файл
    fh = RotatingFileHandler(logfile, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    fh.setLevel(level_file)
    fh.setFormatter(fmt)
    fh.addFilter(PIIFilter())
    root.addHandler(fh)

    # консоль
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level_console)
    ch.setFormatter(fmt)
    ch.addFilter(PIIFilter())
    root.addHandler(ch)

    # Глушим болтливые сторонние либы
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("gspread").setLevel(logging.INFO)
    logging.captureWarnings(True)
    return logfile