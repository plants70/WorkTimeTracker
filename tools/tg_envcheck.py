from __future__ import annotations

import logging
import os

from logging_setup import setup_logging
from config import (
    TELEGRAM_BOT_TOKEN as CFG_TELEGRAM_BOT_TOKEN,
    TELEGRAM_ADMIN_CHAT_ID as CFG_TELEGRAM_ADMIN_CHAT_ID,
    TELEGRAM_BROADCAST_CHAT_ID as CFG_TELEGRAM_BROADCAST_CHAT_ID,
)


logger = logging.getLogger(__name__)

def _mask(s: str, keep=6) -> str:
    if not s:
        return ""
    s = str(s)
    return s[:keep] + "..." if len(s) > keep else s

def main() -> int:
    setup_logging(app_name="wtt-tg-envcheck", force_console=True)
    logger.info("TELEGRAM effective settings")
    tok = (CFG_TELEGRAM_BOT_TOKEN or os.getenv("TELEGRAM_BOT_TOKEN", ""))
    adm = (CFG_TELEGRAM_ADMIN_CHAT_ID or os.getenv("TELEGRAM_ADMIN_CHAT_ID", ""))
    brc = (CFG_TELEGRAM_BROADCAST_CHAT_ID or os.getenv("TELEGRAM_BROADCAST_CHAT_ID", ""))
    logger.info("TELEGRAM_BOT_TOKEN status=%s value=%s", "set" if tok else "EMPTY", _mask(tok))
    logger.info("TELEGRAM_ADMIN_CHAT_ID value=%s", adm or "<empty>")
    logger.info("TELEGRAM_BROADCAST_CHAT_ID value=%s", brc or "<empty>")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
