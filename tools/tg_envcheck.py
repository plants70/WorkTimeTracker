from __future__ import annotations
import os
from config import (
    TELEGRAM_BOT_TOKEN as CFG_TELEGRAM_BOT_TOKEN,
    TELEGRAM_ADMIN_CHAT_ID as CFG_TELEGRAM_ADMIN_CHAT_ID,
    TELEGRAM_BROADCAST_CHAT_ID as CFG_TELEGRAM_BROADCAST_CHAT_ID,
)

def _mask(s: str, keep=6) -> str:
    if not s:
        return ""
    s = str(s)
    return s[:keep] + "..." if len(s) > keep else s

def main():
    print("=== TELEGRAM effective settings ===")
    tok = (CFG_TELEGRAM_BOT_TOKEN or os.getenv("TELEGRAM_BOT_TOKEN", ""))
    adm = (CFG_TELEGRAM_ADMIN_CHAT_ID or os.getenv("TELEGRAM_ADMIN_CHAT_ID", ""))
    brc = (CFG_TELEGRAM_BROADCAST_CHAT_ID or os.getenv("TELEGRAM_BROADCAST_CHAT_ID", ""))
    print("TELEGRAM_BOT_TOKEN:", "set" if tok else "EMPTY", _mask(tok))
    print("TELEGRAM_ADMIN_CHAT_ID:", adm or "<empty>")
    print("TELEGRAM_BROADCAST_CHAT_ID:", brc or "<empty>")

if __name__ == "__main__":
    main()
