# tools/tg_send.py
from __future__ import annotations

import argparse
import logging

from logging_setup import setup_logging
from telegram_bot.notifier import TelegramNotifier


logger = logging.getLogger(__name__)


def main() -> int:
    ap = argparse.ArgumentParser("Отправка уведомлений в Telegram")
    ap.add_argument("--type", choices=["service", "personal", "group"], required=True)
    ap.add_argument("--email", help="для personal: e-mail сотрудника")
    ap.add_argument("--group", help="для group: пометка в сообщении")
    ap.add_argument("--all", action="store_true", help="для group: отправить всем (без метки)")
    ap.add_argument("--text", required=True, help="текст (HTML допустим)")
    ap.add_argument("--silent", action="store_true", help="тихое уведомление")
    args = ap.parse_args()

    n = TelegramNotifier()
    if args.type == "service":
        ok = n.send_service(args.text, silent=args.silent)
    elif args.type == "personal":
        if not args.email: ap.error("--email обязателен для personal")
        ok = n.send_personal(args.email, args.text, silent=args.silent)
    else:
        ok = n.send_group(args.text, group=None if args.all else args.group, for_all=args.all, silent=args.silent)
    if ok:
        logger.info("Telegram notification sent")
        return 0
    logger.error("Telegram notification failed")
    return 1

if __name__ == "__main__":
    setup_logging(app_name="wtt-tg-send", force_console=True)
    raise SystemExit(main())
