# tools/tg_send.py
from __future__ import annotations
import argparse, logging
from telegram_bot.notifier import TelegramNotifier

logging.basicConfig(level=logging.INFO)

def main():
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
    print("OK" if ok else "FAIL")

if __name__ == "__main__":
    main()
