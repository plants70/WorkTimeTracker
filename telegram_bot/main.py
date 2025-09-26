# telegram_bot/main.py
from __future__ import annotations
import logging, re, time, requests, os
from typing import Optional
from config import GOOGLE_SHEET_NAME, USERS_SHEET, TELEGRAM_BOT_TOKEN as CFG_TELEGRAM_BOT_TOKEN
from sheets_api import SheetsAPI

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def _base() -> str:
    # config → ENV
    token = (CFG_TELEGRAM_BOT_TOKEN or os.getenv("TELEGRAM_BOT_TOKEN", "")).strip()
    if not token:
        raise SystemExit(
            "TELEGRAM_BOT_TOKEN не задан. "
            "В PowerShell установите переменную так:\n"
            '$env:TELEGRAM_BOT_TOKEN = "123456:ABC..."\n'
            "Без угловых скобок."
        )
    return f"https://api.telegram.org/bot{token}"

def _send(chat_id: int | str, text: str) -> None:
    requests.post(_base()+"/sendMessage", json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=20)

def _num_to_col(n: int) -> str:
    res = ""
    while n:
        n, r = divmod(n - 1, 26)
        res = chr(65 + r) + res
    return res

def _set_user_telegram(email: str, chat_id: int | str) -> bool:
    api = SheetsAPI()
    ws = api.client.open(GOOGLE_SHEET_NAME).worksheet(USERS_SHEET)
    header = api._request_with_retry(ws.row_values, 1) or []
    values = api._request_with_retry(ws.get_all_values) or []
    lh = [str(h or "").strip().lower() for h in header]
    if "email" not in lh:
        raise RuntimeError("В листе Users нет колонки 'Email'")
    ix_email = lh.index("email")
    ix_tg = lh.index("telegram") if "telegram" in lh else None
    row_ix = None
    for i, r in enumerate(values[1:], start=2):
        e = (r[ix_email] if ix_email < len(r) else "").strip().lower()
        if e == email:
            row_ix = i; break
    if row_ix is None:
        return False
    if ix_tg is None:
        header.append("Telegram")
        api._request_with_retry(ws.update, "A1", [header])
        ix_tg = len(header) - 1
    
    # надёжная запись в одну ячейку
    try:
        api._request_with_retry(ws.update_cell, row_ix, ix_tg + 1, str(chat_id))
    except Exception:
        # fallback: если вдруг update_cell недоступен — используем update с 2D-матрицей
        cell = f"{_num_to_col(ix_tg + 1)}{row_ix}"
        api._request_with_retry(ws.update, cell, [[str(chat_id)]])
    return True

def main():
    log.info("Telegram linker bot started")
    base = _base()
    offset: Optional[int] = None
    hello = ("👋 Привет! Отправь свой рабочий e-mail (например, user@company.com), "
             "и я привяжу этот чат к уведомлениям системы.")
    while True:
        try:
            params = {"timeout": 60}
            if offset is not None:
                params["offset"] = offset
            r = requests.get(base+"/getUpdates", params=params, timeout=70)
            data = r.json()
            if not data.get("ok"):
                time.sleep(2); continue
            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                text = (msg.get("text") or "").strip()
                chat_id = (msg.get("chat") or {}).get("id")
                if not chat_id:
                    continue
                if text.startswith("/start"):
                    _send(chat_id, hello); continue
                if EMAIL_RE.match(text):
                    email = text.lower()
                    ok = _set_user_telegram(email, chat_id)
                    _send(chat_id, "✅ Готово! Связал <b>%s</b> с этим чатом." % email if ok
                                   else "⚠️ Не нашёл e-mail <b>%s</b> в списке пользователей." % email)
                else:
                    _send(chat_id, "Это не похоже на e-mail. Пришлите адрес вида <b>user@company.com</b>.")
        except KeyboardInterrupt:
            break
        except Exception as e:
            log.warning("Loop error: %s", e); time.sleep(3)

if __name__ == "__main__":
    main()