# notifications/engine.py
from __future__ import annotations
import logging
import sqlite3
import threading
import datetime as dt

from notifications.rules_manager import load_rules, Rule
from telegram_bot.notifier import TelegramNotifier
from config import LOCAL_DB_PATH

log = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS status_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    status TEXT NOT NULL,
    ts_utc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_status_events_email_ts ON status_events(email, ts_utc);

CREATE TABLE IF NOT EXISTS rule_last_sent (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id INTEGER NOT NULL,
    email TEXT,
    context TEXT,
    last_sent_utc TEXT NOT NULL,
    UNIQUE(rule_id, email, context)
);
"""

_poller_stop = None


def start_background_poller(interval_sec: int = 60):
    """Запускает фоновый опрос long_status/status_window."""
    global _poller_stop
    if _poller_stop:
        return _poller_stop
    _poller_stop = threading.Event()

    def _loop():
        while not _poller_stop.is_set():
            try:
                poll_long_running_remote()
            except Exception:
                log.exception("Long-status poll failed")
            _poller_stop.wait(interval_sec)

    threading.Thread(target=_loop, daemon=True).start()
    return _poller_stop


def _open_db() -> sqlite3.Connection:
    con = sqlite3.connect(LOCAL_DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.executescript(DDL)
    return con


def _now_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()


class _SafeDict(dict):
    # Возвращаем {placeholder} как текст, если ключ не найден
    def __missing__(self, key):
        return "{" + key + "}"


# === Событие: записать факт смены статуса (для status_window) ===
def record_status_event(
    email: str, status_name: str, ts_iso: str | None = None
) -> None:
    email = (email or "").strip().lower()
    if not email:
        return
    ts_iso = ts_iso or _now_iso()
    try:
        con = _open_db()
        with con:
            con.execute(
                "INSERT INTO status_events(email, status, ts_utc) VALUES (?,?,?)",
                (email, status_name or "", ts_iso),
            )
        _maybe_fire_status_window_rules(email)
    except Exception as e:
        log.exception("record_status_event error: %s", e)


def _maybe_fire_status_window_rules(email: str) -> None:
    rules = [r for r in load_rules() if r.kind == "status_window"]
    if not rules:
        return
    con = _open_db()
    for rule in rules:
        try:
            window_min = rule.window_min or 0
            limit = rule.limit or 0
            if window_min <= 0 or limit <= 0:
                continue
            start_ts = (
                (dt.datetime.now(dt.UTC) - dt.timedelta(minutes=window_min))
                .replace(microsecond=0)
                .isoformat()
            )
            cur = con.execute(
                "SELECT COUNT(*) FROM status_events WHERE email=? AND ts_utc>=?",
                (email, start_ts),
            )
            cnt = int(cur.fetchone()[0])
            if cnt < limit:
                continue
            # антиспам по окну
            context = f"window:{window_min}:{start_ts[:16]}"  # приблизим до минуты
            if not _ratelimit_ok(con, rule, email, context):
                continue

            ctx = {
                "email": email,
                "status": "",
                "duration_min": "",
                "limit": limit,
                "window_min": window_min,
                "group": rule.group_tag,
                "count": cnt,
            }
            _send_by_scope(rule, email, ctx)
        except Exception as e:
            log.debug("status_window check failed: %s", e)


# === Событие: длительный статус (подаётся подготовленными данными) ===
def long_status_check(
    email: str, status_name: str, started_dt: dt.datetime, elapsed_min: int
) -> None:
    email = (email or "").strip().lower()
    rules = [r for r in load_rules() if r.kind == "long_status"]
    if not rules:
        return
    con = _open_db()
    s_lc = (status_name or "").strip().lower()
    for rule in rules:
        try:
            # если rule.statuses пуст — применяем ко всем, иначе матчим по списку
            if rule.statuses and s_lc not in [x.lower() for x in rule.statuses]:
                continue
            need = rule.min_duration_min or 0
            if need <= 0 or elapsed_min < need:
                continue
            context = f"long:{s_lc}:{started_dt.replace(microsecond=0).astimezone(dt.UTC).isoformat()}"
            if not _ratelimit_ok(con, rule, email, context):
                continue

            ctx = {
                "email": email,
                "status": status_name,
                "duration_min": elapsed_min,
                "min_duration_min": need,
                "limit": "",
                "window_min": "",
                "group": rule.group_tag,
            }
            _send_by_scope(rule, email, ctx)
        except Exception as e:
            log.debug("long_status check failed: %s", e)


def poll_long_running_remote() -> None:
    """
    Периодический бэкграунд-чек «длительных статусов» для текущего пользователя.
    1) Берём последний статус по (email, session_id) из локальной БД (без IS NULL).
    2) Если локально ничего нет — читаем ActiveSessions (только чтение).
    3) Считаем длительность с учётом локальной TZ → UTC и применяем long_status-правила.
    """
    logger = logging.getLogger(__name__)
    try:
        from user_app import session as session_state
        from user_app.db_local import LocalDB
    except Exception:
        logger.debug("poll_long_running_remote: session/LocalDB not available")
        return

    email = (session_state.get_user_email() or "").strip().lower()
    if not email:
        return

    db = LocalDB()
    sess = db.get_active_session(email)
    if not sess:
        sess = {}

    session_id = (sess.get("session_id") or "").strip()

    # 1) Пробуем локальную БД: последняя запись статуса в рамках сессии
    status_name = None
    started_iso = None
    if session_id:
        with db._lock:
            cur = db.conn.cursor()
            cur.execute(
                """
                SELECT status, COALESCE(status_start_time, timestamp) AS started_iso
                FROM logs
                WHERE email=? AND session_id=? AND action_type IN ('LOGIN','STATUS_CHANGE')
                ORDER BY id DESC LIMIT 1
            """,
                (email, session_id),
            )
            row = cur.fetchone()
            if row:
                status_name, started_iso = row

    # 2) Если локально не нашли — фолбэк к ActiveSessions
    if not status_name or not started_iso:
        try:
            from sheets_api import SheetsAPI
            from config import GOOGLE_SHEET_NAME

            api = SheetsAPI()
            ss = api.client.open(GOOGLE_SHEET_NAME)
            ws = ss.worksheet("ActiveSessions")
            header = api._request_with_retry(ws.row_values, 1) or []
            values = api._request_with_retry(ws.get_all_values) or []

            def _find_idx(names: list[str]) -> int | None:
                h = [str(x or "").strip().lower() for x in header]
                for n in names:
                    if n.strip().lower() in h:
                        return h.index(n.strip().lower())
                return None

            ix_email = _find_idx(["email", "e-mail"])
            ix_status = _find_idx(["status", "статус"])
            ix_start = _find_idx(["starttime", "start", "начало", "startedat"])
            if ix_email is not None and ix_status is not None and ix_start is not None:
                for r in values[1:]:
                    e = (r[ix_email] if ix_email < len(r) else "").strip().lower()
                    if e == email:
                        status_name = (
                            r[ix_status] if ix_status < len(r) else ""
                        ).strip()
                        started_iso = (r[ix_start] if ix_start < len(r) else "").strip()
                        break
        except Exception as e:
            logger.debug("ActiveSessions fallback failed: %s", e)

    if not status_name or not started_iso:
        return

    # 3) Парсим время: если «наивное» — трактуем как локальное и конвертируем в UTC
    try:
        parsed = dt.datetime.fromisoformat(started_iso.replace("Z", "+00:00"))
    except Exception:
        logger.debug("poll_long_running_remote: bad started_iso=%r", started_iso)
        return
    if parsed.tzinfo is None:
        local_tz = dt.datetime.now().astimezone().tzinfo or dt.UTC
        started_dt = parsed.replace(tzinfo=local_tz)
    else:
        started_dt = parsed
    started_utc = started_dt.astimezone(dt.UTC)
    elapsed_min = max(
        0, int((dt.datetime.now(dt.UTC) - started_utc).total_seconds() // 60)
    )

    logger.debug(
        "long-status poll: status=%s started_local=%s started_utc=%s elapsed_min=%d",
        status_name,
        started_dt.isoformat(),
        started_utc.isoformat(),
        elapsed_min,
    )
    try:
        long_status_check(
            email=email,
            status_name=status_name,
            started_dt=started_utc,
            elapsed_min=elapsed_min,
        )
    except Exception:
        logger.exception("poll_long_running_remote: long_status_check failed")


# === helpers ===
def _ratelimit_ok(
    con: sqlite3.Connection, rule: Rule, email: str | None, context: str
) -> bool:
    cur = con.execute(
        "SELECT last_sent_utc FROM rule_last_sent WHERE rule_id=? AND COALESCE(email,'')=COALESCE(?, '') AND context=?",
        (rule.id, email, context),
    )
    row = cur.fetchone()
    if not row:
        return True
    try:
        prev = dt.datetime.fromisoformat(row[0])
    except Exception:
        return True
    gap = (dt.datetime.now(dt.UTC) - prev).total_seconds()
    return gap >= max(1, rule.rate_limit_sec)


def _touch_last_sent(
    con: sqlite3.Connection, rule: Rule, email: str | None, context: str
) -> None:
    con.execute(
        """INSERT OR REPLACE INTO rule_last_sent(rule_id, email, context, last_sent_utc)
           VALUES(?,?,?,?)""",
        (rule.id, email, context, _now_iso()),
    )


def _default_template(rule: Rule) -> str:
    """Генерирует дефолтный шаблон по типу правила."""
    if rule.kind == "long_status":
        return "⏱ Длительный статус: {status} уже {duration_min} мин (порог {min_duration_min} мин)."
    if rule.kind == "status_window":
        return "⚠️ Много изменений статусов: {count}/{limit} за {window_min} мин."
    return "⚙️ Уведомление: {context}"


def _send_by_scope(rule: Rule, email: str, ctx: dict[str, object]) -> None:
    n = TelegramNotifier()
    # 1) Нормализуем шаблон
    raw = (rule.template or "").strip()
    if raw.upper() in ("TRUE", "FALSE"):  # защитимся от булевых из Sheets
        raw = ""
    # 2) Дефолт если шаблона нет
    text = raw or _default_template(rule)
    # 3) Безопасное форматирование (не падаем, плейсхолдеры сохраняем как {name})
    try:
        text = text.format_map(_SafeDict(ctx))
    except Exception:
        log.debug("template format failed; ctx=%s", ctx)
        # оставляем text как есть

    if rule.scope == "service":
        n.send_service(text, silent=rule.silent)
    elif rule.scope == "group":
        n.send_group(
            text,
            group=rule.group_tag or None,
            for_all=not bool(rule.group_tag),
            silent=rule.silent,
        )
    else:
        n.send_personal(email, text, silent=rule.silent)
