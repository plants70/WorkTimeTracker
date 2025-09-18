# collect_diagnostics.py
# Сбор всего нужного для быстрой доработки и диагностики проекта в один текстовый файл.

from __future__ import annotations
import argparse
import contextlib
import hashlib
import io
import json
import os
import platform
import re
import shutil
import sqlite3
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, Optional

# ---------- Аргументы CLI ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Собрать технический отчёт по проекту: пути, код, таблицы, БД, окружение."
    )
    p.add_argument("--project-root", type=Path, default=Path.cwd(),
                   help="Корень проекта (по умолчанию текущая папка).")
    p.add_argument("--output", type=Path, default=Path("./diagnostics_report.txt"),
                   help="Куда сохранить итоговый отчёт (txt).")
    p.add_argument("--full-code", action="store_true",
                   help="Положить в отчёт полный текст исходников (по расширениям).")
    p.add_argument("--code-extensions", default="py,yaml,yml,ini,toml,env,txt,md,json,sql,sh,ps1,bat,js,ts,css,html",
                   help="Какие расширения файлов включать в раздел с кодом (через запятую).")
    p.add_argument("--exclude-dirs", default=".git,.venv,venv,env,__pycache__,build,dist,node_modules,.mypy_cache,.pytest_cache,.idea,.vscode",
                   help="Каталоги, которые исключаем из обхода (через запятую).")
    p.add_argument("--max-file-kb", type=int, default=256,
                   help="Максимальный размер одного файла для включения целиком (КБ). Больше — обрезаем.")
    p.add_argument("--max-total-mb", type=int, default=16,
                   help="Глобальный бюджет на текст исходников (МБ). Дальше начнётся агрессивная обрезка.")
    p.add_argument("--logs-lines", type=int, default=300,
                   help="Сколько последних строк читать из каждого лог-файла.")
    p.add_argument("--db", action="store_true",
                   help="Включить анализ SQLite-БД.")
    p.add_argument("--db-path", type=Path,
                   help="Путь к SQLite-БД. Если не задан, попробуем импортировать из config или найдём автоматически.")
    p.add_argument("--sheets", action="store_true",
                   help="Включить анализ Google Sheets (заголовки листов).")
    p.add_argument("--cred", type=Path,
                   help="Путь к JSON-ключу сервисного аккаунта (если нельзя импортировать из config).")
    p.add_argument("--sheet", type=str,
                   help="ID или имя Google Sheets (если нельзя импортировать из config).")
    p.add_argument("--headers-only", action="store_true",
                   help="Для Sheets: только заголовки листов без данных.")
    p.add_argument("--env", action="store_true",
                   help="Добавить содержимое .env и переменные окружения (значения секретов редактируются).")
    p.add_argument("--no-redact", action="store_true",
                   help="Не редактировать секреты (ОПАСНО).")
    p.add_argument("--git", action="store_true",
                   help="Добавить сводку git (ветка, HEAD, статус).")
    p.add_argument("--pip", action="store_true",
                   help="Добавить pip freeze.")
    return p.parse_args()

# ---------- Утилиты форматирования ----------

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def hr(title: str = "", char: str = "─", width: int = 88) -> str:
    if title:
        title = f" {title} "
    core = char * width
    if title:
        mid = width // 2 - len(title) // 2
        core = core[:max(0, mid)] + title + core[max(0, mid + len(title)):]
    return core

def indent(text: str, n: int = 2) -> str:
    pad = " " * n
    return "\n".join(pad + line if line.strip() else line for line in text.splitlines())

def safe_rel(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except Exception:
        return str(path)

# ---------- Редакция секретов ----------

SECRET_KEYS = re.compile(r"(PASSWORD|PASS|SECRET|TOKEN|KEY|PRIVATE|CREDENTIAL|API|BEARER|AUTH)", re.I)

def redact(value: str) -> str:
    if not value:
        return value
    if len(value) <= 8:
        return "****"
    return value[:3] + "…" + value[-3:]

def redact_env_line(line: str) -> str:
    if "=" not in line:
        return line
    k, v = line.split("=", 1)
    if SECRET_KEYS.search(k):
        return f"{k}=<redacted>"
    return line

# ---------- Чтение файлов безопасно ----------

def read_text_safely(path: Path, max_bytes: int | None = None) -> str:
    encodings = ["utf-8", "cp1251", "latin-1"]
    for enc in encodings:
        try:
            data = path.read_bytes()
            if max_bytes is not None and len(data) > max_bytes:
                data = data[:max_bytes]
            return data.decode(enc, errors="replace")
        except Exception:
            continue
    return f"<<не удалось прочитать {path.name}>>"

def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()[:16]

# ---------- Поиск логов ----------

def find_log_files(project_root: Path) -> list[Path]:
    candidates: list[Path] = []
    # типовое расположение WorkTimeTracker под Windows
    if os.name == "nt":
        appdata = os.getenv("APPDATA")
        if appdata:
            candidates.append(Path(appdata) / "WorkTimeTracker" / "logs")
    # рядом с проектом
    for name in ("logs", "log", "output/logs"):
        candidates.append(project_root / name)
    out: list[Path] = []
    for base in candidates:
        if base.exists() and base.is_dir():
            out.extend(p for p in base.rglob("*.log") if p.is_file())
    return sorted(set(out))

# ---------- Поиск SQLite ----------

def autodetect_sqlite(project_root: Path) -> Optional[Path]:
    # эвристика: выбрать самый свежий *.db известного вида
    candidates = list(project_root.rglob("*.db"))
    candidates += [p for p in project_root.rglob("*local*backup*.db")]
    if not candidates:
        return None
    candidates = list(set(candidates))
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]

def summarize_sqlite(db_path: Path) -> str:
    buf = io.StringIO()
    buf.write(f"DB file: {db_path} ({db_path.stat().st_size/1024:.1f} KB)\n")
    with contextlib.closing(sqlite3.connect(str(db_path))) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        # схемы
        cur.execute("PRAGMA database_list;")
        dblist = cur.fetchall()
        buf.write("PRAGMA database_list:\n")
        for r in dblist:
            buf.write(f"  name={r['name']} file={r['file']}\n")
        # объекты
        cur.execute("SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name;")
        rows = cur.fetchall()
        buf.write("Schema objects (sqlite_master):\n")
        for r in rows:
            sql = (r["sql"] or "").strip()
            short = (sql[:500] + "…") if len(sql) > 500 else sql
            buf.write(f"  [{r['type']}] {r['name']} (table={r['tbl_name']})\n")
            if short:
                buf.write(indent(short, 4) + "\n")
        # количество строк по таблицам (осторожно)
        tables = [r["name"] for r in rows if r["type"] == "table" and not r["name"].startswith("sqlite_")]
        buf.write("Row counts (approx):\n")
        for t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) AS c FROM '{t}'")
                c = cur.fetchone()[0]
                buf.write(f"  {t}: {c}\n")
            except Exception as e:
                buf.write(f"  {t}: error: {e}\n")
    return buf.getvalue()

# ---------- Google Sheets ----------

@dataclass
class SheetsConfig:
    cred: Optional[Path]
    sheet: Optional[str]

def import_config(project_root: Path) -> dict:
    sys.path.insert(0, str(project_root))
    try:
        import config  # type: ignore
        out = {}
        for name in dir(config):
            if name.isupper():
                out[name] = getattr(config, name)
        # полезные функции, если есть
        for name in ("get_credentials_file", "GOOGLE_SHEET_NAME"):
            if hasattr(config, name):
                out[name] = getattr(config, name)
        return out
    except Exception:
        return {}

def _json_default(o):
    """Безопасная сериализация нестандартных объектов в JSON."""
    try:
        if isinstance(o, Path):
            return str(o)
        if isinstance(o, (set, tuple)):
            return list(o)
        # datetime/ZoneInfo и пр.
        if hasattr(o, "isoformat"):
            try:
                return o.isoformat()
            except Exception:
                pass
        if callable(o):
            return "<callable>"
        return str(o)
    except Exception:
        return f"<unserializable:{type(o).__name__}>"

def derive_sheets_conf(args: argparse.Namespace, cfg: dict) -> SheetsConfig:
    cred = args.cred
    sheet = args.sheet
    # попытка из config
    if cred is None:
        try:
            gcf = cfg.get("get_credentials_file")
            if callable(gcf):
                cred = Path(gcf())
        except Exception:
            pass
    if sheet is None:
        sheet = cfg.get("GOOGLE_SHEET_NAME") if isinstance(cfg.get("GOOGLE_SHEET_NAME"), str) else None
    return SheetsConfig(cred=cred, sheet=sheet)

def summarize_sheets(headers_only: bool, cred_path: Path, sheet_name_or_id: str) -> str:
    import gspread
    from google.oauth2.service_account import Credentials
    buf = io.StringIO()
    buf.write(f"Credentials: {cred_path}\n")
    buf.write(f"Spreadsheet: {sheet_name_or_id}\n")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_file(str(cred_path), scopes=scopes)
    client = gspread.client.Client(auth=creds)
    client.session = gspread.auth.AuthorizedSession(creds)
    # открытие по ID или имени
    try:
        ss = client.open_by_key(sheet_name_or_id)
    except Exception:
        ss = client.open(sheet_name_or_id)
    ws_list = ss.worksheets()
    buf.write(f"Worksheets ({len(ws_list)}): {[w.title for w in ws_list]}\n")
    for ws in ws_list:
        buf.write(hr(f"SHEET: {ws.title}") + "\n")
        header = ws.row_values(1)
        buf.write("Header: " + json.dumps(header, ensure_ascii=False) + "\n")
        if not headers_only:
            vals = ws.get_all_values()
            nrows = max(0, min(len(vals) - 1, 5))
            for i in range(1, 1 + nrows):
                row = vals[i] if i < len(vals) else []
                buf.write(f"Row{i}: " + json.dumps(row, ensure_ascii=False) + "\n")
    return buf.getvalue()

# ---------- Код и дерево проекта ----------

def should_skip_dir(path: Path, exclude: set[str]) -> bool:
    name = path.name
    return name in exclude

def walk_files(root: Path, exclude_dirs: set[str]) -> Iterator[Path]:
    for base, dirs, files in os.walk(root):
        base_p = Path(base)
        # фильтруем каталоги на месте
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        for f in files:
            yield base_p / f

def is_text_candidate(path: Path, allowed_exts: set[str]) -> bool:
    return path.suffix.lower().lstrip(".") in allowed_exts

def summarize_tree(root: Path, exclude_dirs: set[str]) -> str:
    # компактное древо вида: size, mtime, sha256(16)
    items: list[str] = []
    for p in sorted(walk_files(root, exclude_dirs)):
        try:
            st = p.stat()
            size_kb = f"{st.st_size/1024:.1f}KB"
            mtime = datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            digest = sha256_of_file(p)
            items.append(f"{safe_rel(p, root)} | {size_kb} | mtime={mtime} | sha256={digest}")
        except Exception as e:
            items.append(f"{safe_rel(p, root)} | <error: {e}>")
    return "\n".join(items)

def dump_sources(root: Path, allowed_exts: set[str], exclude_dirs: set[str], full: bool,
                 per_file_limit_kb: int, total_budget_mb: int) -> str:
    buf = io.StringIO()
    total_budget = total_budget_mb * 1024 * 1024
    written = 0
    for p in sorted(walk_files(root, exclude_dirs)):
        if not is_text_candidate(p, allowed_exts):
            continue
        try:
            st = p.stat()
        except Exception:
            continue
        max_bytes = None
        if not full or st.st_size > per_file_limit_kb * 1024 or written > total_budget:
            # обрезка: берём начальные и конечные куски
            max_bytes = per_file_limit_kb * 1024
        content = read_text_safely(p, None if full else max_bytes)
        if max_bytes and len(content.encode("utf-8", "ignore")) > max_bytes:
            # дополнительно «двухголовая» выборка (начало и конец)
            head = read_text_safely(p, max_bytes // 2)
            tail_bytes = max_bytes // 2
            data = p.read_bytes()
            tail = data[-tail_bytes:].decode("utf-8", "replace")
            content = head + "\n…\n" + tail
        section = f"{hr(safe_rel(p, root))}\n{content}\n"
        data_len = len(section.encode("utf-8", "ignore"))
        if written + data_len > total_budget and not full:
            buf.write(hr("BUDGET LIMIT REACHED — TRUNCATED") + "\n")
            break
        buf.write(section)
        written += data_len
    return buf.getvalue()

# ---------- Git и pip ----------

def git_summary(root: Path) -> str:
    def run(cmd: list[str]) -> str:
        try:
            out = subprocess.check_output(cmd, cwd=root, stderr=subprocess.STDOUT, text=True, timeout=10)
            return out.strip()
        except Exception as e:
            return f"<<git error: {e}>>"
    head = run(["git", "rev-parse", "--short", "HEAD"])
    branch = run(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    status = run(["git", "status", "-s", "-b"])
    return f"HEAD: {head}\nBRANCH: {branch}\nSTATUS:\n{status}\n"

def pip_freeze() -> str:
    try:
        out = subprocess.check_output([sys.executable, "-m", "pip", "freeze"], text=True, timeout=20)
        return out.strip()
    except Exception as e:
        return f"<<pip freeze error: {e}>>"

# ---------- .env и окружение ----------

def read_env_files(root: Path) -> list[Path]:
    out: list[Path] = []
    for name in (".env", ".env.local", ".env.example"):
        p = root / name
        if p.exists():
            out.append(p)
    # ищем ещё в config/ или app/ подпапках
    for p in root.rglob(".env"):
        if p.is_file():
            out.append(p)
    return sorted(set(out))

def dump_env(root: Path, redact_secrets: bool) -> str:
    buf = io.StringIO()
    env_files = read_env_files(root)
    buf.write(f"Found .env files: {[safe_rel(p, root) for p in env_files]}\n\n")
    for p in env_files:
        buf.write(hr(f"FILE: {safe_rel(p, root)}") + "\n")
        content = read_text_safely(p)
        if redact_secrets:
            lines = [redact_env_line(line) for line in content.splitlines()]
            content = "\n".join(lines)
        buf.write(content + "\n\n")
    # переменные окружения (с фильтром)
    buf.write(hr("ENVIRONMENT VARIABLES") + "\n")
    env_vars = sorted(os.environ.items())
    for k, v in env_vars:
        if redact_secrets and SECRET_KEYS.search(k):
            v = "<redacted>"
        buf.write(f"{k}={v}\n")
    return buf.getvalue()

# ---------- Основная логика ----------

def main() -> int:
    args = parse_args()
    project_root = args.project_root.resolve()
    exclude_dirs = set(d.strip() for d in args.exclude_dirs.split(","))
    allowed_exts = set(e.strip() for e in args.code_extensions.split(","))
    buf = io.StringIO()
    # заголовок
    buf.write(hr("DIAGNOSTICS REPORT") + "\n")
    buf.write(f"Generated: {now_str()}\n")
    buf.write(f"Project: {project_root}\n")
    buf.write(f"Platform: {platform.platform()}\n")
    buf.write(f"Python: {sys.version}\n\n")
    # конфиг
    cfg = import_config(project_root)
    if cfg:
        buf.write(hr("CONFIG EXPORT (UPPERCASE NAMES + helpers)") + "\n")
        pretty = json.dumps(cfg, ensure_ascii=False, indent=2, default=_json_default)
        buf.write(pretty + "\n\n")
    # источники
    buf.write(hr("SOURCES") + "\n")
    full_flag = bool(args.full_code)
    buf.write(f"Extensions: {sorted(allowed_exts)} | full_code={full_flag} | "
              f"max_file_kb={args.max_file_kb} | total_budget_mb={args.max_total_mb}\n\n")
    # дерево файлов
    buf.write(hr("FILE TREE (size, mtime, sha256)") + "\n")
    buf.write(summarize_tree(project_root, exclude_dirs) + "\n\n")
    # код
    buf.write(hr("SOURCE CODE") + "\n")
    code = dump_sources(project_root, allowed_exts, exclude_dirs, args.full_code,
                        args.max_file_kb, args.max_total_mb)
    buf.write(code + "\n\n")
    # логи
    logs = find_log_files(project_root)
    buf.write(hr("LOG FILES (last lines)") + "\n")
    buf.write(f"Found {len(logs)} log files.\n")
    for log in logs:
        buf.write(f"\n{hr(safe_rel(log, project_root))}\n")
        try:
            lines = read_text_safely(log).splitlines()
            tail = lines[-args.logs_lines:] if len(lines) > args.logs_lines else lines
            buf.write("\n".join(tail) + "\n")
        except Exception as e:
            buf.write(f"<<error reading log: {e}>>\n")
    # БД
    if args.db:
        buf.write(hr("SQLITE DATABASE") + "\n")
        db_path = args.db_path or autodetect_sqlite(project_root)
        if db_path and db_path.exists():
            buf.write(summarize_sqlite(db_path) + "\n")
        else:
            buf.write("No SQLite database found.\n")
    # Google Sheets
    if args.sheets:
        buf.write(hr("GOOGLE SHEETS") + "\n")
        conf = derive_sheets_conf(args, cfg)
        if conf.cred and conf.sheet and conf.cred.exists():
            try:
                buf.write(summarize_sheets(args.headers_only, conf.cred, conf.sheet) + "\n")
            except Exception as e:
                buf.write(f"<<error accessing Google Sheets: {e}>>\n")
        else:
            buf.write("Google Sheets not configured (missing cred or sheet).\n")
    # .env и окружение
    if args.env:
        buf.write(hr("ENVIRONMENT") + "\n")
        buf.write(dump_env(project_root, redact_secrets=not args.no_redact) + "\n")
    # git
    if args.git:
        buf.write(hr("GIT") + "\n")
        buf.write(git_summary(project_root) + "\n")
    # pip
    if args.pip:
        buf.write(hr("PIP FREEZE") + "\n")
        buf.write(pip_freeze() + "\n")
    # запись
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(buf.getvalue(), encoding="utf-8")
    print(f"Report written to: {args.output}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())