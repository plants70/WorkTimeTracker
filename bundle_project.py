#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
inspect_and_bundle.py — единый инструмент:
  1) Снимок дерева проекта (с исключениями)
  2) TXT-бандл исходников/конфигов
  3) Обзор локальной SQLite (схема, индексы, триггеры, счётчики, примеры)
  4) Обзор Google Sheets (книга, листы, заголовки, количество строк, примеры)

Зависимости:
  - stdlib
  - Ваш проект (config.py, sheets_api.py) — для путей/доступа к Google Sheets.
    sheets_api уже содержит «ленивый» прокси/клиент и ретраи.

Примеры:
  python inspect_and_bundle.py -r . -o project_report.txt
  python inspect_and_bundle.py -r . -o report.txt --db C:/path/to/local_backup.db
  python inspect_and_bundle.py --git-only --no-sheets
"""

from __future__ import annotations

import argparse
import hashlib
import mimetypes
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Iterable, List, Dict, Optional, Tuple

# -----------------------------------
# 1) Настройки для дерева и бандла
# -----------------------------------

EXCLUDE_TREE = {'.venv', '__pycache__', '.git', '.idea', 'dist', 'build', '.vscode', '.mypy_cache', '.pytest_cache', '.ruff_cache', 'node_modules', 'target', 'out'}
DEFAULT_EXCLUDE_DIRS = {
    ".git", ".hg", ".svn", ".idea", ".vscode",
    ".venv", "venv", "env",
    "__pycache__", ".mypy_cache", ".pytest_cache", ".ruff_cache",
    "build", "dist", ".cache", ".eggs", ".tox", ".coverage",
    "node_modules", "target", "out"
}

# Текстовые расширения (можно расширить через --include-ext)
DEFAULT_INCLUDE_EXTS = {
    # code
    ".py", ".pyw", ".ipynb",
    ".js", ".jsx", ".ts", ".tsx", ".vue", ".svelte",
    ".java", ".kt", ".kts", ".scala", ".go", ".rb", ".php",
    ".c", ".cc", ".cpp", ".h", ".hpp", ".cs", ".rs", ".swift",
    # web / markup
    ".html", ".htm", ".css", ".scss", ".sass",
    ".xml", ".xsd", ".xslt",
    # config
    ".json", ".jsonc", ".yaml", ".yml", ".ini", ".cfg", ".toml", ".env",
    # scripts
    ".bat", ".cmd", ".ps1", ".psm1", ".sh", ".bash",
    # data-ish (text)
    ".md", ".rst", ".txt", ".csv", ".tsv", ".sql",
    # project files
    ".sln", ".csproj", ".vbproj", ".props", ".targets", ".cmake", "CMakeLists.txt",
    ".gradle", ".pro", ".pri", "Makefile", "Dockerfile", "Procfile",
}
SPECIAL_FILENAMES = {"Makefile", "Dockerfile", "Procfile", "CMakeLists.txt", ".gitignore", ".gitattributes"}

# -----------------------------------
# 2) Вспомогалки
# -----------------------------------

def normalize_extensions_set(exts: Iterable[str]) -> set[str]:
    out = set()
    for e in exts:
        e = (e or "").strip()
        if not e:
            continue
        if not e.startswith(".") and e not in SPECIAL_FILENAMES:
            e = "." + e
        out.add(e)
    return out

def is_binary_by_chunk(p: Path, chunk_size: int = 2048) -> bool:
    try:
        with p.open("rb") as f:
            chunk = f.read(chunk_size)
        if b"\x00" in chunk:
            return True
        text_chars = bytearray({7,8,9,10,12,13,27} | set(range(0x20, 0x100)) - {0x7f})
        nontext = chunk.translate(None, text_chars)
        return len(nontext) / max(1, len(chunk)) > 0.30
    except Exception:
        return True

def should_include_file(p: Path, include_exts: set[str]) -> bool:
    name = p.name
    if name in SPECIAL_FILENAMES:
        return True
    ext = p.suffix
    if ext in include_exts:
        return True
    mtype, _ = mimetypes.guess_type(str(p))
    if mtype and mtype.startswith("text/"):
        return True
    return False

def sha256_of_text(s: str) -> str:
    import hashlib as _h
    return _h.sha256(s.encode("utf-8", errors="replace")).hexdigest()

def read_text_best_effort(p: Path) -> str:
    for enc in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
        try:
            return p.read_text(encoding=enc)
        except Exception:
            continue
    try:
        return p.read_bytes().decode("latin-1", errors="replace")
    except Exception:
        return ""

# -----------------------------------
# 3) Дерево проекта (в строку)
# -----------------------------------

def render_tree(dir_path: Path, prefix: str = "", exclude: set[str] = None) -> str:
    exclude = exclude or set()
    lines: List[str] = []
    try:
        entries = [e for e in os.listdir(dir_path) if e not in exclude]
    except Exception as e:
        return f"[tree] Ошибка доступа к {dir_path}: {e}\n"
    entries.sort()
    for i, name in enumerate(entries):
        path = dir_path / name
        connector = "└── " if i == len(entries) - 1 else "├── "
        lines.append(prefix + connector + name)
        if path.is_dir():
            extension = "    " if i == len(entries) - 1 else "│   "
            lines.append(render_tree(path, prefix + extension, exclude))
    return "\n".join(lines)

# -----------------------------------
# 4) Сбор файлов проекта в TXT
# -----------------------------------

def collect_files(root: Path, include_exts: set[str], exclude_dirs: set[str], max_bytes: int) -> List[Path]:
    files: List[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in exclude_dirs]
        dp = Path(dirpath)
        for fname in filenames:
            p = dp / fname
            try:
                if not p.is_file():
                    continue
                if p.stat().st_size > max_bytes:
                    continue
                if not should_include_file(p, include_exts):
                    continue
                if is_binary_by_chunk(p):
                    continue
                files.append(p)
            except Exception:
                continue
    files.sort(key=lambda x: str(x).lower())
    return files

def write_bundle(out, root: Path, files: List[Path]) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    header = [
        "=" * 80,
        f"PROJECT REPORT",
        f"Generated:   {ts}",
        f"Root:        {root}",
        f"Python:      {sys.version.split()[0]}",
        f"Files:       {len(files)}",
        "=" * 80,
        ""
    ]
    out.write("\n".join(header) + "\n")
    for p in files:
        try:
            rel = p.relative_to(root)
        except Exception:
            rel = p
        try:
            text = read_text_best_effort(p)
            size = p.stat().st_size
            h = sha256_of_text(text)
            out.write("\n" + "-" * 80 + "\n")
            out.write(f"# FILE: {rel}\n")
            out.write(f"# SIZE: {size} bytes | SHA256(text): {h}\n")
            out.write("-" * 80 + "\n")
            out.write(text)
            if not text.endswith("\n"):
                out.write("\n")
        except Exception as e:
            out.write("\n" + "-" * 80 + "\n")
            out.write(f"# FILE: {rel}\n")
            out.write(f"# ERROR: {e}\n")
            out.write("-" * 80 + "\n\n")

# -----------------------------------
# 5) Инспекция SQLite
# -----------------------------------

def _sql_fetchall_safe(cur: sqlite3.Cursor, sql: str, params: Tuple = ()) -> List[Tuple]:
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    except Exception:
        return []

def introspect_sqlite(db_path: Path, sample_limit: int = 5) -> str:
    out: List[str] = []
    out.append("=" * 80)
    out.append("LOCAL SQLITE OVERVIEW")
    out.append(f"DB Path: {db_path}")
    out.append("=" * 80)

    if not db_path.exists():
        out.append(f"[warn] Файл БД не найден: {db_path}")
        return "\n".join(out) + "\n"

    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()

        # Базовая инфо
        db_list = _sql_fetchall_safe(cur, "PRAGMA database_list;")
        out.append(f"database_list: {db_list}")

        # Таблицы (кроме служебных)
        tables = _sql_fetchall_safe(
            cur, "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
        )
        for name, create_sql in tables:
            out.append("-" * 80)
            out.append(f"[TABLE] {name}")
            out.append(f"schema: {create_sql}")

            # Колонки
            cols = _sql_fetchall_safe(cur, f"PRAGMA table_info({name});")
            if cols:
                out.append("columns:")
                for cid, cname, ctype, notnull, dflt, pk in cols:
                    out.append(f"  - {cname} {ctype or ''} NOTNULL={notnull} PK={pk} DEFAULT={dflt}")

            # Индексы
            idxs = _sql_fetchall_safe(cur, f"PRAGMA index_list({name});")
            if idxs:
                out.append("indexes:")
                for idx in idxs:
                    iname = idx[1]
                    unique = idx[2]
                    out.append(f"  - {iname} UNIQUE={unique}")
                    idxcols = _sql_fetchall_safe(cur, f"PRAGMA index_info({iname});")
                    for _, seqno, cname in idxcols:
                        out.append(f"      * {seqno}: {cname}")

            # Триггеры по таблице
            trigs = _sql_fetchall_safe(
                cur, "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? ORDER BY name;", (name,)
            )
            if trigs:
                out.append("triggers:")
                for tname, tsql in trigs:
                    out.append(f"  - {tname}: {tsql}")

            # Счётчик строк
            cnt = _sql_fetchall_safe(cur, f"SELECT COUNT(*) FROM {name};")
            if cnt:
                out.append(f"rows_count: {cnt[0][0]}")

            # Примеры строк
            samples = _sql_fetchall_safe(cur, f"SELECT * FROM {name} ORDER BY ROWID DESC LIMIT {int(sample_limit)};")
            if samples:
                out.append("sample rows (last):")
                for row in samples:
                    out.append(f"  • {row}")

        # Глобальные триггеры
        gtrigs = _sql_fetchall_safe(cur, "SELECT name, tbl_name, sql FROM sqlite_master WHERE type='trigger' ORDER BY name;")
        if gtrigs:
            out.append("-" * 80)
            out.append("[TRIGGERS GLOBAL]")
            for name, tbl, sql in gtrigs:
                out.append(f"  - {name} on {tbl}: {sql}")

        conn.close()
    except Exception as e:
        out.append(f"[error] Ошибка открытия/чтения SQLite: {e}")

    out.append("")
    return "\n".join(out)

# -----------------------------------
# 6) Инспекция Google Sheets
# -----------------------------------

def introspect_gsheets(sample_limit: int = 3) -> str:
    """
    Пытается импортировать ваш sheets_api и построить обзор книги:
      - Название книги из config
      - Список листов
      - Заголовок каждой таблицы
      - Кол-во непустых строк (по get_all_values)
      - Первые sample_limit строк
    """
    out: List[str] = []
    out.append("=" * 80)
    out.append("GOOGLE SHEETS OVERVIEW")
    out.append("=" * 80)

    try:
        # Импортируем ленивый прокси/клиент из вашего проекта
        # В проекте есть методы list_worksheet_titles(), get_users(), get_all_active_sessions() и т.п. :contentReference[oaicite:2]{index=2}
        import importlib

        try:
            sheets_mod = importlib.import_module("sheets_api")
        except Exception as e:
            out.append(f"[warn] Не удалось импортировать sheets_api: {e}")
            return "\n".join(out) + "\n"

        # Возьмём реальный инстанс
        if hasattr(sheets_mod, "get_sheets_api"):
            sheets = sheets_mod.get_sheets_api()
        elif hasattr(sheets_mod, "SheetsAPI"):
            sheets = sheets_mod.SheetsAPI()
        else:
            out.append("[warn] sheets_api не содержит SheetsAPI/get_sheets_api")
            return "\n".join(out) + "\n"

        # Заголовок и листы
        try:
            from config import GOOGLE_SHEET_NAME  # имя книги хранится в конфиге :contentReference[oaicite:3]{index=3}
        except Exception:
            GOOGLE_SHEET_NAME = "(см. config)"

        out.append(f"Spreadsheet: {GOOGLE_SHEET_NAME}")

        titles: List[str] = []
        try:
            titles = list(sheets.list_worksheet_titles())
        except Exception as e:
            out.append(f"[warn] list_worksheet_titles error: {e}")

        if not titles:
            out.append("[warn] Листы не найдены или нет доступа.")
            return "\n".join(out) + "\n"

        for t in titles:
            out.append("-" * 80)
            out.append(f"[SHEET] {t}")

            try:
                ws = sheets._get_ws(t)  # внутренний helper у вас есть
            except Exception:
                # fallback: через клиент
                try:
                    ss = sheets.client.open(GOOGLE_SHEET_NAME)
                    ws = ss.worksheet(t)
                except Exception as e:
                    out.append(f"  [warn] Не удалось открыть лист '{t}': {e}")
                    continue

            # Заголовок
            try:
                header = sheets._request_with_retry(lambda: ws.row_values(1))
                out.append(f"header: {header}")
            except Exception as e:
                out.append(f"  [warn] header read error: {e}")
                header = []

            # Все значения, чтобы посчитать заполненные строки (без пустых хвостов)
            values: List[List[str]] = []
            try:
                values = sheets._request_with_retry(lambda: ws.get_all_values())
            except Exception as e:
                out.append(f"  [warn] get_all_values error: {e}")

            if values:
                # Непустые строки (грубая оценка)
                nonempty = [r for r in values[1:] if any((c or "").strip() for c in r)]
                out.append(f"rows_count (non-empty): {len(nonempty)}")
                # Примеры первых N
                if nonempty:
                    out.append(f"sample rows (first {sample_limit}):")
                    for row in nonempty[:sample_limit]:
                        out.append("  • " + str(row))

    except Exception as e:
        out.append(f"[error] Ошибка обзора Google Sheets: {e}")

    out.append("")
    return "\n".join(out)

# -----------------------------------
# 7) Главная функция/CLI
# -----------------------------------

def main():
    ap = argparse.ArgumentParser(description="Собрать отчёт по проекту (дерево, бандл, SQLite, Google Sheets).")
    ap.add_argument("-r", "--root", type=str, default=".", help="Корень проекта (default .)")
    ap.add_argument("-o", "--output", type=str, default="project_report.txt", help="Путь к выходному TXT.")
    ap.add_argument("--max-bytes", type=int, default=3_000_000, help="Макс. размер одного файла (байт).")
    ap.add_argument("--include-ext", type=str, nargs="*", default=None, help="Доп. расширения (py toml conf ...)")
    ap.add_argument("--exclude-dir", type=str, nargs="*", default=None, help="Доп. каталоги для исключения (имена).")
    ap.add_argument("--git-only", action="store_true", help="Собирает только файлы, отслеживаемые Git.")
    ap.add_argument("--no-tree", action="store_true", help="Не добавлять дерево проекта.")
    ap.add_argument("--no-db", action="store_true", help="Не добавлять обзор SQLite.")
    ap.add_argument("--no-sheets", action="store_true", help="Не добавлять обзор Google Sheets.")
    ap.add_argument("--db", type=str, default=None, help="Путь к SQLite (иначе возьмём из config.LOCAL_DB_PATH).")
    ap.add_argument("--db-sample", type=int, default=5, help="Количество строк-примеров из таблиц SQLite.")
    ap.add_argument("--sheets-sample", type=int, default=3, help="Количество строк-примеров из листов Google Sheets.")

    args = ap.parse_args()

    root = Path(args.root).resolve()
    out_path = Path(args.output).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    include_exts = set(DEFAULT_INCLUDE_EXTS)
    if args.include_ext:
        include_exts |= normalize_extensions_set(set(args.include_ext))

    exclude_dirs = set(DEFAULT_EXCLUDE_DIRS)
    if args.exclude_dir:
        exclude_dirs |= set(args.exclude_dir)

    # --- Подготовим список файлов ---
    if args.git_only:
        try:
            import subprocess
            res = subprocess.run(
                ["git", "ls-files"],
                check=True,
                cwd=str(root),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
            )
            files = []
            tracked = [root / line.strip() for line in res.stdout.splitlines() if line.strip()]
            for p in tracked:
                try:
                    if not p.exists() or not p.is_file():
                        continue
                    if p.stat().st_size > args.max_bytes:
                        continue
                    if not should_include_file(p, include_exts):
                        continue
                    if is_binary_by_chunk(p):
                        continue
                    files.append(p)
                except Exception:
                    continue
            files.sort(key=lambda x: str(x).lower())
        except Exception as e:
            print(f"[WARN] git-only режим не удался: {e}. Переход к файловому обходу.", file=sys.stderr)
            files = collect_files(root, include_exts, exclude_dirs, args.max_bytes)
    else:
        files = collect_files(root, include_exts, exclude_dirs, args.max_bytes)

    # --- Путь к локальной БД ---
    db_path: Optional[Path] = None
    if not args.no_db:
        if args.db:
            db_path = Path(args.db)
        else:
            # Попробуем достать из config.LOCAL_DB_PATH (у вас так и сделано в проекте) :contentReference[oaicite:4]{index=4}
            try:
                import importlib
                cfg = importlib.import_module("config")
                db_path = Path(getattr(cfg, "LOCAL_DB_PATH"))
            except Exception:
                # Фолбэк — ищем *.db рядом
                candidates = list(root.glob("**/*.db"))
                db_path = candidates[0] if candidates else None

    # --- Запись отчёта ---
    with out_path.open("w", encoding="utf-8", newline="\n") as out:
        # Шапка + TREE
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        out.write("=" * 80 + "\n")
        out.write("PROJECT SNAPSHOT\n")
        out.write(f"Generated:   {ts}\n")
        out.write(f"Root:        {root}\n")
        out.write(f"Python:      {sys.version.split()[0]}\n")
        out.write("=" * 80 + "\n\n")

        if not args.no_tree:
            out.write("=" * 80 + "\n")
            out.write("PROJECT TREE\n")
            out.write("=" * 80 + "\n")
            out.write(render_tree(root, exclude=EXCLUDE_TREE))
            out.write("\n\n")

        # DB overview
        if not args.no_db and db_path:
            out.write(introspect_sqlite(db_path, sample_limit=args.db_sample))

        # Sheets overview
        if not args.no_sheets:
            out.write(introspect_gsheets(sample_limit=args.sheets_sample))

        # Бандл исходников
        write_bundle(out, root, files)

    print(f"✓ Готово: {out_path}")

if __name__ == "__main__":
    main()
