# tools/doctor.py
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

from config import LOG_DIR, get_credentials_file
from user_app.db_local import LocalDB
from sheets_api import sheets_api

# Минимальные ожидания под вашу фактическую схему:
EXPECTED = {
    "Users": ["Email", "Name", "Group"],  # базовые атрибуты пользователя
    "ActiveSessions": [
        "Email", "Name", "SessionID", "LoginTime", "Status", "LogoutTime", "RemoteCommand"
        # RemoteCommandAck — опционально (рекомендуем добавить)
    ],
}


def dump_sqlite_schema(conn: sqlite3.Connection, sample_limit: int = 5) -> Dict[str, Any]:
    cur = conn.cursor()
    cur.execute("SELECT name, type, sql FROM sqlite_master WHERE type IN ('table','index','trigger') ORDER BY name;")
    items = [{"name": n, "type": t, "sql": s} for (n, t, s) in cur.fetchall()]
    tables = [i["name"] for i in items if i["type"] == "table" and not i["name"].startswith("sqlite_")]
    stats = {}
    extra = {}
    samples = {}
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            stats[t] = cur.fetchone()[0]
            cur.execute(f"SELECT * FROM {t} ORDER BY ROWID DESC LIMIT {sample_limit}")
            samples[t] = cur.fetchall()
        except Exception:
            pass
    # спец-метрики: несинхронизированные записи в очереди/логах
    try:
        if "logs" in tables:
            cur.execute("SELECT COUNT(*) FROM logs WHERE synced=0")
            extra["logs_unsynced"] = cur.fetchone()[0]
        if "offline_actions" in tables:
            cur.execute("SELECT COUNT(*) FROM offline_actions WHERE status<>'synced'")
            extra["offline_actions_pending"] = cur.fetchone()[0]
    except Exception:
        pass
    return {"objects": items, "stats": stats, "samples": samples, "extra": extra}


def dump_sheets_structure() -> Dict[str, Any]:
    client = sheets_api
    book_name = "CONFIGURED"
    data: Dict[str, Any] = {"worksheets": []}
    # перечислим листы и их заголовки
    titles = client.list_worksheet_titles()
    for t in titles:
        try:
            ws = client._get_ws(t)  # внутренняя помощ. функция допустима для диагностики
            header = [h.strip() for h in ws.row_values(1)]
            data["worksheets"].append({"title": t, "header": header, "rows_hint": ws.row_count, "cols_hint": ws.col_count})
        except Exception as e:
            data["worksheets"].append({"title": t, "error": str(e)})
    # лёгкая валидация ожидаемых колонок
    mismatches: List[Dict[str, Any]] = []
    for w in data["worksheets"]:
        if "header" not in w:
            continue
        exp = EXPECTED.get(w["title"])
        if exp:
            missing = [x for x in exp if x not in w["header"]]
            if missing:
                mismatches.append({"sheet": w["title"], "missing": missing})
    data["expectations"] = mismatches
    return data


def dump_sheets(sample_limit: int = 3) -> Dict[str, Any]:
    out = dump_sheets_structure()
    # добавим немного данных для примера
    client = sheets_api
    for ws in out["worksheets"]:
        if "error" in ws:
            continue
        try:
            title = ws["title"]
            data = client.get_worksheet_data(title, limit=sample_limit)
            ws["sample"] = data
            ws["rows_count"] = len(data)
        except Exception as e:
            ws["sample_error"] = str(e)
    return out


def render_markdown(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# Diagnostics Report — {report.get('ts')}")
    lines.append("")
    lines.append("## Credentials")
    cred = report.get("credentials_file", "unknown")
    lines.append(f"- State: **{cred}**")
    lines.append("")
    # SQLite
    s = report.get("sqlite", {})
    lines.append("## SQLite")
    stats = s.get("stats", {})
    if stats:
        lines.append("| Table | Rows |")
        lines.append("|---|---:|")
        for k, v in stats.items():
            lines.append(f"| {k} | {v} |")
        lines.append("")
    # Extra metrics
    extra = s.get("extra", {})
    if extra:
        lines.append("**Extra metrics:**")
        for k, v in extra.items():
            lines.append(f"- {k}: {v}")
        lines.append("")
    # Sheets
    sh = report.get("sheets", {})
    lines.append("## Google Sheets")
    problems = sh.get("expectations", [])
    if problems:
        lines.append("**Missing columns:**")
        for p in problems:
            lines.append(f"- `{p['sheet']}`: {', '.join(p['missing'])}")
        lines.append("")
    ws = sh.get("worksheets", [])
    for w in ws:
        lines.append(f"### {w.get('title','<no title>')}")
        if "error" in w:
            lines.append(f"> Error: {w['error']}")
            continue
        header = w.get("header", [])
        lines.append("**Header:** " + ", ".join(f"`{h}`" for h in header))
        lines.append(f"**Rows:** {w.get('rows_hint', 0)}")
        sample = w.get("sample", [])
        if sample:
            lines.append("")
            lines.append("```")
            for r in sample:
                lines.append(str(r))
            lines.append("```")
        lines.append("")
    return "\n".join(lines)


def run(out: Path) -> None:
    report: Dict[str, Any] = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "log_dir": str(LOG_DIR),
    }
    # creds check
    try:
        cf = get_credentials_file()
        report["credentials_file"] = str(cf)
    except Exception as e:
        report["credentials_error"] = str(e)

    # DB
    db = LocalDB()
    conn = db.conn  # type: ignore
    report["sqlite"] = dump_sqlite_schema(conn)

    # Sheets
    try:
        report["sheets"] = dump_sheets_structure()
    except Exception as e:
        report["sheets_error"] = str(e)

    out_path = Path(out)
    if out_path.suffix.lower() == ".md":
        out_path.write_text(render_markdown(report), encoding="utf-8")
    else:
        out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK: written {out_path}")


def main():
    ap = argparse.ArgumentParser(description="WorkTimeTracker Doctor: локальная БД + структура Google Sheets + быстрая валидация.")
    ap.add_argument("-o", "--output", default="diagnostics_report.json", help="Путь к итоговому отчёту (JSON или MD).")
    args = ap.parse_args()
    run(Path(args.output))


if __name__ == "__main__":
    main()