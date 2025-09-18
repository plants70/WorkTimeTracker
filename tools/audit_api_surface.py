#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ищет по проекту обращения к methods/attrs SheetsAPI и печатает сводку:
 - какие методы вызываются (и с какими именованными параметрами)
 - какие из них отсутствуют в sheets_api.SheetsAPI

Запуск:
  python tools/audit_api_surface.py -r .
"""
import ast
import argparse
import inspect
import logging
import os
from pathlib import Path
from typing import Dict, List, Set, Tuple

from logging_setup import setup_logging


logger = logging.getLogger(__name__)

PROJECT_EXTS = {".py"}

def walk_py(root: Path) -> List[Path]:
    files = []
    for dp, dn, fn in os.walk(root):
        if any(skip in dp for skip in (".venv", "__pycache__", ".git")):
            continue
        for f in fn:
            p = Path(dp) / f
            if p.suffix.lower() in PROJECT_EXTS:
                files.append(p)
    return files

def load_api_methods() -> Set[str]:
    import importlib
    m = importlib.import_module("sheets_api")
    cls = getattr(m, "SheetsAPI")
    methods = set()
    for name, obj in inspect.getmembers(cls, inspect.isfunction):
        if name.startswith("_"):
            continue
        methods.add(name)
    return methods

class Finder(ast.NodeVisitor):
    def __init__(self):
        self.calls: Dict[str, Set[Tuple[str, Tuple[str, ...]]]] = {}

    def visit_Call(self, node: ast.Call):
        # ищем вызовы вида something.method(...)
        if isinstance(node.func, ast.Attribute):
            method = node.func.attr
            recv = ast.unparse(node.func.value) if hasattr(ast, "unparse") else "<recv>"
            # захватим имена именованных аргументов
            kw = tuple(sorted([k.arg for k in node.keywords if k.arg]))
            self.calls.setdefault(method, set()).add((recv, kw))
        self.generic_visit(node)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-r", "--root", default=".")
    args = ap.parse_args()
    root = Path(args.root).resolve()
    files = walk_py(root)

    # что реально есть в SheetsAPI
    api_methods = load_api_methods()

    found: Dict[str, Set[Tuple[str, Tuple[str, ...]]]] = {}
    f = Finder()
    for p in files:
        try:
            code = p.read_text(encoding="utf-8", errors="ignore")
            tree = ast.parse(code)
            f = Finder()
            f.visit(tree)
            for k, v in f.calls.items():
                found.setdefault(k, set()).update(v)
        except Exception:
            pass

    logger.info("=== SheetsAPI surface (public) ===")
    for m in sorted(api_methods):
        logger.info(" - %s", m)
    logger.info("=== Calls found in project ===")
    missing = []
    for m in sorted(found.keys()):
        logger.info("%s:", m)
        for recv, kw in sorted(found[m]):
            logger.info("  recv=%s, kwargs=%s", recv, kw)
        if m not in api_methods:
            missing.append(m)
    logger.info("=== Missing in SheetsAPI ===")
    for m in missing:
        logger.info(" * %s", m)

if __name__ == "__main__":
    setup_logging(app_name="wtt-audit-api", force_console=True)
    main()
