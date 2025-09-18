
.PHONY: setup test lint fmt run-user run-admin

setup:
	python -m venv .venv
	@echo "Activate venv and install deps:"
	@echo "  Windows: .\\.venv\\Scripts\\Activate.ps1"
	@echo "  POSIX:   source .venv/bin/activate"
	pip install -r requirements.txt || true
	pip install -r requirements-dev.txt || true

test:
	pytest -q || true

lint:
	ruff check . || true

fmt:
	black . || true

run-user:
	python -m user_app.main

run-admin:
	python -m admin_app.main
