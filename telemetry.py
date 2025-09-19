from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Iterator


_WARN_THRESHOLDS_MS: dict[str, float] = {
    "login": 3000.0,
    "finish_active_session": 2000.0,
}

_WARN_HINTS: dict[str, str] = {
    "login": "Замер входа превысил 3 секунды — проверьте подключение к сети или кеш API.",
    "finish_active_session": "Завершение сессии выполняется слишком долго (более 2 секунд).",
}


@contextmanager
def trace_time(label: str, *, logger: logging.Logger | None = None) -> Iterator[None]:
    """Log execution time for a block in milliseconds with optional warnings."""

    log = logger or logging.getLogger("telemetry")
    start = time.perf_counter()
    try:
        yield
    finally:
        duration_ms = (time.perf_counter() - start) * 1000
        threshold = _WARN_THRESHOLDS_MS.get(label)
        message = f"op={label} took {duration_ms:.1f} ms"
        if threshold is not None and duration_ms > threshold:
            hint = _WARN_HINTS.get(label)
            if hint:
                message = f"{message} — {hint}"
            log.warning(message)
        else:
            log.info(message)
