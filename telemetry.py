from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Iterator


@contextmanager
def trace_time(label: str, *, logger: logging.Logger | None = None) -> Iterator[None]:
    """Log execution time for a block in milliseconds."""

    log = logger or logging.getLogger("telemetry")
    start = time.perf_counter()
    try:
        yield
    finally:
        duration_ms = (time.perf_counter() - start) * 1000
        log.info("op=%s took %.1f ms", label, duration_ms)
