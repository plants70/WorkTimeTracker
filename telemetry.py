"""Helpers for telemetry and network call metrics."""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Iterator, Optional

from logging_setup import correlation_context, get_session_id, new_request_id


_metrics_logger = logging.getLogger("metrics")


@contextmanager
def record_network_call(name: str, *, session_id: Optional[str] = None) -> Iterator[str]:
    """Context manager to capture metrics for outbound network calls."""

    request_id = new_request_id()
    effective_session = session_id or get_session_id()
    with correlation_context(request_id=request_id, session_id=effective_session):
        started_at = time.perf_counter()
        try:
            yield request_id
        except Exception:
            duration = time.perf_counter() - started_at
            _metrics_logger.error(
                "network_call name=%s status=error count=1 duration=%.3f",
                name,
                duration,
            )
            raise
        else:
            duration = time.perf_counter() - started_at
            _metrics_logger.info(
                "network_call name=%s status=success count=1 duration=%.3f",
                name,
                duration,
            )
