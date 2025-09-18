"""Utility helpers for guarding GUI thread from long-running operations."""
from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager

log = logging.getLogger(__name__)

_MAIN_THREAD_IDENT = threading.main_thread().ident
_DEFAULT_THRESHOLD = 0.5  # seconds
_FATAL_MULTIPLIER = 4  # log error if exceeded threshold*N


def is_gui_thread() -> bool:
    """Return True if code executes in the main (GUI) thread."""
    return threading.get_ident() == _MAIN_THREAD_IDENT


@contextmanager
def guard_gui_long_operation(operation: str, threshold: float = _DEFAULT_THRESHOLD):
    """
    Guard GUI thread from long operations.

    If called from the main thread, a watchdog timer is started. When the
    operation exceeds the threshold the watchdog emits a warning, and an error
    is logged if the operation lasts substantially longer.
    """
    if not is_gui_thread() or threshold <= 0:
        yield
        return

    start = time.perf_counter()
    fired = threading.Event()

    def _warn_long():
        if fired.is_set():
            return
        fired.set()
        log.warning(
            "GUI thread blocked by '%s' for more than %.3fs", operation, threshold
        )

    timer = threading.Timer(threshold, _warn_long)
    timer.daemon = True
    timer.start()

    try:
        yield
    finally:
        fired.set()
        timer.cancel()
        elapsed = time.perf_counter() - start
        if elapsed > threshold * _FATAL_MULTIPLIER:
            log.error(
                "GUI thread spent %.3fs inside '%s' (threshold %.3fs)",
                elapsed,
                operation,
                threshold,
            )
        elif elapsed > threshold:
            log.debug(
                "GUI thread finished '%s' in %.3fs (threshold %.3fs)",
                operation,
                elapsed,
                threshold,
            )
