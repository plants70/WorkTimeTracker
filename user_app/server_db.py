from __future__ import annotations

import logging
import threading
from typing import Any, Mapping

import requests

from config import (
    GOOGLE_API_TIMEOUT,
    SERVER_DB_AUTH_TOKEN,
    SERVER_DB_BASE_URL,
    SERVER_DB_ENABLED,
    SERVER_DB_TIMEOUT,
)
from telemetry import trace_time

logger = logging.getLogger(__name__)


class ServerDBClient:
    """Thin HTTP client for optional server-side persistence."""

    def __init__(
        self, base_url: str, *, timeout: float, token: str | None = None
    ) -> None:
        normalized = (base_url or "").strip().rstrip("/")
        if not normalized:
            raise ValueError("Server DB base URL must be provided")
        self._base_url = normalized
        self._timeout = max(1.0, float(timeout))
        self._session = requests.Session()
        self._lock = threading.RLock()
        if token:
            self._session.headers["Authorization"] = f"Bearer {token}"
        self._session.headers.setdefault("Content-Type", "application/json")

    def close(self) -> None:
        with self._lock:
            try:
                self._session.close()
            except Exception:  # pragma: no cover - best effort
                logger.debug("Server DB session close failed", exc_info=True)

    def ping(self) -> bool:
        """Best-effort health check; returns True on HTTP 200."""

        try:
            with trace_time("server_db_ping"):
                response = self._session.get(
                    f"{self._base_url}/health",
                    timeout=min(self._timeout, GOOGLE_API_TIMEOUT),
                )
            if response.ok:
                return True
            logger.debug(
                "Server DB ping returned non-OK status: %s", response.status_code
            )
        except Exception as exc:  # pragma: no cover - network failures are tolerated
            logger.debug("Server DB ping failed: %s", exc)
        return False

    # --- High level operations -------------------------------------------------
    def record_session_start(self, payload: Mapping[str, Any]) -> None:
        self._post("sessions/start", payload)

    def record_session_finish(self, payload: Mapping[str, Any]) -> None:
        self._post("sessions/finish", payload)

    def record_action(self, payload: Mapping[str, Any]) -> None:
        self._post("actions", payload)

    # --- Internal helpers ------------------------------------------------------
    def _post(self, path: str, payload: Mapping[str, Any]) -> None:
        if not payload:
            return
        url = f"{self._base_url}/{path.lstrip('/')}"
        try:
            with trace_time("server_db_request"):
                response = self._session.post(
                    url,
                    json=dict(payload),
                    timeout=self._timeout,
                )
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover - fire and forget
            logger.warning("Server DB request to %s failed: %s", url, exc)


def get_server_db() -> ServerDBClient | None:
    if not SERVER_DB_ENABLED:
        logger.info("Server DB integration disabled via configuration")
        return None
    if not SERVER_DB_BASE_URL:
        logger.warning(
            "SERVER_DB_BASE_URL is not configured; disabling server DB client"
        )
        return None
    try:
        client = ServerDBClient(
            SERVER_DB_BASE_URL,
            timeout=SERVER_DB_TIMEOUT,
            token=SERVER_DB_AUTH_TOKEN,
        )
        return client
    except Exception as exc:
        logger.error("Failed to initialize Server DB client: %s", exc)
        return None
