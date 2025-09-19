from __future__ import annotations

import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable

from PyQt5.QtCore import QThread

from auto_sync import SyncManager
from config import DB_FALLBACK_PATH, DB_MAIN_PATH, HEARTBEAT_PERIOD_SEC
from telemetry import trace_time
from user_app import db_local
from user_app.api import UserAPI
from user_app.db_local import LocalDB
from user_app.signals import SessionSignals, SyncSignals

try:
    from sheets_api import SheetsAPI, get_sheets_api
except ImportError:  # pragma: no cover - PyInstaller fallback
    from ..sheets_api import SheetsAPI, get_sheets_api

logger = logging.getLogger(__name__)


class HeartbeatService:
    def __init__(self, services: "Services") -> None:
        self._services = services
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._stop_evt: threading.Event | None = None
        self._session_id: str | None = None
        self._email: str | None = None
        self._callback: Callable[[str], None] | None = None
        self._remote_emitted = False

    def start(
        self,
        *,
        email: str,
        session_id: str,
        callback: Callable[[str], None] | None = None,
        period: int | None = None,
    ) -> None:
        with self._lock:
            self.stop()
            normalized_session = (session_id or "").strip()
            normalized_email = (email or "").strip().lower()
            if not normalized_session or not normalized_email:
                return

            self._stop_evt = threading.Event()
            self._session_id = normalized_session
            self._email = normalized_email
            self._callback = callback
            self._remote_emitted = False

            period_value = period or HEARTBEAT_PERIOD_SEC
            if period_value <= 0:
                period_value = HEARTBEAT_PERIOD_SEC

            thread = threading.Thread(
                target=self._run,
                args=(self._stop_evt, period_value),
                name="session-heartbeat",
                daemon=True,
            )
            self._thread = thread
            thread.start()
            logger.info(
                "Heartbeat started (session=%s, period=%s)",
                normalized_session,
                period_value,
            )

    def stop(self) -> None:
        with self._lock:
            stop_evt = self._stop_evt
            thread = self._thread
            session_id = self._session_id
            self._stop_evt = None
            self._thread = None
            self._session_id = None
            self._email = None
            self._callback = None
            self._remote_emitted = False

        if stop_evt:
            stop_evt.set()
        if thread and thread.is_alive():
            thread.join(timeout=2.0)
        if session_id:
            logger.info("Heartbeat stopped (session=%s)", session_id)

    def _run(self, stop_evt: threading.Event, period: int) -> None:
        session_id = self._session_id or ""
        email = self._email or ""
        api = self._services.user_api

        def _check_remote() -> None:
            if self._remote_emitted:
                return
            if not session_id or not email:
                return
            try:
                with trace_time("check_user_session_status"):
                    status = api.get_session_status(session_id=session_id, email=email)
            except Exception as exc:  # pragma: no cover - network errors are tolerated
                logger.debug("Heartbeat remote check failed: %s", exc)
                return
            normalized = (status or "").strip().lower()
            if normalized and normalized != "в работе" and not self._remote_emitted:
                self._remote_emitted = True
                logger.info(
                    "Heartbeat detected remote logout (session=%s, status=%s)",
                    session_id,
                    normalized,
                )
                if self._callback:
                    self._callback("remote_force_logout")

        while not stop_evt.is_set():
            try:
                with trace_time("heartbeat"):
                    api.heartbeat_session(session_id=session_id)
            except Exception as exc:  # pragma: no cover
                logger.debug("Heartbeat failed for %s: %s", session_id, exc)
            _check_remote()
            if stop_evt.wait(period):
                break


class AutoSyncService:
    def __init__(self, services: "Services") -> None:
        self._services = services
        self._lock = threading.RLock()
        self._thread: QThread | None = None
        self._worker: SyncManager | None = None

    def start(
        self,
        *,
        signals: SyncSignals,
        session_signals: SessionSignals,
        offline_mode: bool = False,
        remote_callback: Callable[[str], None] | None = None,
    ) -> None:
        with self._lock:
            if self._worker:
                return

            worker = SyncManager(
                signals=signals,
                background_mode=True,
                session_signals=session_signals,
                db=self._services.db,
                remote_force_logout_callback=remote_callback,
            )
            if offline_mode:
                worker._is_offline_recovery = True

            thread = QThread()
            worker.moveToThread(thread)
            thread.started.connect(worker.run)
            thread.start()

            self._thread = thread
            self._worker = worker
            logger.info("Auto-sync service started (offline_mode=%s)", offline_mode)

    def stop(self) -> None:
        with self._lock:
            worker = self._worker
            thread = self._thread
            self._worker = None
            self._thread = None

        if worker:
            try:
                worker.stop()
            except Exception as exc:  # pragma: no cover
                logger.debug("Sync worker stop error: %s", exc)
        if thread:
            thread.quit()
            thread.wait(2000)
            logger.info("Auto-sync service stopped")


class Services:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._db: LocalDB | None = None
        self._sheets: SheetsAPI | None = None
        self._user_api: UserAPI | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._heartbeat = HeartbeatService(self)
        self._auto_sync = AutoSyncService(self)
        self._session_signals: SessionSignals | None = None
        self._sync_signals: SyncSignals | None = None

    @property
    def db(self) -> LocalDB:
        with self._lock:
            if self._db is None:
                self._ensure_db_initialized()
                self._db = LocalDB()
            return self._db

    def reset_db(self) -> None:
        with self._lock:
            if self._db:
                try:
                    self._db.close()
                except Exception:  # pragma: no cover - best effort cleanup
                    logger.debug("Failed to close LocalDB on reset", exc_info=True)
            self._db = None

    @property
    def sheets(self) -> SheetsAPI:
        with self._lock:
            if self._sheets is None:
                with trace_time("sheets_api_init"):
                    self._sheets = get_sheets_api()
            return self._sheets

    @property
    def user_api(self) -> UserAPI:
        with self._lock:
            if self._user_api is None:
                self._user_api = UserAPI(self.sheets)
            return self._user_api

    @property
    def executor(self) -> ThreadPoolExecutor:
        with self._lock:
            if self._executor is None:
                self._executor = ThreadPoolExecutor(
                    max_workers=4,
                    thread_name_prefix="wtt-worker",
                )
            return self._executor

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        return self.executor.submit(fn, *args, **kwargs)

    @property
    def heartbeat(self) -> HeartbeatService:
        return self._heartbeat

    @property
    def auto_sync(self) -> AutoSyncService:
        return self._auto_sync

    @property
    def session_signals(self) -> SessionSignals:
        with self._lock:
            if self._session_signals is None:
                self._session_signals = SessionSignals()
            return self._session_signals

    @property
    def sync_signals(self) -> SyncSignals:
        with self._lock:
            if self._sync_signals is None:
                self._sync_signals = SyncSignals()
            return self._sync_signals

    def shutdown(self) -> None:
        self.auto_sync.stop()
        self.heartbeat.stop()
        with self._lock:
            executor = self._executor
            self._executor = None
        if executor:
            executor.shutdown(wait=False)
        self.reset_db()

    def _ensure_db_initialized(self) -> None:
        try:
            db_local.init_db(DB_MAIN_PATH, DB_FALLBACK_PATH)
        except Exception:
            logger.exception("Local DB initialization failed")


services = Services()
