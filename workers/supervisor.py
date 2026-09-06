from __future__ import annotations

import asyncio
import logging
import multiprocessing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from utils.config import settings
from utils.logging import log_event

from .protocol import Heartbeat, WorkerState, WorkerStatus
from . import candles_worker, price_worker

logger = logging.getLogger(__name__)

_RESTART_BACKOFF_SECONDS = 10.0
_MAX_RESTART_BACKOFF_SECONDS = 300.0
_REAP_INTERVAL_SECONDS = 15.0


@dataclass
class _Child:
    worker_id: str
    terminal_path: str
    target: Callable[..., None]
    options: dict[str, Any]
    status: WorkerStatus
    process: Any = None
    connection: Any = None
    reader: asyncio.Task | None = None
    backoff: float = _RESTART_BACKOFF_SECONDS


class Supervisor:
    def __init__(self) -> None:
        self._context = multiprocessing.get_context("spawn")
        self._children: dict[str, _Child] = {}
        self._reaper: asyncio.Task | None = None
        self._stopping = False

    def register(self, worker_id: str, terminal_path: str, target: Callable[..., None], options: dict[str, Any]) -> None:
        self._children[worker_id] = _Child(
            worker_id=worker_id,
            terminal_path=terminal_path,
            target=target,
            options=options,
            status=WorkerStatus(worker_id=worker_id, terminal_path=terminal_path),
        )

    async def start(self) -> None:
        for child in self._children.values():
            if child.terminal_path:
                await self._spawn(child)
            else:
                child.status.state = WorkerState.stopped
                child.status.last_error = "no terminal path configured"
        self._reaper = asyncio.create_task(self._reap_loop(), name="worker-reaper")

    async def stop(self) -> None:
        self._stopping = True
        if self._reaper is not None:
            self._reaper.cancel()
            try:
                await self._reaper
            except asyncio.CancelledError:
                pass
        for child in self._children.values():
            await self._terminate(child)

    def status(self) -> list[WorkerStatus]:
        return [child.status for child in self._children.values()]

    def healthy(self) -> bool:
        active = [child for child in self._children.values() if child.terminal_path]
        if not active:
            return False
        return all(self._alive(child) for child in active)

    def _alive(self, child: _Child) -> bool:
        if child.status.state is not WorkerState.running:
            return False
        last = child.status.last_heartbeat_at
        if last is None:
            return False
        age = datetime.now(timezone.utc) - last
        return age < timedelta(seconds=settings.worker_heartbeat_timeout_seconds)

    async def _spawn(self, child: _Child) -> bool:
        parent_conn, child_conn = self._context.Pipe()
        try:
            process = self._context.Process(
                target=child.target,
                args=(child.worker_id, child.terminal_path, child_conn),
                kwargs=child.options,
                daemon=True,
                name=f"scope-candles-{child.worker_id}",
            )
            process.start()
            child_conn.close()
        except Exception as exc:
            child.status.state = WorkerState.failed
            child.status.last_error = f"could not spawn: {type(exc).__name__}: {exc}"
            log_event(logger, "error", "worker.spawn.failed", worker_id=child.worker_id, error=str(exc))
            return False

        child.process = process
        child.connection = parent_conn
        child.status.pid = process.pid
        child.status.state = WorkerState.starting
        try:
            ready = await asyncio.wait_for(
                asyncio.to_thread(parent_conn.recv), timeout=settings.worker_start_timeout_seconds
            )
        except (asyncio.TimeoutError, EOFError, OSError) as exc:
            child.status.state = WorkerState.failed
            child.status.last_error = f"did not report ready: {exc}"
            log_event(logger, "error", "worker.start.timeout", worker_id=child.worker_id, error=str(exc))
            await self._terminate(child)
            return False
        if ready != child.worker_id:
            child.status.state = WorkerState.failed
            child.status.last_error = f"reported {ready!r} instead of ready"
            await self._terminate(child)
            return False

        child.status.state = WorkerState.running
        child.status.last_error = None
        child.status.last_heartbeat_at = datetime.now(timezone.utc)
        child.backoff = _RESTART_BACKOFF_SECONDS
        child.reader = asyncio.create_task(self._read_loop(child), name=f"reader-{child.worker_id}")
        log_event(logger, "info", "worker.started", worker_id=child.worker_id, pid=process.pid)
        return True

    async def _read_loop(self, child: _Child) -> None:
        connection = child.connection
        while connection is not None:
            try:
                message = await asyncio.to_thread(connection.recv)
            except (EOFError, OSError):
                break
            except asyncio.CancelledError:
                return
            if isinstance(message, Heartbeat):
                child.status.last_heartbeat_at = message.at or datetime.now(timezone.utc)
                child.status.detail = message.detail
                error = message.detail.get("error")
                if message.state == WorkerState.failed.value:
                    child.status.last_error = error
                else:
                    child.status.last_error = None
                child.status.state = WorkerState(message.state)

    async def _terminate(self, child: _Child) -> None:
        if child.reader is not None:
            child.reader.cancel()
            try:
                await child.reader
            except asyncio.CancelledError:
                pass
            child.reader = None
        process, connection = child.process, child.connection
        child.process, child.connection = None, None
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
        if process is None:
            return
        try:
            if process.is_alive():
                process.terminate()
            await asyncio.to_thread(process.join, 10.0)
            if process.is_alive():
                process.kill()
                await asyncio.to_thread(process.join, 5.0)
        except Exception as exc:
            log_event(logger, "warning", "worker.terminate.failed", worker_id=child.worker_id, error=str(exc))
        if not self._stopping:
            child.status.state = WorkerState.failed
        else:
            child.status.state = WorkerState.stopped

    async def _reap_loop(self) -> None:
        while True:
            await asyncio.sleep(_REAP_INTERVAL_SECONDS)
            for child in self._children.values():
                if not child.terminal_path:
                    continue
                dead = child.process is None or not child.process.is_alive()
                stalled = child.status.state is WorkerState.running and not self._alive(child)
                if not dead and not stalled:
                    continue
                log_event(
                    logger, "warning", "worker.restarting",
                    worker_id=child.worker_id, dead=dead, stalled=stalled, restarts=child.status.restarts,
                )
                await self._terminate(child)
                child.status.restarts += 1
                if not await self._spawn(child):
                    await asyncio.sleep(child.backoff)
                    child.backoff = min(child.backoff * 2, _MAX_RESTART_BACKOFF_SECONDS)


def build(start_date: datetime) -> Supervisor:
    shared = {
        "login": settings.mt5_login,
        "password": settings.mt5_password,
        "server": settings.mt5_server,
        "init_timeout_ms": settings.terminal_init_timeout_ms,
        "login_timeout_ms": settings.terminal_login_timeout_ms,
        "portable": settings.terminal_portable,
        "suffixes": settings.suffixes,
        "path_prefixes": settings.path_prefixes,
        "log_level": settings.log_level,
        "log_json": settings.log_json,
    }

    supervisor = Supervisor()
    supervisor.register(
        "candles",
        settings.candles_terminal_path,
        candles_worker.worker_main,
        {
            **shared,
            "start_date": start_date,
            "interval_seconds": settings.backfill_interval_minutes * 60,
            "chunk_days": settings.backfill_chunk_days,
            "history_retries": settings.symbol_history_retries,
            "prune_cache": settings.prune_cache,
        },
    )
    supervisor.register(
        "price",
        settings.price_terminal_path,
        price_worker.worker_main,
        {
            **shared,
            "redis_url": settings.redis_url,
            "tick_poll_ms": settings.tick_poll_ms,
            "price_ttl_seconds": settings.price_ttl_seconds,
        },
    )
    return supervisor
