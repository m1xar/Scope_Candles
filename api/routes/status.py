from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from api.deps import COMMON_RESPONSES, get_clickhouse, get_supervisor, verify_auth
from storage import clickhouse
from utils.config import settings
from workers.protocol import WorkerState

router = APIRouter(tags=["Status"], dependencies=[Depends(verify_auth)], responses=COMMON_RESPONSES)
public = APIRouter(tags=["Status"])


class Worker(BaseModel):
    worker_id: str
    terminal_path: str
    state: str
    pid: int | None = None
    restarts: int = 0
    last_heartbeat_at: datetime | None = None
    last_error: str | None = None
    detail: dict[str, Any] = {}


class Status(BaseModel):
    workers: list[Worker]
    symbols: int
    newest_candle_at: datetime | None = None
    watermark_lag_seconds: int | None = None
    backfill_start_date: datetime
    freshness_lag_minutes: int


@router.get("/status", response_model=Status)
async def get_status(
    request: Request,
    supervisor: Any = Depends(get_supervisor),
    client: Any = Depends(get_clickhouse),
) -> Status:
    def read() -> tuple[int, datetime | None]:
        with client.borrow() as connection:
            marks = clickhouse.watermarks(connection)
            newest = max(marks.values()) if marks else None
            return len(marks), newest

    counted, newest = await asyncio.to_thread(read)
    lag = int((datetime.now(timezone.utc) - newest).total_seconds()) if newest else None

    return Status(
        workers=[
            Worker(
                worker_id=item.worker_id,
                terminal_path=item.terminal_path,
                state=item.state.value,
                pid=item.pid,
                restarts=item.restarts,
                last_heartbeat_at=item.last_heartbeat_at,
                last_error=item.last_error,
                detail=item.detail,
            )
            for item in supervisor.status()
        ],
        symbols=counted,
        newest_candle_at=newest,
        watermark_lag_seconds=lag,
        backfill_start_date=request.app.state.start_date,
        freshness_lag_minutes=settings.freshness_lag_minutes,
    )


@public.get("/healthz")
async def healthz(request: Request) -> JSONResponse:
    supervisor = getattr(request.app.state, "supervisor", None)
    client = getattr(request.app.state, "clickhouse", None)
    if supervisor is None or client is None:
        return JSONResponse({"status": "starting"}, status_code=503)

    def ping() -> bool:
        try:
            with client.borrow() as connection:
                connection.command("SELECT 1")
            return True
        except Exception:
            return False

    storage_ok = await asyncio.to_thread(ping)
    workers = supervisor.status()
    running = sum(1 for item in workers if item.state is WorkerState.running)
    healthy = storage_ok and supervisor.healthy()
    return JSONResponse(
        {
            "status": "ok" if healthy else "degraded",
            "clickhouse": storage_ok,
            "workers_running": running,
            "workers_total": len(workers),
        },
        status_code=200 if healthy else 503,
    )
