from __future__ import annotations

import asyncio
import logging
import sys
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from storage import clickhouse
from utils.config import settings
from utils.logging import bind_context, configure_logging, log_event, reset_context
from workers import supervisor as supervisor_module
from workers.candles_worker import DEFAULT_START_DATE

from .routes import candles, prices, status, symbols

configure_logging(settings.log_level, settings.log_json)
logger = logging.getLogger(__name__)

TAGS = [
    {"name": "Candles", "description": "Minute candles from ClickHouse, aggregated to any timeframe."},
    {"name": "Prices", "description": "Live bid/ask from Redis, over HTTP or WebSocket."},
    {"name": "Symbols", "description": "Normalized broker symbols and their ingest watermarks."},
    {"name": "Status", "description": "Worker health and backfill progress."},
]


def start_date() -> datetime:
    if not settings.backfill_start_date:
        return DEFAULT_START_DATE
    parsed = datetime.fromisoformat(settings.backfill_start_date)
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    for problem in settings.problems():
        log_event(logger, "warning", "app.startup.config", problem=problem)

    since = start_date()
    app.state.start_date = since

    await asyncio.to_thread(clickhouse.init_schema)
    app.state.clickhouse = clickhouse.ClientPool()
    app.state.redis = aioredis.from_url(settings.redis_url, decode_responses=True)

    workers = supervisor_module.build(since)
    app.state.supervisor = workers
    await workers.start()
    log_event(logger, "info", "app.startup.completed", start_date=since.isoformat())

    try:
        yield
    finally:
        await workers.stop()
        await app.state.redis.aclose()
        app.state.clickhouse.close()
        app.state.supervisor = None
        app.state.clickhouse = None
        app.state.redis = None
        log_event(logger, "info", "app.shutdown.completed")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Scope Candles",
        version="0.1.0",
        description=(
            "Minute candles and live prices for every symbol on one MetaTrader 5 broker, "
            "with broker-specific symbol suffixes normalized away."
        ),
        openapi_tags=TAGS,
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[settings.cors_allow_origin],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        tokens = bind_context(request_id=request.headers.get("x-request-id") or str(uuid.uuid4()))
        try:
            return await call_next(request)
        finally:
            reset_context(tokens)

    for router in (
        candles.router,
        prices.router,
        prices.stream,
        symbols.router,
        status.router,
        status.public,
    ):
        app.include_router(router)
    return app


app = create_app()


def run() -> None:
    import uvicorn

    server = uvicorn.Server(uvicorn.Config(app, host=settings.api_host, port=settings.api_port))
    if sys.platform != "win32":
        server.run()
        return

    loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(server.serve())
    finally:
        asyncio.set_event_loop(None)
        loop.close()


if __name__ == "__main__":
    run()
