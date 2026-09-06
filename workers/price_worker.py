from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import redis

from mt5.clock import ServerClock, measure_server_clock
from mt5.fetch import list_symbols, read_tick, select
from mt5.terminal import MT5Terminal, TerminalError
from storage.redis_store import encode, price_channel, price_key
from symbols.normalize import SymbolInfo, resolve
from utils.logging import bind_context, configure_logging, log_event, reset_context

from .protocol import Heartbeat, WorkerState

logger = logging.getLogger(__name__)

_FAILURE_BACKOFF_SECONDS = 30.0
_RESYNC_SYMBOLS_SECONDS = 900.0
_CLOCK_REFRESH_SECONDS = 6 * 3600.0
_HEARTBEAT_SECONDS = 30.0


def worker_main(worker_id: str, terminal_path: str, connection: Any, **options: Any) -> None:
    configure_logging(options["log_level"], options["log_json"])
    tokens = bind_context(worker_id=worker_id)
    terminal = MT5Terminal(
        terminal_path,
        init_timeout_ms=options["init_timeout_ms"],
        login_timeout_ms=options["login_timeout_ms"],
        portable=options["portable"],
    )
    connection.send(worker_id)
    log_event(logger, "info", "price.worker.started", terminal_path=terminal_path)

    store = redis.Redis.from_url(options["redis_url"], decode_responses=True)
    poll_seconds = max(options["tick_poll_ms"], 50) / 1000.0
    ttl = options["price_ttl_seconds"]

    watched: list[SymbolInfo] = []
    seen: dict[str, tuple[int, float, float]] = {}
    clock = ServerClock()
    symbols_at = 0.0
    clock_at = 0.0
    heartbeat_at = 0.0

    try:
        while True:
            try:
                terminal.connect(options["login"], options["password"], options["server"])
                now = time.monotonic()
                if not clock.known or now - clock_at > _CLOCK_REFRESH_SECONDS:
                    clock = measure_server_clock(terminal)
                    clock_at = time.monotonic()
                    log_event(
                        logger, "info", "price.clock.resolved",
                        offset_minutes=clock.offset_minutes, scanned=clock.scanned,
                    )
                if not watched or now - symbols_at > _RESYNC_SYMBOLS_SECONDS:
                    watched = _subscribe(terminal, options)
                    symbols_at = time.monotonic()

                published = _publish(terminal, store, watched, seen, clock, ttl)

                if time.monotonic() - heartbeat_at >= _HEARTBEAT_SECONDS:
                    heartbeat_at = time.monotonic()
                    _report(
                        connection, worker_id, WorkerState.running,
                        symbols=len(watched), published=published, quoted=len(seen),
                    )
                time.sleep(poll_seconds)
            except TerminalError as exc:
                _report(connection, worker_id, WorkerState.failed, error=str(exc))
                log_event(logger, "error", "price.poll.failed", error=str(exc), error_code=exc.code)
                if exc.terminal_lost:
                    terminal.reset()
                watched = []
                time.sleep(_FAILURE_BACKOFF_SECONDS)
            except Exception as exc:
                _report(connection, worker_id, WorkerState.failed, error=f"{type(exc).__name__}: {exc}")
                log_event(logger, "error", "price.poll.crashed", error=str(exc), exc_info=True)
                time.sleep(_FAILURE_BACKOFF_SECONDS)
    finally:
        try:
            store.close()
        except Exception:
            pass
        terminal.shutdown()
        reset_context(tokens)
        log_event(logger, "info", "price.worker.stopped")


def _subscribe(terminal: MT5Terminal, options: dict[str, Any]) -> list[SymbolInfo]:
    found = list_symbols(terminal, options["suffixes"], options["path_prefixes"])
    chosen, collisions = resolve(found)
    for symbol, losers in collisions:
        log_event(logger, "warning", "symbols.collision", symbol=symbol, skipped=losers)

    watched = []
    for info in sorted(chosen.values(), key=lambda item: item.symbol):
        if select(terminal, info.raw_symbol):
            watched.append(info)
    log_event(
        logger, "info", "price.symbols.subscribed",
        watched=len(watched), skipped=len(chosen) - len(watched),
    )
    return watched


def _publish(
    terminal: MT5Terminal,
    store: Any,
    watched: list[SymbolInfo],
    seen: dict[str, tuple[int, float, float]],
    clock: ServerClock,
    ttl: int,
) -> int:
    pipeline = store.pipeline(transaction=False)
    published = 0
    for info in watched:
        tick = read_tick(terminal, info.raw_symbol, clock)
        if tick is None:
            continue
        fingerprint = (tick["epoch"], tick["bid"], tick["ask"])
        if seen.get(info.symbol) == fingerprint:
            continue
        seen[info.symbol] = fingerprint
        quote = {
            "symbol": info.symbol,
            "raw_symbol": info.raw_symbol,
            "bid": tick["bid"],
            "ask": tick["ask"],
            "last": tick["last"],
            "volume": tick["volume"],
            "ts": tick["ts"],
        }
        payload = encode(quote)
        pipeline.set(price_key(info.symbol), payload, ex=ttl)
        pipeline.publish(price_channel(info.symbol), payload)
        published += 1
    if published:
        pipeline.execute()
    else:
        pipeline.reset()
    return published


def _report(
    connection: Any, worker_id: str, state: WorkerState, error: str | None = None, **detail: Any
) -> None:
    payload = Heartbeat(
        worker_id=worker_id, state=state.value, at=datetime.now(timezone.utc), detail=dict(detail)
    )
    if error:
        payload.detail["error"] = error
    try:
        connection.send(payload)
    except Exception:
        pass
