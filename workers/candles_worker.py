from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from mt5.cache import prune_price_cache
from mt5.clock import ServerClock, measure_history_clock
from mt5.fetch import chunks, copy_rates, list_symbols, log_clock, select
from mt5.terminal import MT5Terminal, TerminalError
from storage import clickhouse
from symbols.normalize import SymbolInfo, resolve
from utils.logging import bind_context, configure_logging, log_event, reset_context

from .protocol import Heartbeat, WorkerState

logger = logging.getLogger(__name__)

DEFAULT_START_DATE = datetime(2020, 1, 1, tzinfo=timezone.utc)

_CLOCK_REFRESH_SECONDS = 6 * 3600.0
_FAILURE_BACKOFF_SECONDS = 60.0
_INSERT_BATCH = 200000


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
    log_event(logger, "info", "candles.worker.started", terminal_path=terminal_path)

    client = None
    clock = ServerClock()
    clock_measured_at = 0.0
    try:
        while True:
            started = time.monotonic()
            try:
                terminal.connect(options["login"], options["password"], options["server"])
                if client is None:
                    client = clickhouse.connect()
                if not clock.known or time.monotonic() - clock_measured_at > _CLOCK_REFRESH_SECONDS:
                    clock = measure_history_clock(terminal, options["start_date"])
                    clock_measured_at = time.monotonic()
                    log_clock(clock, options["server"])
                _run_pass(terminal, client, clock, connection, worker_id, options)
                if options["prune_cache"]:
                    prune_price_cache(terminal_path)
            except TerminalError as exc:
                _report(connection, worker_id, WorkerState.failed, error=str(exc))
                log_event(logger, "error", "candles.pass.failed", error=str(exc), error_code=exc.code)
                if exc.terminal_lost:
                    terminal.reset()
                time.sleep(_FAILURE_BACKOFF_SECONDS)
                continue
            except Exception as exc:
                _report(connection, worker_id, WorkerState.failed, error=f"{type(exc).__name__}: {exc}")
                log_event(logger, "error", "candles.pass.crashed", error=str(exc), exc_info=True)
                client = _close(client)
                time.sleep(_FAILURE_BACKOFF_SECONDS)
                continue

            elapsed = time.monotonic() - started
            sleep_for = max(options["interval_seconds"] - elapsed, 0.0)
            _report(
                connection, worker_id, WorkerState.running,
                phase="idle", next_pass_in_seconds=int(sleep_for),
            )
            log_event(
                logger, "info", "candles.pass.completed",
                elapsed_s=round(elapsed, 1), sleep_s=int(sleep_for),
            )
            time.sleep(sleep_for)
    finally:
        _close(client)
        terminal.shutdown()
        reset_context(tokens)
        log_event(logger, "info", "candles.worker.stopped")


def _run_pass(
    terminal: MT5Terminal,
    client: Any,
    clock: ServerClock,
    connection: Any,
    worker_id: str,
    options: dict[str, Any],
) -> None:
    found = list_symbols(terminal, options["suffixes"], options["path_prefixes"])
    chosen, collisions = resolve(found)
    for symbol, losers in collisions:
        log_event(logger, "warning", "symbols.collision", symbol=symbol, skipped=losers)

    clickhouse.upsert_symbols(
        client,
        [
            [info.symbol, info.raw_symbol, info.description, info.digits, info.path]
            for info in chosen.values()
        ],
    )

    marks = clickhouse.watermarks(client)
    ordered = sorted(chosen.values(), key=lambda info: info.symbol)
    total = len(ordered)
    pass_started = datetime.now(timezone.utc)
    log_event(logger, "info", "candles.pass.started", symbols=total, raw_symbols=len(found))

    for index, info in enumerate(ordered, start=1):
        _report(
            connection, worker_id, WorkerState.running,
            phase="backfill", symbol=info.symbol, symbols_done=index - 1, symbols_total=total,
            pass_started_at=pass_started.isoformat(),
        )
        try:
            written = _sync_symbol(terminal, client, clock, info, marks, options)
        except TerminalError:
            raise
        except Exception as exc:
            log_event(
                logger, "warning", "candles.symbol.failed",
                symbol=info.symbol, raw_symbol=info.raw_symbol,
                error=f"{type(exc).__name__}: {exc}",
            )
            continue
        if written:
            log_event(logger, "info", "candles.symbol.synced", symbol=info.symbol, rows=written)

    _report(
        connection, worker_id, WorkerState.running,
        phase="backfill", symbols_done=total, symbols_total=total,
        pass_started_at=pass_started.isoformat(),
    )


def _sync_symbol(
    terminal: MT5Terminal,
    client: Any,
    clock: ServerClock,
    info: SymbolInfo,
    marks: dict[str, datetime],
    options: dict[str, Any],
) -> int:
    last = marks.get(info.symbol)
    if last is None:
        last = clickhouse.candle_watermark(client, info.symbol)
    start = max(last + timedelta(minutes=1), options["start_date"]) if last else options["start_date"]
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    if start >= end:
        return 0
    if not select(terminal, info.raw_symbol):
        log_event(
            logger, "warning", "candles.symbol.unselectable",
            symbol=info.symbol, raw_symbol=info.raw_symbol,
        )
        return 0

    written = 0
    highest = last
    buffer: list[list[Any]] = []
    retries = options["history_retries"] if last is None else 1
    for window_start, window_end in chunks(start, end, options["chunk_days"]):
        candles = copy_rates(
            terminal, info.raw_symbol, window_start, window_end, clock, retries=retries
        )
        retries = 1
        if not candles:
            continue
        for moment, open_, high, low, close, tick_volume, real_volume, spread in candles:
            buffer.append([info.symbol, moment, open_, high, low, close, tick_volume, real_volume, spread])
            if highest is None or moment > highest:
                highest = moment
        if len(buffer) >= _INSERT_BATCH:
            written += clickhouse.insert_candles(client, buffer)
            buffer = []
            if highest is not None:
                clickhouse.set_watermark(client, info.symbol, highest, written)

    if buffer:
        written += clickhouse.insert_candles(client, buffer)
    if highest is not None and (last is None or highest > last):
        clickhouse.set_watermark(client, info.symbol, highest, written)
    return written


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


def _close(client: Any) -> None:
    if client is not None:
        try:
            client.close()
        except Exception:
            pass
    return None
