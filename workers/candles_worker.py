from __future__ import annotations

import logging
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

from mt5.cache import prune_price_cache
from mt5.clock import ServerClock, measure_history_clock
from mt5.fetch import (
    TIMEFRAME_D1, TIMEFRAME_H1, HistorySync, chunks, copy_rates, last_quote_at, list_symbols, log_clock,
    select, sync_history,
)
from mt5.terminal import MT5Terminal, TerminalError
from storage import clickhouse
from symbols.catalog import BACKFILL_DONE, BACKFILL_PENDING, SymbolState, limit_extra, reconcile
from symbols.normalize import SymbolInfo, resolve
from utils.logging import bind_context, configure_logging, log_event, reset_context

from .protocol import Heartbeat, WorkerState

logger = logging.getLogger(__name__)

DEFAULT_START_DATE = datetime(2020, 1, 1, tzinfo=timezone.utc)

_CLOCK_REFRESH_SECONDS = 6 * 3600.0
_FAILURE_BACKOFF_SECONDS = 60.0
_INSERT_BATCH = 20000

# copy_rates_range answers from the terminal's local history and does not wait for the
# server to send what is missing. After an outage the local copy holds only the bars the
# terminal saw since it reconnected, so a catch-up request can come back "successful" but
# starting days late; taking it at face value moves the watermark past the hole for good.
_CATCH_UP_LAG = timedelta(hours=2)
_LATE_FIRST_BAR = timedelta(hours=1)
_CATCH_UP_WAIT_SECONDS = 2.0
_IDLE_LOG_EVERY = timedelta(hours=24)
# A new symbol whose terminal history stays empty this many times joins the regular pass
# anyway (it will then just log as idle) instead of sitting in the queue forever.
_EMPTY_BACKFILL_LIMIT = 3


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
    catalog: dict[str, SymbolInfo] = {}
    states: dict[str, SymbolState] = {}
    catalog_due_at = 0.0
    # The first repair waits for the terminal to finish syncing after (re)connect: run straight
    # away it would read the same incomplete local history the catch-up just read.
    repair_due_at = time.monotonic() + float(options.get("repair_warmup_seconds") or 0)
    repair_since = _parse_since(options.get("repair_since"))
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
                if not catalog or time.monotonic() >= catalog_due_at:
                    refreshed = _refresh_catalog(terminal, client, options)
                    if refreshed is not None:
                        catalog, states = refreshed
                        catalog_due_at = time.monotonic() + float(
                            options.get("catalog_refresh_seconds") or 86400
                        )
                _run_pass(terminal, client, clock, connection, worker_id, options, catalog, states)
                budget = float(options.get("backfill_budget_seconds") or 0)
                if not any(_state(states, symbol).ready for symbol in catalog):
                    budget = float("inf")  # fresh install: nothing to protect, backfill flat out
                if budget > 0:
                    _run_backfill(
                        terminal, client, clock, connection, worker_id, options, catalog, states,
                        deadline=time.monotonic() + budget,
                    )
                interval = float(options.get("repair_interval_seconds") or 0)
                if interval > 0 and time.monotonic() >= repair_due_at:
                    _run_repair(
                        terminal, client, clock, connection, worker_id, options, catalog, states,
                        repair_since,
                    )
                    repair_since = None
                    repair_due_at = time.monotonic() + interval
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


# --- catalogue ---------------------------------------------------------------------------


def _refresh_catalog(
    terminal: MT5Terminal, client: Any, options: dict[str, Any]
) -> tuple[dict[str, SymbolInfo], dict[str, SymbolState]] | None:
    """Once a day: the broker's full list against ours. New symbols are queued for backfill,
    vanished ones are marked delisted (never deleted). Between refreshes the pass works from
    this snapshot."""
    base = list(options.get("path_prefixes") or [])
    extra = list(options.get("extra_path_prefixes") or [])
    max_extra = int(options.get("extra_max") or 0)
    prefixes = base + extra if base and max_extra > 0 else base
    found = list_symbols(terminal, options["suffixes"], prefixes)
    if not found:
        log_event(logger, "warning", "candles.catalog.empty")
        return None
    found = limit_extra(found, extra, list(options.get("extra_allowlist") or []), max_extra)
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
    known = clickhouse.symbol_states(client)
    marks = clickhouse.watermarks(client)
    change = reconcile(
        known, chosen, set(marks), datetime.now(timezone.utc),
        min_ratio=float(options.get("catalog_min_ratio") or 0.5),
    )
    clickhouse.write_symbol_states(client, change.rows)
    states = dict(known)
    states.update({row.symbol: row for row in change.rows})
    log_event(
        logger, "info", "candles.catalog.refreshed",
        symbols=len(chosen), raw_symbols=len(found), added=change.added[:50],
        added_count=len(change.added), delisted=change.delisted, returned=change.returned,
        queued=sum(1 for symbol in chosen if states[symbol].queued),
    )
    if change.delisting_skipped:
        log_event(
            logger, "warning", "candles.catalog.delisting_skipped",
            found=len(chosen), active=sum(1 for s in known.values() if s.status == "active"),
        )
    return chosen, states


# --- regular pass: last candle -> now ------------------------------------------------------


def _run_pass(
    terminal: MT5Terminal,
    client: Any,
    clock: ServerClock,
    connection: Any,
    worker_id: str,
    options: dict[str, Any],
    catalog: dict[str, SymbolInfo],
    states: dict[str, SymbolState],
) -> None:
    marks = clickhouse.watermarks(client)
    ordered = sorted(
        (info for symbol, info in catalog.items() if _state(states, symbol).ready),
        key=lambda info: info.symbol,
    )
    total = len(ordered)
    pass_started = datetime.now(timezone.utc)
    log_event(logger, "info", "candles.pass.started", symbols=total, catalog=len(catalog))

    for index, info in enumerate(ordered, start=1):
        _report(
            connection, worker_id, WorkerState.running,
            phase="sync", symbol=info.symbol, symbols_done=index - 1, symbols_total=total,
            pass_started_at=pass_started.isoformat(),
        )
        if info.symbol not in marks and clickhouse.candle_watermark(client, info.symbol) is None:
            # Marked done but holds nothing (data removed by hand): back to the backfill queue,
            # so a full history read never runs inside the regular pass.
            _requeue(client, states, info.symbol)
            continue
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
        phase="sync", symbols_done=total, symbols_total=total,
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

    catching_up = last is not None and end - start > _CATCH_UP_LAG
    reference: list[datetime] | None = None
    if catching_up:
        quote = last_quote_at(terminal, info.raw_symbol, clock)
        if quote is not None and last is not None and quote < last + timedelta(minutes=1):
            # The broker has not quoted it since our last candle: a stopped feed, not a hole.
            _log_idle(terminal, info, last, end)
            return 0
        # Have the terminal fetch the server's history for the whole gap first and keep the
        # hourly bars as the yardstick for what each M1 answer must cover.
        reference = sync_history(
            terminal, info.raw_symbol, start, end, clock,
            timeframe=TIMEFRAME_H1, attempts=int(options.get("history_retries", 3)) + 1,
            wait_seconds=_CATCH_UP_WAIT_SECONDS,
        ).times

    written = 0
    highest = last
    buffer: list[list[Any]] = []
    retries = options["history_retries"] if last is None else 1
    for window_start, window_end in chunks(start, end, options["chunk_days"]):
        candles = copy_rates(
            terminal, info.raw_symbol, window_start, window_end, clock, retries=retries
        )
        retries = 1
        if catching_up:
            candles = _settle(
                terminal, info, window_start, window_end, clock, candles, options, reference
            )
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
    if not written and last is not None:
        _log_idle(terminal, info, last, end)
    return written


def missing_hours(
    candles: list[Any], reference: list[datetime] | None, start: datetime, end: datetime
) -> list[datetime]:
    """Hours the coarse reference says traded inside [start, end) that the M1 answer lacks."""
    if not reference:
        return []
    covered = {moment.replace(minute=0, second=0, microsecond=0) for moment, *_ in candles}
    hour = timedelta(hours=1)
    # Only whole hours inside the window: a partial edge hour may legitimately be empty here.
    return [at for at in reference if start <= at and at + hour <= end and at not in covered]


def _settle(
    terminal: MT5Terminal,
    info: SymbolInfo,
    window_start: datetime,
    window_end: datetime,
    clock: ServerClock,
    candles: list[Any],
    options: dict[str, Any],
    reference: list[datetime] | None = None,
) -> list[Any]:
    """Re-ask while the answer still looks short and keep the fullest one.

    Short means: an hour the H1 reference has is missing from M1, or (no reference) the first
    bar comes more than an hour after the window opens."""

    def short(answer: list[Any]) -> bool:
        if reference:
            return bool(missing_hours(answer, reference, window_start, window_end))
        return not answer or answer[0][0] - window_start > _LATE_FIRST_BAR

    if reference is not None and not reference:
        return candles  # the server itself has nothing in the gap (closed market)
    best = candles
    for _ in range(max(int(options.get("history_retries", 3)), 1)):
        if not short(best):
            break
        time.sleep(_CATCH_UP_WAIT_SECONDS)
        again = copy_rates(terminal, info.raw_symbol, window_start, window_end, clock, retries=1)
        if len(again) > len(best):
            best = again
        elif again == best:
            break
    if short(best):
        gaps = missing_hours(best, reference, window_start, window_end)
        log_event(
            logger, "info", "candles.symbol.late_start" if not gaps else "candles.symbol.incomplete",
            symbol=info.symbol, requested_from=window_start.isoformat(),
            first_bar=best[0][0].isoformat() if best else None, rows=len(best),
            missing_hours=len(gaps), first_missing=gaps[0].isoformat() if gaps else None,
        )
    return best


_idle_logged: dict[str, datetime] = {}


def _log_idle(terminal: MT5Terminal, info: SymbolInfo, last: datetime, now: datetime) -> None:
    """A symbol whose feed stopped writes nothing and used to log nothing. Say so once a day,
    with what the terminal itself reports, so a dead feed is not mistaken for a stuck sync."""
    if now - last < _IDLE_LOG_EVERY:
        return
    seen = _idle_logged.get(info.symbol)
    if seen is not None and now - seen < _IDLE_LOG_EVERY:
        return
    _idle_logged[info.symbol] = now
    detail: dict[str, Any] = {}
    try:
        row = terminal.mt5.symbol_info(info.raw_symbol)
        if row is not None:
            detail = {"trade_mode": int(row.trade_mode), "last_quote_label": int(row.time)}
    except Exception:
        pass
    log_event(
        logger, "warning", "candles.symbol.idle",
        symbol=info.symbol, raw_symbol=info.raw_symbol, last_candle=last.isoformat(),
        idle_hours=int((now - last).total_seconds() // 3600), **detail,
    )


# --- backfill of new symbols ---------------------------------------------------------------

_empty_backfills: dict[str, int] = {}


def _run_backfill(
    terminal: MT5Terminal,
    client: Any,
    clock: ServerClock,
    connection: Any,
    worker_id: str,
    options: dict[str, Any],
    catalog: dict[str, SymbolInfo],
    states: dict[str, SymbolState],
    *,
    deadline: float,
) -> None:
    """History for newly listed symbols, from BACKFILL_START_DATE (or the symbol's first bar,
    if later) to now, in chunks, inside a time budget after each regular pass. The regular
    pass never waits for it; a symbol joins that pass once its history reaches the present."""
    queue = sorted(
        (info for symbol, info in catalog.items() if _state(states, symbol).queued),
        key=lambda info: info.symbol,
    )
    if not queue:
        return
    marks = clickhouse.watermarks(client)
    log_event(logger, "info", "candles.backfill.started", queued=len(queue))
    for position, info in enumerate(queue, start=1):
        if time.monotonic() >= deadline:
            break
        _report(
            connection, worker_id, WorkerState.running,
            phase="backfill", symbol=info.symbol, queue_done=position - 1, queue_total=len(queue),
        )
        try:
            finished = _backfill_symbol(
                terminal, client, clock, info, marks, options, deadline, connection, worker_id
            )
        except TerminalError:
            raise
        except Exception as exc:
            log_event(
                logger, "warning", "candles.backfill.failed",
                symbol=info.symbol, error=f"{type(exc).__name__}: {exc}",
            )
            continue
        if finished:
            state = replace(_state(states, info.symbol), backfill=BACKFILL_DONE)
            clickhouse.write_symbol_states(client, [state])
            states[info.symbol] = state
            log_event(logger, "info", "candles.backfill.completed", symbol=info.symbol)


def _backfill_symbol(
    terminal: MT5Terminal,
    client: Any,
    clock: ServerClock,
    info: SymbolInfo,
    marks: dict[str, datetime],
    options: dict[str, Any],
    deadline: float,
    connection: Any = None,
    worker_id: str = "",
) -> bool:
    """Returns True once the series reaches the present. Progress is the ordinary watermark,
    so a restart, or the budget running out, resumes where it stopped."""
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    if not select(terminal, info.raw_symbol):
        log_event(logger, "warning", "candles.symbol.unselectable", symbol=info.symbol)
        return False
    last = marks.get(info.symbol) or clickhouse.candle_watermark(client, info.symbol)
    if last is None:
        # Daily bars over the whole range: a few thousand rows, and the request makes the
        # terminal download the full M1 history it is built from. The first one tells where
        # the broker's history actually starts, so empty years are not walked month by month.
        daily = sync_history(
            terminal, info.raw_symbol, options["start_date"], end, clock,
            timeframe=TIMEFRAME_D1, attempts=int(options.get("history_retries", 3)) + 2,
            wait_seconds=3.0,
        )
        if not daily.stable:
            # Still downloading: a first bar read now could be later than the real one and
            # the early years would be skipped for good. The terminal keeps loading meanwhile.
            log_event(logger, "info", "candles.backfill.history_loading", symbol=info.symbol,
                      daily_bars=len(daily.times))
            return False
        days = daily.times
        if not days:
            seen = _empty_backfills.get(info.symbol, 0) + 1
            _empty_backfills[info.symbol] = seen
            log_event(logger, "info", "candles.backfill.no_history", symbol=info.symbol, attempt=seen)
            return seen >= _EMPTY_BACKFILL_LIMIT
        start = max(options["start_date"], days[0] - timedelta(days=1))
    else:
        start = last + timedelta(minutes=1)
    if start >= end:
        return True

    pause = float(options.get("backfill_pause_seconds") or 0)
    highest = last
    written = 0
    for window_start, window_end in chunks(start, end, options["chunk_days"]):
        if time.monotonic() >= deadline:
            return False
        reference = sync_history(
            terminal, info.raw_symbol, window_start, window_end, clock,
            timeframe=TIMEFRAME_H1, attempts=3, wait_seconds=_CATCH_UP_WAIT_SECONDS,
        ).times
        candles = copy_rates(
            terminal, info.raw_symbol, window_start, window_end, clock,
            retries=options["history_retries"],
        )
        if reference:
            candles = _settle(
                terminal, info, window_start, window_end, clock, candles, options, reference
            )
        rows = [
            [info.symbol, moment, open_, high, low, close, tick_volume, real_volume, spread]
            for moment, open_, high, low, close, tick_volume, real_volume, spread in candles
        ]
        for offset in range(0, len(rows), _INSERT_BATCH):
            written += clickhouse.insert_candles(client, rows[offset: offset + _INSERT_BATCH])
        if rows:
            highest = rows[-1][1] if highest is None or rows[-1][1] > highest else highest
            clickhouse.set_watermark(client, info.symbol, highest, written)
        if connection is not None:
            _report(
                connection, worker_id, WorkerState.running, phase="backfill",
                symbol=info.symbol, window_end=window_end.isoformat(), rows=written,
            )
        if pause > 0:
            time.sleep(pause)
    if written:
        log_event(logger, "info", "candles.backfill.synced", symbol=info.symbol, rows=written)
    return True


# --- repair: re-read from the verified cursor ------------------------------------------------


def _parse_since(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        moment = value
    else:
        moment = datetime.fromisoformat(str(value))
    return moment.replace(tzinfo=timezone.utc) if moment.tzinfo is None else moment


def repair_window(
    now: datetime,
    last: datetime,
    cursor: datetime | None,
    options: dict[str, Any],
    since: datetime | None = None,
) -> tuple[datetime, datetime] | None:
    """What one repair run re-reads for a symbol.

    Always the trailing REPAIR_LOOKBACK_DAYS, and further back to the symbol's verified cursor
    when that is older: after an outage of any length the cursor still sits where the outage
    began, so the hole is swept without anyone setting REPAIR_SINCE. One run covers at most
    REPAIR_MAX_DAYS so a long sweep is spread over several runs."""
    start = now - timedelta(days=int(options.get("repair_lookback_days", 7)))
    if cursor is not None and cursor < start:
        start = cursor
    if since is not None and since < start:
        start = since
    start = max(start, options["start_date"])
    stop = min(last, now)
    span = int(options.get("repair_max_days") or 0)
    if span > 0:
        stop = min(stop, start + timedelta(days=span))
    if stop <= start:
        return None
    return start, stop


def _run_repair(
    terminal: MT5Terminal,
    client: Any,
    clock: ServerClock,
    connection: Any,
    worker_id: str,
    options: dict[str, Any],
    catalog: dict[str, SymbolInfo],
    states: dict[str, SymbolState],
    since: datetime | None,
) -> None:
    """Fill holes inside history: re-read a window from the terminal and insert only the
    minutes ClickHouse does not have. Normal passes only ever move forward from the
    watermark, so without this a hole, once skipped, stays forever."""
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    min_age = timedelta(hours=float(options.get("repair_min_age_hours") or 0))
    marks = clickhouse.watermarks(client)
    cursors = clickhouse.repair_cursors(client)
    total = 0
    swept = 0
    for symbol in sorted(catalog):
        info = catalog[symbol]
        last = marks.get(symbol)
        if last is None or not _state(states, symbol).ready:
            continue
        window = repair_window(now, last, cursors.get(symbol), options, since)
        if window is None:
            continue
        start, stop = window
        _report(connection, worker_id, WorkerState.running, phase="repair", symbol=symbol)
        try:
            filled, verified = _repair_symbol(terminal, client, clock, info, start, stop, options)
        except TerminalError:
            raise
        except Exception as exc:
            log_event(
                logger, "warning", "candles.repair.failed",
                symbol=symbol, error=f"{type(exc).__name__}: {exc}",
            )
            continue
        total += filled
        swept += 1
        # Only history old enough for the terminal to have it in full counts as verified.
        verified = min(verified, now - min_age)
        previous = cursors.get(symbol)
        if previous is None or verified > previous:
            clickhouse.set_repair_cursor(client, symbol, verified)
    log_event(logger, "info", "candles.repair.completed", symbols=swept, rows=total)


def _repair_symbol(
    terminal: MT5Terminal,
    client: Any,
    clock: ServerClock,
    info: SymbolInfo,
    start: datetime,
    end: datetime,
    options: dict[str, Any],
) -> tuple[int, datetime]:
    """Returns (rows inserted, verified-until). Verification stops at the first window whose
    M1 answer still lacks hours the hourly reference has: that part is retried next run."""
    filled = 0
    verified = start
    complete = True
    for window_start, window_end in chunks(start, end, options["chunk_days"]):
        synced = sync_history(
            terminal, info.raw_symbol, window_start, window_end, clock,
            timeframe=TIMEFRAME_H1, attempts=3, wait_seconds=1.0,
        )
        reference = synced.times
        candles = copy_rates(terminal, info.raw_symbol, window_start, window_end, clock, retries=1)
        if reference:
            candles = _settle(
                terminal, info, window_start, window_end, clock, candles, options, reference
            )
        if complete:
            if not synced.stable or missing_hours(candles, reference, window_start, window_end):
                complete = False
            else:
                verified = window_end
        if not candles:
            continue
        have = clickhouse.existing_minutes(client, info.symbol, window_start, window_end)
        missing = [
            [info.symbol, moment, open_, high, low, close, tick_volume, real_volume, spread]
            for moment, open_, high, low, close, tick_volume, real_volume, spread in candles
            if moment not in have
        ]
        for offset in range(0, len(missing), _INSERT_BATCH):
            filled += clickhouse.insert_candles(client, missing[offset: offset + _INSERT_BATCH])
    if filled:
        log_event(
            logger, "info", "candles.repair.filled",
            symbol=info.symbol, rows=filled, since=start.isoformat(), until=end.isoformat(),
        )
    return filled, verified


# --- helpers -------------------------------------------------------------------------------


def _state(states: dict[str, SymbolState], symbol: str) -> SymbolState:
    # A symbol the catalogue has not recorded yet (state table unreachable at refresh) keeps
    # the old behaviour: synced in the regular pass.
    return states.get(symbol) or SymbolState(symbol, backfill=BACKFILL_DONE)


def _requeue(client: Any, states: dict[str, SymbolState], symbol: str) -> None:
    state = replace(_state(states, symbol), backfill=BACKFILL_PENDING)
    clickhouse.write_symbol_states(client, [state])
    states[symbol] = state
    log_event(logger, "info", "candles.backfill.requeued", symbol=symbol)


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
