from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from symbols.normalize import SymbolInfo, normalize
from utils.logging import log_event

from .clock import ServerClock
from .terminal import MT5Terminal

logger = logging.getLogger(__name__)

TIMEFRAME_M1 = 1
TIMEFRAME_H1 = 16385

_EMPTY_RETRY_SLEEP_SECONDS = 2.0


def list_symbols(terminal: MT5Terminal, suffixes: list[str], path_prefixes: list[str]) -> list[SymbolInfo]:
    rows = terminal.check_call(terminal.mt5.symbols_get(), "symbols_get")
    found: list[SymbolInfo] = []
    for row in rows:
        raw = getattr(row, "name", "") or ""
        if not raw:
            continue
        path = getattr(row, "path", "") or ""
        if path_prefixes and not any(path.startswith(prefix) for prefix in path_prefixes):
            continue
        found.append(
            SymbolInfo(
                symbol=normalize(raw, suffixes),
                raw_symbol=raw,
                description=getattr(row, "description", "") or "",
                digits=int(getattr(row, "digits", 0) or 0),
                path=path,
            )
        )
    return found


def select(terminal: MT5Terminal, raw_symbol: str) -> bool:
    try:
        return bool(terminal.mt5.symbol_select(raw_symbol, True))
    except Exception:
        return False


def copy_rates(
    terminal: MT5Terminal,
    raw_symbol: str,
    start: datetime,
    end: datetime,
    clock: ServerClock,
    *,
    retries: int = 3,
) -> list[tuple[datetime, float, float, float, float, int, int, int]]:
    server_start = clock.to_server(start) or start.replace(tzinfo=None)
    server_end = clock.to_server(end) or end.replace(tzinfo=None)

    rows: Any = None
    for attempt in range(1, max(retries, 1) + 1):
        rows = terminal.mt5.copy_rates_range(
            raw_symbol, TIMEFRAME_M1, server_start.replace(tzinfo=timezone.utc), server_end.replace(tzinfo=timezone.utc)
        )
        if rows is not None and len(rows):
            break
        code, description = terminal.mt5.last_error()
        if code not in (1, 0) and code is not None and code < 0:
            raise terminal.failure(f"copy_rates_range failed for {raw_symbol}: {description}", code)
        if attempt < retries:
            time.sleep(_EMPTY_RETRY_SLEEP_SECONDS)

    if rows is None or not len(rows):
        return []

    candles = []
    for row in rows:
        label = datetime.fromtimestamp(int(row["time"]), timezone.utc).replace(tzinfo=None)
        moment = clock.to_utc(label)
        if moment is None or moment < start or moment >= end:
            continue
        candles.append(
            (
                moment,
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                int(row["tick_volume"]),
                int(row["real_volume"]),
                int(row["spread"]),
            )
        )
    candles.sort(key=lambda item: item[0])
    return candles


def read_tick(terminal: MT5Terminal, raw_symbol: str, clock: ServerClock) -> dict[str, Any] | None:
    try:
        tick = terminal.mt5.symbol_info_tick(raw_symbol)
    except Exception:
        return None
    if tick is None or not getattr(tick, "time", 0):
        return None
    label = datetime.fromtimestamp(int(tick.time), timezone.utc).replace(tzinfo=None)
    moment = clock.to_utc(label) or label.replace(tzinfo=timezone.utc)
    return {
        "bid": float(tick.bid),
        "ask": float(tick.ask),
        "last": float(tick.last),
        "volume": int(getattr(tick, "volume", 0) or 0),
        "ts": moment.isoformat(),
        "epoch": int(tick.time),
    }


def chunks(start: datetime, end: datetime, days: int) -> list[tuple[datetime, datetime]]:
    step = timedelta(days=max(days, 1))
    windows: list[tuple[datetime, datetime]] = []
    cursor = start
    while cursor < end:
        stop = min(cursor + step, end)
        windows.append((cursor, stop))
        cursor = stop
    return windows


def log_clock(clock: ServerClock, server: str) -> None:
    log_event(
        logger,
        "info",
        "clock.resolved",
        server=server,
        scanned=clock.scanned,
        offset_minutes=clock.offset_minutes,
        steps=clock.rows,
    )
