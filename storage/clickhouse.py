from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

import clickhouse_connect

from utils.config import settings
from utils.logging import log_event

logger = logging.getLogger(__name__)

CANDLE_COLUMNS = (
    "symbol", "ts", "open", "high", "low", "close", "tick_volume", "real_volume", "spread",
)

_DDL = (
    """
    CREATE TABLE IF NOT EXISTS candles (
        symbol      LowCardinality(String),
        ts          DateTime('UTC'),
        open        Float64,
        high        Float64,
        low         Float64,
        close       Float64,
        tick_volume UInt64,
        real_volume UInt64,
        spread      UInt32,
        ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = ReplacingMergeTree(ingested_at)
    PARTITION BY toYYYYMM(ts)
    ORDER BY (symbol, ts)
    """,
    """
    CREATE TABLE IF NOT EXISTS symbols (
        symbol      LowCardinality(String),
        raw_symbol  String,
        description String,
        digits      UInt8,
        path        String,
        updated_at  DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY symbol
    """,
    """
    CREATE TABLE IF NOT EXISTS sync_state (
        symbol       LowCardinality(String),
        last_ts      DateTime('UTC'),
        last_run_at  DateTime64(3, 'UTC'),
        rows_written UInt64,
        updated_at   DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY symbol
    """,
)

_FINAL = {"do_not_merge_across_partitions_select_final": 1}


def connect(database: str | None = None) -> Any:
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=database if database is not None else settings.clickhouse_database,
        connect_timeout=15,
        send_receive_timeout=300,
    )


def init_schema() -> None:
    bootstrap = connect(database="default")
    try:
        bootstrap.command(f"CREATE DATABASE IF NOT EXISTS {settings.clickhouse_database}")
    finally:
        bootstrap.close()

    client = connect()
    try:
        for statement in _DDL:
            client.command(statement)
    finally:
        client.close()
    log_event(logger, "info", "clickhouse.schema.ready", database=settings.clickhouse_database)


def insert_candles(client: Any, rows: Sequence[Sequence[Any]]) -> int:
    if not rows:
        return 0
    client.insert("candles", rows, column_names=list(CANDLE_COLUMNS))
    return len(rows)


def upsert_symbols(client: Any, rows: Iterable[Sequence[Any]]) -> int:
    payload = list(rows)
    if not payload:
        return 0
    client.insert(
        "symbols", payload, column_names=["symbol", "raw_symbol", "description", "digits", "path"]
    )
    return len(payload)


def set_watermark(client: Any, symbol: str, last_ts: datetime, rows_written: int) -> None:
    client.insert(
        "sync_state",
        [[symbol, last_ts, datetime.now(timezone.utc), int(rows_written)]],
        column_names=["symbol", "last_ts", "last_run_at", "rows_written"],
    )


def watermarks(client: Any) -> dict[str, datetime]:
    result = client.query("SELECT symbol, last_ts FROM sync_state FINAL", settings=_FINAL)
    return {row[0]: _utc(row[1]) for row in result.result_rows if row[1] is not None}


def candle_watermark(client: Any, symbol: str) -> datetime | None:
    result = client.query(
        "SELECT max(ts) FROM candles WHERE symbol = {symbol:String}",
        parameters={"symbol": symbol},
    )
    rows = result.result_rows
    if not rows or rows[0][0] is None:
        return None
    value = _utc(rows[0][0])
    return value if value.year > 1970 else None


def known_symbols(client: Any) -> list[dict[str, Any]]:
    result = client.query(
        """
        SELECT symbol, raw_symbol, description, digits, path
        FROM symbols FINAL
        ORDER BY symbol
        """,
        settings=_FINAL,
    )
    return [
        {"symbol": row[0], "raw_symbol": row[1], "description": row[2], "digits": row[3], "path": row[4]}
        for row in result.result_rows
    ]


def get_symbol(client: Any, symbol: str) -> dict[str, Any] | None:
    result = client.query(
        """
        SELECT symbol, raw_symbol, description, digits, path
        FROM symbols FINAL
        WHERE symbol = {symbol:String}
        """,
        parameters={"symbol": symbol},
        settings=_FINAL,
    )
    rows = result.result_rows
    if not rows:
        return None
    row = rows[0]
    return {
        "symbol": row[0], "raw_symbol": row[1], "description": row[2],
        "digits": row[3], "path": row[4],
    }


def symbol_exists(client: Any, symbol: str) -> bool:
    result = client.query(
        "SELECT count() FROM symbols WHERE symbol = {symbol:String}", parameters={"symbol": symbol}
    )
    return bool(result.result_rows and result.result_rows[0][0])


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


TIMEFRAMES: dict[str, int] = {
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "4h": 14400, "1d": 86400, "1w": 604800,
}

_BUCKETS = {
    "1m": "toStartOfInterval(ts, INTERVAL 1 minute)",
    "5m": "toStartOfInterval(ts, INTERVAL 5 minute)",
    "15m": "toStartOfInterval(ts, INTERVAL 15 minute)",
    "30m": "toStartOfInterval(ts, INTERVAL 30 minute)",
    "1h": "toStartOfInterval(ts, INTERVAL 1 hour)",
    "4h": "toStartOfInterval(ts, INTERVAL 4 hour)",
    "1d": "toStartOfDay(ts)",
    "1w": "toDateTime(toStartOfWeek(ts, 1), 'UTC')",
}


def select_candles(
    client: Any, symbol: str, start: datetime, end: datetime, timeframe: str, limit: int
) -> list[dict[str, Any]]:
    bucket = _BUCKETS[timeframe]
    result = client.query(
        f"""
        SELECT {bucket} AS bucket,
               argMin(open, ts) AS open,
               max(high) AS high,
               min(low) AS low,
               argMax(close, ts) AS close,
               sum(tick_volume) AS tick_volume,
               sum(real_volume) AS real_volume
        FROM candles FINAL
        WHERE symbol = {{symbol:String}} AND ts >= {{start:DateTime}} AND ts < {{end:DateTime}}
        GROUP BY bucket
        ORDER BY bucket
        LIMIT {{limit:UInt32}}
        """,
        parameters={"symbol": symbol, "start": start, "end": end, "limit": limit},
        settings=_FINAL,
    )
    return [
        {
            "ts": _utc(row[0]),
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4],
            "tick_volume": int(row[5]),
            "real_volume": int(row[6]),
        }
        for row in result.result_rows
    ]


def count_buckets(client: Any, symbol: str, start: datetime, end: datetime, timeframe: str) -> int:
    bucket = _BUCKETS[timeframe]
    result = client.query(
        f"""
        SELECT uniqExact({bucket})
        FROM candles
        WHERE symbol = {{symbol:String}} AND ts >= {{start:DateTime}} AND ts < {{end:DateTime}}
        """,
        parameters={"symbol": symbol, "start": start, "end": end},
    )
    return int(result.result_rows[0][0]) if result.result_rows else 0


class ClientPool:
    def __init__(self, size: int = 8) -> None:
        self._size = max(size, 1)
        self._idle: list[Any] = []
        self._lock = threading.Lock()

    def acquire(self) -> Any:
        with self._lock:
            if self._idle:
                return self._idle.pop()
        return connect()

    def release(self, client: Any) -> None:
        with self._lock:
            if len(self._idle) < self._size:
                self._idle.append(client)
                return
        try:
            client.close()
        except Exception:
            pass

    def discard(self, client: Any) -> None:
        try:
            client.close()
        except Exception:
            pass

    def close(self) -> None:
        with self._lock:
            clients, self._idle = self._idle, []
        for client in clients:
            self.discard(client)

    @contextmanager
    def borrow(self) -> Any:
        client = self.acquire()
        try:
            yield client
        except Exception:
            self.discard(client)
            raise
        else:
            self.release(client)
