from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from api.deps import COMMON_RESPONSES, get_clickhouse, verify_auth
from storage import clickhouse
from symbols.normalize import normalize
from utils.config import settings

router = APIRouter(tags=["Candles"], dependencies=[Depends(verify_auth)], responses=COMMON_RESPONSES)


class Candle(BaseModel):
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    tick_volume: int
    real_volume: int


class CandleResponse(BaseModel):
    symbol: str
    raw_symbol: str
    timeframe: str
    start: datetime = Field(alias="from")
    end: datetime = Field(alias="to")
    count: int
    candles: list[Candle]

    model_config = {"populate_by_name": True}


@router.get("/candles/{symbol}", response_model=CandleResponse, response_model_by_alias=True)
async def get_candles(
    symbol: str,
    start: datetime = Query(alias="from"),
    end: datetime | None = Query(default=None, alias="to"),
    timeframe: str = Query(default="1m"),
    client: Any = Depends(get_clickhouse),
) -> CandleResponse:
    if timeframe not in clickhouse.TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported timeframe; expected one of {', '.join(clickhouse.TIMEFRAMES)}",
        )

    normalized = normalize(symbol, settings.suffixes)
    if not normalized:
        raise HTTPException(status_code=404, detail="symbol not found")

    cutoff = _cutoff()
    window_start = _utc(start)
    window_end = _utc(end) if end is not None else cutoff

    if window_start >= cutoff:
        raise HTTPException(
            status_code=400,
            detail=(
                f"no candles available for this window; data lags live by "
                f"{settings.freshness_lag_minutes} minutes, so 'from' must be before "
                f"{cutoff.isoformat()}"
            ),
        )
    if window_end > cutoff:
        window_end = cutoff
    if window_start >= window_end:
        raise HTTPException(status_code=400, detail="'from' must be earlier than 'to'")

    def read() -> tuple[dict[str, Any] | None, int, list[dict[str, Any]]]:
        with client.borrow() as connection:
            known = clickhouse.get_symbol(connection, normalized)
            if known is None:
                return None, 0, []
            buckets = clickhouse.count_buckets(
                connection, normalized, window_start, window_end, timeframe
            )
            if buckets > settings.max_rows:
                return known, buckets, []
            rows = clickhouse.select_candles(
                connection, normalized, window_start, window_end, timeframe, settings.max_rows
            )
            return known, buckets, rows

    info, buckets, rows = await asyncio.to_thread(read)
    if info is None:
        raise HTTPException(status_code=404, detail="symbol not found")
    if buckets > settings.max_rows:
        raise HTTPException(
            status_code=400,
            detail=(
                f"window yields {buckets} candles which exceeds the {settings.max_rows} row limit; "
                f"narrow the range or use a larger timeframe"
            ),
        )

    return CandleResponse(
        symbol=normalized,
        raw_symbol=info["raw_symbol"],
        timeframe=timeframe,
        start=window_start,
        end=window_end,
        count=len(rows),
        candles=[Candle(**row) for row in rows],
    )


def _cutoff() -> datetime:
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    return now - timedelta(minutes=settings.freshness_lag_minutes)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
