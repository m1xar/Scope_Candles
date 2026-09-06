from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from api.deps import COMMON_RESPONSES, get_clickhouse, verify_auth
from storage import clickhouse

router = APIRouter(tags=["Symbols"], dependencies=[Depends(verify_auth)], responses=COMMON_RESPONSES)


class Symbol(BaseModel):
    symbol: str
    raw_symbol: str
    description: str
    digits: int
    path: str
    last_candle_at: datetime | None = None


class SymbolList(BaseModel):
    count: int
    symbols: list[Symbol]


@router.get("/symbols", response_model=SymbolList)
async def list_symbols(
    search: str | None = Query(default=None),
    client: Any = Depends(get_clickhouse),
) -> SymbolList:
    def read() -> tuple[list[dict[str, Any]], dict[str, datetime]]:
        with client.borrow() as connection:
            return clickhouse.known_symbols(connection), clickhouse.watermarks(connection)

    rows, marks = await asyncio.to_thread(read)
    needle = search.upper() if search else None
    symbols = [
        Symbol(**row, last_candle_at=marks.get(row["symbol"]))
        for row in rows
        if needle is None or needle in row["symbol"] or needle in row["description"].upper()
    ]
    return SymbolList(count=len(symbols), symbols=symbols)
