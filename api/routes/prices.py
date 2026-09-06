from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from api.deps import COMMON_RESPONSES, get_redis, verify_auth, verify_websocket_auth
from storage.redis_store import decode, price_channel, price_key
from symbols.normalize import normalize
from utils.config import settings
from utils.logging import log_event

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Prices"], dependencies=[Depends(verify_auth)], responses=COMMON_RESPONSES)
stream = APIRouter(tags=["Prices"])

_IDLE_PING_SECONDS = 20.0


class Quote(BaseModel):
    symbol: str
    raw_symbol: str
    bid: float
    ask: float
    last: float
    volume: int
    ts: str


@router.get("/price/{symbol}", response_model=Quote)
async def get_price(symbol: str, store: Any = Depends(get_redis)) -> Quote:
    normalized = normalize(symbol, settings.suffixes)
    if not normalized:
        raise HTTPException(status_code=404, detail="symbol not found")
    payload = decode(await store.get(price_key(normalized)))
    if payload is None:
        raise HTTPException(status_code=404, detail="no live price for this symbol")
    return Quote(**payload)


@stream.websocket("/ws/price/{symbol}")
async def stream_price(websocket: WebSocket, symbol: str) -> None:
    if not verify_websocket_auth(websocket):
        await websocket.close(code=4401, reason="Invalid or missing bearer token")
        return

    normalized = normalize(symbol, settings.suffixes)
    if not normalized:
        await websocket.close(code=4404, reason="symbol not found")
        return

    store = getattr(websocket.app.state, "redis", None)
    if store is None:
        await websocket.close(code=1013, reason="Redis is not ready")
        return

    await websocket.accept()
    pubsub = store.pubsub()
    try:
        await pubsub.subscribe(price_channel(normalized))
        snapshot = await store.get(price_key(normalized))
        if snapshot is not None:
            await websocket.send_text(snapshot)

        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=_IDLE_PING_SECONDS)
            if message is None:
                await websocket.send_json({"type": "ping"})
                continue
            data = message.get("data")
            if isinstance(data, bytes):
                data = data.decode("utf-8")
            if data:
                await websocket.send_text(data)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log_event(logger, "warning", "price.stream.failed", symbol=normalized, error=str(exc))
    finally:
        try:
            await pubsub.unsubscribe(price_channel(normalized))
            await pubsub.aclose()
        except Exception:
            pass
