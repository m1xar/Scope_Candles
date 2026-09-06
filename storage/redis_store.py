from __future__ import annotations

import json
from typing import Any

PRICE_KEY = "price:{symbol}"
PRICE_CHANNEL = "px:{symbol}"


def price_key(symbol: str) -> str:
    return PRICE_KEY.format(symbol=symbol)


def price_channel(symbol: str) -> str:
    return PRICE_CHANNEL.format(symbol=symbol)


def encode(quote: dict[str, Any]) -> str:
    return json.dumps(quote, separators=(",", ":"))


def decode(payload: str | bytes | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    try:
        return json.loads(payload)
    except ValueError:
        return None
