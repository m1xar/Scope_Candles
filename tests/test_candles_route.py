from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.routes.candles as candles_route
from storage import clickhouse


class FakePool:
    @contextmanager
    def borrow(self):
        yield object()


def make_client(monkeypatch, catalog):
    asked = []

    def get_symbol(_conn, symbol):
        asked.append(symbol)
        if symbol in catalog:
            return {"symbol": symbol, "raw_symbol": catalog[symbol], "description": "", "digits": 2, "path": ""}
        return None

    monkeypatch.setattr(clickhouse, "get_symbol", get_symbol)
    monkeypatch.setattr(clickhouse, "count_buckets", lambda *a, **k: 1)
    monkeypatch.setattr(
        clickhouse,
        "select_candles",
        lambda _c, symbol, start, *a, **k: [
            {"ts": start, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "tick_volume": 3, "real_volume": 0}
        ],
    )
    monkeypatch.setattr(candles_route.settings, "api_token", "")
    app = FastAPI()
    app.state.clickhouse = FakePool()
    app.include_router(candles_route.router)
    return TestClient(app), asked


def window():
    start = datetime.now(timezone.utc) - timedelta(days=1)
    return {"from": start.isoformat(), "to": (start + timedelta(hours=1)).isoformat()}


def test_alias_request(monkeypatch):
    client, asked = make_client(monkeypatch, {"DE40": "DE40"})
    response = client.get("/candles/GER40.cash", params=window())
    assert response.status_code == 200
    body = response.json()
    assert (body["symbol"], body["requested"], body["matched_by"]) == ("DE40", "GER40.cash", "alias")
    assert asked == ["GER40", "DE40"]


def test_exact_request_any_case(monkeypatch):
    client, _ = make_client(monkeypatch, {"EURUSD": "EURUSD"})
    body = client.get("/candles/eurusd.pro", params=window()).json()
    assert (body["symbol"], body["matched_by"]) == ("EURUSD", "exact")


def test_space_in_path(monkeypatch):
    client, _ = make_client(monkeypatch, {"DE40": "DE40"})
    assert client.get("/candles/Germany%2040", params=window()).json()["symbol"] == "DE40"


def test_unknown_is_404(monkeypatch):
    client, _ = make_client(monkeypatch, {"DE40": "DE40"})
    assert client.get("/candles/Volatility%2075%20Index", params=window()).status_code == 404
