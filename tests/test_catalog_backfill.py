from datetime import datetime, timedelta, timezone

import pytest

import workers.candles_worker as worker
from mt5.clock import ServerClock
from symbols.catalog import (
    ACTIVE, BACKFILL_DONE, BACKFILL_PENDING, DELISTED, SymbolState, limit_extra, reconcile,
)
from symbols.normalize import SymbolInfo

UTC = timezone.utc
NOW = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
START = datetime(2020, 1, 1, tzinfo=UTC)


def info(symbol, path="Forex\\" , raw=None):
    return SymbolInfo(symbol=symbol, raw_symbol=raw or symbol, path=path + (raw or symbol))


# --- catalogue -------------------------------------------------------------------------------


def test_reconcile_adds_marks_and_returns():
    known = {
        "EURUSD": SymbolState("EURUSD", ACTIVE, BACKFILL_DONE),
        "UK100": SymbolState("UK100", ACTIVE, BACKFILL_DONE),
        "XAUCHF": SymbolState("XAUCHF", DELISTED, BACKFILL_DONE, delisted_at=NOW - timedelta(days=3)),
        "GBPUSD": SymbolState("GBPUSD", ACTIVE, BACKFILL_DONE),
    }
    found = {s: info(s) for s in ("EURUSD", "GBPUSD", "XAUCHF", "COPPER", "USDJPY")}
    change = reconcile(known, found, has_history={"EURUSD", "GBPUSD", "USDJPY"}, now=NOW)
    rows = {row.symbol: row for row in change.rows}

    assert change.added == ["COPPER", "USDJPY"]
    assert rows["COPPER"].backfill == BACKFILL_PENDING  # new, no candles -> queued
    assert rows["USDJPY"].backfill == BACKFILL_DONE  # first run after deploy: has candles already
    assert change.delisted == ["UK100"]
    assert rows["UK100"].status == DELISTED and rows["UK100"].delisted_at == NOW
    assert change.returned == ["XAUCHF"]
    assert rows["XAUCHF"].status == ACTIVE and rows["XAUCHF"].delisted_at is None
    assert rows["EURUSD"].last_seen_at == NOW


def test_reconcile_never_delists_on_a_shrunken_answer():
    known = {f"S{i:03d}": SymbolState(f"S{i:03d}", ACTIVE, BACKFILL_DONE) for i in range(100)}
    found = {s: info(s) for s in list(known)[:10]}
    change = reconcile(known, found, has_history=set(known), now=NOW)
    assert change.delisting_skipped
    assert change.delisted == []
    assert all(row.status == ACTIVE for row in change.rows)


def test_first_run_after_deploy_queues_nothing_that_has_candles():
    found = {s: info(s) for s in ("EURUSD", "US500", "DE40")}
    change = reconcile({}, found, has_history={"EURUSD", "US500", "DE40"}, now=NOW)
    assert all(row.backfill == BACKFILL_DONE for row in change.rows)
    assert change.delisted == []


def test_extra_groups_are_off_by_default_and_capped():
    base = [info("EURUSD"), info("XAUUSD", "Metals\\")]
    stocks = [info(t, "Nasdaq\\Stock\\") for t in ("TSLA", "AAPL", "NVDA", "ZZZZ")]
    extra = ["Nasdaq\\Stock"]

    assert limit_extra(base + stocks, extra, [], 0) == base
    picked = limit_extra(base + stocks, extra, ["TSLA", "NVDA"], 10)
    assert [i.symbol for i in picked] == ["EURUSD", "XAUUSD", "NVDA", "TSLA"]
    capped = limit_extra(base + stocks, extra, [], 2)
    assert len(capped) == 4


# --- fake terminal ---------------------------------------------------------------------------


def bars(start, end, step=timedelta(minutes=1), skip_weekends=True):
    out = []
    moment = start
    while moment < end:
        if not skip_weekends or moment.weekday() < 5:
            out.append((moment, 1.0, 1.0, 1.0, 1.0, 1, 0, 0))
        moment += step
    return out


class Market:
    """The server's truth and a terminal whose local copy fills up only as it is asked."""

    def __init__(self, series, local_from=None, lag_calls=0):
        self.series = series
        self.local_from = local_from or {}
        self.lag_calls = lag_calls
        self.calls = 0

    def m1(self, raw, start, end):
        self.calls += 1
        rows = [c for c in self.series.get(raw, []) if start <= c[0] < end]
        cut = self.local_from.get(raw)
        if cut is not None and self.calls <= self.lag_calls:
            rows = [c for c in rows if c[0] >= cut]
        return rows

    def hours(self, raw, start, end):
        seen = sorted({c[0].replace(minute=0) for c in self.series.get(raw, [])})
        return [h for h in seen if start <= h + timedelta(hours=1) and h < end]

    def days(self, raw, start, end):
        seen = sorted({c[0].replace(hour=0, minute=0) for c in self.series.get(raw, [])})
        return [d for d in seen if start <= d < end]


class FakeCH:
    def __init__(self):
        self.rows = {}
        self.marks = {}
        self.cursors = {}
        self.states = []

    def minutes(self, symbol):
        return sorted(ts for (s, ts) in self.rows if s == symbol)


def wire(monkeypatch, market, ch, now=NOW, quote=None):
    def copy_rates(_t, raw, start, end, _clock, retries=1):
        return market.m1(raw, start, end)

    def sync_history(_t, raw, start, end, _clock, timeframe=worker.TIMEFRAME_H1, **_kw):
        if timeframe == worker.TIMEFRAME_D1:
            return worker.HistorySync(market.days(raw, start, end), True)
        return worker.HistorySync(market.hours(raw, start, end), True)

    def insert(_c, rows):
        for row in rows:
            ch.rows[(row[0], row[1])] = row
        return len(rows)

    monkeypatch.setattr(worker, "copy_rates", copy_rates)
    monkeypatch.setattr(worker, "sync_history", sync_history)
    monkeypatch.setattr(worker, "last_quote_at", lambda _t, raw, _c: quote(raw) if quote else None)
    monkeypatch.setattr(worker, "select", lambda *_: True)
    monkeypatch.setattr(worker.time, "sleep", lambda *_: None)
    monkeypatch.setattr(worker, "datetime", _frozen(now))
    monkeypatch.setattr(worker.clickhouse, "insert_candles", insert)
    monkeypatch.setattr(worker.clickhouse, "set_watermark", lambda _c, s, ts, n: ch.marks.__setitem__(s, ts))
    monkeypatch.setattr(worker.clickhouse, "watermarks", lambda _c: dict(ch.marks))
    monkeypatch.setattr(
        worker.clickhouse, "candle_watermark",
        lambda _c, s: max(ch.minutes(s)) if ch.minutes(s) else None,
    )
    monkeypatch.setattr(
        worker.clickhouse, "existing_minutes",
        lambda _c, s, a, b: {ts for ts in ch.minutes(s) if a <= ts < b},
    )
    monkeypatch.setattr(worker.clickhouse, "repair_cursors", lambda _c: dict(ch.cursors))
    monkeypatch.setattr(
        worker.clickhouse, "set_repair_cursor", lambda _c, s, ts: ch.cursors.__setitem__(s, ts)
    )
    monkeypatch.setattr(worker.clickhouse, "write_symbol_states", lambda _c, rows: ch.states.extend(rows))


class Terminal:
    class mt5:
        @staticmethod
        def symbol_info(_raw):
            return None


OPTIONS = {
    "start_date": START, "chunk_days": 30, "history_retries": 3,
    "repair_lookback_days": 7, "repair_max_days": 31, "repair_min_age_hours": 2,
    "backfill_pause_seconds": 0,
}


class Pipe:
    def send(self, _):
        pass


def _frozen(moment):
    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return moment

    return Frozen


# --- catch-up and the 5-day outage -----------------------------------------------------------


def test_catch_up_retries_when_hours_are_missing_in_the_middle(monkeypatch):
    # Not only a late start: the answer has the edges of the gap but not its middle.
    last = NOW - timedelta(days=2)
    full = bars(last + timedelta(minutes=1), NOW)
    holey = [c for c in full if not (NOW - timedelta(hours=30) <= c[0] < NOW - timedelta(hours=20))]
    answers = [holey, full]
    market = Market({"US500": full})
    market.m1 = lambda raw, a, b: [c for c in (answers.pop(0) if len(answers) > 1 else answers[0]) if a <= c[0] < b]
    ch = FakeCH()
    ch.marks["US500"] = last
    wire(monkeypatch, market, ch)
    written = worker._sync_symbol(Terminal(), None, ServerClock(), info("US500"), dict(ch.marks), OPTIONS)
    assert written == len(full)


def test_dead_feed_is_not_waited_on(monkeypatch):
    last = datetime(2026, 5, 15, 14, 15, tzinfo=UTC)
    market = Market({"UK100": []})
    ch = FakeCH()
    ch.marks["UK100"] = last
    wire(monkeypatch, market, ch, quote=lambda raw: last)
    written = worker._sync_symbol(Terminal(), None, ServerClock(), info("UK100"), dict(ch.marks), OPTIONS)
    assert written == 0
    assert market.calls == 0


@pytest.mark.parametrize("outage_days", [5, 20])
def test_outage_heals_without_repair_since(monkeypatch, outage_days):
    """Worker down 5 days. On restart the terminal keeps answering with only the last day
    (it has not downloaded the rest yet), so the catch-up writes a partial series and the
    watermark jumps. The next repair starts from the verified cursor left before the outage,
    reads the full history the terminal has by then, and fills the hole."""
    outage_start = NOW - timedelta(days=outage_days, hours=1)
    full = bars(NOW - timedelta(days=outage_days + 7), NOW)
    before = [c for c in full if c[0] < outage_start]
    ch = FakeCH()
    for c in before:
        ch.rows[("AUDJPY", c[0])] = ("AUDJPY",) + c
    ch.marks["AUDJPY"] = before[-1][0]
    ch.cursors["AUDJPY"] = outage_start - timedelta(hours=2)  # verified before the outage

    market = Market({"AUDJPY": full}, local_from={"AUDJPY": NOW - timedelta(days=1)}, lag_calls=10**6)
    wire(monkeypatch, market, ch)
    worker._sync_symbol(Terminal(), None, ServerClock(), info("AUDJPY"), dict(ch.marks), OPTIONS)
    hole = [c[0] for c in full if outage_start <= c[0] < NOW - timedelta(days=1)]
    assert ch.marks["AUDJPY"] == full[-1][0]  # watermark jumped
    assert not any(("AUDJPY", ts) in ch.rows for ts in hole[:10])  # the hole is there

    # Six hours later the terminal has the whole history locally.
    later = NOW + timedelta(hours=6)
    market.lag_calls = 0
    wire(monkeypatch, market, ch, now=later)
    catalog = {"AUDJPY": info("AUDJPY")}
    states = {"AUDJPY": SymbolState("AUDJPY", ACTIVE, BACKFILL_DONE)}
    worker._run_repair(Terminal(), None, ServerClock(), Pipe(), "candles", OPTIONS, catalog, states, None)

    assert all(("AUDJPY", ts) in ch.rows for ts in hole)
    assert ch.cursors["AUDJPY"] > NOW - timedelta(days=1)


def test_repair_does_not_advance_cursor_over_a_hole_it_could_not_fill(monkeypatch):
    full = bars(NOW - timedelta(days=10), NOW)
    ch = FakeCH()
    ch.marks["AUDJPY"] = full[-1][0]
    cursor = NOW - timedelta(days=9)
    ch.cursors["AUDJPY"] = cursor
    # Terminal never gets past the last 2 days: hours before that stay missing in every answer.
    market = Market({"AUDJPY": full}, local_from={"AUDJPY": NOW - timedelta(days=2)}, lag_calls=10**6)
    wire(monkeypatch, market, ch)
    catalog = {"AUDJPY": info("AUDJPY")}
    states = {"AUDJPY": SymbolState("AUDJPY", ACTIVE, BACKFILL_DONE)}
    worker._run_repair(Terminal(), None, ServerClock(), Pipe(), "candles", OPTIONS, catalog, states, None)
    assert ch.cursors["AUDJPY"] == cursor


def test_repair_window_rules():
    last = NOW
    assert worker.repair_window(NOW, last, None, OPTIONS) == (NOW - timedelta(days=7), NOW)
    old = NOW - timedelta(days=20)
    assert worker.repair_window(NOW, last, old, OPTIONS) == (old, NOW)
    very_old = NOW - timedelta(days=90)
    assert worker.repair_window(NOW, last, very_old, OPTIONS) == (very_old, very_old + timedelta(days=31))
    since = datetime(2026, 9, 1, tzinfo=UTC)
    assert worker.repair_window(NOW, last, None, OPTIONS, since)[0] == since
    dead = datetime(2026, 5, 15, tzinfo=UTC)
    assert worker.repair_window(NOW, dead, None, OPTIONS) is None


# --- backfill of new symbols -----------------------------------------------------------------


def test_backfill_starts_at_first_bar_runs_in_budget_and_resumes(monkeypatch):
    listed = datetime(2025, 6, 2, tzinfo=UTC)
    full = bars(listed, NOW, step=timedelta(minutes=15))
    market = Market({"COPPER": full})
    ch = FakeCH()
    wire(monkeypatch, market, ch)
    clock_ticks = iter(range(10**6))
    monkeypatch.setattr(worker.time, "monotonic", lambda: next(clock_ticks))

    first = worker._backfill_symbol(Terminal(), None, ServerClock(), info("COPPER"), {}, OPTIONS, deadline=4)
    assert first is False
    got = ch.minutes("COPPER")
    assert got and got[0] == listed  # did not walk 2020..2025 month by month
    assert got[-1] < NOW - timedelta(days=60)

    monkeypatch.setattr(worker.time, "monotonic", lambda: 0)
    second = worker._backfill_symbol(
        Terminal(), None, ServerClock(), info("COPPER"), dict(ch.marks), OPTIONS, deadline=1
    )
    assert second is True
    assert ch.minutes("COPPER") == [c[0] for c in full]


def test_backfill_waits_while_terminal_history_is_still_loading(monkeypatch):
    market = Market({"NEW": bars(NOW - timedelta(days=3), NOW)})
    ch = FakeCH()
    wire(monkeypatch, market, ch)
    monkeypatch.setattr(worker, "sync_history", lambda *a, **k: worker.HistorySync([NOW], False))
    monkeypatch.setattr(worker.time, "monotonic", lambda: 0)
    assert worker._backfill_symbol(Terminal(), None, ServerClock(), info("NEW"), {}, OPTIONS, deadline=1) is False
    assert ch.minutes("NEW") == []


def test_regular_pass_skips_queued_and_backfill_marks_done(monkeypatch):
    market = Market({
        "EURUSD": bars(NOW - timedelta(hours=1), NOW),
        "COPPER": bars(NOW - timedelta(days=3), NOW),
    })
    ch = FakeCH()
    ch.marks["EURUSD"] = NOW - timedelta(minutes=61)
    wire(monkeypatch, market, ch)
    monkeypatch.setattr(worker.time, "monotonic", lambda: 0)
    catalog = {"EURUSD": info("EURUSD"), "COPPER": info("COPPER", "Metals\\")}
    states = {
        "EURUSD": SymbolState("EURUSD", ACTIVE, BACKFILL_DONE),
        "COPPER": SymbolState("COPPER", ACTIVE, BACKFILL_PENDING),
    }
    worker._run_pass(Terminal(), None, ServerClock(), Pipe(), "candles", OPTIONS, catalog, states)
    assert ch.minutes("COPPER") == []
    assert len(ch.minutes("EURUSD")) == 60

    worker._run_backfill(Terminal(), None, ServerClock(), Pipe(), "candles", OPTIONS, catalog, states, deadline=1)
    assert states["COPPER"].backfill == BACKFILL_DONE
    assert ch.states[-1].symbol == "COPPER" and ch.states[-1].backfill == BACKFILL_DONE
    assert len(ch.minutes("COPPER")) == len(market.series["COPPER"])


def test_missing_hours_ignores_partial_edge_hours():
    start = NOW.replace(minute=31)
    end = NOW + timedelta(hours=3, minutes=10)
    reference = [NOW, NOW + timedelta(hours=1), NOW + timedelta(hours=2), NOW + timedelta(hours=3)]
    candles = bars(NOW + timedelta(hours=1), NOW + timedelta(hours=2))
    assert worker.missing_hours(candles, reference, start, end) == [NOW + timedelta(hours=2)]


# --- terminal helpers and the catalogue refresh ----------------------------------------------


def test_sync_history_waits_until_two_answers_agree(monkeypatch):
    import mt5.fetch as fetch

    answers = [[], [{"time": 1}], [{"time": 1}, {"time": 3601}], [{"time": 1}, {"time": 3601}]]

    class T:
        class mt5:
            @staticmethod
            def copy_rates_range(*_):
                return answers.pop(0)

            @staticmethod
            def last_error():
                return (1, "Success")

    monkeypatch.setattr(fetch.time, "sleep", lambda *_: None)
    synced = fetch.sync_history(T(), "X", NOW - timedelta(days=1), NOW, ServerClock(), attempts=6)
    assert synced.stable and len(synced.times) == 2
    answers[:] = [[], [{"time": 1}], [{"time": 1}, {"time": 3601}]]
    assert not fetch.sync_history(T(), "X", NOW - timedelta(days=1), NOW, ServerClock(), attempts=3).stable


def test_refresh_catalog_writes_states_and_respects_extra_switch(monkeypatch):
    found = [
        info("EURUSD"), info("COPPER", "Metals\\"),
        info("TSLA", "Nasdaq\\Stock\\"), info("AAPL", "Nasdaq\\Stock\\"),
    ]
    asked = {}

    def list_symbols(_t, _suffixes, prefixes):
        asked["prefixes"] = prefixes
        return [f for f in found if not prefixes or any(f.path.startswith(p) for p in prefixes)]

    ch = FakeCH()
    ch.marks["EURUSD"] = NOW
    wire(monkeypatch, Market({}), ch)
    monkeypatch.setattr(worker, "list_symbols", list_symbols)
    monkeypatch.setattr(worker.clickhouse, "upsert_symbols", lambda _c, rows: len(list(rows)))
    monkeypatch.setattr(worker.clickhouse, "symbol_states", lambda _c: {})

    options = {"suffixes": [], "path_prefixes": ["Forex", "Metals"], "extra_path_prefixes": ["Nasdaq\\Stock"]}
    catalog, states = worker._refresh_catalog(Terminal(), None, {**options, "extra_max": 0})
    assert sorted(catalog) == ["COPPER", "EURUSD"]
    assert asked["prefixes"] == ["Forex", "Metals"]
    assert states["EURUSD"].ready and states["COPPER"].queued

    catalog, _ = worker._refresh_catalog(
        Terminal(), None, {**options, "extra_max": 5, "extra_allowlist": ["TSLA"]}
    )
    assert sorted(catalog) == ["COPPER", "EURUSD", "TSLA"]
