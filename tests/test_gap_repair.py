from datetime import datetime, timedelta, timezone

import workers.candles_worker as worker
from mt5.clock import ServerClock
from symbols.normalize import SymbolInfo

UTC = timezone.utc
T0 = datetime(2026, 9, 25, 20, 0, tzinfo=UTC)


def bar(moment):
    return (moment, 1.0, 1.0, 1.0, 1.0, 1, 0, 0)


def minutes(start, count):
    return [bar(start + timedelta(minutes=i)) for i in range(count)]


class FakeCH:
    def __init__(self, have=()):
        self.rows = {row for row in have}
        self.marks = {}

    def insert(self, rows):
        for row in rows:
            self.rows.add(row[1])
        return len(rows)


def patch(monkeypatch, ch, answers):
    calls = []

    def copy_rates(_t, raw, start, end, _clock, retries=1):
        calls.append((start, end))
        answer = answers.pop(0) if len(answers) > 1 else answers[0]
        return [c for c in answer if start <= c[0] < end]

    monkeypatch.setattr(worker, "copy_rates", copy_rates)
    monkeypatch.setattr(worker, "select", lambda *_: True)
    monkeypatch.setattr(worker.time, "sleep", lambda *_: None)
    monkeypatch.setattr(worker.clickhouse, "insert_candles", lambda _c, rows: ch.insert(rows))
    monkeypatch.setattr(worker.clickhouse, "set_watermark", lambda _c, s, ts, n: ch.marks.__setitem__(s, ts))
    monkeypatch.setattr(
        worker.clickhouse, "existing_minutes",
        lambda _c, s, a, b: {m for m in ch.rows if a <= m < b},
    )
    return calls


class FakeTerminal:
    class mt5:
        @staticmethod
        def symbol_info(_raw):
            return None


OPTIONS = {"start_date": datetime(2020, 1, 1, tzinfo=UTC), "chunk_days": 30, "history_retries": 3}
INFO = SymbolInfo(symbol="US500", raw_symbol="US500")


def test_catch_up_waits_for_terminal_history(monkeypatch, freezer=None):
    # After a 5-day outage the terminal first answers with only the bars since it reconnected.
    late = minutes(T0 + timedelta(days=5), 60)
    full = minutes(T0, 60) + late
    ch = FakeCH()
    patch(monkeypatch, ch, [late, full])
    monkeypatch.setattr(worker, "datetime", _frozen(T0 + timedelta(days=5, hours=1)))
    written = worker._sync_symbol(
        FakeTerminal(), None, ServerClock(), INFO, {"US500": T0 - timedelta(minutes=1)}, OPTIONS
    )
    assert written == 120
    assert min(ch.rows) == T0


def test_repair_inserts_only_missing_minutes(monkeypatch):
    source = minutes(T0, 600)
    have = {c[0] for c in source[:100]} | {c[0] for c in source[400:]}
    ch = FakeCH(have)
    patch(monkeypatch, ch, [source])
    filled = worker._repair_symbol(
        FakeTerminal(), None, ServerClock(), INFO, T0, T0 + timedelta(hours=10), OPTIONS
    )
    assert filled == 300
    assert len(ch.rows) == 600


def test_repair_is_idempotent(monkeypatch):
    source = minutes(T0, 120)
    ch = FakeCH({c[0] for c in source})
    patch(monkeypatch, ch, [source])
    assert worker._repair_symbol(FakeTerminal(), None, ServerClock(), INFO, T0, T0 + timedelta(hours=2), OPTIONS) == 0


def test_steady_state_does_not_wait(monkeypatch):
    now = T0 + timedelta(minutes=15)
    ch = FakeCH()
    calls = patch(monkeypatch, ch, [minutes(T0, 15)])
    monkeypatch.setattr(worker, "datetime", _frozen(now))
    worker._sync_symbol(FakeTerminal(), None, ServerClock(), INFO, {"US500": T0 - timedelta(minutes=1)}, OPTIONS)
    assert len(calls) == 1


def _frozen(moment):
    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return moment

    return Frozen
