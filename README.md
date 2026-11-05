# Scope Candles

Market data from a single MetaTrader 5 broker: minute candles in ClickHouse, live prices in Redis, both served over HTTP with a WebSocket for streaming quotes.

Two things happen continuously:

1. A backfill job pulls M1 candles for every symbol on the broker, from a fixed start date up to now, and upserts them into ClickHouse. It runs at startup and every 15 minutes after that.
2. A price job reads the current tick for every symbol and writes it into Redis, publishing each change so WebSocket clients see it immediately.

Only M1 is stored. Every other timeframe is aggregated by ClickHouse at read time, so there is one source of truth and nothing can drift out of sync.

---

## Why it looks like this

**MT5 only speaks through a logged-in terminal.** Symbols, candles and ticks all belong to a broker account, so the service pins one account and reads the whole broker catalogue through it.

**The `MetaTrader5` package is a process-global singleton.** One terminal per OS process, no exceptions. The backfill job and the price job therefore run as two separate child processes, each owning its own terminal installation, supervised by the API process:

```
main process (FastAPI/uvicorn)                     reads only
  |-- supervisor: spawns, watches, restarts ------------+
  |-- HTTP + WS routes --- ClickHouse (candles) --------|
  |                    --- Redis (live prices) ---------|
                                                        |
child "candles"  -> terminal C:\MT5\c1 ------------------|  writes ClickHouse
child "price"    -> terminal C:\MT5\c2 ------------------+  writes Redis
```

Children write to storage themselves. Millions of candle rows must not cross a `multiprocessing.Pipe`; the pipe carries heartbeats and nothing else. Both children log into the same MT5 account on two separate installations, which MT5 allows.

**Backfill passes cannot overlap.** The candles child is one sequential loop, so a long initial sync simply delays the next pass rather than stacking a second one behind it. Its heartbeat reports the symbol it is on and how many are left, so a multi-hour first sync is visible in `/status` instead of looking hung.

---

## Time

**MT5 hands back broker-local time, not UTC**, and the offset changes with daylight saving. Candles are stored in real UTC, so the offset has to be reconstructed rather than assumed.

The service derives it by scanning H1 bars over the whole backfill range and locating the weekend gaps: every gap of 24 hours or more is a market close, and the broker's label for a close that really happened at 17:00 New York time gives the offset directly. Two later weekends have to agree before a switch is accepted, which keeps holidays out of the result.

Measured on a real EET/EEST broker (41 781 H1 bars, 348 weekends, 2019-12 to 2026-09) it recovers all fourteen DST steps:

| Period | Offset |
|---|---|
| winter (early Nov to mid Mar) | **UTC+2** |
| summer (mid Mar to early Nov) | **UTC+3** |

Switch dates found: 2020-03-09, 2020-11-02, 2021-03-15, 2021-11-08, 2022-03-14, 2022-11-07, 2023-03-13, 2023-11-06, 2024-03-11, 2024-11-04, 2025-03-10, 2025-11-03, 2026-03-09. Note these are broker week boundaries, not the exact civil DST dates, because a broker only changes its clock over a weekend.

Verified end to end: a bar the terminal labels `2026-09-04 03:00:00` is stored as `2026-09-04T00:00:00Z` with identical OHLC, and a cross-check of 84 real trade fills against their covering minute bars put 83 inside the bar's low-high range (the one outlier was 0.11 outside on gold, i.e. spread).

The measured offset steps are logged at startup under `clock.resolved`.

---

## Symbol normalization

Brokers decorate the same instrument differently: `XAUUSD`, `XAUUSD.r`, `XAUUSD_i`, `XAUUSDm`. Symbols are normalized on write and on read, so a client asks for `XAUUSD` and gets an answer regardless of which broker backs the store.

The rule is deliberately small, and it is shaped by what real broker catalogues actually contain:

1. Collapse whitespace, drop `/` (`EUR/USD`) and any leading non-alphanumerics (`.DE40Cash`, `#AAPL`).
2. **Cut at the first separator** (`. _ - # \ + ! * ^ ~ , : ; | ( ) [ ] { }`) — but only when the tail is at most 4 characters and contains no digit. `EURUSD.pro` -> `EURUSD`, `XAUUSD_i` -> `XAUUSD`, `BTCUSD#` -> `BTCUSD`.
3. **Strip a glued suffix** (`micro`, `cash`, `Cash`, `pro`, `ecn`, `raw`, `std`, `sc`, `cent`, `m`, `c`, `z`, `e`, `b`) when the character before it is uppercase or a digit. Case matters here: `XAUUSDm` -> `XAUUSD`, but `US30M` (a mini contract) stays `US30M`.
4. Drop every remaining space and special character, then uppercase. `Germany 40` -> `GERMANY40`, `S&P500` -> `SP500`.

The same function builds the catalogue key when the worker writes and the lookup key when `/candles/{symbol}` or `/price/{symbol}` is called, so a symbol is always found under the key it was stored with. An empty `MT5_CANDLES_SYMBOL_SUFFIXES` means the default list above.

### Aliases

`symbols/aliases.json` maps other names of the same cash/spot instrument onto a catalogue symbol (`GER40`, `DAX40`, `GERMANY40` -> `DE40`; `US100`, `NAS100`, `NDX100` -> `USTEC`; `GOLD` -> `XAUUSD`). Lookup order: the normalized key is a catalogue symbol (`exact`), else it is an alias of one (`alias`), else 404. The response says which (`matched_by`) and echoes `requested`. Futures, minis and dated contracts (`NQ`, `ES`, `FDAX`, `US30M`, `FUTMNQZ23`) are never aliases of cash; `tests/test_aliases.py` enforces that, and that no alias shadows a real catalogue name.

Two guards matter, and both come from real data:

- **Space is not a separator.** It is removed, never cut on: cutting would turn `Boom 1000 Index`, `Boom 300 Index` and `Boom 50 Index` into one symbol, and would collapse `AUDUSD DFX 10 Index` onto the real `AUDUSD`.
- **A tail containing digits is not a suffix.** It is part of the instrument name: `BOOM_100` and `BOOM_200` stay distinct, and `Si-9.24` keeps its contract month.

One case the rule cannot get right is share classes: a broker that lists `AGM-A` and `ABR-PD` as separate stock CFDs will see both folded onto their base ticker, because nothing distinguishes a class suffix from a broker suffix. Keep stock groups out of `SYMBOL_INCLUDE_PATH_PREFIXES` unless you need them.

When two raw symbols on the same broker normalize to the same key, the plain form wins (then the shortest, then alphabetical); the rest are skipped and logged once as `symbols.collision`. Both forms are kept in the `symbols` table, so `raw_symbol` always says which broker feed a series came from.

Override the suffix list with `MT5_CANDLES_SYMBOL_SUFFIXES` (comma separated) if a broker needs something else.

---

## Storage

Three ClickHouse tables, created at startup with `CREATE TABLE IF NOT EXISTS`. There is no migration tool; a schema change means dropping the volume.

```sql
CREATE TABLE candles (
    symbol      LowCardinality(String),
    ts          DateTime('UTC'),
    open Float64, high Float64, low Float64, close Float64,
    tick_volume UInt64, real_volume UInt64, spread UInt32,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts);
```

Upsert is a plain insert. `ReplacingMergeTree` collapses duplicate `(symbol, ts)` pairs and keeps the newest `ingested_at`, so re-syncing an overlapping window corrects the old rows instead of doubling them; reads use `FINAL`.

`symbols` holds the normalized catalogue. `sync_state` holds one watermark per symbol, which is what makes a restart resume rather than start over — a pass reads every watermark in a single query instead of scanning `max(ts)` across billions of rows, falling back to that scan only for a symbol it has never seen.

Watermarks are read with `FINAL`, so an operator can deliberately rewind one and have the next pass refill a gap:

```sql
DELETE FROM candles WHERE symbol = 'XAUUSD' AND ts >= '2026-09-04 20:00:00' AND ts < '2026-09-04 21:00:00';
INSERT INTO sync_state (symbol, last_ts, last_run_at, rows_written)
VALUES ('XAUUSD', '2026-09-04 19:59:00', now64(3), 0);
```

Redis holds only what is disposable: `price:{SYMBOL}` (the latest quote, 5 minute TTL) and the `px:{SYMBOL}` pub/sub channel. Losing it costs nothing.

---

### Catalogue, backfill and holes

The worker does four things, in this order, every `BACKFILL_INTERVAL_MINUTES`:

1. **Catalogue refresh** (at start and every `CATALOG_REFRESH_HOURS`, default 24). `symbols_get` over the allowed groups, compared with `symbol_state`: a new symbol becomes `active` with backfill `pending` (or `done` if it already has candles, which is the case for every symbol on the first run after deploy); a symbol the broker no longer lists becomes `delisted` (kept with its data, never deleted) and comes back to `active` if it reappears. An answer smaller than `CATALOG_MIN_RATIO` of the active set is taken as a terminal glitch and delists nothing. Stock/ETF groups are opt-in: `SYMBOL_EXTRA_PATH_PREFIXES` plus `SYMBOL_EXTRA_ALLOWLIST`, capped by `SYMBOL_EXTRA_MAX` (0 = off).
2. **Regular pass**, last candle to now, for symbols whose backfill is done. When it is behind by more than two hours (`copy_rates_range` answers from the terminal's local history and does not wait for the server) it asks each window again up to `SYMBOL_HISTORY_RETRIES` times, 2 s apart, stops once an answer repeats the previous one, and keeps the fullest (`candles.symbol.late_start` if that still starts late). A symbol the broker has not quoted since our last candle is skipped without waiting (`candles.symbol.idle`, once a day).
3. **Backfill of new symbols** within `BACKFILL_BUDGET_SECONDS` (default 240) after each pass, so the regular pass never waits for it. A D1 request over the whole range starts the download and finds the symbol's first bar (asked until two answers agree, so a half-loaded history does not cut off the early years); then 30-day M1 chunks with `BACKFILL_PAUSE_SECONDS` between them. Progress is the ordinary watermark, so it resumes after a restart; when it reaches now the symbol joins the regular pass.
4. **Repair** every `REPAIR_INTERVAL_HOURS` (default 6): re-reads the last `REPAIR_LOOKBACK_DAYS` (default 7) from the terminal and inserts only the minutes ClickHouse does not have (`candles.repair.filled`). Set `REPAIR_SINCE=2026-09-01` once to sweep further back after a long outage. `REPAIR_INTERVAL_HOURS=0` turns it off.

Why the pass alone is not enough: `copy_rates_range` answers from the terminal's local history without waiting for the server. Right after a reconnect it can return the edges of a gap without its middle, with no error; the watermark then moves past the hole and a forward-only pass never comes back to it. The catch-up re-asks and the 7-day repair are what closes such holes; one older than the lookback needs `REPAIR_SINCE` once.

---

## API

Every route except `/healthz` needs the shared bearer token from `MT5_CANDLES_API_TOKEN`. The WebSocket also accepts `?token=`, because a browser cannot set headers on a WS handshake. An empty token disables auth entirely, which is only acceptable on a closed network.

| Route | |
|---|---|
| `GET /symbols` | Normalized catalogue with `raw_symbol`, description and ingest watermark. `?search=` filters on symbol or description. |
| `GET /candles/{symbol}` | `?from=` `&to=` `&timeframe=`. Timeframes: `1m 5m 15m 30m 1h 4h 1d 1w`. |
| `GET /price/{symbol}` | Latest quote from Redis. 404 once it goes stale. |
| `WS /ws/price/{symbol}` | Sends the current quote, then every change. A `{"type":"ping"}` every 20 idle seconds keeps the socket open. |
| `GET /status` | Worker state, current pass progress, symbol count, watermark lag. |
| `GET /healthz` | Public. `ok` (200) or `degraded` (503). |

**Freshness.** Candles lag live by the backfill interval, so a window that close to now has no data by construction rather than by accident. With `cutoff = now - 15min`:

- `from >= cutoff` is a **400** that says so, and names the newest usable timestamp.
- `to > cutoff` is silently clamped, and the response echoes the effective `to`.

Symbols are normalized on input, so `/candles/xauusd.r` and `/candles/XAUUSD` are the same request. An unknown symbol is a 404.

A window that would produce more than `MT5_CANDLES_MAX_ROWS` candles is a 400 rather than a truncated series, so a client can never mistake a partial answer for the whole range.

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "http://host:8040/candles/XAUUSD?from=2026-09-04T00:00:00Z&to=2026-09-04T06:00:00Z&timeframe=1h"

curl -H "Authorization: Bearer $TOKEN" "http://host:8040/price/EURUSD"

wscat -c "ws://host:8040/ws/price/EURUSD?token=$TOKEN"
```

Interactive docs at `/docs`.

---

## Running it

Requires Windows (the terminals are Windows processes), Python 3.12 x64, and Docker for ClickHouse and Redis.

**1. The master terminal.** Install MetaTrader 5 once into `C:\MT5\master` and run it with `/portable`, so its data lives beside `terminal64.exe` instead of in `%APPDATA%\MetaQuotes`.

It must already know the broker you intend to use. `initialize()` cannot reach a server the terminal has never heard of — it will simply never answer, and you get an IPC timeout with no other clue. Add the broker under *File > Open an Account*, then **close the terminal**: `Config\servers.dat` is only written on exit.

While you are in there: set *Max bars in chart* to unlimited (otherwise history is capped at roughly 70 days), and turn off news and sounds.

```powershell
.\deploy\open-master.ps1
```

**2. Clone it.** Each worker needs its own installation.

```powershell
.\deploy\clone.ps1 -Names c1,c2
```

This copies the master, wipes the per-instance caches and logs, and prints the two `.env` lines to paste.

**3. Storage.**

```powershell
docker compose up -d
```

**4. The service.**

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
copy .env.example .env      # then fill it in
.\deploy\run.ps1
```

**5. As a service.** `deploy\install-task.ps1` registers a scheduled task that starts at boot and restarts on failure:

```powershell
.\deploy\install-task.ps1
Start-ScheduledTask -TaskName ScopeCandles
```

Open the API port in Windows Firewall if it should be reachable from outside, and set `MT5_CANDLES_API_HOST=0.0.0.0`.

---

## Configuration

Every setting reads `MT5_CANDLES_<NAME>` first and the bare `<NAME>` second. `.env.example` lists them all; the ones worth thinking about:

| | |
|---|---|
| `BACKFILL_START_DATE` | Empty means 2020-01-01. |
| `BACKFILL_INTERVAL_MINUTES` | 15. Also the freshness lag the API enforces — keep `FRESHNESS_LAG_MINUTES` equal to it. |
| `BACKFILL_CHUNK_DAYS` | 30. How much history one `copy_rates_range` call asks for. |
| `SYMBOL_INCLUDE_PATH_PREFIXES` | Empty means every symbol. **Set this.** A broker with thousands of stock CFDs will otherwise make the first sync enormous and slow the tick loop to a crawl; `Forex,Commodities,Indices,Crypto` is a sane default. |
| `SYMBOL_EXTRA_PATH_PREFIXES`, `SYMBOL_EXTRA_ALLOWLIST`, `SYMBOL_EXTRA_MAX` | Stocks/ETFs on top of the base groups. `SYMBOL_EXTRA_MAX=0` (default) keeps them out; with it set, only allowlisted tickers (or the first N alphabetically) are taken. |
| `CATALOG_REFRESH_HOURS` | 24. How often the broker list is reconciled with `symbol_state`. |
| `BACKFILL_BUDGET_SECONDS` | 240. Time per pass given to the history of new symbols. Unlimited while no symbol has finished (fresh install). |
| `SYMBOL_SUFFIXES` | Overrides the glued-suffix list. |
| `TICK_POLL_MS` | 300. One pass over every watched symbol per tick; raise it if the symbol count is large. |
| `PRUNE_CACHE` | `false`. The terminal's `Bases\` directory grows without bound while pulling years of minute history, but pruning forces a re-download on the next pass — leave it off during the initial backfill and turn it on once the service is in steady state. |
| `MAX_ROWS` | 50000. The candle-count ceiling for one request. |

The first backfill of a full broker catalogue from 2020 runs for hours. It is resumable per symbol, so stopping and restarting costs only the chunk that was in flight.
