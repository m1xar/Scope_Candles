"""Daily reconciliation of the broker catalogue with what we monitor.

Pure logic, no terminal and no ClickHouse: the worker feeds it what ``symbols_get`` returned and
what ``symbol_state`` already holds, and writes back the rows it returns. That keeps the rules
(what counts as new, as gone, as back) testable without either system.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Iterable

from .normalize import SymbolInfo

ACTIVE = "active"
DELISTED = "delisted"

BACKFILL_PENDING = "pending"
BACKFILL_DONE = "done"


@dataclass(frozen=True, slots=True)
class SymbolState:
    symbol: str
    status: str = ACTIVE
    backfill: str = BACKFILL_PENDING
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    delisted_at: datetime | None = None

    @property
    def ready(self) -> bool:
        """In the regular last-to-now pass."""
        return self.status == ACTIVE and self.backfill == BACKFILL_DONE

    @property
    def queued(self) -> bool:
        """Waiting for (or part way through) its history backfill."""
        return self.status == ACTIVE and self.backfill != BACKFILL_DONE


@dataclass(frozen=True, slots=True)
class CatalogChange:
    added: list[str]
    delisted: list[str]
    returned: list[str]
    delisting_skipped: bool
    rows: list[SymbolState]


def limit_extra(
    found: Iterable[SymbolInfo],
    extra_prefixes: list[str],
    allowlist: list[str],
    max_extra: int,
) -> list[SymbolInfo]:
    """Keep the base groups as they are and let only a bounded slice of the extra groups
    (stocks, ETFs) through. A demo broker lists ~12 000 of those; taking them all would turn
    one pass into hours and the backfill into weeks."""
    allowed = {item.upper() for item in allowlist}
    kept: list[SymbolInfo] = []
    extra: list[SymbolInfo] = []
    for info in found:
        if extra_prefixes and any(info.path.startswith(prefix) for prefix in extra_prefixes):
            if max_extra <= 0:
                continue
            if allowed and info.symbol not in allowed and info.raw_symbol.upper() not in allowed:
                continue
            extra.append(info)
        else:
            kept.append(info)
    extra.sort(key=lambda info: info.symbol)
    return kept + extra[: max(max_extra, 0)]


def reconcile(
    known: dict[str, SymbolState],
    found: dict[str, SymbolInfo],
    has_history: set[str],
    now: datetime,
    *,
    min_ratio: float = 0.5,
) -> CatalogChange:
    """Compare today's broker list with the monitored set.

    - new on the broker -> active; backfill ``done`` if candles already exist (first run after
      deploy, or a symbol that came back), otherwise ``pending``;
    - missing from the broker -> ``delisted`` (kept, never deleted);
    - back on the broker -> ``active`` again, its backfill state untouched;
    - an answer that shrank below ``min_ratio`` of the active set is treated as a terminal
      glitch: nothing gets delisted on it.
    """
    rows: list[SymbolState] = []
    added: list[str] = []
    returned: list[str] = []
    for symbol in sorted(found):
        state = known.get(symbol)
        if state is None:
            backfill = BACKFILL_DONE if symbol in has_history else BACKFILL_PENDING
            rows.append(SymbolState(symbol, ACTIVE, backfill, now, now, None))
            added.append(symbol)
        elif state.status != ACTIVE:
            rows.append(replace(state, status=ACTIVE, last_seen_at=now, delisted_at=None))
            returned.append(symbol)
        else:
            rows.append(replace(state, last_seen_at=now))

    active = [symbol for symbol, state in known.items() if state.status == ACTIVE]
    missing = sorted(symbol for symbol in active if symbol not in found)
    skipped = bool(missing) and len(found) < min_ratio * len(active)
    delisted: list[str] = []
    if not skipped:
        for symbol in missing:
            rows.append(replace(known[symbol], status=DELISTED, delisted_at=now))
            delisted.append(symbol)
    return CatalogChange(added, delisted, returned, skipped, rows)
