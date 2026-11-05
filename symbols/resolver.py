from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Callable, Iterable, Mapping

from .normalize import normalize

ALIASES_PATH = Path(__file__).with_name("aliases.json")


@dataclass(frozen=True, slots=True)
class Resolution:
    requested: str
    key: str
    symbol: str
    matched_by: str


def load_aliases(path: Path = ALIASES_PATH) -> dict[str, str]:
    """Alias key -> canonical symbol. Keys and targets go through normalize() so the
    file can be written by hand without worrying about case or punctuation."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    table: dict[str, str] = {}
    for canonical, names in raw["aliases"].items():
        target = normalize(canonical)
        for name in names:
            key = normalize(name)
            if not key or key == target:
                continue
            existing = table.get(key)
            if existing is not None and existing != target:
                raise ValueError(f"alias {name!r} points at both {existing} and {target}")
            table[key] = target
    return table


@lru_cache(maxsize=1)
def default_aliases() -> Mapping[str, str]:
    return load_aliases()


def resolve_symbol(
    requested: str,
    is_known: Callable[[str], bool],
    *,
    suffixes: Iterable[str] | None = None,
    aliases: Mapping[str, str] | None = None,
) -> Resolution | None:
    """Lookup order for /candles/{symbol} and /price/{symbol}:

    1. exact: the normalized key is a catalogue symbol (EURUSD.pro -> EURUSD);
    2. alias: the normalized key is a known alias of a catalogue symbol (GER40.cash -> DE40);
    3. nothing -> None (the route answers 404).

    An exact catalogue name always wins, so an alias can never shadow a real series.
    """
    key = normalize(requested, list(suffixes) if suffixes else None)
    if not key:
        return None
    if is_known(key):
        return Resolution(requested, key, key, "exact")
    table = default_aliases() if aliases is None else aliases
    target = table.get(key)
    if target and is_known(target):
        return Resolution(requested, key, target, "alias")
    return None


def canonical(requested: str, *, suffixes: Iterable[str] | None = None) -> str:
    """Catalogue-free form of the same rule, for Redis lookups: aliases never collide with
    a catalogue symbol (tests/test_aliases.py), so mapping them unconditionally is safe."""
    key = normalize(requested, list(suffixes) if suffixes else None)
    return default_aliases().get(key, key)
