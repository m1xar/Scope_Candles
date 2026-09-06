from __future__ import annotations

from dataclasses import dataclass

SEPARATORS = frozenset(r'._-#/\+!*^~,:;|()[]{}')

DEFAULT_SUFFIXES = ("micro", "pro", "ecn", "raw", "std", "sc", "cent", "m", "c", "z", "e")

_MAX_TAIL = 4
_MIN_LENGTH = 3


@dataclass(frozen=True, slots=True)
class SymbolInfo:
    symbol: str
    raw_symbol: str
    description: str = ""
    digits: int = 0
    path: str = ""


def strip_separator(raw: str) -> str:
    for index, char in enumerate(raw):
        if char not in SEPARATORS:
            continue
        head, tail = raw[:index], raw[index + 1 :]
        if len(head) < _MIN_LENGTH:
            return raw
        if len(tail) > _MAX_TAIL or any(char.isdigit() for char in tail):
            return raw
        return head
    return raw


def strip_suffix(raw: str, suffixes: tuple[str, ...] | list[str]) -> str:
    for suffix in suffixes:
        if not suffix or not raw.endswith(suffix) or raw == suffix:
            continue
        head = raw[: -len(suffix)]
        if len(head) < _MIN_LENGTH:
            continue
        anchor = head[-1]
        if not (anchor.isupper() or anchor.isdigit()):
            continue
        return head
    return raw


def normalize(raw: str, suffixes: tuple[str, ...] | list[str] | None = None) -> str:
    text = " ".join(raw.split())
    if not text:
        return ""
    text = strip_separator(text)
    text = strip_suffix(text, DEFAULT_SUFFIXES if suffixes is None else suffixes)
    return text.upper()


def _rank(candidate: SymbolInfo) -> tuple[int, int, str]:
    exact = 0 if candidate.raw_symbol.upper() == candidate.symbol else 1
    return exact, len(candidate.raw_symbol), candidate.raw_symbol


def resolve(candidates: list[SymbolInfo]) -> tuple[dict[str, SymbolInfo], list[tuple[str, list[str]]]]:
    grouped: dict[str, list[SymbolInfo]] = {}
    for candidate in candidates:
        if not candidate.symbol:
            continue
        grouped.setdefault(candidate.symbol, []).append(candidate)

    chosen: dict[str, SymbolInfo] = {}
    collisions: list[tuple[str, list[str]]] = []
    for symbol, group in grouped.items():
        group.sort(key=_rank)
        chosen[symbol] = group[0]
        if len(group) > 1:
            collisions.append((symbol, [item.raw_symbol for item in group[1:]]))
    return chosen, collisions
