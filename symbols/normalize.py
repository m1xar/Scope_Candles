from __future__ import annotations

from dataclasses import dataclass

SEPARATORS = frozenset(r'._-#/\+!*^~,:;|()[]{}')

DEFAULT_SUFFIXES = (
    "micro", "cash", "Cash", "pro", "ecn", "raw", "std", "sc", "cent", "m", "c", "z", "e", "b",
)

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


def strip_leading(raw: str) -> str:
    for index, char in enumerate(raw):
        if char.isalnum():
            return raw[index:]
    return ""


def compact(raw: str) -> str:
    return "".join(char for char in raw if char.isascii() and char.isalnum())


def normalize(raw: str, suffixes: tuple[str, ...] | list[str] | None = None) -> str:
    """The one symbol key used everywhere: catalogue writes, sync state and every lookup.

    Broker decoration is cut first, while case still tells a broker suffix (``m`` in
    ``XAUUSDm``) from part of the name (``M`` in ``US30M``). Then every space and special
    character is dropped and the rest is uppercased, so ``Germany 40``, ``germany40`` and
    ``GERMANY-40`` all give ``GERMANY40``. A slash joins the legs of a pair (``EUR/USD``). An empty suffix list means the defaults.
    """
    text = strip_leading(" ".join((raw or "").replace("/", "").split()))
    if not text:
        return ""
    text = strip_separator(text)
    text = strip_suffix(text, suffixes or DEFAULT_SUFFIXES)
    return compact(text).upper()


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
