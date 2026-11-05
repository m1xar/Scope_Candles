from symbols.resolver import canonical, resolve_symbol
from tests.conftest import CATALOG

KNOWN = set(CATALOG)


def resolve(raw):
    return resolve_symbol(raw, KNOWN.__contains__)


def test_exact_wins():
    r = resolve("EURUSD.pro")
    assert (r.symbol, r.matched_by) == ("EURUSD", "exact")
    assert resolve("USDRMB").symbol == "USDRMB"


def test_alias():
    r = resolve("GER40.cash")
    assert (r.key, r.symbol, r.matched_by) == ("GER40", "DE40", "alias")


def test_minis_are_not_cash():
    assert resolve("US30M").symbol == "US30M"
    assert resolve("US30m").symbol == "US30"
    assert resolve("NQ") is None
    assert resolve("FDAX") is None


def test_unknown_and_empty():
    assert resolve("Volatility 75 Index") is None
    assert resolve("BTCUSDT") is None
    assert resolve("") is None


def test_alias_to_missing_series_is_404():
    assert resolve_symbol("GER40", {"EURUSD"}.__contains__) is None


def test_canonical_for_prices():
    assert canonical("GER40.cash") == "DE40"
    assert canonical("eurusd") == "EURUSD"
