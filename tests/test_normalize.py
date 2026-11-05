import pytest

from symbols.normalize import DEFAULT_SUFFIXES, normalize
from tests.conftest import CATALOG
from utils.config import Settings


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("EURUSD", "EURUSD"),
        ("eurusd", "EURUSD"),
        ("EURUSD.pro", "EURUSD"),
        ("EUR/USD", "EURUSD"),
        ("XAUUSD_i", "XAUUSD"),
        ("XAUUSD+", "XAUUSD"),
        ("XAUUSDm", "XAUUSD"),
        ("XAUUSDb", "XAUUSD"),
        ("GOLD.i#", "GOLD"),
        ("EURAUDmicro", "EURAUD"),
        ("GER40.cash", "GER40"),
        ("GER40Cash", "GER40"),
        (".DE40Cash", "DE40"),
        (".USTECHCash", "USTECH"),
        ("US500.b", "US500"),
        ("DE30.cft", "DE30"),
        ("Germany 40", "GERMANY40"),
        ("  germany-40 ", "GERMANY40"),
        ("S&P500", "SP500"),
        ("NASDAQ-100", "NASDAQ100"),
        ("US Tech 100", "USTECH100"),
        ("USTECH100m", "USTECH100"),
        ("#AAPL", "AAPL"),
    ],
)
def test_normalize(raw, expected):
    assert normalize(raw) == expected


@pytest.mark.parametrize(
    "a, b",
    [
        ("Boom 1000 Index", "Boom 300 Index"),
        ("AUDUSD DFX 10 Index", "AUDUSD"),
        ("BOOM_100", "BOOM_200"),
        ("US30M", "US30"),
        ("USTECH100M", "USTECH100"),
        ("FDAX.Dec24", "FDAX"),
    ],
)
def test_distinct_instruments_stay_distinct(a, b):
    assert normalize(a) != normalize(b)


def test_uppercase_m_is_part_of_the_name():
    assert normalize("US30M") == "US30M"
    assert normalize("US500M") == "US500M"
    assert normalize("US30m") == "US30"


def test_empty_suffix_list_means_defaults():
    assert normalize("XAUUSDm", []) == "XAUUSD"
    assert normalize("XAUUSDm", None) == "XAUUSD"
    assert normalize("XAUUSDm", ["pro"]) == "XAUUSDM"


def test_settings_empty_suffixes_fall_back_to_defaults():
    assert Settings(symbol_suffixes="").suffixes == list(DEFAULT_SUFFIXES)
    assert Settings(symbol_suffixes=" , ").suffixes == list(DEFAULT_SUFFIXES)
    assert Settings(symbol_suffixes="pro, ecn").suffixes == ["pro", "ecn"]


def test_catalog_keys_do_not_move():
    # Every series already stored in ClickHouse keeps its key after the change.
    assert len(CATALOG) == 166
    for raw in CATALOG:
        assert normalize(raw) == raw, raw


def test_empty_and_garbage():
    assert normalize("") == ""
    assert normalize("   ") == ""
    assert normalize("...#") == ""
