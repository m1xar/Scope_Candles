import json
import re

import pytest

from symbols.normalize import normalize
from symbols.resolver import ALIASES_PATH, load_aliases
from tests.conftest import CATALOG

# A different contract is a different instrument: never an alias of the cash/spot series.
FUTURES_AND_MINIS = {
    "NQ", "MNQ", "ES", "MES", "YM", "MYM", "RTY", "M2K", "FDAX", "FDXM", "FDXS", "FESX",
    "NKD", "GC", "MGC", "SI", "CL", "QM", "BRN", "US30M", "US500M", "USTECH100M",
}
# Dated contracts as brokers spell them: FDAXDEC24, USDXDEC24, GERMANY40MAR25.
MONTH_NAME = re.compile(r"(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2,4}$")

RAW = json.loads(ALIASES_PATH.read_text())["aliases"]
TABLE = load_aliases()


def test_targets_are_catalog_cash_symbols():
    for canonical in RAW:
        assert canonical in CATALOG, canonical
        assert canonical not in FUTURES_AND_MINIS


def test_aliases_are_in_normal_form():
    for names in RAW.values():
        for name in names:
            assert normalize(name) == name, name


def test_no_alias_shadows_a_catalog_symbol():
    for key in TABLE:
        assert key not in CATALOG, key


def test_no_futures_minis_or_dated_contracts():
    for key in TABLE:
        assert key not in FUTURES_AND_MINIS, key
        assert not key.startswith("FUT"), key
        assert not MONTH_NAME.search(key), key


def test_one_alias_one_target():
    seen = {}
    for canonical, names in RAW.items():
        for name in names:
            assert seen.setdefault(name, canonical) == canonical, name


@pytest.mark.parametrize(
    "raw, canonical",
    [
        ("GER40.cash", "DE40"), ("GER30.b", "DE40"), ("DAX40", "DE40"), ("DAX.c", "DE40"),
        ("Germany 40", "DE40"), ("DE30.cft", "DE40"),
        ("US100.cash", "USTEC"), ("NAS100.r", "USTEC"), (".USTECHCash", "USTEC"),
        ("NDX100", "USTEC"), ("USTECH100m", "USTEC"), ("NASDAQ-100", "USTEC"),
        ("SPX500.cft", "US500"), ("S&P500", "US500"),
        ("DJ30.r", "US30"), ("DJI30", "US30"),
        (".JP225Cash", "JPN225"), ("JAP225.x", "JPN225"), ("Nikkei225.", "JPN225"),
        ("GOLD.i#", "XAUUSD"), ("GOLDmicro", "XAUUSD"), ("SILVERmicro", "XAGUSD"),
        ("EU50.cash", "EUSTX50"), ("FTSE100", "UK100"),
    ],
)
def test_real_broker_names(raw, canonical):
    assert TABLE[normalize(raw)] == canonical


def test_conflicting_file_is_rejected(tmp_path):
    bad = tmp_path / "aliases.json"
    bad.write_text(json.dumps({"aliases": {"DE40": ["DAX"], "US30": ["DAX"]}}))
    with pytest.raises(ValueError):
        load_aliases(bad)
