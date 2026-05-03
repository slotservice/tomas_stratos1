"""Tests for symbol_overrides.toml loader + parser hook.

Tomas (client) 2026-05-03 explicit: operator must be able to skip
non-tradable symbols ("not on Bybit perp"), rename to Bybit's
canonical form ("SHIB -> 1000SHIBUSDT"), and attach notes that show
on warnings ("checked manually 2026-05-03").
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from core import symbol_overrides
from core.signal_parser import parse_signal_detailed


@pytest.fixture(autouse=True)
def _silence_logs():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


@pytest.fixture
def overrides_file(tmp_path: Path) -> Path:
    p = tmp_path / "symbol_overrides.toml"
    p.write_text(
        """
[TAGUSDT]
skip = true
note = "not on Bybit perp"

[SHIB]
rename_to = "1000SHIBUSDT"
note = "Bybit lists 1000SHIB"

[BTCUSDT]
note = "high-conviction"
""".strip(),
        encoding="utf-8",
    )
    return p


def test_load_parses_all_three_keys(overrides_file: Path):
    m = symbol_overrides.load_overrides(overrides_file)
    assert "TAGUSDT" in m
    assert "SHIBUSDT" in m  # bare "SHIB" should be normalised
    assert "BTCUSDT" in m


def test_skip_entry(overrides_file: Path):
    symbol_overrides.load_overrides(overrides_file)
    e = symbol_overrides.get_override("TAGUSDT")
    assert e is not None
    assert e["skip"] is True
    assert e["note"] == "not on Bybit perp"


def test_rename_entry_normalises_to_uppercase(overrides_file: Path):
    symbol_overrides.load_overrides(overrides_file)
    e = symbol_overrides.get_override("SHIBUSDT")
    assert e is not None
    assert e["rename_to"] == "1000SHIBUSDT"
    assert e["skip"] is False


def test_note_only_entry(overrides_file: Path):
    symbol_overrides.load_overrides(overrides_file)
    e = symbol_overrides.get_override("BTCUSDT")
    assert e is not None
    assert e["skip"] is False
    assert e["rename_to"] is None
    assert e["note"] == "high-conviction"


def test_lookup_is_case_insensitive(overrides_file: Path):
    symbol_overrides.load_overrides(overrides_file)
    assert symbol_overrides.get_override("tagusdt") is not None
    assert symbol_overrides.get_override("TAGUSDT") is not None


def test_missing_file_returns_empty(tmp_path: Path):
    m = symbol_overrides.load_overrides(tmp_path / "no_such_file.toml")
    assert m == {}


def test_malformed_file_returns_empty(tmp_path: Path):
    p = tmp_path / "bad.toml"
    p.write_text("this is = not valid toml [[[", encoding="utf-8")
    m = symbol_overrides.load_overrides(p)
    assert m == {}


def test_parser_skip_drops_signal(overrides_file: Path):
    symbol_overrides.load_overrides(overrides_file)
    text = """#TAG/USDT LONG
Entry: 0.001
TP1: 0.0011
SL: 0.0009"""
    r = parse_signal_detailed(text=text, channel_id=-1, channel_name="t")
    assert r.reason == "skipped_by_override"
    assert r.signal is None
    assert r.symbol == "TAGUSDT"
    assert r.override_note == "not on Bybit perp"


def test_parser_rename_replaces_symbol(overrides_file: Path):
    symbol_overrides.load_overrides(overrides_file)
    text = """#SHIB/USDT LONG
Entry: 0.000028
TP1: 0.000031
SL: 0.000026"""
    r = parse_signal_detailed(text=text, channel_id=-1, channel_name="t")
    assert r.reason == "ok"
    assert r.signal is not None
    assert r.symbol == "1000SHIBUSDT"
    assert r.signal.symbol == "1000SHIBUSDT"
    assert r.override_note == "Bybit lists 1000SHIB"


def test_parser_note_only_passes_through(overrides_file: Path):
    symbol_overrides.load_overrides(overrides_file)
    text = """#BTC/USDT LONG
Entry: 65000
TP1: 66000
SL: 64000"""
    r = parse_signal_detailed(text=text, channel_id=-1, channel_name="t")
    assert r.reason == "ok"
    assert r.symbol == "BTCUSDT"
    assert r.override_note == "high-conviction"


def test_no_override_means_no_note(tmp_path: Path):
    # Reset overrides to empty to isolate.
    symbol_overrides.load_overrides(tmp_path / "missing.toml")
    text = """#ETH/USDT LONG
Entry: 3500
TP1: 3550
SL: 3450"""
    r = parse_signal_detailed(text=text, channel_id=-1, channel_name="t")
    assert r.reason == "ok"
    assert r.override_note is None
