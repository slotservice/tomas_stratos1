"""Tests for direction-extracted-before-override-skip ordering and the
new status detector patterns.

Tomas 2026-05-13:
  1. The "🔒 Skippad av operator-override" notification was showing
     "Riktning: ?" because the parser applied the override-skip
     check BEFORE extracting the direction. ParseResult carried
     no direction, the notifier defaulted to "?".
  2. CoinMaster Trading's status messages ("✅ 730.82% Profit", and
     "#TRUTH/USDT Reached 0.0193410") were not caught by
     is_status_update — they got reparsed as new signals and produced
     a misleading "Blokerad, Entre saknas".
"""

from __future__ import annotations

from pathlib import Path
import pytest

from core import symbol_overrides
from core.signal_parser import (
    parse_signal_detailed,
    is_status_update,
)


@pytest.fixture
def overrides_loaded(tmp_path: Path) -> Path:
    p = tmp_path / "symbol_overrides.toml"
    p.write_text(
        """
[GUA]
skip = true
note = "Not listed on Bybit perp."
""".strip(),
        encoding="utf-8",
    )
    symbol_overrides.load_overrides(p)
    yield p
    symbol_overrides.load_overrides(tmp_path / "nonexistent.toml")  # reset


def test_skipped_by_override_now_carries_direction_long(overrides_loaded):
    """Real signal '#GUA/USDT LONG ...' should now return
    skipped_by_override with direction='LONG' so the notifier renders
    'Riktning: LONG' instead of the confusing 'Riktning: ?'."""
    text = """#GUA/USDT LONG
Entry: 1.2
TP1: 1.3
SL: 1.1"""
    r = parse_signal_detailed(text=text, channel_id=-1, channel_name="Panda")
    assert r.reason == "skipped_by_override"
    assert r.symbol == "GUAUSDT"
    assert r.direction == "LONG"


def test_skipped_by_override_carries_direction_short(overrides_loaded):
    text = """#GUA SHORT
Entry: 1.2
Targets: 1.0
SL: 1.4"""
    r = parse_signal_detailed(text=text, channel_id=-1, channel_name="X")
    assert r.reason == "skipped_by_override"
    assert r.direction == "SHORT"


def test_gua_chatter_no_direction_does_not_fire_override_notif(overrides_loaded):
    """Pure chatter mentioning #GUA but no LONG/SHORT should exit at
    no_direction (silent — chatter stays silent per spec) instead of
    confusingly firing the skipped_by_override notification."""
    r = parse_signal_detailed(
        text="news about #GUA price action",
        channel_id=-1, channel_name="X",
    )
    assert r.reason == "no_direction"


def test_coinmaster_x_pct_profit_without_plus_is_status():
    """The CoinMaster Trading format uses '✅ 730.82% Profit' with no
    leading '+'. Previously failed is_status_update because the regex
    required '+', so the message got re-parsed as a signal."""
    text = (
        "#TRUTH/USDT Reached 0.0193410\n"
        "We hope you have all benefited from this signal\n"
        "✅ 730.82% Profit"
    )
    assert is_status_update(text) is True


def test_plus_prefix_pct_profit_still_status():
    """Existing behaviour must still hold — '+646% GAIN' format."""
    assert is_status_update("Big trade! +646% GAIN") is True


def test_usdt_reached_pattern_catches_completion():
    """'/USDT Reached <price>' alone is a strong completion signal."""
    assert is_status_update("#FOO/USDT Reached 1.2345") is True
    assert is_status_update("#BAR/USDT REACHED 0.5") is True


def test_reached_in_normal_signal_text_does_not_misfire():
    """Defensive: a real entry signal that happens to use 'reached'
    in body text (not as a /USDT-Reached completion phrase) should
    NOT be flagged as status. The pattern requires '/USDT?\\s+reached'
    so plain English commentary is safe."""
    text = """#BTC/USDT LONG
Entry: 65000
Targets:
1) 66000
SL: 64000

(after support reached we can scale up)"""
    assert is_status_update(text) is False


def test_pct_without_profit_word_does_not_misfire():
    """A bare 'Risk: 1.5%' or 'Leverage uses 5%' must NOT match the
    relaxed `\\d+% gain|profit` pattern — the `(gain|profit|in profit)`
    suffix anchor still gates it."""
    assert is_status_update("Risk: 1.5%") is False
    assert is_status_update("Take 0.5% of position") is False
