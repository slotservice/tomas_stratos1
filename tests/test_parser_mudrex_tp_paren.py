"""Tests for parser handling of "Take Profit (TP) N: $price" format.

Tomas 2026-05-13 TAO incident (trade 1705): Mudrex Insights formats
every TP line as `🎯 Take Profit (TP) 1: $302` with a parenthetical
`(TP)` tag between the "Take Profit" keyword and the index. Pattern 1
required the index to follow the keyword with only whitespace in
between, so the regex bailed at `(`. Pattern 3 then captured the
"1" of "TP 1:" as a same-line list value, giving TP1 = 1.0. The
trade opened with an effectively-missing TP1 at $1 (99% below entry).

This file pins the fix: pattern 1 now accepts an optional short
parenthetical between the keyword and the index. It also pins the
critical 2026-05-04 BOB bare-price-list regression — that case must
still NOT match pattern 1, because relaxing too far would let
`Take Profit Target\\n0.0180` capture "0" as index and "0180" as price.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _mudrex_text import MUDREX_TAO  # noqa: E402

from core.signal_parser import parse_signal_detailed


def test_mudrex_tao_short_parses_clean():
    r = parse_signal_detailed(
        text=MUDREX_TAO, channel_id=-1, channel_name="Mudrex Insights",
    )
    assert r.symbol == "TAOUSDT"
    assert r.direction == "SHORT"
    assert r.entry == 310.0
    assert r.tps == [302.0, 295.0]
    assert r.sl == 325.0
    assert r.reason == "ok"


def test_traditional_tp1_format_still_parses():
    """The pattern was unchanged for the common 'TP1: 66000' form
    because the new parenthetical group is optional."""
    text = """#BTC/USDT LONG
Entry: 65000
TP1: 66000
TP2: 67500
SL: 64000"""
    r = parse_signal_detailed(
        text=text, channel_id=-1, channel_name="t",
    )
    assert r.reason == "ok"
    assert r.signal.tps == [66000.0, 67500.0]


def test_target_1_format_still_parses():
    text = """#ETH/USDT LONG
Entry: 3500
Target 1: 3600
Target 2: 3700
SL: 3400"""
    r = parse_signal_detailed(
        text=text, channel_id=-1, channel_name="t",
    )
    assert r.reason == "ok"
    assert r.signal.tps == [3600.0, 3700.0]


def test_bob_bare_price_list_2026_05_04_regression_still_held():
    """The header is "Take Profit Target" with no inline index, and
    the prices are on subsequent lines as bare values. Pattern 1 must
    NOT match here — if it did, the newline-permissive whitespace
    would let it capture "0" of "0.0180" as the index and "0180" as
    the price (the original 2026-05-04 BOB regression)."""
    text = """#BOBUSDT LONG
Entry: 0.0200
Take Profit Target
0.0180
0.0172
0.0156
SL: 0.025"""
    r = parse_signal_detailed(
        text=text, channel_id=-1, channel_name="BOB",
    )
    if r.signal is not None:
        assert 180 not in r.signal.tps
        assert 180.0 not in r.signal.tps


def test_take_profit_paren_lowercase_also_works():
    """Defensive: case-insensitive — '(tp)' lowercase works too."""
    text = """#XYZ/USDT LONG
Entry: 100
Take Profit (tp) 1: 110
Take Profit (tp) 2: 120
Stop Loss (sl): 90"""
    r = parse_signal_detailed(
        text=text, channel_id=-1, channel_name="t",
    )
    assert r.reason == "ok"
    assert r.signal.tps == [110.0, 120.0]
