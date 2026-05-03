"""Regression: bare-price-per-line TP lists must not be shredded.

2026-05-04 BOB incident (#1000000BOBUSDT):
    Take Profit Target
    0.0180
    0.0172
    0.0156
    0.0142
    SL: 0.0212

was being parsed as TPs=[180.0] because pattern 0 matched "Target\n0"
with idx=0 + price=180 (the parser greedy-consumed "0." as separator
and treated the rest as the price), and pattern 1's outer regex was
loose enough to also collect garbage from the per-line bare prices.

This test locks in the correct behaviour: all four TPs extracted in
order, with no spurious entries from index/price digit confusion.
"""

from __future__ import annotations

import logging

import pytest

from core.signal_parser import parse_signal_detailed


@pytest.fixture(autouse=True)
def _silence_logs():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


def test_bob_signal_extracts_all_tps():
    """1000000BOBUSDT signal — 4 TPs as bare price-per-line under
    'Take Profit Target' header."""
    text = """#1000000BOBUSDT
#SHORT
Leverage cross 10-50 X

Entry: 0.02050- 0.02120

Take Profit Target
0.0180
0.0172
0.0156
0.0142
SL: 0.0212"""
    r = parse_signal_detailed(text=text, channel_id=-1, channel_name="t")
    assert r.reason == "ok", f"expected ok, got {r.reason} ({r.detail})"
    assert r.symbol == "1000000BOBUSDT"
    assert r.direction == "SHORT"
    assert r.sl == 0.0212
    # All four TPs in correct order, NOT [180.0] / [180, 172, ...].
    assert r.tps == [0.018, 0.0172, 0.0156, 0.0142]


def test_bare_price_list_no_spurious_indices():
    """A bare-price-per-line block must not produce TP indices > 9."""
    text = """#FOO/USDT
LONG
Entry: 1.000

Take Profit
1.100
1.200
1.300
SL: 0.900"""
    r = parse_signal_detailed(text=text, channel_id=-1, channel_name="t")
    assert r.reason == "ok"
    assert r.tps == [1.1, 1.2, 1.3]


def test_numbered_list_with_emoji_gap_still_works():
    """Pattern 1 must still match `1)🟥 1.3731` (CRYPTO WORLD UPTADES
    XRP format) — the emoji between marker and price is a real format
    we support."""
    text = """#XRP/USDT
SHORT
Entry: 1.4000

Take Profit Targets:
1)🟥 1.3731
2)🟥 1.3593
3)🟥 1.3454
SL: 1.4500"""
    r = parse_signal_detailed(text=text, channel_id=-1, channel_name="t")
    assert r.reason == "ok"
    assert r.tps == [1.3731, 1.3593, 1.3454]


def test_numbered_list_with_arrow_gap_works():
    """Pattern 1 must still match `1)> 0.096` (ALCH format with `>`
    between marker and price)."""
    text = """#ALCH/USDT
LONG
Entry: 0.0930

Take Profit:
1)> 0.096
2)> 0.099
3)> 0.102
SL: 0.0900"""
    r = parse_signal_detailed(text=text, channel_id=-1, channel_name="t")
    assert r.reason == "ok"
    assert r.tps == [0.096, 0.099, 0.102]
