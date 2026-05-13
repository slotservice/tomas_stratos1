"""Tests for keycap-enclosed-digit TP list parsing.

Tomas 2026-05-13 CryptoApple #MBOXUSDT incident: real entry signal
arrived with TPs formatted as bare-keycap digits (`1⃣ price`,
where the marker is U+0031 U+20E3 enclosing-keycap without the
VS-16 variation selector). Parser dropped it as no_tps because:
  - The "🎯 TP Targets:" header had an emoji prefix that broke the
    start-of-line match in _TP_PATTERNS[1].
  - The inner per-line extractor required ")", ":" or "-" as the
    list marker, not the keycap.

This file pins the fix so the format keeps parsing, and the
neighbouring bare-price BOB regression (2026-05-04) keeps NOT
matching.
"""

from __future__ import annotations

from core.signal_parser import parse_signal_detailed


CRYPTOAPPLE_MBOX = """👑 #MBOX/USDT

⚡ Leverage: 20x

✈️ Short 🔴

🔆 Entry Zone : 0.01597

🎯 TP Targets:

1⃣ 0.01539
2⃣ 0.01524
3⃣ 0.01509
4⃣ 0.01492
5⃣ 0.01478
6⃣ 0.01452

🛑 Stop Price : 0.01638"""


def test_cryptoapple_keycap_list_parses_all_six_tps():
    r = parse_signal_detailed(
        text=CRYPTOAPPLE_MBOX,
        channel_id=-1002424040440,
        channel_name="Crypto Apple",
    )
    assert r.reason == "ok", r.reason
    assert r.symbol == "MBOXUSDT"
    assert r.direction == "SHORT"
    assert r.entry == 0.01597
    assert r.tps == [0.01539, 0.01524, 0.01509, 0.01492, 0.01478, 0.01452]


def test_traditional_paren_list_still_parses():
    """Don't regress the standard ")"/":"/"-" list-marker form."""
    text = """#TST/USDT LONG
Entry: 100
Targets:
1) 101
2) 102
3) 103
SL: 99"""
    r = parse_signal_detailed(
        text=text, channel_id=-1, channel_name="TestChannel",
    )
    assert r.reason == "ok"
    assert r.signal.tps == [101.0, 102.0, 103.0]


def test_traditional_colon_list_still_parses():
    text = """#TST/USDT LONG
Entry: 100
Targets:
1 : 101
2 : 102
SL: 99"""
    r = parse_signal_detailed(
        text=text, channel_id=-1, channel_name="TestChannel",
    )
    assert r.reason == "ok"
    assert r.signal.tps == [101.0, 102.0]


def test_bare_price_list_2026_05_04_bob_regression_still_holds():
    """Bare-price-per-line list without ANY index marker must NOT be
    captured by pattern 2 — it should fall through to other patterns
    (or be no_tps). This guards against the 2026-05-04 BOB incident
    where a bare list got misread as numbered, collapsing every entry
    into idx=0."""
    text = """#BOBUSDT LONG
Entry: 0.0200
Take Profit Targets:

0.0180
0.0172
0.0156

SL: 0.025"""
    r = parse_signal_detailed(
        text=text, channel_id=-1, channel_name="TestChannel",
    )
    # Whichever pattern catches it, the result must NOT collapse to
    # a single price under idx=0. Either tps is empty (no_tps reason)
    # or all three prices are picked up by a different pattern.
    if r.signal is not None and r.signal.tps:
        # If picked up, must contain all three real prices.
        assert sorted(r.signal.tps) == [0.0156, 0.0172, 0.0180]
    # Critical: NOT a single fake "idx=0" entry of 39200 or similar.
    if r.signal is not None:
        assert 39200 not in r.signal.tps
        assert 180 not in r.signal.tps
