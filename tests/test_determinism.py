"""Phase 6.B.1 — determinism tests (client 2026-05-02 audit point #1).

"The same signal must always produce the exact same result. Test the
same signal multiple times under the same conditions. The result must
be identical: entry, SL, TP, leverage, Telegram output."

We exercise the deterministic part of the pipeline: parser +
leverage calculator + classification. The downstream Bybit calls
involve external state (instrument info, current price, fill price)
so they're tested with mocked inputs in other suites; here we verify
that the PURE-FUNCTION components produce bit-identical output across
repeated runs.
"""

from __future__ import annotations

import pytest

from core.signal_parser import parse_signal_detailed
from core.leverage import calculate_leverage


SAMPLE_SIGNALS = [
    # Standard format with explicit entry / SL / TPs.
    """#BTCUSDT
LONG
Entry: 76000
SL: 74000
TP1: 77000
TP2: 78000
TP3: 80000
""",
    # Range entry, multi-TP.
    """#ETHUSDT
SHORT
Entry: 3550 - 3580
SL: 3650
TP1: 3500
TP2: 3450
TP3: 3400
TP4: 3300
""",
    # Numbered list under "Entry:" header.
    """Pair: SOL/USDT
SHORT

Entry:
1) 145.20
2) 146.80

Targets:
1) 142.00
2) 138.50
3) 132.00

Stop Loss: 152
""",
    # American Crypto BUY format.
    """#BRUSDT
#SHORT

Leverage cross 25-50 X

Entry : 0.1120 - 0.1160

Take Profit Targets
0.1080
0.1045
0.0980

Stop Loss : 0.1185
""",
]


class TestParserDeterminism:
    """parse_signal_detailed must return bit-identical results across runs."""

    @pytest.mark.parametrize("text", SAMPLE_SIGNALS)
    def test_parse_is_deterministic(self, text):
        first = parse_signal_detailed(
            text=text, channel_id=-100, channel_name="DetermTest",
        )
        # Run 50 times, every result must equal the first.
        for _ in range(50):
            again = parse_signal_detailed(
                text=text, channel_id=-100, channel_name="DetermTest",
            )
            # Compare every relevant field.
            assert again.reason == first.reason
            assert again.symbol == first.symbol
            assert again.direction == first.direction
            assert again.entry == first.entry
            assert again.tps == first.tps
            assert again.sl == first.sl
            if first.signal is not None:
                assert again.signal is not None
                assert again.signal.symbol == first.signal.symbol
                assert again.signal.direction == first.signal.direction
                assert again.signal.entry == first.signal.entry
                assert again.signal.entry_low == first.signal.entry_low
                assert again.signal.entry_high == first.signal.entry_high
                assert again.signal.tps == first.signal.tps
                assert again.signal.sl == first.signal.sl
                assert again.signal.signal_type == first.signal.signal_type


class TestLeverageDeterminism:
    """calculate_leverage must return bit-identical results across runs."""

    @pytest.mark.parametrize(
        "entry,sl",
        [
            (100.0, 97.0),       # +3% SL distance
            (76000.0, 74000.0),  # ~2.6%
            (0.0560, 0.0540),    # ~3.6%
            (0.2215, 0.2149),    # IOTA example, ~3%
            (12345.678, 11999.0),
        ],
    )
    def test_leverage_is_deterministic(self, entry, sl):
        first = calculate_leverage(entry=entry, sl=sl)
        for _ in range(50):
            again = calculate_leverage(entry=entry, sl=sl)
            assert again == first, (
                f"non-deterministic leverage for entry={entry} sl={sl}: "
                f"first={first} got={again}"
            )

    def test_leverage_differs_only_when_inputs_differ(self):
        # Tiny SL change should change the leverage; identical inputs
        # should not. Verifies the function is a pure deterministic
        # transform of (entry, sl).
        a = calculate_leverage(entry=100.0, sl=97.0)
        b = calculate_leverage(entry=100.0, sl=97.0)
        c = calculate_leverage(entry=100.0, sl=96.5)
        assert a == b
        assert a != c


class TestParseAndLeverageStability:
    """End-to-end: parse -> compute leverage -> compare across runs."""

    @pytest.mark.parametrize("text", SAMPLE_SIGNALS)
    def test_parse_and_leverage_stable(self, text):
        result_a = parse_signal_detailed(
            text=text, channel_id=0, channel_name="x",
        )
        if result_a.signal is None:
            pytest.skip("sample didn't parse — separate suite covers this")
        lev_a = calculate_leverage(
            entry=result_a.signal.entry, sl=result_a.signal.sl,
        )
        for _ in range(20):
            result_b = parse_signal_detailed(
                text=text, channel_id=0, channel_name="x",
            )
            assert result_b.signal is not None
            lev_b = calculate_leverage(
                entry=result_b.signal.entry, sl=result_b.signal.sl,
            )
            assert lev_a == lev_b
            assert result_a.signal.entry == result_b.signal.entry
            assert result_a.signal.sl == result_b.signal.sl
            assert result_a.signal.tps == result_b.signal.tps
