"""
Tests for the dynamic leverage calculator (core/leverage.py).

Covers all bucketing zones, the cap at max_entry_leverage (25x),
the IOTA example from the project chat, and the FIXED vs DYNAMIC
classifier.
"""

from __future__ import annotations

import pytest

from core.leverage import (
    calculate_leverage,
    classify_leverage,
    classify_leverage_with_settings,
)


# Default settings:
#   wallet=402.1, risk_pct=0.02, initial_margin=20.0
#   min_lev=6.0, split=6.75, upper=7.5, max_entry=25.0
#
# Formula:
#   stop_distance_pct = |entry - sl| / entry
#   raw = (402.1 * 0.02) / (20.0 * stop_distance_pct)
#       = 8.042 / (20 * stop_distance_pct)
#       = 0.4021 / stop_distance_pct


class TestLeverageBuckets:
    """Test leverage bucketing at each zone boundary."""

    def test_leverage_below_6_fixed(self):
        """
        raw < 6.0 -> clamped to 6.0.
        Need stop_distance > 0.4021 / 6.0 = 0.06702 (6.7%).
        Use entry=100, SL=93 -> dist = 7% -> raw ~ 5.74.
        """
        lev = calculate_leverage(entry=100.0, sl=93.0)
        assert lev == 6.0

    def test_leverage_6_to_6_75_fixed(self):
        """
        6.0 <= raw < 6.75 -> fixed at 6.0.
        Need dist in (0.4021/6.75, 0.4021/6.0) = (0.05957, 0.06702).
        Use entry=100, SL=93.7 -> dist = 6.3% -> raw ~ 6.38.
        """
        lev = calculate_leverage(entry=100.0, sl=93.7)
        assert lev == 6.0

    def test_leverage_6_75_to_7_5_dynamic_floor(self):
        """
        6.75 <= raw < 7.5 -> floored to 7.5 (DYNAMIC).
        Need dist in (0.4021/7.5, 0.4021/6.75) = (0.05361, 0.05957).
        Use entry=100, SL=94.3 -> dist = 5.7% -> raw ~ 7.05.
        """
        lev = calculate_leverage(entry=100.0, sl=94.3)
        assert lev == 7.5

    def test_leverage_above_7_5_dynamic(self):
        """
        7.5 <= raw <= 25.0 -> use computed value.
        Use entry=100, SL=97 -> dist = 3.0% -> raw ~ 13.40.
        """
        lev = calculate_leverage(entry=100.0, sl=97.0)
        assert 7.5 <= lev <= 25.0
        # raw = 0.4021 / 0.03 = 13.40
        assert lev == round(0.4021 / 0.03, 2)

    def test_leverage_capped_at_25(self):
        """
        raw > 25.0 -> capped at 25.0.
        Need dist < 0.4021 / 25.0 = 0.01608 (1.6%).
        Use entry=100, SL=99 -> dist = 1% -> raw ~ 40.21.
        """
        lev = calculate_leverage(entry=100.0, sl=99.0)
        assert lev == 25.0


class TestLeverageIOTA:
    """
    Test the IOTA example from the project chat.

    IOTA entry=0.2215, SL=0.2149
    stop_distance = |0.2215 - 0.2149| / 0.2215 = 0.0066 / 0.2215 ~ 0.02979
    raw = 0.4021 / 0.02979 ~ 13.50
    Expected: ~x13.49 DYNAMIC (7.5 <= 13.50 <= 25.0)
    """

    def test_leverage_iota_example(self):
        lev = calculate_leverage(entry=0.2215, sl=0.2149)
        # Should be in the dynamic range.
        assert 13.0 <= lev <= 14.0
        # Verify it's classified as DYNAMIC.
        assert classify_leverage(lev) == "DYNAMIC"


class TestClassifyLeverage:
    """Test FIXED vs DYNAMIC classification."""

    def test_classify_fixed_vs_dynamic(self):
        # 6.0 is the default minimum -> FIXED
        assert classify_leverage(6.0) == "FIXED"
        assert classify_leverage(5.0) == "FIXED"

        # Anything above 6.0 -> DYNAMIC
        assert classify_leverage(6.01) == "DYNAMIC"
        assert classify_leverage(7.5) == "DYNAMIC"
        assert classify_leverage(25.0) == "DYNAMIC"

    def test_classify_with_custom_min(self):
        """Settings-aware classify with a custom min_leverage."""
        assert classify_leverage_with_settings(8.0, min_leverage=8.0) == "FIXED"
        assert classify_leverage_with_settings(8.01, min_leverage=8.0) == "DYNAMIC"


class TestLeverageEdgeCases:
    """Edge cases and error handling."""

    def test_zero_entry_raises(self):
        with pytest.raises(ValueError, match="zero"):
            calculate_leverage(entry=0.0, sl=100.0)

    def test_entry_equals_sl_raises(self):
        with pytest.raises(ValueError, match="zero"):
            calculate_leverage(entry=100.0, sl=100.0)
