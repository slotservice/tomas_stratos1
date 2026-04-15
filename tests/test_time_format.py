"""
Tests for the custom AM/PM time formatting (core/time_utils.py).

The Stratos1 convention:
    AM hours       -> zero-padded  HH:MM AM  (02:06 AM, 09:45 AM)
    PM hours 1-9   -> single digit  H:MM PM  (2:06 PM, 7:30 PM)
    PM hours 10-12 -> two digits   HH:MM PM  (10:15 PM, 12:00 PM)
    12:00 AM (midnight) -> "12:00 AM"
    12:00 PM (noon)     -> "12:00 PM"
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from core.time_utils import format_time


def _utc_dt(hour: int, minute: int = 0) -> datetime:
    """Create a naive datetime at the given hour:minute for testing.

    We pass naive datetimes so format_time treats them as already in
    the target timezone (avoids timezone conversion complexity in tests).
    """
    return datetime(2025, 6, 15, hour, minute)


class TestAMZeroPadded:
    """AM hours are always zero-padded to two digits."""

    def test_02_06_am(self):
        assert format_time(_utc_dt(2, 6)) == "02:06 AM"

    def test_09_45_am(self):
        assert format_time(_utc_dt(9, 45)) == "09:45 AM"

    def test_01_00_am(self):
        assert format_time(_utc_dt(1, 0)) == "01:00 AM"

    def test_11_30_am(self):
        assert format_time(_utc_dt(11, 30)) == "11:30 AM"


class TestPMSingleDigit:
    """PM hours 1-9 use a single digit (no leading zero)."""

    def test_2_06_pm(self):
        # 14:06 -> 2:06 PM
        assert format_time(_utc_dt(14, 6)) == "2:06 PM"

    def test_7_30_pm(self):
        # 19:30 -> 7:30 PM
        assert format_time(_utc_dt(19, 30)) == "7:30 PM"

    def test_1_00_pm(self):
        # 13:00 -> 1:00 PM
        assert format_time(_utc_dt(13, 0)) == "1:00 PM"

    def test_9_59_pm(self):
        # 21:59 -> 9:59 PM
        assert format_time(_utc_dt(21, 59)) == "9:59 PM"


class TestPMDoubleDigit:
    """PM hours 10-12 use two digits."""

    def test_10_15_pm(self):
        # 22:15 -> 10:15 PM
        assert format_time(_utc_dt(22, 15)) == "10:15 PM"

    def test_12_00_pm(self):
        # 12:00 -> 12:00 PM (noon)
        assert format_time(_utc_dt(12, 0)) == "12:00 PM"

    def test_11_45_pm(self):
        # 23:45 -> 11:45 PM
        assert format_time(_utc_dt(23, 45)) == "11:45 PM"


class TestMidnightAndNoon:
    """Special cases for 12 AM (midnight) and 12 PM (noon)."""

    def test_midnight(self):
        # 00:00 -> 12:00 AM
        assert format_time(_utc_dt(0, 0)) == "12:00 AM"

    def test_noon(self):
        # 12:00 -> 12:00 PM
        assert format_time(_utc_dt(12, 0)) == "12:00 PM"

    def test_midnight_with_minutes(self):
        # 00:30 -> 12:30 AM
        assert format_time(_utc_dt(0, 30)) == "12:30 AM"
