"""Tests for the rejection / info notification throttle.

Tomas 2026-05-12 spec: "Nothing should happen silently inside the bot.
If the bot blocks or skips something, the exact reason must be visible
in Telegram." The previous 5-minute dedup window (set 2026-04-30) was
silencing rejection notifications for the same (symbol, direction)
within 5 minutes. As of 2026-05-12 the window is 0 — every event
notifies.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from managers.position_manager import PositionManager


def _pm() -> PositionManager:
    pm = PositionManager.__new__(PositionManager)
    pm._reject_notify_last = {}
    pm._info_notify_last = {}
    # Use the live default from PositionManager.__init__.
    pm._dedup_window_s = 0.0
    return pm


def test_reject_notify_window_is_zero_so_first_call_fires():
    pm = _pm()
    assert pm._should_send_reject_notify("duplicate", "BTCUSDT", "LONG") is True


def test_reject_notify_second_immediate_call_also_fires_with_zero_window():
    """Two near-duplicate signals on the same (symbol, direction)
    within milliseconds must both produce a notification — that was
    Tomas's Group A / Group B example."""
    pm = _pm()
    assert pm._should_send_reject_notify("duplicate", "BTCUSDT", "LONG") is True
    # Immediately after — same key. Old behaviour returned False; new
    # behaviour must return True.
    assert pm._should_send_reject_notify("duplicate", "BTCUSDT", "LONG") is True


def test_reject_notify_handles_different_kinds_on_same_symbol():
    """Symbol gets rejected for duplicate AND for capacity in quick
    succession — both must surface to the operator."""
    pm = _pm()
    assert pm._should_send_reject_notify("duplicate", "ETHUSDT", "SHORT") is True
    assert pm._should_send_reject_notify("max_capacity", "ETHUSDT", "SHORT") is True
    assert pm._should_send_reject_notify("stale_signal_age", "ETHUSDT", "SHORT") is True


def test_info_notify_window_is_zero_so_repeat_calls_fire():
    """Mirror behaviour for the INFORMATIONAL notification dedup
    (e.g. SL_ADJUSTED). Same rule — nothing silent."""
    pm = _pm()
    assert pm._should_send_info_notify("SL_ADJUSTED", "DOGEUSDT", "LONG") is True
    assert pm._should_send_info_notify("SL_ADJUSTED", "DOGEUSDT", "LONG") is True
