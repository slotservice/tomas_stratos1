"""
Tests for the direction-validity guard in
PositionManager._update_existing_trade (Tomas 2026-05-08).

The guard prevents the bot from pushing TPs/SL to Bybit when the
incoming signal's values are on the wrong side of the active trade's
entry. The operator was getting spammy "❌ TP/SL UPPDATERING
MISSLYCKADES" messages when late same-direction signals (entry shifted
20+%) tried to update an active trade with TPs that Bybit rejects.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from managers.position_manager import PositionManager


def _pm() -> PositionManager:
    pm = PositionManager.__new__(PositionManager)
    pm._bybit = MagicMock()
    pm._bybit.set_trading_stop = AsyncMock()
    pm._bybit.get_position = AsyncMock(return_value=None)
    pm._notifier = MagicMock()
    pm._notifier.tp_sl_update_failed = AsyncMock()
    pm._notifier.signal_updated_tp_sl = AsyncMock()
    pm._db = MagicMock()
    pm._db.update_trade = AsyncMock()
    pm._db.log_event = AsyncMock()
    pm._settings = MagicMock()
    pm._settings.tp_sl = MagicMock()
    pm._settings.tp_sl.trigger_type = "LastPrice"
    pm._settings.wallet = MagicMock()
    pm._settings.wallet.initial_margin = 100.0
    pm._reject_notify_seen = {}
    return pm


def _signal(direction="LONG", tps=None, sl=None, symbol="NILUSDT"):
    s = MagicMock()
    s.symbol = symbol
    s.direction = direction
    s.tps = tps or []
    s.sl = sl
    s.tp_list = s.tps
    return s


@pytest.mark.asyncio
async def test_skips_tp_when_new_tps_below_entry_for_long():
    """The NIL trade 805 production scenario: existing LONG @ 0.09741,
    new signal has TPs at 0.0775/0.0815/0.086 (all below entry —
    invalid for LONG). The SL @ 0.097 is on the correct side AND
    tighter than the existing 0.094, so it can still flow through.

    Result: Bybit gets only stop_loss in the update — never the
    invalid TP. No "TP must be above entry" rejection, no
    operator notification spam.
    """
    pm = _pm()
    # SL 0.097 = above existing 0.094 (tighter for LONG) but still
    # below entry 0.09741 (correct side).
    signal = _signal(direction="LONG", tps=[0.0775, 0.0815, 0.086], sl=0.097)
    existing = {
        "id": 805,
        "entry_price": 0.09741,
        "highest_tp_price": None,
        "tp_list": None,
        "sl_price": 0.094,
    }
    await pm._update_existing_trade(signal, existing)
    pm._bybit.set_trading_stop.assert_awaited_once()
    kwargs = pm._bybit.set_trading_stop.await_args.kwargs
    assert "take_profit" not in kwargs
    assert kwargs.get("stop_loss") == 0.097
    pm._notifier.tp_sl_update_failed.assert_not_awaited()


@pytest.mark.asyncio
async def test_skips_tp_when_new_tps_above_entry_for_short():
    """Mirror case for SHORT: TP must be below entry. New TPs above
    entry are the wrong side. SL is on the correct side for SHORT
    AND tighter than the existing 0.103, so it stays in the update."""
    pm = _pm()
    # SL 0.101 = below existing 0.103 (tighter for SHORT) but still
    # above entry 0.100 (correct side).
    signal = _signal(direction="SHORT", tps=[0.105, 0.110, 0.115], sl=0.101)
    existing = {
        "id": 900,
        "entry_price": 0.100,
        "highest_tp_price": None,
        "tp_list": None,
        "sl_price": 0.103,
    }
    await pm._update_existing_trade(signal, existing)
    pm._bybit.set_trading_stop.assert_awaited_once()
    kwargs = pm._bybit.set_trading_stop.await_args.kwargs
    assert "take_profit" not in kwargs
    assert kwargs.get("stop_loss") == 0.101
    pm._notifier.tp_sl_update_failed.assert_not_awaited()


@pytest.mark.asyncio
async def test_skips_sl_when_new_sl_loosens_existing_for_long():
    """FIL/ONDO/EDU production scenario (Tomas 2026-05-08):
    later same-direction signals propose a wider SL than the active
    trade's existing SL. Pushing it would loosen protection. Skip
    silently — operator should not see a MISSLYCKADES message."""
    pm = _pm()
    # LONG @ 0.100, existing SL 0.097 (3% below). New signal proposes
    # SL 0.070 (30% below — much looser). TP 0.110 is otherwise valid.
    signal = _signal(direction="LONG", tps=[0.110], sl=0.070)
    existing = {
        "id": 956,
        "entry_price": 0.100,
        "highest_tp_price": None,
        "tp_list": None,
        "sl_price": 0.097,
    }
    await pm._update_existing_trade(signal, existing)
    # SL skipped (loosened); TP still valid.
    pm._bybit.set_trading_stop.assert_awaited_once()
    kwargs = pm._bybit.set_trading_stop.await_args.kwargs
    assert "take_profit" in kwargs
    assert kwargs["take_profit"] == 0.110
    assert "stop_loss" not in kwargs
    pm._notifier.tp_sl_update_failed.assert_not_awaited()


@pytest.mark.asyncio
async def test_skips_sl_when_new_sl_loosens_existing_for_short():
    """Mirror case for SHORT: SL must be ABOVE entry; tighter = lower."""
    pm = _pm()
    # SHORT @ 0.100, existing SL 0.103. New signal SL 0.130 = looser.
    signal = _signal(direction="SHORT", tps=[0.090], sl=0.130)
    existing = {
        "id": 957,
        "entry_price": 0.100,
        "highest_tp_price": None,
        "tp_list": None,
        "sl_price": 0.103,
    }
    await pm._update_existing_trade(signal, existing)
    pm._bybit.set_trading_stop.assert_awaited_once()
    kwargs = pm._bybit.set_trading_stop.await_args.kwargs
    assert "take_profit" in kwargs
    assert kwargs["take_profit"] == 0.090
    assert "stop_loss" not in kwargs


@pytest.mark.asyncio
async def test_skips_entire_update_when_both_sides_invalid():
    """Pathological case: TP and SL both on the wrong side relative
    to the existing trade's entry. Bot must skip everything — no
    Bybit call, no notification."""
    pm = _pm()
    # LONG existing @ 0.100, new signal looks like a SHORT setup:
    # TPs below entry (would be SHORT TPs), SL above entry (would be
    # SHORT SL). Both wrong-side for an active LONG trade.
    signal = _signal(direction="LONG", tps=[0.090, 0.085], sl=0.110)
    existing = {
        "id": 850,
        "entry_price": 0.100,
        "highest_tp_price": None,
        "tp_list": None,
        "sl_price": 0.094,
    }
    await pm._update_existing_trade(signal, existing)
    pm._bybit.set_trading_stop.assert_not_awaited()
    pm._notifier.tp_sl_update_failed.assert_not_awaited()


@pytest.mark.asyncio
async def test_skips_update_when_new_sl_above_entry_for_long():
    """SL above entry on a LONG belongs to the profit-lock cascade
    path (_move_sl_to), not to a signal-driven update. Skip silently
    even if the new TP is otherwise valid."""
    pm = _pm()
    # New TP would be valid (above entry), but SL is above entry —
    # belongs to a different code path.
    signal = _signal(direction="LONG", tps=[0.105], sl=0.105)
    existing = {
        "id": 810,
        "entry_price": 0.100,
        "highest_tp_price": 0.103,  # current TP at 0.103
        "tp_list": None,
        "sl_price": 0.097,
    }
    await pm._update_existing_trade(signal, existing)
    # TP improvement (0.103 -> 0.105) IS valid; SL update is NOT
    # (above entry). Bot should send only the TP update, not the SL.
    pm._bybit.set_trading_stop.assert_awaited_once()
    kwargs = pm._bybit.set_trading_stop.await_args.kwargs
    assert "take_profit" in kwargs
    assert kwargs["take_profit"] == 0.105
    assert "stop_loss" not in kwargs


@pytest.mark.asyncio
async def test_legitimate_update_still_goes_through():
    """Signal with valid TPs (better than existing TP) and valid SL
    (correct side for LONG AND tighter than existing) should still
    update normally — no guard should block it."""
    pm = _pm()
    # LONG @ 0.100, existing TP 0.105, existing SL 0.094.
    # New TP 0.120 = better. New SL 0.097 = tighter (above 0.094,
    # still below entry).
    signal = _signal(direction="LONG", tps=[0.110, 0.115, 0.120], sl=0.097)
    existing = {
        "id": 820,
        "entry_price": 0.100,
        "highest_tp_price": 0.105,
        "tp_list": None,
        "sl_price": 0.094,
    }
    await pm._update_existing_trade(signal, existing)
    pm._bybit.set_trading_stop.assert_awaited_once()
    kwargs = pm._bybit.set_trading_stop.await_args.kwargs
    assert kwargs["take_profit"] == 0.120
    assert kwargs["stop_loss"] == 0.097


@pytest.mark.asyncio
async def test_no_existing_entry_does_not_block_update():
    """Defensive: if existing_trade_row is missing entry_price,
    fall back to existing improvement-only logic — don't accidentally
    block a legitimate update."""
    pm = _pm()
    signal = _signal(direction="LONG", tps=[0.110], sl=0.092)
    existing = {
        "id": 830,
        "entry_price": None,  # missing
        "highest_tp_price": 0.105,
        "tp_list": None,
        "sl_price": 0.094,
    }
    await pm._update_existing_trade(signal, existing)
    pm._bybit.set_trading_stop.assert_awaited_once()
