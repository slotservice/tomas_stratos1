"""Tests for the re-entry SL-source filter.

Tomas 2026-05-12 spec: re-entry must only fire when the SL that hit
was the original signal SL (never moved) OR a break-even / BE+buffer
SL. Cascaded TP-level SLs (SL moved to TP1/TP2/TP3/TP4 via the
TP2-N cascade) and profit-lock SLs (BE+1.5%, BE+2.5%) do NOT qualify
— those represent profitable closes, not real losses, and a re-entry
there would chase the trade.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from managers.position_manager import (
    _reentry_qualifies_for_trade,
    _REENTRY_QUALIFYING_SL_REASON_PREFIXES,
)


def _trade(history=None):
    return SimpleNamespace(
        id="42",
        sl_movement_history=list(history or []),
        reentry_count=0,
        signal=SimpleNamespace(
            symbol="BTCUSDT",
            direction="LONG",
            channel_name="TestChannel",
        ),
    )


# --- The qualifier function itself -----------------------------------------

def test_qualifies_when_history_empty():
    """Original SL fired (never moved) — re-entry allowed."""
    assert _reentry_qualifies_for_trade(_trade(history=[])) is True


def test_qualifies_when_last_move_is_tp2_breakeven():
    t = _trade(history=[
        {"reason": "tp2_hit_sl_to_breakeven", "to_sl": 100.0},
    ])
    assert _reentry_qualifies_for_trade(t) is True


def test_qualifies_when_last_move_is_fallback_be_buffer():
    t = _trade(history=[
        {"reason": "fallback_be_buffer_at_2pct", "to_sl": 100.2},
    ])
    assert _reentry_qualifies_for_trade(t) is True


def test_does_not_qualify_when_last_move_is_tp3_cascade():
    """TP3 hit -> SL moved to TP1. That SL fire is a profitable close,
    not a real loss — re-entry must be blocked."""
    t = _trade(history=[
        {"reason": "tp2_hit_sl_to_breakeven", "to_sl": 100.0},
        {"reason": "tp3_hit_sl_to_tp1", "to_sl": 102.0},
    ])
    assert _reentry_qualifies_for_trade(t) is False


def test_does_not_qualify_when_last_move_is_tp4_cascade():
    t = _trade(history=[
        {"reason": "tp4_hit_sl_to_tp2", "to_sl": 103.0},
    ])
    assert _reentry_qualifies_for_trade(t) is False


def test_does_not_qualify_when_last_move_is_profit_lock_4pct():
    """Profit-lock fallback at +4% -> SL = entry +1.5%. That SL fire
    is still a profitable close, not a loss."""
    t = _trade(history=[
        {"reason": "fallback_profit_lock_1_at_4pct", "to_sl": 101.5},
    ])
    assert _reentry_qualifies_for_trade(t) is False


def test_does_not_qualify_when_last_move_is_profit_lock_5pct():
    t = _trade(history=[
        {"reason": "fallback_profit_lock_2_at_5pct", "to_sl": 102.5},
    ])
    assert _reentry_qualifies_for_trade(t) is False


def test_only_LAST_move_matters_even_if_earlier_qualifies():
    """Trade that went BE then cascaded to TP1 — last move was
    cascade, so re-entry is blocked even though an earlier move was
    BE-qualifying."""
    t = _trade(history=[
        {"reason": "tp2_hit_sl_to_breakeven", "to_sl": 100.0},
        {"reason": "tp3_hit_sl_to_tp1", "to_sl": 102.0},
    ])
    assert _reentry_qualifies_for_trade(t) is False


def test_qualifying_prefixes_are_documented():
    """Sanity: the qualifying prefixes constant matches what the spec
    intends. If this fails it means someone changed the set without
    updating the test (and presumably the corresponding spec note)."""
    assert "tp2_hit_sl_to_breakeven" in _REENTRY_QUALIFYING_SL_REASON_PREFIXES
    assert "fallback_be_buffer" in _REENTRY_QUALIFYING_SL_REASON_PREFIXES
    assert len(_REENTRY_QUALIFYING_SL_REASON_PREFIXES) == 2


# --- close_trade integration -----------------------------------------------
#
# Verify that close_trade actually consults the qualifier before
# invoking _reentry_mgr.activate_after_sl, and fires the
# reentry_skipped_non_qualifying_sl notifier when the SL doesn't
# qualify.

from managers.position_manager import PositionManager
from core.models import Trade, TradeState


def _pm_with_real_close_trade():
    """Build a PositionManager whose close_trade we can drive directly.
    Mocks every collaborator that close_trade touches so the test
    isolates the re-entry decision path."""
    pm = PositionManager.__new__(PositionManager)
    pm._active_trades = {}
    pm._db = MagicMock()
    pm._db.update_trade = AsyncMock()
    pm._db.log_event = AsyncMock()
    pm._notifier = MagicMock()
    pm._notifier.position_closed = AsyncMock()
    pm._notifier.reentry_skipped_non_qualifying_sl = AsyncMock()
    pm._bybit = MagicMock()
    pm._bybit.get_open_orders = AsyncMock(return_value=[])
    pm._bybit.cancel_order = AsyncMock()
    pm._hedge_mgr = MagicMock()
    pm._hedge_mgr.cancel_pre_armed = AsyncMock()
    pm._reentry_mgr = MagicMock()
    pm._reentry_mgr.activate_after_sl = AsyncMock()
    pm._settings = MagicMock()
    pm._settings.reentry = MagicMock()
    pm._settings.reentry.max_reentries = 2
    pm._settings.reporting = MagicMock()
    pm._settings.reporting.audit_snapshot_every_n_trades = 0
    pm._closes_since_snapshot = 0
    pm._update_report_stats = AsyncMock()
    return pm


def _real_trade(history=None, reentry_count=0):
    """Build a real Trade dataclass so close_trade's transition + db
    update paths work."""
    from core.signal_parser import ParsedSignal
    sig = ParsedSignal(
        symbol="BTCUSDT", direction="LONG", entry=100.0,
        tps=[102.0, 104.0, 106.0], sl=98.0,
        channel_id=1, channel_name="TestChannel",
    )
    t = Trade(
        id="99",
        state=TradeState.POSITION_OPEN,
        signal=sig,
        avg_entry=100.0,
        quantity=1.0,
        leverage=10.0,
        margin=10.0,
        sl_price=98.0,
        sl_movement_history=list(history or []),
        reentry_count=reentry_count,
    )
    return t


@pytest.mark.asyncio
async def test_close_trade_stop_loss_empty_history_activates_reentry():
    pm = _pm_with_real_close_trade()
    t = _real_trade(history=[])
    pm._active_trades[t.id] = t
    await pm.close_trade(t.id, "stop_loss", 98.0)
    pm._reentry_mgr.activate_after_sl.assert_awaited_once()
    pm._notifier.reentry_skipped_non_qualifying_sl.assert_not_called()


@pytest.mark.asyncio
async def test_close_trade_stop_loss_after_be_move_activates_reentry():
    pm = _pm_with_real_close_trade()
    t = _real_trade(history=[
        {"reason": "tp2_hit_sl_to_breakeven", "to_sl": 100.0},
    ])
    pm._active_trades[t.id] = t
    await pm.close_trade(t.id, "stop_loss", 100.0)
    pm._reentry_mgr.activate_after_sl.assert_awaited_once()
    pm._notifier.reentry_skipped_non_qualifying_sl.assert_not_called()


@pytest.mark.asyncio
async def test_close_trade_stop_loss_after_cascade_skips_reentry_and_notifies():
    pm = _pm_with_real_close_trade()
    t = _real_trade(history=[
        {"reason": "tp2_hit_sl_to_breakeven", "to_sl": 100.0},
        {"reason": "tp3_hit_sl_to_tp1", "to_sl": 102.0},
    ])
    pm._active_trades[t.id] = t
    await pm.close_trade(t.id, "stop_loss", 102.0)
    pm._reentry_mgr.activate_after_sl.assert_not_called()
    pm._notifier.reentry_skipped_non_qualifying_sl.assert_awaited_once()
    kwargs = pm._notifier.reentry_skipped_non_qualifying_sl.await_args.kwargs
    assert kwargs["last_sl_reason"] == "tp3_hit_sl_to_tp1"


@pytest.mark.asyncio
async def test_close_trade_stop_loss_after_profit_lock_skips_reentry():
    pm = _pm_with_real_close_trade()
    t = _real_trade(history=[
        {"reason": "fallback_profit_lock_1_at_4pct", "to_sl": 101.5},
    ])
    pm._active_trades[t.id] = t
    await pm.close_trade(t.id, "stop_loss", 101.5)
    pm._reentry_mgr.activate_after_sl.assert_not_called()
    pm._notifier.reentry_skipped_non_qualifying_sl.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_trade_non_sl_reason_does_not_evaluate_reentry():
    """trailing_stop close / TP close should never even consult the
    re-entry filter."""
    pm = _pm_with_real_close_trade()
    t = _real_trade(history=[])
    pm._active_trades[t.id] = t
    await pm.close_trade(t.id, "trailing_stop", 105.0)
    pm._reentry_mgr.activate_after_sl.assert_not_called()
    pm._notifier.reentry_skipped_non_qualifying_sl.assert_not_called()


@pytest.mark.asyncio
async def test_close_trade_reentry_max_count_blocks_even_qualifying_sl():
    """The max-reentries guard runs BEFORE the SL-source filter — a
    trade that has already used its 2 re-entries doesn't get a third
    just because the SL was original."""
    pm = _pm_with_real_close_trade()
    t = _real_trade(history=[], reentry_count=2)
    pm._active_trades[t.id] = t
    await pm.close_trade(t.id, "stop_loss", 98.0)
    pm._reentry_mgr.activate_after_sl.assert_not_called()
    pm._notifier.reentry_skipped_non_qualifying_sl.assert_not_called()
