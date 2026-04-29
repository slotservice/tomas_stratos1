"""
Tests for the on_position_update WebSocket handler.

History:
- 2026-04-27 incident: Phase 3 hedge pre-arm placed a conditional order
  on positionIdx=2 (the empty hedge side); Bybit emitted position_update
  events with size=0 + side="" + positionIdx=2; the handler matched
  those against the LONG main trade at idx=1 and closed it ~1s after
  open. Fixed by checking positionIdx.
- 2026-04-28 strict architecture: on_position_update no longer triggers
  closes at all. The single Bybit-driven close path lives in
  on_order_update via _classify_bybit_close_fill (it reads stopOrderType
  / execType on the fill and calls close_trade with the right reason).
  The test below now asserts close_trade is NEVER called from
  on_position_update — for any size or positionIdx — because the
  handler is purely observational.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from managers.position_manager import PositionManager


@dataclass
class _FakeSignal:
    symbol: str = "BTCUSDT"
    direction: str = "LONG"
    tps: list = field(default_factory=list)


@dataclass
class _FakeTrade:
    id: str = "trade-1"
    signal: Optional[_FakeSignal] = None
    avg_entry: float = 100.0
    leverage: float = 10.0
    quantity: Optional[float] = 1.0
    sl_price: Optional[float] = None
    be_price: Optional[float] = None
    trailing_sl: Optional[float] = None
    trailing_activation_price: Optional[float] = None
    trailing_distance: Optional[float] = None
    trailing_activation_pct: Optional[float] = None
    trailing_distance_pct: Optional[float] = None
    trailing_activated_notified: bool = False
    last_trailing_stop_price: Optional[float] = None
    bybit_order_ids: list = field(default_factory=list)
    tp_hits: list = field(default_factory=list)
    hedge_trade_id: Optional[str] = None
    hedge_conditional_order_id: Optional[str] = None
    scaling_step: int = 0
    state: object = MagicMock(value="POSITION_OPEN")

    @property
    def is_terminal(self) -> bool:
        return False


def _make_pm() -> PositionManager:
    pm = PositionManager.__new__(PositionManager)
    pm._active_trades = {}
    pm.close_trade = AsyncMock()
    return pm


@pytest.mark.asyncio
async def test_position_update_never_closes_idx2_empty_event():
    pm = _make_pm()
    long_trade = _FakeTrade(
        id="long-1",
        signal=_FakeSignal(symbol="BTCUSDT", direction="LONG"),
    )
    pm._active_trades[long_trade.id] = long_trade

    await pm.on_position_update({
        "symbol": "BTCUSDT",
        "side": "",
        "size": "0",
        "positionIdx": 2,
        "markPrice": "100",
    })

    pm.close_trade.assert_not_called()


@pytest.mark.asyncio
async def test_position_update_never_closes_idx1_size_zero():
    """Even a real positionIdx=1 size=0 event must not trigger close
    from on_position_update — the close path is on_order_update."""
    pm = _make_pm()
    long_trade = _FakeTrade(
        id="long-1",
        signal=_FakeSignal(symbol="BTCUSDT", direction="LONG"),
        avg_entry=100.0,
        sl_price=99.0,
    )
    pm._active_trades[long_trade.id] = long_trade

    await pm.on_position_update({
        "symbol": "BTCUSDT",
        "side": "",
        "size": "0",
        "positionIdx": 1,
        "markPrice": "99.0",
    })

    pm.close_trade.assert_not_called()


@pytest.mark.asyncio
async def test_position_update_never_closes_size_nonzero():
    pm = _make_pm()
    long_trade = _FakeTrade(
        id="long-1",
        signal=_FakeSignal(symbol="BTCUSDT", direction="LONG"),
    )
    pm._active_trades[long_trade.id] = long_trade

    await pm.on_position_update({
        "symbol": "BTCUSDT",
        "side": "Buy",
        "size": "0.5",
        "positionIdx": 1,
        "markPrice": "100",
    })

    pm.close_trade.assert_not_called()


@pytest.mark.asyncio
async def test_position_update_handles_missing_position_idx():
    """Defensive: malformed events (no positionIdx) must not crash."""
    pm = _make_pm()
    await pm.on_position_update({
        "symbol": "BTCUSDT",
        "side": "",
        "size": "0",
        "markPrice": "100",
    })
    pm.close_trade.assert_not_called()


# ===================================================================
# Trailing-stop activation notification (client 2026-04-28)
# ===================================================================

def _make_pm_with_notifier():
    pm = PositionManager.__new__(PositionManager)
    pm._active_trades = {}
    pm.close_trade = AsyncMock()
    pm._notifier = MagicMock()
    pm._notifier.trailing_stop_activated = AsyncMock()
    pm._notifier.trailing_stop_updated = AsyncMock()
    return pm


@pytest.mark.asyncio
async def test_trailing_notification_fires_when_long_crosses_activation():
    """LONG: notification fires the FIRST time Last price >= activation
    (gating moved from on_position_update to handle_price_update per
    client 2026-04-29). Once fired, it must not repeat."""
    pm = _make_pm_with_notifier()
    pm._last_price_by_symbol = {}
    pm._scaling_mgr = MagicMock()
    pm._scaling_mgr.check_and_apply = AsyncMock(return_value=False)
    pm._hedge_mgr = MagicMock()
    pm._hedge_mgr.check_and_activate = AsyncMock()
    pm._settings = MagicMock()
    pm._settings.scaling.enabled = False
    trade = _FakeTrade(
        id="long-1",
        signal=_FakeSignal(symbol="BTCUSDT", direction="LONG"),
        trailing_activation_price=80155.0,
        trailing_distance=1896.5,
        trailing_activation_pct=6.10,
        trailing_distance_pct=2.50,
    )
    trade.state = MagicMock(value="POSITION_OPEN")
    pm._active_trades[trade.id] = trade

    # Below activation → no notification.
    await pm.handle_price_update("BTCUSDT", 78000.0)
    pm._notifier.trailing_stop_activated.assert_not_called()
    assert trade.trailing_activated_notified is False

    # Crosses activation → fires once.
    await pm.handle_price_update("BTCUSDT", 80200.0)
    assert pm._notifier.trailing_stop_activated.await_count == 1
    assert trade.trailing_activated_notified is True

    # Subsequent ticks above activation → no repeat.
    await pm.handle_price_update("BTCUSDT", 80500.0)
    assert pm._notifier.trailing_stop_activated.await_count == 1


@pytest.mark.asyncio
async def test_trailing_notification_fires_when_short_crosses_activation():
    """SHORT: notification fires when Last price <= activation."""
    pm = _make_pm_with_notifier()
    pm._last_price_by_symbol = {}
    pm._scaling_mgr = MagicMock()
    pm._scaling_mgr.check_and_apply = AsyncMock(return_value=False)
    pm._hedge_mgr = MagicMock()
    pm._hedge_mgr.check_and_activate = AsyncMock()
    pm._settings = MagicMock()
    pm._settings.scaling.enabled = False
    trade = _FakeTrade(
        id="short-1",
        signal=_FakeSignal(symbol="BTCUSDT", direction="SHORT"),
        trailing_activation_price=70000.0,
        trailing_distance=1750.0,
        trailing_activation_pct=6.10,
        trailing_distance_pct=2.50,
    )
    trade.state = MagicMock(value="POSITION_OPEN")
    pm._active_trades[trade.id] = trade

    # Above activation → no notification.
    await pm.handle_price_update("BTCUSDT", 72000.0)
    pm._notifier.trailing_stop_activated.assert_not_called()

    # Crosses activation downward → fires.
    await pm.handle_price_update("BTCUSDT", 69900.0)
    assert pm._notifier.trailing_stop_activated.await_count == 1
    assert trade.trailing_activated_notified is True


@pytest.mark.asyncio
async def test_trailing_notification_skipped_when_no_activation_price_set():
    """Trades that don't have trailing armed (no activation_price) must
    not produce phantom trailing notifications on price ticks."""
    pm = _make_pm_with_notifier()
    pm._last_price_by_symbol = {}
    pm._scaling_mgr = MagicMock()
    pm._scaling_mgr.check_and_apply = AsyncMock(return_value=False)
    pm._hedge_mgr = MagicMock()
    pm._hedge_mgr.check_and_activate = AsyncMock()
    pm._settings = MagicMock()
    pm._settings.scaling.enabled = False
    trade = _FakeTrade(
        id="no-trail",
        signal=_FakeSignal(symbol="BTCUSDT", direction="LONG"),
        trailing_activation_price=None,
    )
    trade.state = MagicMock(value="POSITION_OPEN")
    pm._active_trades[trade.id] = trade

    await pm.handle_price_update("BTCUSDT", 999999.0)
    pm._notifier.trailing_stop_activated.assert_not_called()


# ---- TRAILING STOP UPPDATERAD --------------------------------------

@pytest.mark.asyncio
async def test_trailing_update_fires_when_long_stop_moves_up():
    """LONG: each time Bybit pushes a new higher stopLoss after
    activation, the bot fires TRAILING STOP UPPDATERAD exactly once
    per move."""
    pm = _make_pm_with_notifier()
    trade = _FakeTrade(
        id="long-1",
        signal=_FakeSignal(symbol="BTCUSDT", direction="LONG"),
        avg_entry=75000.0,
        leverage=10.0,
        trailing_activation_price=80000.0,
        trailing_distance=2000.0,
        trailing_activation_pct=6.10,
        trailing_distance_pct=2.50,
        # Trailing already active, last reported stop = 78100
        trailing_activated_notified=True,
        last_trailing_stop_price=78100.0,
    )
    pm._active_trades[trade.id] = trade

    # Bybit pushes a new higher stop -> notification fires.
    await pm.on_position_update({
        "symbol": "BTCUSDT", "size": "0.002", "side": "Buy",
        "positionIdx": 1, "markPrice": "80500.0",
        "stopLoss": "78500.0",
        "unrealisedPnl": "11.0",
    })
    assert pm._notifier.trailing_stop_updated.await_count == 1
    assert trade.last_trailing_stop_price == 78500.0

    # Same stopLoss -> no fire.
    await pm.on_position_update({
        "symbol": "BTCUSDT", "size": "0.002", "side": "Buy",
        "positionIdx": 1, "markPrice": "80600.0",
        "stopLoss": "78500.0",
        "unrealisedPnl": "11.5",
    })
    assert pm._notifier.trailing_stop_updated.await_count == 1

    # Higher stop again -> fires again.
    await pm.on_position_update({
        "symbol": "BTCUSDT", "size": "0.002", "side": "Buy",
        "positionIdx": 1, "markPrice": "81000.0",
        "stopLoss": "79000.0",
        "unrealisedPnl": "12.0",
    })
    assert pm._notifier.trailing_stop_updated.await_count == 2
    assert trade.last_trailing_stop_price == 79000.0


@pytest.mark.asyncio
async def test_trailing_update_fires_when_short_stop_moves_down():
    """SHORT: stopLoss moves DOWN as price drops. Each new lower
    stop triggers a notification."""
    pm = _make_pm_with_notifier()
    trade = _FakeTrade(
        id="short-1",
        signal=_FakeSignal(symbol="BTCUSDT", direction="SHORT"),
        avg_entry=75000.0,
        leverage=10.0,
        trailing_activation_price=70000.0,
        trailing_activated_notified=True,
        last_trailing_stop_price=71800.0,
    )
    pm._active_trades[trade.id] = trade

    await pm.on_position_update({
        "symbol": "BTCUSDT", "size": "0.002", "side": "Sell",
        "positionIdx": 2, "markPrice": "69500.0",
        "stopLoss": "71200.0",
        "unrealisedPnl": "11.0",
    })
    assert pm._notifier.trailing_stop_updated.await_count == 1
    assert trade.last_trailing_stop_price == 71200.0

    # stopLoss moved UP (against the SHORT) -> ignored, not "favourable".
    await pm.on_position_update({
        "symbol": "BTCUSDT", "size": "0.002", "side": "Sell",
        "positionIdx": 2, "markPrice": "69400.0",
        "stopLoss": "71500.0",
        "unrealisedPnl": "11.0",
    })
    assert pm._notifier.trailing_stop_updated.await_count == 1


@pytest.mark.asyncio
async def test_trailing_update_skipped_before_activation():
    """If trailing hasn't activated yet, stopLoss changes (e.g. the
    initial static SL) must not trigger TRAILING STOP UPPDATERAD."""
    pm = _make_pm_with_notifier()
    trade = _FakeTrade(
        id="long-1",
        signal=_FakeSignal(symbol="BTCUSDT", direction="LONG"),
        avg_entry=75000.0,
        trailing_activation_price=80000.0,
        trailing_activated_notified=False,  # NOT activated
        last_trailing_stop_price=None,
    )
    pm._active_trades[trade.id] = trade

    await pm.on_position_update({
        "symbol": "BTCUSDT", "size": "0.002", "side": "Buy",
        "positionIdx": 1, "markPrice": "78000.0",
        "stopLoss": "73500.0",   # static SL move, before activation
        "unrealisedPnl": "5.0",
    })
    pm._notifier.trailing_stop_updated.assert_not_called()
