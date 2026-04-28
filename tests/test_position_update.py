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
    sl_price: Optional[float] = None
    be_price: Optional[float] = None
    trailing_sl: Optional[float] = None
    tp_hits: list = field(default_factory=list)
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
