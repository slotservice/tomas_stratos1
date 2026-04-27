"""
Regression test for the on_position_update WebSocket handler.

Reproduces the 2026-04-27 incident where every signal closed ~1 second
after open with reason ``near_entry_close``: the Phase 3 hedge pre-arm
placed a conditional order on the empty hedge side (positionIdx=2),
which made Bybit emit position_update events with size=0 + side="" +
positionIdx=2. The handler used to match those against the LONG main
trade at positionIdx=1 and close it.
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
    """Build a PositionManager with all heavy deps mocked."""
    pm = PositionManager.__new__(PositionManager)
    pm._active_trades = {}
    return pm


@pytest.mark.asyncio
async def test_idx2_empty_event_does_not_close_long_main():
    """Empty positionIdx=2 update during hedge pre-arm must NOT close LONG main."""
    pm = _make_pm()
    pm.close_trade = AsyncMock()
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
async def test_idx1_empty_event_does_not_close_short_main():
    """Empty positionIdx=1 update during hedge pre-arm must NOT close SHORT main."""
    pm = _make_pm()
    pm.close_trade = AsyncMock()
    short_trade = _FakeTrade(
        id="short-1",
        signal=_FakeSignal(symbol="BTCUSDT", direction="SHORT"),
        avg_entry=100.0,
    )
    pm._active_trades[short_trade.id] = short_trade

    await pm.on_position_update({
        "symbol": "BTCUSDT",
        "side": "",
        "size": "0",
        "positionIdx": 1,
        "markPrice": "100",
    })

    pm.close_trade.assert_not_called()


@pytest.mark.asyncio
async def test_idx1_close_event_does_close_long_main():
    """A real positionIdx=1 close (size=0 after a real LONG fill) must close the LONG."""
    pm = _make_pm()
    pm.close_trade = AsyncMock()
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

    pm.close_trade.assert_awaited_once()
    kwargs = pm.close_trade.await_args.kwargs
    assert kwargs["trade_id"] == "long-1"


@pytest.mark.asyncio
async def test_idx2_close_event_does_close_short_main():
    """A real positionIdx=2 close (size=0 after a real SHORT fill) must close the SHORT."""
    pm = _make_pm()
    pm.close_trade = AsyncMock()
    short_trade = _FakeTrade(
        id="short-1",
        signal=_FakeSignal(symbol="BTCUSDT", direction="SHORT"),
        avg_entry=100.0,
        sl_price=101.0,
    )
    pm._active_trades[short_trade.id] = short_trade

    await pm.on_position_update({
        "symbol": "BTCUSDT",
        "side": "",
        "size": "0",
        "positionIdx": 2,
        "markPrice": "101.0",
    })

    pm.close_trade.assert_awaited_once()
    kwargs = pm.close_trade.await_args.kwargs
    assert kwargs["trade_id"] == "short-1"


@pytest.mark.asyncio
async def test_size_nonzero_does_not_close():
    """Open / running positions (size > 0) must never trigger a close."""
    pm = _make_pm()
    pm.close_trade = AsyncMock()
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
async def test_missing_position_idx_is_ignored():
    """Defensive: events with no positionIdx (one-way mode, malformed) are skipped."""
    pm = _make_pm()
    pm.close_trade = AsyncMock()
    long_trade = _FakeTrade(
        id="long-1",
        signal=_FakeSignal(symbol="BTCUSDT", direction="LONG"),
    )
    pm._active_trades[long_trade.id] = long_trade

    await pm.on_position_update({
        "symbol": "BTCUSDT",
        "side": "",
        "size": "0",
        "markPrice": "100",
    })

    pm.close_trade.assert_not_called()
