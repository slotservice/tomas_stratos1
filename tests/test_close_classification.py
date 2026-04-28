"""
Tests for the Bybit-driven close-classification pipeline.

Strict architecture (client IZZU 2026-04-28): every close reason and
every "POSITION CLOSED - X" notification is set by the Bybit fill event,
never by bot inference. ``on_order_update`` is the SOLE source of close
events: it reads ``stopOrderType`` and ``execType`` on the fill and
calls ``close_trade`` directly with the right reason.

These tests cover the classifier (``_classify_bybit_close_fill``) and
the ``_format_close_source`` mapping.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from managers.position_manager import PositionManager, _format_close_source


@dataclass
class _FakeSignal:
    symbol: str = "BTCUSDT"
    direction: str = "LONG"
    tps: list = field(default_factory=lambda: [101.0, 103.0, 110.0, 115.0])


@dataclass
class _FakeTrade:
    id: str = "trade-1"
    signal: Optional[_FakeSignal] = None
    avg_entry: float = 100.0
    quantity: float = 1.0
    leverage: float = 10.0
    margin: float = 10.0
    sl_price: Optional[float] = None
    tp_hits: list = field(default_factory=list)
    tp_order_ids: list = field(default_factory=list)
    state: object = MagicMock(value="POSITION_OPEN")
    hedge_trade_id: Optional[int] = None
    hedge_conditional_order_id: Optional[str] = None

    @property
    def is_terminal(self) -> bool:
        return False


def _pm() -> PositionManager:
    pm = PositionManager.__new__(PositionManager)
    pm._active_trades = {}
    pm._tp_notified = set()
    pm._notifier = MagicMock()
    pm._notifier.take_profit_hit = AsyncMock()
    # Mock close_trade so we can assert it without running the full close path.
    pm.close_trade = AsyncMock()
    return pm


# --------------------------------------------------------------------------
# _format_close_source mapping
# --------------------------------------------------------------------------

def test_format_close_source_stop_loss():
    assert _format_close_source("stop_loss") == "stop loss"


def test_format_close_source_trailing_stop():
    assert _format_close_source("trailing_stop") == "trailing stop"


def test_format_close_source_liquidation():
    assert _format_close_source("liquidation") == "liquidation"


def test_format_close_source_external():
    assert _format_close_source("external_close") == "external close"


def test_format_close_source_tp_levels():
    assert _format_close_source("tp_1") == "TP1"
    assert _format_close_source("tp_2") == "TP2"
    assert _format_close_source("tp_3") == "TP3"
    assert _format_close_source("tp_10") == "TP10"


def test_format_close_source_unknown_reason_passes_through():
    assert _format_close_source("something_weird") == "something_weird"


# --------------------------------------------------------------------------
# _classify_bybit_close_fill — strict: directly fires close_trade
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_loss_fill_calls_close_trade_with_stop_loss():
    pm = _pm()
    trade = _FakeTrade(signal=_FakeSignal(direction="LONG"))
    pm._active_trades[trade.id] = trade
    await pm._classify_bybit_close_fill("oid-sl", {
        "symbol": "BTCUSDT",
        "positionIdx": 1,
        "stopOrderType": "StopLoss",
        "reduceOnly": True,
        "avgPrice": "99.0",
    })
    pm.close_trade.assert_awaited_once_with(trade.id, "stop_loss", 99.0)


@pytest.mark.asyncio
async def test_trailing_stop_fill_calls_close_trade_with_trailing_stop():
    pm = _pm()
    trade = _FakeTrade(signal=_FakeSignal(direction="SHORT"))
    pm._active_trades[trade.id] = trade
    await pm._classify_bybit_close_fill("oid-trail", {
        "symbol": "BTCUSDT",
        "positionIdx": 2,
        "stopOrderType": "TrailingStop",
        "reduceOnly": True,
        "avgPrice": "98.0",
    })
    pm.close_trade.assert_awaited_once_with(trade.id, "trailing_stop", 98.0)


@pytest.mark.asyncio
async def test_liquidation_fill_calls_close_trade_with_liquidation():
    pm = _pm()
    trade = _FakeTrade(signal=_FakeSignal(direction="LONG"))
    trade.avg_entry = 100.0
    pm._active_trades[trade.id] = trade
    await pm._classify_bybit_close_fill("oid-liq", {
        "symbol": "BTCUSDT",
        "positionIdx": 1,
        "execType": "Liquidation",
    })
    pm.close_trade.assert_awaited_once()
    args = pm.close_trade.await_args.args
    assert args[0] == trade.id and args[1] == "liquidation"


@pytest.mark.asyncio
async def test_manual_close_fires_close_trade_external_close():
    """Reduce-only Filled with no stopOrderType = manual close from
    Bybit UI; bot must label it 'external_close'."""
    pm = _pm()
    trade = _FakeTrade(signal=_FakeSignal(direction="LONG"))
    pm._active_trades[trade.id] = trade
    await pm._classify_bybit_close_fill("oid-manual", {
        "symbol": "BTCUSDT",
        "positionIdx": 1,
        "stopOrderType": "",
        "reduceOnly": True,
        "avgPrice": "99.5",
    })
    pm.close_trade.assert_awaited_once_with(trade.id, "external_close", 99.5)


@pytest.mark.asyncio
async def test_partial_tp_fill_notifies_only_does_not_close():
    pm = _pm()
    trade = _FakeTrade(signal=_FakeSignal(direction="LONG"))
    trade.tp_order_ids = ["oid-tp2"]
    pm._active_trades[trade.id] = trade
    await pm._classify_bybit_close_fill("oid-tp2", {
        "symbol": "BTCUSDT",
        "positionIdx": 1,
        "stopOrderType": "Stop",
        "reduceOnly": True,
        "triggerPrice": "103.0",
        "cumExecQty": "0.25",
    })
    pm._notifier.take_profit_hit.assert_awaited_once()
    pm.close_trade.assert_not_called()
    assert 103.0 in trade.tp_hits


@pytest.mark.asyncio
async def test_unrelated_stop_order_does_not_match():
    pm = _pm()
    trade = _FakeTrade(signal=_FakeSignal(direction="LONG"))
    trade.tp_order_ids = ["oid-tp1"]
    pm._active_trades[trade.id] = trade
    await pm._classify_bybit_close_fill("some-other-order", {
        "symbol": "BTCUSDT",
        "positionIdx": 1,
        "stopOrderType": "Stop",
        "reduceOnly": True,
        "triggerPrice": "101.0",
        "cumExecQty": "0.25",
    })
    pm._notifier.take_profit_hit.assert_not_called()
    pm.close_trade.assert_not_called()


@pytest.mark.asyncio
async def test_tp_fill_dedup_does_not_fire_twice():
    pm = _pm()
    trade = _FakeTrade(signal=_FakeSignal(direction="LONG"))
    trade.tp_order_ids = ["oid-tp1"]
    pm._active_trades[trade.id] = trade
    payload = {
        "symbol": "BTCUSDT",
        "positionIdx": 1,
        "stopOrderType": "Stop",
        "reduceOnly": True,
        "triggerPrice": "101.0",
        "cumExecQty": "0.25",
    }
    await pm._classify_bybit_close_fill("oid-tp1", payload)
    await pm._classify_bybit_close_fill("oid-tp1", payload)
    pm._notifier.take_profit_hit.assert_awaited_once()


@pytest.mark.asyncio
async def test_classifier_skips_when_no_matching_active_trade():
    pm = _pm()
    await pm._classify_bybit_close_fill("oid-sl", {
        "symbol": "BTCUSDT",
        "positionIdx": 1,
        "stopOrderType": "StopLoss",
        "reduceOnly": True,
    })
    pm._notifier.take_profit_hit.assert_not_called()
    pm.close_trade.assert_not_called()
