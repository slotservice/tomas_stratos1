"""
Tests for the Bybit-driven close-classification pipeline.

Strict architecture (client IZZU 2026-04-28): every close reason and
every "POSITION CLOSED - X" notification is set by the Bybit fill event,
never by bot inference. The bot reads ``stopOrderType`` and ``execType``
on the order-update WebSocket message and records the matching reason on
the trade. When ``on_position_update`` later reports size=0, it uses the
recorded reason verbatim.

These tests cover the classifier (``_classify_bybit_close_fill``) and the
``_format_close_source`` mapping to the human-readable suffix.
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
    _pending_close_reason: Optional[str] = None

    @property
    def is_terminal(self) -> bool:
        return False


def _pm() -> PositionManager:
    pm = PositionManager.__new__(PositionManager)
    pm._active_trades = {}
    pm._tp_notified = set()
    pm._notifier = MagicMock()
    pm._notifier.take_profit_hit = AsyncMock()
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
# _classify_bybit_close_fill
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_loss_fill_records_reason_stop_loss():
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
    assert trade._pending_close_reason == "stop_loss"


@pytest.mark.asyncio
async def test_trailing_stop_fill_records_reason_trailing_stop():
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
    assert trade._pending_close_reason == "trailing_stop"


@pytest.mark.asyncio
async def test_liquidation_fill_records_reason_liquidation():
    pm = _pm()
    trade = _FakeTrade(signal=_FakeSignal(direction="LONG"))
    pm._active_trades[trade.id] = trade
    await pm._classify_bybit_close_fill("oid-liq", {
        "symbol": "BTCUSDT",
        "positionIdx": 1,
        "execType": "Liquidation",
    })
    assert trade._pending_close_reason == "liquidation"


@pytest.mark.asyncio
async def test_partial_tp_fill_records_tp_level_and_fires_notification():
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
    assert trade._pending_close_reason == "tp_2"
    assert 103.0 in trade.tp_hits


@pytest.mark.asyncio
async def test_unrelated_stop_order_does_not_match():
    """A 'Stop' fill whose order_id we did not place must not be treated
    as a TP fill."""
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
    assert trade._pending_close_reason is None


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
    # No trade in _active_trades — nothing should crash, nothing recorded.
    pm._notifier.take_profit_hit.assert_not_called()
