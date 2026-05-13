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
    realized_pnl_usdt_total: float = 0.0
    tp_order_ids: list = field(default_factory=list)
    state: object = MagicMock(value="POSITION_OPEN")
    hedge_trade_id: Optional[int] = None
    hedge_conditional_order_id: Optional[str] = None
    original_force_close_order_id: Optional[str] = None

    @property
    def is_terminal(self) -> bool:
        return False

    def transition(self, _new_state) -> None:
        # No-op in tests; real Trade has a state-machine guard. The
        # _maybe_record_force_close_fill path calls this before
        # close_trade so it must exist on the stub.
        return None


def _pm() -> PositionManager:
    pm = PositionManager.__new__(PositionManager)
    pm._active_trades = {}
    pm._tp_notified = set()
    pm._notifier = MagicMock()
    pm._notifier.take_profit_hit = AsyncMock()
    pm._db = MagicMock()
    pm._db.update_trade = AsyncMock()
    # Mock close_trade so we can assert it without running the full close path.
    pm.close_trade = AsyncMock()
    return pm


# --------------------------------------------------------------------------
# _format_close_source mapping
# --------------------------------------------------------------------------

def test_format_close_source_stop_loss_no_history():
    """Stop-loss fire on a trade that never moved its SL — bare 'SL'."""
    assert _format_close_source("stop_loss") == "SL"


def test_format_close_source_trailing_stop():
    assert _format_close_source("trailing_stop") == "Trailingstop"


def test_format_close_source_liquidation():
    assert _format_close_source("liquidation") == "likvidation"


def test_format_close_source_external():
    assert _format_close_source("external_close") == "extern stängning"


def test_format_close_source_force_close():
    """Tomas 2026-05-09 incident: -2% emergency close needs its own
    Swedish label so the operator sees what Bybit actually did."""
    assert _format_close_source("force_close") == "-2% nödstängning"


def test_format_close_source_tp_levels():
    assert _format_close_source("tp_1") == "TP1"
    assert _format_close_source("tp_2") == "TP2"
    assert _format_close_source("tp_3") == "TP3"
    assert _format_close_source("tp_10") == "TP10"


def test_format_close_source_unknown_reason_passes_through():
    assert _format_close_source("something_weird") == "something_weird"


# --------------------------------------------------------------------------
# Granular SL labels (Tomas 2026-05-08): when the close reason is a
# stop-loss fire, look at the trade's last SL movement to differentiate
# which SL the price hit. Operator can tell apart a real loss-side SL
# fire from a profit-locked SL fire.
# --------------------------------------------------------------------------

class _SLHistoryStub:
    def __init__(self, last_reason):
        self.sl_movement_history = (
            [{"reason": last_reason}] if last_reason else []
        )


def test_format_close_source_stop_loss_at_be_via_tp2_cascade():
    trade = _SLHistoryStub("tp2_hit_sl_to_breakeven")
    assert _format_close_source("stop_loss", trade=trade) == "BE, SL"


def test_format_close_source_stop_loss_at_tp1_via_tp3_cascade():
    trade = _SLHistoryStub("tp3_hit_sl_to_tp1")
    assert _format_close_source("stop_loss", trade=trade) == "TP1, SL"


def test_format_close_source_stop_loss_at_tp4_via_tp6_cascade():
    trade = _SLHistoryStub("tp6_hit_sl_to_tp4")
    assert _format_close_source("stop_loss", trade=trade) == "TP4, SL"


def test_format_close_source_stop_loss_at_be_via_fallback():
    trade = _SLHistoryStub("fallback_be_buffer_at_2pct")
    assert _format_close_source("stop_loss", trade=trade) == "BE, SL"


def test_format_close_source_stop_loss_profitlock_4pct():
    trade = _SLHistoryStub("fallback_profit_lock_1_at_4pct")
    assert _format_close_source("stop_loss", trade=trade) == "Profitlock 4%"


def test_format_close_source_stop_loss_profitlock_5pct():
    trade = _SLHistoryStub("fallback_profit_lock_2_at_5pct")
    assert _format_close_source("stop_loss", trade=trade) == "Profitlock 5%"


def test_format_close_source_stop_loss_history_empty_falls_back_bare():
    """Empty history (no SL moves) — bare 'SL' label."""
    trade = _SLHistoryStub(None)
    assert _format_close_source("stop_loss", trade=trade) == "SL"


def test_format_close_source_trade_none_doesnt_crash():
    """Defensive: passing trade=None is allowed (callers always have
    the trade, but the param is Optional)."""
    assert _format_close_source("stop_loss", trade=None) == "SL"


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


# --------------------------------------------------------------------------
# _maybe_record_force_close_fill — Tomas 2026-05-09 incident (trade 1080).
# The -2% emergency-close fills with stopOrderType="Stop" and order_id NOT
# in tp_order_ids, so _classify_bybit_close_fill falls through. Without
# this handler driving close_trade, the trade stayed in _active_trades
# and poisoned subsequent TP/SL updates with "zero position" Bybit
# rejections. 91% of force-close fires leaked before the fix.
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_force_close_fill_drives_close_trade_with_avg_price():
    pm = _pm()
    trade = _FakeTrade(signal=_FakeSignal(direction="LONG"))
    trade.original_force_close_order_id = "oid-fc"
    pm._active_trades[trade.id] = trade
    await pm._maybe_record_force_close_fill("oid-fc", {
        "avgPrice": "98.0",
        "triggerPrice": "98.1",
    })
    pm.close_trade.assert_awaited_once_with(
        trade.id, reason="force_close", exit_price=98.0,
    )


@pytest.mark.asyncio
async def test_force_close_fill_falls_back_to_trigger_price_when_avg_missing():
    pm = _pm()
    trade = _FakeTrade(signal=_FakeSignal(direction="SHORT"))
    trade.original_force_close_order_id = "oid-fc"
    pm._active_trades[trade.id] = trade
    # avgPrice missing — execution_update path on a conditional fire.
    await pm._maybe_record_force_close_fill("oid-fc", {
        "triggerPrice": "102.5",
    })
    pm.close_trade.assert_awaited_once_with(
        trade.id, reason="force_close", exit_price=102.5,
    )


@pytest.mark.asyncio
async def test_force_close_fill_unrelated_order_id_is_noop():
    pm = _pm()
    trade = _FakeTrade(signal=_FakeSignal(direction="LONG"))
    trade.original_force_close_order_id = "oid-fc"
    pm._active_trades[trade.id] = trade
    await pm._maybe_record_force_close_fill("some-other-order", {
        "avgPrice": "99.0",
    })
    pm.close_trade.assert_not_called()


# --------------------------------------------------------------------------
# Regression guard: on_order_update must call
# _maybe_record_force_close_fill with the correct argument count.
# 2026-05-12: commit c5bc338 changed the function signature from
# (order_id) to (order_id, data) but missed one of two call sites,
# leaving on_order_update line 1900 with the old one-arg call. Result:
# every Filled WS event raised TypeError, the rest of on_order_update
# (including _classify_bybit_close_fill) never ran. The bug was
# survivable only because on_execution_update is wired in parallel.
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_on_order_update_filled_does_not_raise_argument_error():
    """Construct a Filled order WS event with no matching active trade;
    on_order_update must complete cleanly. Any TypeError /
    missing-argument here flags a callsite-signature mismatch in the
    fill-classification chain.
    """
    pm = _pm()
    pm._fill_data = {}
    pm._fill_events = {}
    pm._db.update_order = AsyncMock()
    # No active trade — every internal helper should look up, find
    # nothing, and return cleanly.
    payload = {
        "orderId": "oid-unmatched",
        "orderStatus": "Filled",
        "symbol": "BTCUSDT",
        "avgPrice": "65000.0",
        "cumExecQty": "0.1",
        "positionIdx": 1,
        "stopOrderType": "Stop",
        "reduceOnly": True,
    }
    await pm.on_order_update(payload)


@pytest.mark.asyncio
async def test_on_order_update_force_close_match_fires_close_trade():
    """End-to-end: a Filled order whose ID matches an active trade's
    original_force_close_order_id must drive close_trade. Verifies
    on_order_update wires through _maybe_record_force_close_fill with
    the data arg so the function receives the avgPrice it needs."""
    pm = _pm()
    pm._fill_data = {}
    pm._fill_events = {}
    pm._db.update_order = AsyncMock()
    trade = _FakeTrade(signal=_FakeSignal(direction="LONG"))
    trade.original_force_close_order_id = "oid-fc-match"
    pm._active_trades[trade.id] = trade
    await pm.on_order_update({
        "orderId": "oid-fc-match",
        "orderStatus": "Filled",
        "symbol": "BTCUSDT",
        "avgPrice": "98.0",
        "cumExecQty": "1.0",
        "positionIdx": 1,
        "stopOrderType": "Stop",
        "reduceOnly": True,
    })
    pm.close_trade.assert_awaited_once_with(
        trade.id, reason="force_close", exit_price=98.0,
    )
