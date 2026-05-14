"""
Tests for PositionManager.handle_trade_update (Tomas 2026-05-15).

"OPEN ENTRY / NEW TP" follow-up messages carry a symbol + new TPs but
no direction word. handle_trade_update routes them:
  Case A — a trade for the symbol is running -> _update_existing_trade,
           keeping the original group name.
  Case B — no running trade -> a fresh fixed-mode signal via
           process_signal, with direction inferred from TP-vs-entry.
           An ambiguous straddle is skipped, never guessed.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from core.signal_parser import ParsedSignal, TradeUpdate
from managers.position_manager import PositionManager


def _pm() -> PositionManager:
    pm = PositionManager.__new__(PositionManager)
    pm._active_trades = {}
    pm._update_existing_trade = AsyncMock()
    pm.process_signal = AsyncMock()
    return pm


def _running_trade(symbol="DOGEUSDT", direction="SHORT",
                   entry=0.118, channel_name="Cryptonus Trade"):
    """A minimal in-memory Trade stand-in for pm._active_trades."""
    tr = MagicMock()
    tr.is_terminal = False
    tr.id = 4242
    tr.state.value = "PROFIT_LOCK_1_ACTIVE"
    tr.signal = ParsedSignal(
        symbol=symbol, direction=direction, entry=entry,
        tps=[0.115, 0.112, 0.110], sl=0.124, channel_name=channel_name,
    )
    return tr


@pytest.mark.asyncio
async def test_case_a_routes_to_update_existing_keeps_original_channel():
    """A running trade exists -> route to _update_existing_trade with
    the running trade's direction and ORIGINAL group name."""
    pm = _pm()
    pm._active_trades["4242"] = _running_trade(
        direction="SHORT", channel_name="Cryptonus Trade"
    )
    update = TradeUpdate(
        symbol="DOGEUSDT", entry=0.11797,
        tps=[0.11234, 0.11004, 0.10775], sl=None,
    )
    await pm.handle_trade_update(update, channel_name="Some Forwarder")

    pm._update_existing_trade.assert_awaited_once()
    pm.process_signal.assert_not_awaited()
    synthetic, row = pm._update_existing_trade.await_args.args
    assert synthetic.direction == "SHORT"          # from the running trade
    assert synthetic.channel_name == "Cryptonus Trade"  # ORIGINAL group
    assert synthetic.tps == [0.11234, 0.11004, 0.10775]
    assert row["id"] == 4242


@pytest.mark.asyncio
async def test_case_b_no_running_trade_infers_short():
    """No running trade, all TPs below entry -> fresh signal, SHORT,
    sl=None (process_signal applies fixed-mode x10/-3%)."""
    pm = _pm()
    update = TradeUpdate(
        symbol="DOGEUSDT", entry=0.11797,
        tps=[0.11234, 0.11004, 0.10775], sl=None,
    )
    await pm.handle_trade_update(update, channel_name="Cryptonus Trade")

    pm.process_signal.assert_awaited_once()
    pm._update_existing_trade.assert_not_awaited()
    synthetic = pm.process_signal.await_args.args[0]
    assert synthetic.direction == "SHORT"
    assert synthetic.entry == 0.11797
    assert synthetic.sl is None
    assert synthetic.channel_name == "Cryptonus Trade"


@pytest.mark.asyncio
async def test_case_b_no_running_trade_infers_long():
    """No running trade, all TPs above entry -> fresh signal, LONG."""
    pm = _pm()
    update = TradeUpdate(
        symbol="IMXUSDT", entry=0.2021,
        tps=[0.2100, 0.2200, 0.2300], sl=None,
    )
    await pm.handle_trade_update(update, channel_name="Cryptonus Trade")

    pm.process_signal.assert_awaited_once()
    synthetic = pm.process_signal.await_args.args[0]
    assert synthetic.direction == "LONG"


@pytest.mark.asyncio
async def test_case_b_ambiguous_straddle_is_skipped():
    """TPs straddling the entry -> direction is ambiguous -> skip,
    never guess. Neither path is taken."""
    pm = _pm()
    update = TradeUpdate(
        symbol="DOGEUSDT", entry=0.118,
        tps=[0.125, 0.118, 0.110], sl=None,  # one above, one below
    )
    await pm.handle_trade_update(update, channel_name="Cryptonus Trade")

    pm.process_signal.assert_not_awaited()
    pm._update_existing_trade.assert_not_awaited()


@pytest.mark.asyncio
async def test_case_b_no_entry_is_skipped():
    """No entry price in the update and no running trade -> nothing to
    infer from -> skip."""
    pm = _pm()
    update = TradeUpdate(symbol="DOGEUSDT", entry=0.0, tps=[0.11, 0.10], sl=None)
    await pm.handle_trade_update(update, channel_name="Cryptonus Trade")

    pm.process_signal.assert_not_awaited()
    pm._update_existing_trade.assert_not_awaited()
