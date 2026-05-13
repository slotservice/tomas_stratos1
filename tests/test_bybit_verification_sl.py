"""Tests for the Bybit SL verification (step 9a).

Tomas 2026-05-12 spec: every value displayed in the operator-channel
templates must be Bybit-verified, not bot-trusted. After
set_trading_stop pushes the SL, the bot reads the position back via
get_position and confirms Bybit's stopLoss field reflects what was
sent. Position öppnad annotates the SL line with the result.

Three outcomes:
  - match within 0.1% tolerance -> sl_bybit_verified = True
  - mismatch / Bybit value missing -> sl_bybit_verified = False
  - read-back errored -> sl_bybit_verified = None (no annotation)
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from types import SimpleNamespace

import pytest

from telegram.notifier import TelegramNotifier


def _notifier() -> TelegramNotifier:
    n = TelegramNotifier.__new__(TelegramNotifier)
    n._send_notify = AsyncMock(side_effect=lambda text: text)
    return n


def _trade_for_position_opened(
    sl_verified=None,
    sl_bybit_value=None,
    sl_price=98.0,
):
    """Stub trade carrying the fields the position_opened template
    reads, including the new SL verification attributes."""
    signal = SimpleNamespace(
        symbol="BTCUSDT",
        direction="LONG",
        entry=100.0,
        tps=[103.0, 105.0],
        sl=98.0,
        signal_type="dynamic",
        channel_name="TestChannel",
    )
    return SimpleNamespace(
        id="42",
        signal=signal,
        sl_price=sl_price,
        sl_bybit_verified=sl_verified,
        sl_bybit_value=sl_bybit_value,
        entry1_fill_price=100.0,
        entry2_fill_price=None,
        leverage=10.0,
        margin=10.0,
        bybit_order_ids=["bybit-1"],
    )


@pytest.mark.asyncio
async def test_sl_verified_annotates_template():
    n = _notifier()
    text = await n.position_opened(
        trade=_trade_for_position_opened(
            sl_verified=True, sl_bybit_value=98.0,
        ),
        signal=_trade_for_position_opened().signal,
    )
    # SL line should carry "Bybit verifierad" marker.
    assert "Bybit verifierad" in text
    assert "EJ MATCH" not in text


@pytest.mark.asyncio
async def test_sl_mismatch_annotates_with_bybit_value():
    n = _notifier()
    text = await n.position_opened(
        trade=_trade_for_position_opened(
            sl_verified=False, sl_bybit_value=97.5, sl_price=98.0,
        ),
        signal=_trade_for_position_opened().signal,
    )
    assert "EJ MATCH" in text
    assert "97.5" in text


@pytest.mark.asyncio
async def test_sl_missing_on_bybit_annotates_ej_satt():
    n = _notifier()
    text = await n.position_opened(
        trade=_trade_for_position_opened(
            sl_verified=False, sl_bybit_value=None, sl_price=98.0,
        ),
        signal=_trade_for_position_opened().signal,
    )
    assert "EJ SATT" in text


@pytest.mark.asyncio
async def test_sl_verification_skipped_when_none_does_not_annotate():
    """Trade where verification was never attempted (e.g. read-back
    errored, or set_trading_stop itself failed) — template should
    fall back to the plain SL line with no annotation."""
    n = _notifier()
    text = await n.position_opened(
        trade=_trade_for_position_opened(
            sl_verified=None, sl_bybit_value=None,
        ),
        signal=_trade_for_position_opened().signal,
    )
    assert "Bybit verifierad" not in text
    assert "EJ MATCH" not in text
    assert "EJ SATT" not in text


# --- Trade dataclass defaults ----------------------------------------------

def test_trade_default_sl_verification_state_is_unset():
    from core.models import Trade, TradeState
    t = Trade(id="1", state=TradeState.PENDING)
    assert t.sl_bybit_verified is None
    assert t.sl_bybit_value is None
    assert t.leverage_bybit_verified is None
    assert t.leverage_bybit_value is None
    assert t.tp_bybit_verified == {}
