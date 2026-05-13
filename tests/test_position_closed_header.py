"""Tests for the position-closed template header word.

Tomas 2026-05-12: when the -2% emergency conditional fires, the
operator-channel notification must read "Huvudposition stängd av
-2% nödstängning" so the close can be distinguished at a glance from
a hedge close (which goes through _close_hedge_child with its own
template). All other close reasons continue to use "Position stängd".
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


def _trade(symbol="BTCUSDT", direction="LONG"):
    signal = SimpleNamespace(
        symbol=symbol,
        direction=direction,
        signal_type="dynamic",
        channel_name="TestChannel",
    )
    return SimpleNamespace(
        id="1234",
        signal=signal,
        bybit_order_ids=["bybit-1"],
        leverage=10.0,
    )


@pytest.mark.asyncio
async def test_force_close_uses_huvudposition():
    n = _notifier()
    text = await n.position_closed(
        trade=_trade(), exit_price=100.0, qty=1.0,
        result_pct_total=-2.0, result_usdt_total=-20.0,
        close_source="-2% nödstängning",
    )
    assert "Huvudposition stängd av -2% nödstängning" in text
    assert "✅ Position stängd" not in text


@pytest.mark.asyncio
async def test_stop_loss_close_still_uses_position():
    """Regular SL close keeps the standard wording."""
    n = _notifier()
    text = await n.position_closed(
        trade=_trade(), exit_price=99.0, qty=1.0,
        result_pct_total=-1.0, result_usdt_total=-10.0,
        close_source="SL",
    )
    assert "✅ Position stängd av SL" in text
    assert "Huvudposition" not in text


@pytest.mark.asyncio
async def test_trailing_close_uses_position():
    n = _notifier()
    text = await n.position_closed(
        trade=_trade(), exit_price=101.0, qty=1.0,
        result_pct_total=+1.0, result_usdt_total=+10.0,
        close_source="Trailingstop",
    )
    assert "✅ Position stängd av Trailingstop" in text
    assert "Huvudposition" not in text


@pytest.mark.asyncio
async def test_empty_close_source_uses_position():
    n = _notifier()
    text = await n.position_closed(
        trade=_trade(), exit_price=100.0, qty=1.0,
        result_pct_total=0.0, result_usdt_total=0.0,
        close_source="",
    )
    assert text.startswith("✅ Position stängd")
    assert "Huvudposition" not in text
