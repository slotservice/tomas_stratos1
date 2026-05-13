"""Tests for the Order placerad template's "(begäran)" markers (9d).

Tomas 2026-05-12 spec: every value in a notification must be either
Bybit-verified or explicitly marked as "request, awaiting verification".
Order placerad fires BEFORE the market-order fill, so entry/TP/SL/
leverage/IM are still the bot's request — they get a "(begäran)"
marker here. The Bybit-verified versions appear in Position öppnad.
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


def _signal():
    return SimpleNamespace(
        symbol="BTCUSDT",
        direction="LONG",
        entry=100.0,
        tps=[103.0, 105.0],
        sl=98.0,
        signal_type="dynamic",
        channel_name="TestChannel",
    )


@pytest.mark.asyncio
async def test_entry_is_marked_as_request():
    n = _notifier()
    text = await n.order_placed(
        signal=_signal(),
        leverage=10.0, im=20.0,
        entry1=100.0, entry2=100.0,
        bot_id="42", bybit_id="bybit-1",
    )
    assert "Entry: 100.0 (begäran)" in text


@pytest.mark.asyncio
async def test_leverage_and_im_marked_as_request():
    n = _notifier()
    text = await n.order_placed(
        signal=_signal(),
        leverage=15.0, im=25.5,
        entry1=100.0, entry2=100.0,
        bot_id="42", bybit_id="bybit-1",
    )
    assert "x15.0 (begäran)" in text
    assert "IM: 25.50 USDT (begäran)" in text


@pytest.mark.asyncio
async def test_status_line_explains_verification_arrives_in_position_oppnad():
    n = _notifier()
    text = await n.order_placed(
        signal=_signal(),
        leverage=10.0, im=20.0,
        entry1=100.0, entry2=100.0,
        bot_id="42", bybit_id="bybit-1",
    )
    assert "Bybit-bekräftad, väntar fill" in text
    assert "verifierad data i Position öppnad" in text


@pytest.mark.asyncio
async def test_bybit_order_id_is_unmarked_already_confirmed():
    """The Bybit order ID itself IS confirmed by Bybit (returned on
    the order-create response), so it doesn't need a (begäran)
    marker on its line."""
    n = _notifier()
    text = await n.order_placed(
        signal=_signal(),
        leverage=10.0, im=20.0,
        entry1=100.0, entry2=100.0,
        bot_id="42", bybit_id="bybit-abc123",
    )
    assert "Order-ID Bybit: bybit-abc123" in text
    # No "(begäran)" on the order-id line.
    last_line = [
        ln for ln in text.splitlines() if "Order-ID Bybit" in ln
    ][0]
    assert "(begäran)" not in last_line


@pytest.mark.asyncio
async def test_two_leg_entries_each_marked_as_request():
    """Future-proof: when entry1 != entry2 both lines carry the marker."""
    n = _notifier()
    text = await n.order_placed(
        signal=_signal(),
        leverage=10.0, im=20.0,
        entry1=100.0, entry2=99.0,
        bot_id="42", bybit_id="bybit-1",
    )
    assert "Entry1: 100.0 (begäran)" in text
    assert "Entry2: 99.0 (begäran)" in text
