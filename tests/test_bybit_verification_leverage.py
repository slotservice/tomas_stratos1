"""Tests for the Bybit leverage verification (step 9c).

After the entry order fills and the position is open on Bybit, the
bot fetches get_position(symbol, side) and confirms Bybit's leverage
field matches what set_leverage was called with. Tolerance 0.5
absorbs Bybit's typical leverage step rounding.

Position öppnad annotates the Hävstång line with the result.
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


def _trade(lev_verified=None, lev_bybit_value=None, leverage=10.0):
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
        sl_price=98.0,
        sl_bybit_verified=None,
        sl_bybit_value=None,
        leverage=leverage,
        leverage_bybit_verified=lev_verified,
        leverage_bybit_value=lev_bybit_value,
        tp_bybit_verified={},
        entry1_fill_price=100.0,
        entry2_fill_price=None,
        margin=10.0,
        bybit_order_ids=["bybit-1"],
    )


@pytest.mark.asyncio
async def test_leverage_verified_annotates_template():
    n = _notifier()
    text = await n.position_opened(
        trade=_trade(lev_verified=True, lev_bybit_value=10.0, leverage=10.0),
        signal=_trade().signal,
    )
    assert "Hävstång" in text
    assert "x10.0 ◉ Verifierad" in text
    assert "EJ MATCH" not in text


@pytest.mark.asyncio
async def test_leverage_mismatch_shows_bybit_value():
    n = _notifier()
    text = await n.position_opened(
        trade=_trade(lev_verified=False, lev_bybit_value=20.0, leverage=10.0),
        signal=_trade().signal,
    )
    assert "⊘ Bybit: x20.0 (EJ MATCH)" in text


@pytest.mark.asyncio
async def test_leverage_missing_on_bybit_shows_unset():
    n = _notifier()
    text = await n.position_opened(
        trade=_trade(lev_verified=False, lev_bybit_value=None, leverage=10.0),
        signal=_trade().signal,
    )
    assert "⊘ EJ MATCH" in text


@pytest.mark.asyncio
async def test_leverage_verification_skipped_when_none_no_annotation():
    n = _notifier()
    text = await n.position_opened(
        trade=_trade(lev_verified=None, lev_bybit_value=None, leverage=10.0),
        signal=_trade().signal,
    )
    # Falls back to plain "Hävstång (...): x10.0" with no Bybit marker.
    assert "x10.0" in text
    # No annotation either way.
    assert "Verifierad" not in text
    assert "EJ MATCH" not in text
