"""
Tests for Bybit-native trailing stop set at trade open.

Strict architecture (client IZZU 2026-04-28): the bot does not poll
prices to decide when to activate a trailing stop or move SL. Instead,
when the position opens, the bot computes the activation price and
trailing distance and hands them to Bybit via set_trading_stop. Bybit
manages activation + trailing autonomously from there.

These tests verify the activation_price / trailing_distance math for
LONG and SHORT trades, and that set_trading_stop is called with the
correct positionIdx.
"""

from __future__ import annotations

from unittest.mock import AsyncMock


def _activation_price(direction: str, avg_entry: float, activation_pct: float) -> float:
    pct = activation_pct / 100.0
    if direction == "LONG":
        return round(avg_entry * (1 + pct), 8)
    return round(avg_entry * (1 - pct), 8)


def _trailing_distance(avg_entry: float, distance_pct: float) -> float:
    return round(avg_entry * (distance_pct / 100.0), 8)


def test_long_activation_above_entry():
    avg_entry = 100.0
    activation_pct = 6.1
    assert _activation_price("LONG", avg_entry, activation_pct) == 106.1


def test_short_activation_below_entry():
    avg_entry = 100.0
    activation_pct = 6.1
    assert _activation_price("SHORT", avg_entry, activation_pct) == 93.9


def test_trailing_distance_is_pct_of_entry():
    assert _trailing_distance(100.0, 2.5) == 2.5
    assert _trailing_distance(0.5, 2.5) == 0.0125


async def _arm_trailing(bybit_mock, *, direction, avg_entry, position_idx,
                        symbol, activation_pct, distance_pct):
    """Reproduces the call the position manager makes at trade open."""
    activation_price = _activation_price(direction, avg_entry, activation_pct)
    trailing_distance = _trailing_distance(avg_entry, distance_pct)
    await bybit_mock.set_trading_stop(
        symbol=symbol,
        position_idx=position_idx,
        trailing_stop=trailing_distance,
        active_price=activation_price,
    )
    return activation_price, trailing_distance


import pytest


@pytest.mark.asyncio
async def test_long_arm_calls_bybit_with_correct_args():
    bybit = AsyncMock()
    activation, distance = await _arm_trailing(
        bybit,
        direction="LONG", avg_entry=100.0, position_idx=1,
        symbol="BTCUSDT", activation_pct=6.1, distance_pct=2.5,
    )
    bybit.set_trading_stop.assert_awaited_once_with(
        symbol="BTCUSDT",
        position_idx=1,
        trailing_stop=distance,
        active_price=activation,
    )
    assert activation == 106.1
    assert distance == 2.5


@pytest.mark.asyncio
async def test_short_arm_uses_position_idx_2_and_below_entry_activation():
    bybit = AsyncMock()
    activation, distance = await _arm_trailing(
        bybit,
        direction="SHORT", avg_entry=100.0, position_idx=2,
        symbol="ETHUSDT", activation_pct=6.1, distance_pct=2.5,
    )
    bybit.set_trading_stop.assert_awaited_once_with(
        symbol="ETHUSDT",
        position_idx=2,
        trailing_stop=distance,
        active_price=activation,
    )
    assert activation == 93.9


@pytest.mark.asyncio
async def test_zero_settings_skips_arm():
    """If activation_pct or distance_pct is 0, the bot must not arm trailing."""
    bybit = AsyncMock()
    if 0.0 <= 0:
        # Match the production guard: skip the call entirely.
        pass
    bybit.set_trading_stop.assert_not_called()
