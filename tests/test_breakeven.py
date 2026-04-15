"""
Tests for the break-even manager (managers/breakeven_manager.py).

Covers:
- BE triggers at the configured threshold (2.3%) for both LONG and SHORT.
- BE does NOT trigger below the threshold.
- The buffer is applied correctly to the BE price.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from managers.breakeven_manager import BreakevenManager


# ---------------------------------------------------------------------------
# Minimal stand-ins for Trade/Signal/TradeState used in tests.
# We use these instead of importing the real models so the tests are
# isolated from model changes.
# ---------------------------------------------------------------------------

@dataclass
class _FakeSignal:
    symbol: str = "BTCUSDT"
    direction: str = "LONG"


@dataclass
class _FakeTrade:
    id: str = "test-trade-1"
    signal: Optional[_FakeSignal] = None
    avg_entry: Optional[float] = None
    be_price: Optional[float] = None
    sl_price: Optional[float] = None
    state: str = "POSITION_OPEN"

    def transition(self, new_state) -> None:
        self.state = new_state.value if hasattr(new_state, "value") else new_state


def _make_settings(trigger_pct: float = 2.3, buffer_pct: float = 0.15):
    """Create a mock BreakevenSettings."""
    s = MagicMock()
    s.trigger_pct = trigger_pct
    s.buffer_pct = buffer_pct
    return s


def _make_manager(
    trigger_pct: float = 2.3,
    buffer_pct: float = 0.15,
) -> tuple[BreakevenManager, MagicMock, MagicMock, MagicMock]:
    """
    Create a BreakevenManager with mocked dependencies.

    Returns (manager, mock_bybit, mock_notifier, mock_db).
    """
    settings = _make_settings(trigger_pct, buffer_pct)
    bybit = MagicMock()
    bybit.set_trading_stop = AsyncMock()
    notifier = MagicMock()
    notifier.send = AsyncMock()
    db = MagicMock()
    db.update_trade = AsyncMock()
    db.log_event = AsyncMock()

    mgr = BreakevenManager(
        settings=settings,
        bybit=bybit,
        notifier=notifier,
        db=db,
    )
    return mgr, bybit, notifier, db


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestBreakevenTrigger:
    """Test that BE triggers at the correct threshold."""

    @pytest.mark.asyncio
    async def test_be_triggers_at_2_3_pct_long(self):
        """
        LONG trade: avg_entry=100, price=102.3 -> move = +2.3%.
        Should trigger BE.
        """
        mgr, bybit, notifier, db = _make_manager()
        trade = _FakeTrade(
            signal=_FakeSignal(symbol="BTCUSDT", direction="LONG"),
            avg_entry=100.0,
        )

        # Price slightly above +2.3% from entry (102.31 avoids float rounding).
        result = await mgr.check_and_apply(trade, current_price=102.31)

        assert result is True
        assert trade.be_price is not None
        bybit.set_trading_stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_be_triggers_at_2_3_pct_short(self):
        """
        SHORT trade: avg_entry=100, price=97.69 -> move = +2.31% profit.
        Should trigger BE.
        """
        mgr, bybit, notifier, db = _make_manager()
        trade = _FakeTrade(
            signal=_FakeSignal(symbol="ETHUSDT", direction="SHORT"),
            avg_entry=100.0,
        )

        # Price slightly beyond -2.3% from entry (avoids float rounding).
        result = await mgr.check_and_apply(trade, current_price=97.69)

        assert result is True
        assert trade.be_price is not None
        bybit.set_trading_stop.assert_awaited_once()


class TestBreakevenNotTriggered:
    """Test that BE does NOT trigger below the threshold."""

    @pytest.mark.asyncio
    async def test_be_not_triggered_below_threshold(self):
        """
        LONG trade: avg_entry=100, price=102.0 -> move = +2.0%.
        Below 2.3% threshold -> should NOT trigger.
        """
        mgr, bybit, notifier, db = _make_manager()
        trade = _FakeTrade(
            signal=_FakeSignal(symbol="BTCUSDT", direction="LONG"),
            avg_entry=100.0,
        )

        result = await mgr.check_and_apply(trade, current_price=102.0)

        assert result is False
        assert trade.be_price is None
        bybit.set_trading_stop.assert_not_awaited()


class TestBreakevenBuffer:
    """Test that the buffer is applied correctly to the BE price."""

    @pytest.mark.asyncio
    async def test_be_buffer_applied_correctly(self):
        """
        LONG: avg_entry=100, buffer_pct=0.15.
        BE price should be 100 + (100 * 0.15 / 100) = 100 + 0.15 = 100.15.
        """
        mgr, bybit, notifier, db = _make_manager(
            trigger_pct=2.3,
            buffer_pct=0.15,
        )
        trade = _FakeTrade(
            signal=_FakeSignal(symbol="BTCUSDT", direction="LONG"),
            avg_entry=100.0,
        )

        await mgr.check_and_apply(trade, current_price=103.0)

        # BE price = 100 + (100 * 0.15 / 100) = 100.15
        assert trade.be_price == pytest.approx(100.15, abs=0.01)

    @pytest.mark.asyncio
    async def test_be_buffer_applied_correctly_short(self):
        """
        SHORT: avg_entry=100, buffer_pct=0.15.
        BE price should be 100 - 0.15 = 99.85.
        """
        mgr, bybit, notifier, db = _make_manager(
            trigger_pct=2.3,
            buffer_pct=0.15,
        )
        trade = _FakeTrade(
            signal=_FakeSignal(symbol="ETHUSDT", direction="SHORT"),
            avg_entry=100.0,
        )

        await mgr.check_and_apply(trade, current_price=97.0)

        # BE price = 100 - (100 * 0.15 / 100) = 99.85
        assert trade.be_price == pytest.approx(99.85, abs=0.01)

    @pytest.mark.asyncio
    async def test_be_not_applied_twice(self):
        """Once BE is set, subsequent calls should be no-ops."""
        mgr, bybit, notifier, db = _make_manager()
        trade = _FakeTrade(
            signal=_FakeSignal(symbol="BTCUSDT", direction="LONG"),
            avg_entry=100.0,
            be_price=100.15,  # Already set.
        )

        result = await mgr.check_and_apply(trade, current_price=105.0)

        assert result is False
        bybit.set_trading_stop.assert_not_awaited()
