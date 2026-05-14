"""
Tests for the duplicate signal detector (core/duplicate_detector.py).

Covers three outcomes:
  - "new"     -> no existing trade, proceed
  - "blocked" -> duplicate within 5%, reject
  - "update"  -> entry diff >5%, update existing trade's TP/SL
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from core.duplicate_detector import DuplicateDetector, DuplicateCheckResult
from core.signal_parser import ParsedSignal


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_signal(
    symbol: str = "BTCUSDT",
    entry: float = 65000.0,
    channel_name: str = "TestGroup",
) -> ParsedSignal:
    """Create a minimal ParsedSignal for testing."""
    return ParsedSignal(
        symbol=symbol,
        direction="LONG",
        entry=entry,
        tps=[66000.0],
        sl=64000.0,
        channel_name=channel_name,
    )


def _make_db(active_trades: list[dict] | None = None) -> MagicMock:
    """
    Create a mock database.  get_trades_by_symbol returns
    *active_trades* (with 'state' field for filtering).
    """
    db = MagicMock()
    trades = active_trades or []
    db.get_trades_by_symbol = AsyncMock(return_value=trades)
    return db


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_opposite_direction_blocked():
    """Client 2026-04-28: bot trades only one direction per symbol.
    A LONG signal arriving while an active SHORT exists must be blocked,
    regardless of how close the entry is."""
    existing = [
        {
            "id": 1,
            "state": "POSITION_OPEN",
            "avg_entry": 100.0,
            "direction": "SHORT",
        },
    ]
    db = _make_db(active_trades=existing)
    detector = DuplicateDetector(db, threshold_pct=5.0, lookback_hours=24)

    # A LONG signal at a totally different price (which would normally
    # be 'update') is still blocked because of the direction mismatch.
    long_signal = ParsedSignal(
        symbol="BTCUSDT",
        direction="LONG",
        entry=110.0,  # >5% diff — would be 'update' if same direction
        tps=[120.0],
        sl=105.0,
    )
    result = await detector.check(long_signal)
    assert result.is_blocked
    assert "Opposite direction" in result.reason


@pytest.mark.asyncio
async def test_same_direction_within_5pct_still_blocked():
    """Same direction + within 5% entry diff — existing block path
    still applies (regression check)."""
    existing = [
        {
            "id": 1,
            "state": "POSITION_OPEN",
            "avg_entry": 100.0,
            "direction": "LONG",
        },
    ]
    db = _make_db(active_trades=existing)
    detector = DuplicateDetector(db, threshold_pct=5.0, lookback_hours=24)

    new = ParsedSignal(
        symbol="BTCUSDT", direction="LONG", entry=101.0,
        tps=[105.0], sl=98.0,
    )
    result = await detector.check(new)
    assert result.is_blocked
    assert "Duplicate" in result.reason


@pytest.mark.asyncio
async def test_no_duplicate_first_signal():
    """First signal for a symbol -> action='new'."""
    db = _make_db(active_trades=[])
    detector = DuplicateDetector(db, threshold_pct=5.0, lookback_hours=24)

    signal = _make_signal(symbol="BTCUSDT", entry=65000.0)
    result = await detector.check(signal)

    assert result.is_new
    assert result.action == "new"


@pytest.mark.asyncio
async def test_duplicate_within_5pct():
    """
    Entry within 5% of existing trade -> action='blocked'.

    Existing at 65000, new at 66000:
    diff = |66000 - 65000| / 65000 * 100 = 1.54% < 5% -> blocked
    """
    existing = [
        {"id": 1, "state": "POSITION_OPEN", "avg_entry": 65000.0},
    ]
    db = _make_db(active_trades=existing)
    detector = DuplicateDetector(db, threshold_pct=5.0, lookback_hours=24)

    signal = _make_signal(symbol="BTCUSDT", entry=66000.0)
    result = await detector.check(signal)

    assert result.is_blocked
    assert result.action == "blocked"
    assert "Duplicate" in result.reason


@pytest.mark.asyncio
async def test_update_beyond_5pct():
    """
    Entry more than 5% away from existing trade -> action='update'.
    Should NOT open a new trade. Should update TP/SL on existing.

    Existing at 65000, new at 70000:
    diff = |70000 - 65000| / 65000 * 100 = 7.69% > 5% -> update
    """
    existing = [
        {"id": 1, "state": "POSITION_OPEN", "avg_entry": 65000.0},
    ]
    db = _make_db(active_trades=existing)
    detector = DuplicateDetector(db, threshold_pct=5.0, lookback_hours=24)

    signal = _make_signal(symbol="BTCUSDT", entry=70000.0)
    result = await detector.check(signal)

    assert result.is_update
    assert result.action == "update"
    assert result.existing_trade is not None
    assert result.existing_trade["id"] == 1
    assert "Updating TP/SL" in result.reason


@pytest.mark.asyncio
async def test_different_symbol_not_duplicate():
    """
    Active trade on BTCUSDT should not affect ETHUSDT signals.
    """
    db = _make_db(active_trades=[])  # DB returns nothing for ETHUSDT
    detector = DuplicateDetector(db, threshold_pct=5.0, lookback_hours=24)

    signal = _make_signal(symbol="ETHUSDT", entry=3200.0)
    result = await detector.check(signal)

    assert result.is_new


@pytest.mark.asyncio
async def test_blocked_has_existing_trade_reference():
    """Blocked result should include a reference to the existing trade."""
    existing = [
        {"id": 42, "state": "BREAKEVEN_ACTIVE", "avg_entry": 65000.0},
    ]
    db = _make_db(active_trades=existing)
    detector = DuplicateDetector(db, threshold_pct=5.0, lookback_hours=24)

    signal = _make_signal(symbol="BTCUSDT", entry=65500.0)
    result = await detector.check(signal)

    assert result.is_blocked
    assert result.existing_trade["id"] == 42


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state",
    [
        "PROFIT_LOCK_1_ACTIVE", "PROFIT_LOCK_2_ACTIVE",
        "SL_MOVED_TO_TP1", "SL_MOVED_TO_TP2",
        "TRAILING_ARMED", "TRAILING_UPDATED",
        "HEDGE_ARMED", "PROTECTION_FAILED",
    ],
)
async def test_mid_lifecycle_states_still_seen_as_active(state):
    """Regression (AIGENSYN, live 2026-05-14): _fetch_active_trades used a
    hardcoded whitelist of active states that drifted out of sync with
    the TradeState enum. A trade that moved its SL into profit-lock
    became invisible to the detector, so the same signal forwarded
    minutes later opened a duplicate position. Every non-terminal state
    must still be treated as an active trade."""
    existing = [{"id": 1, "state": state, "avg_entry": 65000.0}]
    db = _make_db(active_trades=existing)
    detector = DuplicateDetector(db, threshold_pct=5.0, lookback_hours=24)

    signal = _make_signal(symbol="BTCUSDT", entry=65500.0)  # 0.77% diff
    result = await detector.check(signal)

    assert result.is_blocked, f"trade in {state} must still count as active"


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["CLOSED", "CANCELLED", "ERROR"])
async def test_terminal_states_not_treated_as_active(state):
    """Terminal trades must NOT block a new signal — a closed trade is
    not a duplicate."""
    existing = [{"id": 1, "state": state, "avg_entry": 65000.0}]
    db = _make_db(active_trades=existing)
    detector = DuplicateDetector(db, threshold_pct=5.0, lookback_hours=24)

    signal = _make_signal(symbol="BTCUSDT", entry=65500.0)
    result = await detector.check(signal)

    assert result.is_new, f"trade in terminal state {state} must not block"
