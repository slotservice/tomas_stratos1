"""Phase 6.B.2 — idempotency tests (client 2026-05-02 audit point #9).

"Send the same signal more than once. It must never create:
   - duplicate trades
   - duplicate TP/SL
   - duplicate Telegram messages"

Verifies the duplicate-detector blocks repeats, regardless of how
many times the same signal arrives. Mock DB pattern (matches the
existing tests/test_duplicate.py setup).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from core.duplicate_detector import DuplicateDetector
from core.signal_parser import ParsedSignal


def _signal(symbol="BTCUSDT", direction="LONG", entry=76000.0):
    return ParsedSignal(
        symbol=symbol,
        direction=direction,
        entry=entry,
        tps=[77000.0, 78000.0],
        sl=74000.0 if direction == "LONG" else 78000.0,
    )


def _db_with(active_trades: list[dict]) -> MagicMock:
    db = MagicMock()
    db.get_trades_by_symbol = AsyncMock(return_value=active_trades)
    return db


@pytest.mark.asyncio
async def test_same_signal_blocked_first_time():
    """Same symbol + same entry as an active trade -> blocked."""
    existing = [{
        "id": 1, "state": "POSITION_OPEN",
        "avg_entry": 76000.0, "entry_price": 76000.0,
        "direction": "LONG",
    }]
    det = DuplicateDetector(
        db=_db_with(existing), threshold_pct=5.0, lookback_hours=24,
    )
    result = await det.check(_signal(entry=76050.0))  # 0.07% diff
    assert result.is_blocked is True


@pytest.mark.asyncio
async def test_same_signal_repeated_n_times_always_blocked():
    """Send the same signal 10 times — every single one must block."""
    existing = [{
        "id": 1, "state": "POSITION_OPEN",
        "avg_entry": 76000.0, "entry_price": 76000.0,
        "direction": "LONG",
    }]
    det = DuplicateDetector(
        db=_db_with(existing), threshold_pct=5.0, lookback_hours=24,
    )
    sig = _signal(entry=76000.0)
    for _ in range(10):
        result = await det.check(sig)
        assert result.is_blocked is True


@pytest.mark.asyncio
async def test_opposite_direction_always_blocked():
    """Opposite-direction signal on the same symbol is blocked even at
    very different price. Audit point #9 + e3aa476 (2026-04-29)."""
    existing = [{
        "id": 1, "state": "POSITION_OPEN",
        "avg_entry": 76000.0, "entry_price": 76000.0,
        "direction": "LONG",
    }]
    det = DuplicateDetector(
        db=_db_with(existing), threshold_pct=5.0, lookback_hours=24,
    )
    # SHORT signal far from the LONG's entry — opposite-direction guard
    # blocks it regardless.
    result = await det.check(_signal(direction="SHORT", entry=90000.0))
    assert result.is_blocked is True


@pytest.mark.asyncio
async def test_no_duplicate_when_no_active_trade():
    """First-ever signal for a symbol -> always action='new'."""
    det = DuplicateDetector(
        db=_db_with([]), threshold_pct=5.0, lookback_hours=24,
    )
    result = await det.check(_signal(entry=76000.0))
    assert result.is_new is True
    assert result.is_blocked is False


@pytest.mark.asyncio
async def test_far_entry_same_direction_is_update_not_duplicate():
    """Same direction but >5% entry diff is not a duplicate — it's
    an update of the existing trade's TP/SL. Verifies the
    duplicate-vs-update distinction stays stable across repeated calls."""
    existing = [{
        "id": 1, "state": "POSITION_OPEN",
        "avg_entry": 100.0, "entry_price": 100.0,
        "direction": "LONG",
    }]
    det = DuplicateDetector(
        db=_db_with(existing), threshold_pct=5.0, lookback_hours=24,
    )
    sig_far = _signal(entry=110.0)  # +10% diff
    # Idempotent: 5 repeated checks all return the same action.
    actions = set()
    for _ in range(5):
        r = await det.check(sig_far)
        actions.add(r.action)
    assert actions == {"update"}
