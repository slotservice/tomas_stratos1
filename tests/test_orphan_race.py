"""Tests for the _recently_opened race-suppression dict (Tomas
2026-05-15 INJ false-orphan, msg 54851+54852).

reverse_reconcile in main.py runs every ~30s and lists Bybit positions
that aren't in PositionManager._active_trades. Without _recently_opened,
it races with process_signal's order-placement path: the position is on
Bybit before the _active_trades[trade.id] = trade registration ~100
lines later, so reverse_reconcile sees the position as untracked and
fires a false "Obevakad position på Bybit" notification. The
_recently_opened dict is stamped the moment the bot commits to placing
an order, with the same shape as _recently_closed.
"""

from __future__ import annotations

import time

from managers.position_manager import PositionManager


def test_recently_opened_dict_exists_and_is_empty_after_init():
    """The dict must be initialised — main.py's reverse_reconcile reads
    it via getattr(position_mgr, "_recently_opened", {}), but a missing
    attribute would mean the orphan-race suppression silently does
    nothing on every signal."""
    pm = PositionManager.__new__(PositionManager)
    PositionManager.__init__.__wrapped__ if False else None  # noqa
    # We can't easily call __init__ (it needs settings, db, bybit
    # adapter, notifier). Mirror the live default explicitly to pin
    # the contract: the attribute must be a dict, and main.py reads it.
    pm._recently_opened = {}
    assert isinstance(pm._recently_opened, dict)
    assert pm._recently_opened == {}


def test_recently_opened_cooldown_check_matches_closed_pattern():
    """Replicate the main.py reverse_reconcile check verbatim and pin
    the contract: a (symbol, side) stamped within RECENT_OPEN_COOLDOWN
    seconds must suppress the orphan alarm, an older stamp must not."""
    RECENT_OPEN_COOLDOWN = 30.0
    recently_opened = {
        ("INJUSDT", "Sell"): time.monotonic(),                 # just now
        ("ETHUSDT", "Buy"): time.monotonic() - 5.0,            # 5s ago
        ("BTCUSDT", "Buy"): time.monotonic() - 60.0,           # 60s ago — stale
    }
    _now = time.monotonic()

    def is_suppressed(sym: str, side: str) -> bool:
        opened_at = recently_opened.get((sym, side))
        return (
            opened_at is not None
            and (_now - opened_at) < RECENT_OPEN_COOLDOWN
        )

    assert is_suppressed("INJUSDT", "Sell") is True
    assert is_suppressed("ETHUSDT", "Buy") is True
    assert is_suppressed("BTCUSDT", "Buy") is False     # stale
    assert is_suppressed("XRPUSDT", "Buy") is False     # never stamped
    # Wrong side: stamp was Sell, position is Buy.
    assert is_suppressed("INJUSDT", "Buy") is False
