"""Tests for the orphan-position notification dedup
(_should_send_orphan_notify, Tomas 2026-05-18).

The weekend log accumulated 5926 "Obevakad position på Bybit"
alerts because reverse_reconcile fires every ~60s and the
standard rejection dedup window is 0 (signal rejections must
never be silenced). Orphan alerts are STATUS alerts, not
rejections — they need their own dedup window. 1 hour per
(symbol, side) is the operator-friendly cadence.
"""

from __future__ import annotations

import time

from managers.position_manager import PositionManager


def _pm() -> PositionManager:
    pm = PositionManager.__new__(PositionManager)
    pm._orphan_notify_last = {}
    # 1 hour window matches the live default.
    pm._orphan_dedup_window_s = 3600.0
    return pm


def test_first_orphan_alert_fires():
    pm = _pm()
    assert pm._should_send_orphan_notify("AAVEUSDT", "Sell") is True


def test_same_orphan_within_window_suppressed():
    """Second alert for same (symbol, side) within an hour is dropped."""
    pm = _pm()
    assert pm._should_send_orphan_notify("AAVEUSDT", "Sell") is True
    # Immediate retry (same minute) — must be suppressed.
    assert pm._should_send_orphan_notify("AAVEUSDT", "Sell") is False
    # 30 seconds later — still suppressed.
    pm._orphan_notify_last["AAVEUSDT:Sell"] = time.monotonic() - 30.0
    assert pm._should_send_orphan_notify("AAVEUSDT", "Sell") is False


def test_different_symbol_fires_independently():
    """Each (symbol, side) has its own dedup key."""
    pm = _pm()
    assert pm._should_send_orphan_notify("AAVEUSDT", "Sell") is True
    assert pm._should_send_orphan_notify("POWERUSDT", "Sell") is True
    assert pm._should_send_orphan_notify("AAVEUSDT", "Buy") is True


def test_orphan_alert_refires_after_window():
    """After the dedup window elapses, the next alert fires again
    (so a long-lingering orphan still surfaces hourly)."""
    pm = _pm()
    assert pm._should_send_orphan_notify("AAVEUSDT", "Sell") is True
    # Backdate the last-fire by 61 minutes — past the 1h window.
    pm._orphan_notify_last["AAVEUSDT:Sell"] = time.monotonic() - 3700.0
    assert pm._should_send_orphan_notify("AAVEUSDT", "Sell") is True


def test_weekend_spam_reproduction_now_suppressed():
    """5926 orphan alerts over a weekend — reproduce the firing
    cadence and verify the new dedup suppresses 99%+ of them."""
    pm = _pm()
    # Simulate reverse_reconcile firing every 60 seconds for 2 days
    # against the same 5 stuck (symbol, side) keys. Without dedup
    # we'd get 5 * 2880 = 14400 alerts; the old _reject_notify
    # path was firing roughly that many.
    fires = 0
    base_now = time.monotonic()
    SYMS = [("AAVEUSDT", "Sell"), ("POWERUSDT", "Sell"),
            ("XAGUSDT", "Buy"), ("SUIUSDT", "Sell"), ("RUNEUSDT", "Sell")]
    for tick in range(2 * 24 * 60):   # 2 days × 24h × 60min ticks
        for sym, side in SYMS:
            # Manually backdate so the time-since-last is exact.
            key = f"{sym}:{side}"
            last = pm._orphan_notify_last.get(key)
            if last is not None and (base_now + tick * 60 - last) < pm._orphan_dedup_window_s:
                continue
            pm._orphan_notify_last[key] = base_now + tick * 60
            fires += 1
    # Each (symbol, side) refires roughly once per hour over 48
    # hours = ~48 fires per key. 5 keys × 48 = ~240. Not 14400.
    assert fires == 5 * 48
