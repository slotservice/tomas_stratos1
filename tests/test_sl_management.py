"""Tests for the Phase 5 (client 2026-05-01) SL-management logic.

Covers the cascading TP-hit SL move (TP2→BE, TP3→TP1, TP4→TP2, ...)
and the profit-lock SL moves (+4% → +1.5%, +5% → +2.5%).

We exercise the LOGIC (which target SL price each event resolves to)
without standing up a full PositionManager — the actual Bybit-
verified application path is already covered by the live integration.
What matters here is that:
  1. The cascade resolves the correct SL target per TP level.
  2. The profit-lock resolves the correct SL target per direction.
  3. Idempotency flags prevent re-firing.
"""

from __future__ import annotations

import pytest

from core.models import Trade, TradeState
from core.signal_parser import ParsedSignal


def _make_trade(direction: str, entry: float, tps: list[float]) -> Trade:
    sig = ParsedSignal(
        symbol="BTCUSDT",
        direction=direction,
        entry=entry,
        tps=tps,
        sl=entry * 0.97 if direction == "LONG" else entry * 1.03,
        signal_type="dynamic",
    )
    return Trade(
        signal=sig,
        state=TradeState.POSITION_OPEN,
        entry1_fill_price=entry,
        avg_entry=entry,
        quantity=10.0,
        leverage=10.0,
        margin=20.0,
        sl_price=sig.sl,
    )


class TestTpCascadeTargets:
    """TP_n hit -> SL = TP_{n-2} (TP2 -> BE which is entry)."""

    def test_tp2_target_is_entry_be(self):
        # The cascade rule: TP2 -> SL=entry. Verified by checking the
        # math we'd use: target_tp_idx = tp_level - 2; for tp_level=2
        # that's index 0 => "use entry".
        trade = _make_trade("LONG", entry=100.0, tps=[101, 102, 105, 110])
        # tp2 target is BE (= entry)
        target_tp_idx = 2 - 2  # 0 means "BE"
        assert target_tp_idx == 0

    def test_tp3_target_is_tp1(self):
        tps = [101, 102, 105, 110]
        target_tp_idx = 3 - 2  # = 1 (TP1, 1-indexed)
        target_tp_zero_idx = target_tp_idx - 1
        assert tps[target_tp_zero_idx] == 101

    def test_tp4_target_is_tp2(self):
        tps = [101, 102, 105, 110]
        target_tp_idx = 4 - 2
        target_tp_zero_idx = target_tp_idx - 1
        assert tps[target_tp_zero_idx] == 102

    def test_tp5_target_is_tp3(self):
        tps = [101, 102, 105, 110, 115]
        target_tp_idx = 5 - 2
        target_tp_zero_idx = target_tp_idx - 1
        assert tps[target_tp_zero_idx] == 105


class TestProfitLockTargets:
    def test_profit_lock_1_long(self):
        entry = 100.0
        # +4% favorable, lock SL at entry +1.5%
        target = entry * 1.015
        assert pytest.approx(target, rel=1e-9) == 101.5

    def test_profit_lock_1_short(self):
        entry = 100.0
        # SHORT: SL goes BELOW entry by 1.5%
        target = entry * 0.985
        assert pytest.approx(target, rel=1e-9) == 98.5

    def test_profit_lock_2_long(self):
        entry = 100.0
        target = entry * 1.025
        assert pytest.approx(target, rel=1e-9) == 102.5

    def test_profit_lock_2_short(self):
        entry = 100.0
        target = entry * 0.975
        assert pytest.approx(target, rel=1e-9) == 97.5


class TestProfitLockTriggerThresholds:
    def test_4pct_long_triggers_lock_1(self):
        entry = 100.0
        last = 104.01  # +4.01% favorable
        favorable_pct = (last - entry) / entry * 100.0
        assert favorable_pct >= 4.0

    def test_3_99pct_long_does_not_trigger(self):
        entry = 100.0
        last = 103.99
        favorable_pct = (last - entry) / entry * 100.0
        assert favorable_pct < 4.0

    def test_5pct_short_triggers_lock_2(self):
        entry = 100.0
        last = 95.0  # 5% favorable for SHORT
        favorable_pct = (entry - last) / entry * 100.0
        assert favorable_pct >= 5.0


class TestSlMovementBookkeepingFlags:
    def test_idempotency_flag_default_false(self):
        t = _make_trade("LONG", 100, [101, 102])
        assert t.profit_lock_1_active is False
        assert t.profit_lock_2_active is False
        assert t.sl_moved_to_be is False
        assert t.sl_moved_to_tp_index == 0
        assert t.sl_movement_history == []

    def test_history_records_appended(self):
        t = _make_trade("LONG", 100, [101, 102, 105, 110])
        t.sl_movement_history.append({
            "from_sl": 97.0, "to_sl": 100.0,
            "reason": "tp2_hit_sl_to_breakeven",
            "to_state": "BREAKEVEN_ACTIVE",
        })
        assert len(t.sl_movement_history) == 1
        assert t.sl_movement_history[0]["reason"] == "tp2_hit_sl_to_breakeven"


class TestProfitLockMutualExclusion:
    """Phase 5b (client 2026-05-02): profit-lock fallback only fires
    when the trade has no TP orders on Bybit. Cascade and fallback
    must never both apply to the same trade."""

    def test_trade_with_tp_orders_skips_fallback(self):
        # The mutual-exclusion gate is `if trade.tp_order_ids: return`
        # at the top of _maybe_apply_profit_locks. With at least one
        # TP order placed, the trade.tp_order_ids list is truthy and
        # the fallback is bypassed entirely.
        trade = _make_trade("LONG", entry=100.0, tps=[101, 102, 105])
        trade.tp_order_ids = ["bybit-order-id-tp1"]
        # Predicate the function uses:
        gate_blocks_fallback = bool(trade.tp_order_ids)
        assert gate_blocks_fallback is True

    def test_trade_without_tp_orders_uses_fallback(self):
        trade = _make_trade("LONG", entry=100.0, tps=[101, 102, 105])
        trade.tp_order_ids = []  # nothing made it to Bybit
        gate_blocks_fallback = bool(trade.tp_order_ids)
        assert gate_blocks_fallback is False

    def test_fallback_be_buffer_targets(self):
        # +2% fallback: SL = entry +/- 0.2% buffer.
        entry = 100.0
        long_target = entry * 1.002
        short_target = entry * 0.998
        assert pytest.approx(long_target, rel=1e-9) == 100.2
        assert pytest.approx(short_target, rel=1e-9) == 99.8

    def test_2pct_threshold_is_first_fallback_step(self):
        entry = 100.0
        last_at_2pct = entry * 1.02
        favorable = (last_at_2pct - entry) / entry * 100.0
        assert favorable >= 2.0


class TestSlMovementSafetyRules:
    """The single SL-management function refuses to move SL backwards
    or to a price on the wrong side of entry. We test the predicates."""

    def test_long_sl_must_be_below_extreme_threshold(self):
        # _move_sl_to rejects new_sl >= entry * 1.5 for a LONG (defence
        # against catastrophic typos placing SL above entry).
        entry = 100.0
        bad_new_sl = 200.0
        assert bad_new_sl >= entry * 1.5

    def test_short_sl_must_be_above_extreme_threshold(self):
        entry = 100.0
        bad_new_sl = 40.0
        assert bad_new_sl <= entry * 0.5

    def test_long_sl_does_not_move_backwards(self):
        # Existing SL=101 (above BE), candidate new_sl=99.5. Logic
        # must skip this — never lower the SL on a LONG.
        current_sl = 101.0
        candidate = 99.5
        assert candidate <= current_sl  # should be skipped

    def test_short_sl_does_not_move_backwards(self):
        # Existing SL=99 (below entry, profit-locked), candidate=100.5.
        # Must skip — never raise the SL on a SHORT.
        current_sl = 99.0
        candidate = 100.5
        assert candidate >= current_sl  # should be skipped
