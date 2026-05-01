"""Tests for the explicit TradeState machine added in Phase 4
(client 2026-05-01 audit point #16: "Only the state machine may
change status. No other module may directly change trade status").

Covers:
  - Allowed transitions go through and update Trade.state.
  - Disallowed transitions are blocked, state is unchanged, and
    Trade.transition() returns False.
  - Terminal states are sinks.
  - Self-loops on TRAILING_UPDATED are allowed (Bybit moves trailing
    repeatedly).
  - force=True overrides the matrix for emergencies.
"""

from __future__ import annotations

import pytest

from core.models import (
    ALLOWED_TRANSITIONS,
    Trade,
    TradeState,
    is_transition_allowed,
)


class TestAllowedTransitions:
    def test_initial_pending(self):
        assert is_transition_allowed(None, TradeState.PENDING) is True

    def test_initial_non_pending_blocked(self):
        # A fresh trade can only start at PENDING.
        assert is_transition_allowed(None, TradeState.HEDGE_ARMED) is False

    def test_open_to_hedge_armed(self):
        assert is_transition_allowed(
            TradeState.POSITION_OPEN, TradeState.HEDGE_ARMED,
        ) is True

    def test_hedge_armed_to_hedge_active(self):
        assert is_transition_allowed(
            TradeState.HEDGE_ARMED, TradeState.HEDGE_ACTIVE,
        ) is True

    def test_open_to_original_force_closed(self):
        assert is_transition_allowed(
            TradeState.POSITION_OPEN, TradeState.ORIGINAL_FORCE_CLOSED,
        ) is True

    def test_terminal_always_allowed_from_any(self):
        # CLOSED is reachable from every active state.
        for src in (
            TradeState.POSITION_OPEN,
            TradeState.HEDGE_ARMED,
            TradeState.HEDGE_ACTIVE,
            TradeState.TRAILING_ACTIVE,
            TradeState.PROTECTION_FAILED,
            TradeState.ORIGINAL_FORCE_CLOSED,
        ):
            assert is_transition_allowed(src, TradeState.CLOSED) is True

    def test_terminal_states_are_sinks(self):
        # No outgoing non-terminal transitions from CLOSED / CANCELLED.
        assert is_transition_allowed(
            TradeState.CLOSED, TradeState.POSITION_OPEN,
        ) is False
        assert is_transition_allowed(
            TradeState.CANCELLED, TradeState.HEDGE_ARMED,
        ) is False

    def test_trailing_updated_self_loop_allowed(self):
        # Bybit moves the trailing repeatedly — same-state transitions
        # must not be rejected.
        assert is_transition_allowed(
            TradeState.TRAILING_UPDATED, TradeState.TRAILING_UPDATED,
        ) is True

    def test_protection_failed_can_only_close(self):
        # PROTECTION_FAILED is a dead-end: only CLOSED or ERROR.
        assert is_transition_allowed(
            TradeState.PROTECTION_FAILED, TradeState.CLOSED,
        ) is True
        assert is_transition_allowed(
            TradeState.PROTECTION_FAILED, TradeState.ERROR,
        ) is True
        assert is_transition_allowed(
            TradeState.PROTECTION_FAILED, TradeState.HEDGE_ARMED,
        ) is False

    def test_illegal_jump_blocked(self):
        # Cannot jump straight from HEDGE_ACTIVE back to POSITION_OPEN.
        assert is_transition_allowed(
            TradeState.HEDGE_ACTIVE, TradeState.POSITION_OPEN,
        ) is False


class TestTradeTransitionGate:
    def test_legal_transition_returns_true_and_updates(self):
        t = Trade(state=TradeState.POSITION_OPEN)
        assert t.transition(TradeState.HEDGE_ARMED) is True
        assert t.state == TradeState.HEDGE_ARMED

    def test_illegal_transition_returns_false_and_keeps_state(self):
        t = Trade(state=TradeState.HEDGE_ACTIVE)
        before = t.state
        assert t.transition(TradeState.POSITION_OPEN) is False
        assert t.state == before  # unchanged

    def test_force_override_bypasses_matrix(self):
        t = Trade(state=TradeState.HEDGE_ACTIVE)
        # Normally not allowed; force=True bypasses the matrix.
        assert t.transition(TradeState.POSITION_OPEN, force=True) is True
        assert t.state == TradeState.POSITION_OPEN

    def test_terminal_close_always_allowed(self):
        for src in (
            TradeState.POSITION_OPEN,
            TradeState.HEDGE_ARMED,
            TradeState.PROTECTION_FAILED,
        ):
            t = Trade(state=src)
            assert t.transition(TradeState.CLOSED) is True
            assert t.state == TradeState.CLOSED


class TestMatrixCoverage:
    """Sanity check that every named TradeState appears in the matrix."""

    def test_all_states_have_entry(self):
        for state in TradeState:
            assert state in ALLOWED_TRANSITIONS, (
                f"{state} is missing from ALLOWED_TRANSITIONS"
            )
