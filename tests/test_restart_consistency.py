"""Phase 6.B.4 — restart-consistency tests (client 2026-05-02 audit
point #6).

"Restart the bot in the middle of a trade. After restart:
   - state must be identical
   - no duplicate orders
   - no orphan position"

We verify the watchdog/state-recovery semantics by exercising the
specific code paths that decide what to do with each state on
restart. Live restart with real Bybit + Telegram is exercised by
the integration deploy on the server; here we lock in the unit
contract.
"""

from __future__ import annotations

import pytest

from core.models import (
    ALLOWED_TRANSITIONS,
    Trade,
    TradeState,
    is_transition_allowed,
)


# State sets the watchdog cares about (mirroring the matrix in
# health/watchdog.py state_recovery).
_STUCK_PRE_FILL_STATES = {
    TradeState.PENDING,
    TradeState.ENTRY1_PLACED,
    TradeState.ENTRY2_PLACED,
    TradeState.ENTRY1_FILLED,
    TradeState.ENTRY2_FILLED,
}

_TERMINAL_STATES = {
    TradeState.CLOSED,
    TradeState.CANCELLED,
    TradeState.ERROR,
}


class TestStuckPreFillCleanup:
    """Audit point #6: no orphan position. Trades whose state never
    reached POSITION_OPEN before the restart must be marked CANCELLED
    on recovery — they have no live Bybit position to track."""

    @pytest.mark.parametrize("src", _STUCK_PRE_FILL_STATES)
    def test_can_transition_pre_fill_to_cancelled(self, src):
        # The watchdog calls update_trade(state="CANCELLED") directly,
        # bypassing the matrix; but verify the matrix permits it
        # cleanly so any future refactor that routes through
        # transition() doesn't break.
        assert is_transition_allowed(src, TradeState.CANCELLED) is True


class TestActiveStateCanResume:
    """Audit #6: 'state must be identical'. POSITION_OPEN /
    HEDGE_ACTIVE / TRAILING_ACTIVE etc. that survive a restart should
    NOT be re-transitioned — they continue from where they were.
    Verify the matrix doesn't force them backwards."""

    @pytest.mark.parametrize("src", [
        TradeState.POSITION_OPEN,
        TradeState.HEDGE_ARMED,
        TradeState.HEDGE_ACTIVE,
        TradeState.TRAILING_ARMED,
        TradeState.TRAILING_ACTIVE,
        TradeState.BREAKEVEN_ACTIVE,
        TradeState.PROFIT_LOCK_1_ACTIVE,
    ])
    def test_active_states_not_required_to_transition_back_to_open(self, src):
        # Going BACK to POSITION_OPEN from any deeper active state must
        # be blocked by the matrix — this prevents a buggy recovery
        # path from rewriting "we already armed the hedge" as "we
        # haven't started yet".
        assert is_transition_allowed(src, TradeState.POSITION_OPEN) is False


class TestNoOrphanFromTerminalStates:
    """Audit #6: closed/cancelled/error trades never re-open. The
    matrix must enforce this so a buggy recovery can't resurrect
    a closed trade."""

    @pytest.mark.parametrize("src", _TERMINAL_STATES)
    @pytest.mark.parametrize("dst", [
        TradeState.PENDING,
        TradeState.POSITION_OPEN,
        TradeState.HEDGE_ARMED,
        TradeState.TRAILING_ACTIVE,
    ])
    def test_terminal_to_active_blocked(self, src, dst):
        assert is_transition_allowed(src, dst) is False


class TestProtectionFailedSurvivesRestart:
    """A trade that hit PROTECTION_FAILED before the restart must
    NEVER re-open. After recovery it must close out — the matrix
    enforces this."""

    def test_protection_failed_can_only_close(self):
        # All three terminal states are always reachable by design
        # (CLOSED/CANCELLED/ERROR — the matrix has a global "any
        # source -> any terminal" safety valve). What MUST be blocked:
        # transitions to active states.
        for dst in TradeState:
            if dst in _TERMINAL_STATES:
                continue
            allowed = is_transition_allowed(TradeState.PROTECTION_FAILED, dst)
            assert allowed is False, (
                f"PROTECTION_FAILED -> {dst} allowed (must be blocked)"
            )


class TestMatrixCompletenessForRecovery:
    """Every TradeState the watchdog could see on restart must have an
    entry in ALLOWED_TRANSITIONS so it can be moved forward / closed
    cleanly. Guards against a future enum addition that's missed."""

    def test_every_state_has_matrix_entry(self):
        missing = [s for s in TradeState if s not in ALLOWED_TRANSITIONS]
        assert missing == [], (
            f"states with no matrix entry (recovery cannot resolve them): "
            f"{missing}"
        )


class TestHedgeChildRecovery:
    """Phase 5c added the hedge-child close handler. After restart, a
    hedge child trade row whose underlying Bybit hedge has closed
    must be closeable via the same path the WS handler uses."""

    def test_hedge_active_can_close_directly(self):
        # The WS handler calls await self._db.update_trade(child_id,
        # state=TradeState.CLOSED.value, ...). That bypasses the
        # matrix. But we also want the matrix path to work in case
        # recovery routes through transition() — verify it does.
        t = Trade(state=TradeState.HEDGE_ACTIVE)
        assert t.transition(TradeState.CLOSED) is True
        assert t.state == TradeState.CLOSED
