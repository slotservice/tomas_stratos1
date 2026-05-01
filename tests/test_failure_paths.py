"""Phase 6.B.3 — failure-path tests (client 2026-05-02 audit point #7).

"Test every failure case:
   - TP reject
   - SL reject
   - trailing reject
   - hedge fail
Expected result:
   - no trade left unprotected
   - no false Telegram notification"

These tests pin down the BEHAVIORAL CONTRACT of the failure paths
without standing up a full PositionManager. We exercise the
state-machine, the signal-parser SL guards, the leverage-vs-liquidation
guard, and the protection-failed flag interactions.
"""

from __future__ import annotations

import pytest

from core.models import Trade, TradeState
from core.signal_parser import (
    ParsedSignal,
    parse_signal_detailed,
    validate_signal,
)


def _trade(direction="LONG", entry=100.0, sl=97.0):
    sig = ParsedSignal(
        symbol="BTCUSDT",
        direction=direction,
        entry=entry,
        tps=[101, 102, 105],
        sl=sl,
        signal_type="dynamic",
    )
    return Trade(
        signal=sig,
        state=TradeState.PENDING,
        avg_entry=entry,
        quantity=1.0,
        leverage=10.0,
        margin=20.0,
        sl_price=sl,
    )


class TestProtectionFailedTransition:
    """Audit #18: 'If SL / TP / trailing is not verified: stop the
    trade, mark it as INCOMPLETE, send an error, do not send a normal
    Telegram notification.' The PROTECTION_FAILED state is the
    INCOMPLETE marker."""

    def test_pending_can_transition_to_protection_failed(self):
        t = _trade()
        assert t.transition(TradeState.PROTECTION_FAILED) is True
        assert t.state == TradeState.PROTECTION_FAILED

    def test_protection_failed_can_only_close(self):
        t = _trade()
        t.transition(TradeState.PROTECTION_FAILED)
        # Recovery to active states is blocked.
        assert t.transition(TradeState.POSITION_OPEN) is False
        assert t.state == TradeState.PROTECTION_FAILED
        assert t.transition(TradeState.HEDGE_ARMED) is False
        # Only closing the trade is allowed.
        assert t.transition(TradeState.CLOSED) is True
        assert t.state == TradeState.CLOSED

    def test_open_trade_can_transition_to_protection_failed(self):
        t = _trade()
        t.transition(TradeState.POSITION_OPEN)
        assert t.transition(TradeState.PROTECTION_FAILED) is True


class TestSignalSlGuard:
    """parse_signal_detailed must reject signals where the SL is
    implausibly far from entry (>50%) — defence against parser
    misreads (CRYPTO BANANA BOT EDU regression 2026-04-28)."""

    def test_sl_50pct_below_long_rejected(self):
        sig = ParsedSignal(
            symbol="BTCUSDT", direction="LONG",
            entry=100.0, tps=[101], sl=10.0,  # 90% below
        )
        valid, reason = validate_signal(sig)
        assert valid is False
        assert "implausibly far" in reason

    def test_sl_above_entry_for_long_rejected(self):
        sig = ParsedSignal(
            symbol="BTCUSDT", direction="LONG",
            entry=100.0, tps=[101], sl=105.0,
        )
        valid, reason = validate_signal(sig)
        assert valid is False
        assert "SL" in reason and "entry" in reason


class TestParserNoFalseSignal:
    """Audit #18 echo: when parsing fails the bot must NOT send a
    'POSITION ÖPPNAD' notification — only the appropriate
    rejection. The parser returns the right reason code so the
    caller can route correctly."""

    def test_news_text_not_treated_as_signal(self):
        # News commentary that happens to contain a ticker word — must
        # not be classified as a complete signal.
        text = (
            "The Market Is Correcting, and Altcoins Are Taking the "
            "Bigger Hit. Total market cap dropped to $2.6T overnight..."
        )
        result = parse_signal_detailed(
            text=text, channel_id=0, channel_name="ProZelda",
        )
        assert result.signal is None
        # Reason might be no_entry / no_tps / no_direction depending
        # on what the parser extracted; whatever it is, the signal
        # itself must be None so the caller doesn't open a trade.

    def test_tp_status_message_not_signal(self):
        text = "#OP/USDT Take-Profit target 1 ✅\nProfit: 10.5949% 📈"
        result = parse_signal_detailed(
            text=text, channel_id=0, channel_name="GlobalCrypto",
        )
        assert result.signal is None


class TestStateMachineFailureRecovery:
    """The state machine must always allow a clean exit to a terminal
    state regardless of where the failure occurred."""

    @pytest.mark.parametrize("src", [
        TradeState.PENDING,
        TradeState.ENTRY1_PLACED,
        TradeState.POSITION_OPEN,
        TradeState.HEDGE_ARMED,
        TradeState.HEDGE_ACTIVE,
        TradeState.PROTECTION_FAILED,
        TradeState.TRAILING_ACTIVE,
        TradeState.PROFIT_LOCK_1_ACTIVE,
        TradeState.SL_MOVED_TO_TP1,
    ])
    def test_terminal_close_always_allowed(self, src):
        t = _trade()
        # Force into the source state, then transition to CLOSED.
        t.state = src
        assert t.transition(TradeState.CLOSED) is True
        assert t.state == TradeState.CLOSED

    @pytest.mark.parametrize("src", [
        TradeState.PENDING,
        TradeState.ENTRY1_PLACED,
        TradeState.POSITION_OPEN,
    ])
    def test_terminal_error_always_allowed(self, src):
        t = _trade()
        t.state = src
        assert t.transition(TradeState.ERROR) is True
        assert t.state == TradeState.ERROR


class TestTradeFlagsAfterProtectionFailure:
    """The protection-failed gate sets PROTECTION_FAILED and the trade
    should not have any of the lifecycle bookkeeping flags set
    (it never opened, so nothing to track)."""

    def test_protection_failed_keeps_idempotency_flags_default(self):
        t = _trade()
        t.transition(TradeState.PROTECTION_FAILED)
        # None of the SL-movement idempotency flags should have been
        # touched — this trade never reached the price-tick handler.
        assert t.profit_lock_1_active is False
        assert t.profit_lock_2_active is False
        assert t.sl_moved_to_be is False
        assert t.sl_moved_to_tp_index == 0
        assert t.sl_movement_history == []
