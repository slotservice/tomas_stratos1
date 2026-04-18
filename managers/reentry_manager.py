"""
Stratos1 - Re-entry Manager
-----------------------------
After a trade is stopped out (by SL or BE + buffer), the re-entry manager
monitors price for a return to the original signal entry.  If price
reaches that level and re-entries have not been exhausted, a completely
new trade is created from the same signal with fresh parameters.

Rules:
    - Max ``max_reentries`` attempts per signal (default 2).
    - Each re-entry recalculates leverage, resets BE / scaling / trailing
      / hedge flags, and starts as a brand-new trade.
    - Once all re-entries are exhausted the signal is marked as fully
      completed.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Optional

import structlog

if TYPE_CHECKING:
    from config.settings import ReentrySettings
    from core.models import Trade

log = structlog.get_logger(__name__)


class ReentryManager:
    """Monitor stopped-out trades and re-enter when price returns to entry.

    Parameters
    ----------
    settings:
        ``ReentrySettings`` with ``enabled`` and ``max_reentries``.
    bybit:
        Exchange adapter (unused directly, but required for completeness;
        the ``position_manager`` drives actual order placement).
    notifier:
        Telegram notifier.
    db:
        Database instance.
    position_manager:
        Reference to the ``PositionManager`` so a fresh trade can be
        created via ``process_signal``.
    """

    def __init__(
        self,
        settings: ReentrySettings,
        bybit: Any,
        notifier: Any,
        db: Any,
        position_manager: Any,
    ) -> None:
        self._settings = settings
        self._bybit = bybit
        self._notifier = notifier
        self._db = db
        self._position_manager = position_manager

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def check_and_activate(
        self,
        trade: Trade,
        current_price: float,
    ) -> bool:
        """Check whether *trade* qualifies for re-entry.

        This should be called for trades in ``REENTRY_WAITING`` state
        (or trades that were just stopped out).

        Returns ``True`` if a new trade was opened on this call,
        ``False`` otherwise.
        """
        if not self._settings.enabled:
            return False

        if trade.signal is None:
            return False

        # --- Determine eligibility ---
        # The trade must have been closed by SL or BE hit.
        if not self._is_eligible_for_reentry(trade):
            return False

        # --- Check re-entry count limit ---
        if trade.reentry_count >= self._settings.max_reentries:
            await self._handle_exhausted(trade)
            return False

        # --- Transition to REENTRY_WAITING if not already ---
        from core.models import TradeState
        if trade.state != TradeState.REENTRY_WAITING:
            trade.transition(TradeState.REENTRY_WAITING)
            try:
                await self._db.update_trade(
                    int(trade.id),
                    state=trade.state.value,
                )
            except Exception:
                log.exception("reentry.state_update_error", trade_id=trade.id)

        # --- Check if price has returned to the original signal entry ---
        original_entry = trade.signal.entry
        direction = trade.signal.direction

        if not self._price_at_entry(direction, original_entry, current_price):
            return False

        log.info(
            "reentry.price_reached",
            trade_id=trade.id,
            symbol=trade.signal.symbol,
            original_entry=original_entry,
            current_price=current_price,
            reentry_count=trade.reentry_count,
        )

        # --- Create a completely new trade from the same signal ---
        new_trade = await self._open_new_trade(trade)

        if new_trade is None:
            log.warning(
                "reentry.new_trade_failed",
                trade_id=trade.id,
                symbol=trade.signal.symbol,
            )
            await self._safe_notify(
                f"[REENTRY ERROR] {trade.signal.symbol}: "
                f"kunde inte oppna ny trade vid reentry. Se loggar."
            )
            return False

        # --- Update the original trade ---
        trade.reentry_count += 1
        trade.transition(TradeState.CLOSED)
        try:
            await self._db.update_trade(
                int(trade.id),
                reentry_count=trade.reentry_count,
                state=trade.state.value,
                close_reason="reentry_triggered",
            )
            await self._db.log_event(
                trade_id=int(trade.id),
                event_type="reentry_activated",
                details={
                    "new_trade_id": new_trade.id,
                    "reentry_count": trade.reentry_count,
                    "original_entry": original_entry,
                    "current_price": current_price,
                },
            )
        except Exception:
            log.exception("reentry.db_error", trade_id=trade.id)

        # --- Notify ---
        await self._safe_notify(
            f"[REENTRY] {trade.signal.symbol} {direction}\n"
            f"Reentry #{trade.reentry_count} aktiverad vid {current_price}\n"
            f"Ny trade skapad fran samma signal.\n"
            f"Kvarvarande reentries: "
            f"{self._settings.max_reentries - trade.reentry_count}"
        )

        log.info(
            "reentry.activated",
            trade_id=trade.id,
            symbol=trade.signal.symbol,
            new_trade_id=new_trade.id,
            reentry_count=trade.reentry_count,
        )
        return True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_eligible_for_reentry(self, trade: Trade) -> bool:
        """Return True if the trade was closed by SL or BE hit."""
        from core.models import TradeState

        # Already waiting for reentry -> eligible.
        if trade.state == TradeState.REENTRY_WAITING:
            return True

        # Must be closed.
        if trade.state != TradeState.CLOSED:
            return False

        # Only SL-hit or BE-hit closures are eligible.
        eligible_reasons = {"sl_hit", "be_hit", "breakeven_hit", "stop_loss"}
        reason = (trade.close_reason or "").lower()

        return reason in eligible_reasons

    @staticmethod
    def _price_at_entry(
        direction: str,
        original_entry: float,
        current_price: float,
        tolerance_pct: float = 0.3,
    ) -> bool:
        """Check if current price is at or past the original entry.

        Uses a small tolerance (0.3 %) to avoid missing the exact tick.

        For LONG: price must be at or below entry + tolerance.
        For SHORT: price must be at or above entry - tolerance.
        """
        tolerance = original_entry * (tolerance_pct / 100.0)

        if direction == "LONG":
            # Price should have come back down to (or near) the entry.
            return current_price <= original_entry + tolerance
        else:
            # Price should have come back up to (or near) the entry.
            return current_price >= original_entry - tolerance

    async def _open_new_trade(self, original_trade: Trade) -> Optional[Any]:
        """Create a fresh trade from the original trade's signal.

        Delegates entirely to ``PositionManager.process_signal`` so all
        logic (leverage calc, BE, scaling, hedge, trailing) is reset.
        The signal's ``parsed_at`` is refreshed so it passes stale checks.
        """
        signal = original_trade.signal
        if signal is None:
            return None

        # Refresh the timestamp so the signal is not rejected as stale.
        if hasattr(signal, "parsed_at"):
            signal.parsed_at = time.time()
        if hasattr(signal, "received_at"):
            from datetime import datetime, timezone
            signal.received_at = datetime.now(timezone.utc)

        try:
            new_trade = await self._position_manager.process_signal(
                signal,
                is_reentry=True,
                parent_reentry_count=original_trade.reentry_count + 1,
            )
            return new_trade
        except Exception:
            log.exception(
                "reentry.process_signal_error",
                trade_id=original_trade.id,
                symbol=signal.symbol if hasattr(signal, "symbol") else "?",
            )
            return None

    async def _handle_exhausted(self, trade: Trade) -> None:
        """Mark the signal as fully completed when re-entries are used up."""
        if trade.signal is None:
            return

        symbol = trade.signal.symbol
        log.info(
            "reentry.exhausted",
            trade_id=trade.id,
            symbol=symbol,
            reentry_count=trade.reentry_count,
            max_reentries=self._settings.max_reentries,
        )

        # Mark the original trade's signal as completed in the DB.
        try:
            from core.models import TradeState
            if trade.state == TradeState.REENTRY_WAITING:
                trade.transition(TradeState.CLOSED)
                trade.close_reason = "reentries_exhausted"
                await self._db.update_trade(
                    int(trade.id),
                    state=trade.state.value,
                    close_reason="reentries_exhausted",
                )
            await self._db.log_event(
                trade_id=int(trade.id),
                event_type="reentry_exhausted",
                details={
                    "reentry_count": trade.reentry_count,
                    "max_reentries": self._settings.max_reentries,
                },
            )
        except Exception:
            log.exception("reentry.exhausted_db_error", trade_id=trade.id)

        await self._safe_notify(
            f"[REENTRY SLUT] {symbol}\n"
            f"Alla {self._settings.max_reentries} reentries forbrukade.\n"
            f"Signalen ar helt avslutad."
        )

    async def _safe_notify(self, message: str) -> None:
        """Send a Telegram notification, swallowing errors."""
        try:
            await self._notifier._send_notify(message)
        except Exception:
            log.exception("reentry.notify_error", message=message[:80])
