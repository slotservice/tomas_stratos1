"""
Stratos1 - Re-entry Manager
-----------------------------
Bybit-driven re-entry. The trigger is a verified Bybit event: when the
position-level stop-loss order fires, ``PositionManager.close_trade``
labels the close with ``reason == "stop_loss"`` and immediately invokes
``activate_after_sl`` on this manager.

Strict architecture (client IZZU 2026-04-28):
    The bot must never poll prices, infer movement, or decide on its own
    when to re-enter. Re-entry is opened at market the moment Bybit
    confirms the SL fill. Bybit then manages the new position's TP / SL /
    trailing exactly as it does for any first-attempt trade.

Limits:
    * ``settings.max_reentries`` attempts per signal (default 2).
    * Each re-entry is a brand-new trade — leverage, hedge, trailing,
      and TPs are recomputed from the same signal.
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
    """Open a fresh trade from the same signal when Bybit confirms an SL."""

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

    async def activate_after_sl(self, original_trade: "Trade") -> None:
        """Fire a re-entry off a Bybit-confirmed SL hit.

        Called by ``close_trade`` when ``reason == "stop_loss"``. Opens a
        fresh trade from the same signal at market through
        ``PositionManager.process_signal`` and emits the
        RE-ENTRY AKTIVERAD Telegram template. If max re-entries is
        already reached, emits RE-ENTRY AVSTÄNGT and stops.
        """
        if not self._settings.enabled:
            return
        if original_trade.signal is None:
            return

        if original_trade.reentry_count >= self._settings.max_reentries:
            await self._handle_exhausted(original_trade)
            return

        signal = original_trade.signal
        # Refresh timestamps so the signal passes stale-price guards in
        # the entry pipeline (the signal might be many minutes old by the
        # time SL fires).
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
        except Exception:
            log.exception(
                "reentry.process_signal_error",
                trade_id=original_trade.id,
                symbol=getattr(signal, "symbol", "?"),
            )
            return

        if new_trade is None:
            log.warning(
                "reentry.new_trade_failed",
                trade_id=original_trade.id,
                symbol=getattr(signal, "symbol", "?"),
            )
            return

        original_trade.reentry_count += 1
        try:
            await self._db.update_trade(
                int(original_trade.id),
                reentry_count=original_trade.reentry_count,
            )
            await self._db.log_event(
                trade_id=int(original_trade.id),
                event_type="reentry_activated",
                details={
                    "new_trade_id": new_trade.id,
                    "reentry_count": original_trade.reentry_count,
                },
            )
        except Exception:
            log.exception("reentry.db_error", trade_id=original_trade.id)

        try:
            await self._notifier.reentry_activated(
                trade=new_trade,
                signal=signal,
                leverage=new_trade.leverage or 0.0,
                im=new_trade.margin or 0.0,
            )
        except Exception:
            log.exception(
                "reentry.notify_failed", trade_id=original_trade.id,
            )

        log.info(
            "reentry.activated",
            trade_id=original_trade.id,
            symbol=getattr(signal, "symbol", "?"),
            new_trade_id=new_trade.id,
            reentry_count=original_trade.reentry_count,
        )

    async def _handle_exhausted(self, trade: "Trade") -> None:
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
        try:
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
        try:
            await self._notifier.reentry_exhausted(trade=trade)
        except Exception:
            log.exception(
                "reentry.exhausted_notify_failed", trade_id=trade.id,
            )
