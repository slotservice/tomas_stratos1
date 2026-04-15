"""
Stratos1 - Hedge Manager
--------------------------
Opens a counter-directional hedge position when an active trade moves
against the trader by a configured percentage.

Hedge rules:
    - Trigger at ``trigger_pct`` move against the position (default -2 %).
    - Only one hedge per signal (``max_hedge_count = 1``).
    - The hedge uses the *opposite* side and positionIdx (hedge mode).
    - Hedge SL  = original signal entry price.
    - Hedge TP  = original signal SL price.
    - If the original signal had no SL (auto-SL), the auto-SL price is used.

The hedge is tracked as a separate child trade linked via
``trade.hedge_trade_id``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

import structlog

if TYPE_CHECKING:
    from config.settings import HedgeSettings
    from core.models import Trade

log = structlog.get_logger(__name__)


class HedgeManager:
    """Open a hedge position when the main trade moves against us.

    Parameters
    ----------
    settings:
        ``HedgeSettings`` with ``enabled``, ``trigger_pct``, and
        ``max_hedge_count``.
    bybit:
        Exchange adapter with ``place_order``, ``set_trading_stop``,
        and ``cancel_order`` methods.
    notifier:
        Telegram notifier.
    db:
        Database instance.
    """

    def __init__(
        self,
        settings: HedgeSettings,
        bybit: Any,
        notifier: Any,
        db: Any,
    ) -> None:
        self._settings = settings
        self._bybit = bybit
        self._notifier = notifier
        self._db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def check_and_activate(
        self,
        trade: Trade,
        current_price: float,
    ) -> bool:
        """Check whether a hedge should be opened for *trade*.

        Returns ``True`` if a hedge was opened on this call, ``False``
        otherwise.  Idempotent -- will not open a second hedge if one
        already exists.
        """
        if not self._settings.enabled:
            return False

        # Guard: hedge already placed for this trade.
        if trade.hedge_trade_id is not None:
            return False

        if trade.avg_entry is None or trade.avg_entry <= 0:
            return False

        if trade.signal is None:
            return False

        avg_entry = trade.avg_entry
        direction = trade.signal.direction
        symbol = trade.signal.symbol

        # Calculate how far price has moved *against* the position.
        adverse_pct = self._calculate_adverse_move_pct(
            direction, avg_entry, current_price
        )

        # trigger_pct is stored as a negative number (e.g. -2.0).
        trigger = abs(self._settings.trigger_pct)

        if adverse_pct < trigger:
            return False

        log.info(
            "hedge.trigger_reached",
            trade_id=trade.id,
            symbol=symbol,
            direction=direction,
            adverse_pct=round(adverse_pct, 4),
            trigger_pct=trigger,
        )

        # --- Determine hedge parameters ---
        hedge_direction = "SHORT" if direction == "LONG" else "LONG"
        hedge_side = "Sell" if hedge_direction == "SHORT" else "Buy"
        # Hedge mode positionIdx: opposite of the main position.
        hedge_position_idx = 2 if hedge_direction == "SHORT" else 1

        # Hedge SL = original signal entry; Hedge TP = original signal SL.
        hedge_sl = trade.signal.entry
        hedge_tp = trade.sl_price  # This is the effective SL (may be auto-SL).

        if hedge_tp is None:
            log.warning(
                "hedge.no_sl_for_tp",
                trade_id=trade.id,
                symbol=symbol,
            )
            await self._safe_notify(
                f"[HEDGE VARNING] {symbol}: ingen SL tillganglig for hedge TP. "
                f"Hedge avbruten."
            )
            return False

        # Use the same quantity as the main position.
        hedge_qty = trade.quantity
        if hedge_qty is None or hedge_qty <= 0:
            log.warning(
                "hedge.no_quantity",
                trade_id=trade.id,
                symbol=symbol,
            )
            return False

        # --- Place the hedge order ---
        try:
            order_result = await self._bybit.place_order(
                symbol=symbol,
                side=hedge_side,
                order_type="Market",
                qty=hedge_qty,
                position_idx=hedge_position_idx,
            )
            hedge_order_id = order_result.get("orderId", "")
        except Exception:
            log.exception(
                "hedge.order_error",
                trade_id=trade.id,
                symbol=symbol,
            )
            await self._safe_notify(
                f"[HEDGE ERROR] {symbol}: kunde inte oppna hedge-position. "
                f"Se loggar."
            )
            return False

        # --- Set TP/SL on the hedge position ---
        try:
            await self._bybit.set_trading_stop(
                symbol=symbol,
                take_profit=hedge_tp,
                stop_loss=hedge_sl,
                position_idx=hedge_position_idx,
            )
        except Exception:
            log.exception(
                "hedge.tp_sl_error",
                trade_id=trade.id,
                symbol=symbol,
                hedge_tp=hedge_tp,
                hedge_sl=hedge_sl,
            )
            # The hedge position is open but unprotected -- warn but continue.
            await self._safe_notify(
                f"[HEDGE VARNING] {symbol}: hedge oppnad men TP/SL kunde "
                f"inte sattas! Manuell atgard kravs."
            )

        # --- Save hedge trade to DB ---
        hedge_trade_db_id: Optional[int] = None
        try:
            hedge_trade_db_id = await self._db.save_trade({
                "signal_id": None,  # Linked via parent trade, not a new signal.
                "state": "HEDGE_ACTIVE",
                "entry1_order_id_bybit": hedge_order_id,
                "avg_entry": current_price,
                "quantity": hedge_qty,
                "leverage": trade.leverage,
                "margin": trade.margin,
                "sl_price": hedge_sl,
            })
            await self._db.log_event(
                trade_id=int(trade.id),
                event_type="hedge_activated",
                details={
                    "hedge_trade_db_id": hedge_trade_db_id,
                    "hedge_direction": hedge_direction,
                    "hedge_qty": hedge_qty,
                    "hedge_sl": hedge_sl,
                    "hedge_tp": hedge_tp,
                    "adverse_pct": round(adverse_pct, 4),
                    "current_price": current_price,
                },
            )
        except Exception:
            log.exception("hedge.db_error", trade_id=trade.id)

        # --- Update parent trade state ---
        trade.hedge_trade_id = (
            str(hedge_trade_db_id) if hedge_trade_db_id else hedge_order_id
        )
        from core.models import TradeState
        trade.transition(TradeState.HEDGE_ACTIVE)

        try:
            await self._db.update_trade(
                int(trade.id),
                state=trade.state.value,
                hedge_trade_id=trade.hedge_trade_id,
            )
        except Exception:
            log.exception("hedge.parent_update_error", trade_id=trade.id)

        # --- Notify ---
        await self._safe_notify(
            f"[HEDGE] {symbol} {direction} -> {hedge_direction}\n"
            f"Hedge-position oppnad vid {current_price}\n"
            f"Hedge SL: {hedge_sl} | Hedge TP: {hedge_tp}\n"
            f"Rorelse mot position: -{adverse_pct:.2f}%"
        )

        log.info(
            "hedge.activated",
            trade_id=trade.id,
            symbol=symbol,
            hedge_direction=hedge_direction,
            hedge_trade_id=trade.hedge_trade_id,
        )
        return True

    # ------------------------------------------------------------------
    # Close hedge
    # ------------------------------------------------------------------

    async def close_hedge(
        self,
        trade: Trade,
        exit_price: float,
    ) -> None:
        """Close the hedge position associated with *trade*.

        Called when the hedge reaches its TP or SL, or when the main
        trade is being fully closed.
        """
        if trade.hedge_trade_id is None:
            return

        if trade.signal is None:
            return

        symbol = trade.signal.symbol
        direction = trade.signal.direction
        hedge_direction = "SHORT" if direction == "LONG" else "LONG"
        close_side = "Buy" if hedge_direction == "SHORT" else "Sell"
        hedge_position_idx = 2 if hedge_direction == "SHORT" else 1

        log.info(
            "hedge.closing",
            trade_id=trade.id,
            symbol=symbol,
            hedge_trade_id=trade.hedge_trade_id,
        )

        try:
            await self._bybit.place_order(
                symbol=symbol,
                side=close_side,
                order_type="Market",
                qty=trade.quantity or 0,
                position_idx=hedge_position_idx,
                reduce_only=True,
            )
        except Exception:
            log.exception(
                "hedge.close_error",
                trade_id=trade.id,
                symbol=symbol,
            )
            await self._safe_notify(
                f"[HEDGE CLOSE ERROR] {symbol}: kunde inte stanga "
                f"hedge-position. Manuell atgard kravs."
            )
            return

        # Update hedge trade in DB.
        try:
            hedge_id = int(trade.hedge_trade_id)
            await self._db.update_trade(
                hedge_id,
                state="CLOSED",
                close_reason="hedge_completed",
                closed_at=self._now_iso(),
            )
            await self._db.log_event(
                trade_id=int(trade.id),
                event_type="hedge_completed",
                details={
                    "hedge_trade_id": trade.hedge_trade_id,
                    "exit_price": exit_price,
                },
            )
        except (ValueError, Exception):
            log.exception("hedge.close_db_error", trade_id=trade.id)

        await self._safe_notify(
            f"[HEDGE KLAR] {symbol}\n"
            f"Hedge-position stangd vid {exit_price}"
        )

        log.info(
            "hedge.closed",
            trade_id=trade.id,
            symbol=symbol,
            exit_price=exit_price,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _calculate_adverse_move_pct(
        direction: str,
        avg_entry: float,
        current_price: float,
    ) -> float:
        """Return price movement % against the position (always positive)."""
        if direction == "LONG":
            return (avg_entry - current_price) / avg_entry * 100.0
        else:
            return (current_price - avg_entry) / avg_entry * 100.0

    @staticmethod
    def _now_iso() -> str:
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()

    async def _safe_notify(self, message: str) -> None:
        """Send a Telegram notification, swallowing errors."""
        try:
            await self._notifier.send(message)
        except Exception:
            log.exception("hedge.notify_error", message=message[:80])
