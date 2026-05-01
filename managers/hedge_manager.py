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
        """Bot-side hedge activation — DISABLED 2026-04-30.

        Per client production-stable spec, the hedge has ONE source
        of truth: the Bybit conditional placed by ``pre_arm_on_bybit``.
        Two activation paths created duplicate hedges and inconsistent
        state. This method is kept as a stub so existing call sites
        do not break, but it never opens a hedge.
        """
        if getattr(self._settings, "use_bybit_conditional_only", True):
            return False

        # Legacy path retained for the off-toggle case only.
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

        # Base trigger is the configured fixed value (e.g. 2.0%).
        # Client IZZU 2026-04-24: on tight-SL scalping signals (SL
        # under 2% from entry), the fixed 2% trigger fires AFTER SL —
        # hedge never gets a chance. Make the effective trigger the
        # closer of fixed OR (SL distance - small buffer) so the
        # hedge always fires before SL, with a 0.3% buffer to avoid
        # near-simultaneous firing.
        #
        # Client 2026-04-29: previously the floor was 0.5% which fired
        # the hedge on micro-moves (e.g. SUIUSDT at -0.53% adverse
        # produced a hedge before the operator could even react). Floor
        # raised to 1.5% — tight-SL signals still get a pre-SL hedge,
        # but only when the move is meaningful.
        fixed_trigger = abs(self._settings.trigger_pct)
        pre_sl_buffer_pct = 0.3
        pre_sl_floor_pct = 1.5
        effective_trigger = fixed_trigger
        sl_price = trade.sl_price
        if sl_price and avg_entry and avg_entry > 0:
            if direction == "LONG":
                sl_distance_pct = (avg_entry - sl_price) / avg_entry * 100.0
            else:
                sl_distance_pct = (sl_price - avg_entry) / avg_entry * 100.0
            if sl_distance_pct > 0:
                pre_sl_trigger = max(
                    pre_sl_floor_pct, sl_distance_pct - pre_sl_buffer_pct,
                )
                if pre_sl_trigger < effective_trigger:
                    effective_trigger = pre_sl_trigger

        if adverse_pct < effective_trigger:
            return False

        log.info(
            "hedge.trigger_reached",
            trade_id=trade.id,
            symbol=symbol,
            direction=direction,
            adverse_pct=round(adverse_pct, 4),
            trigger_pct=round(effective_trigger, 4),
            fixed_trigger=fixed_trigger,
            source=("pre_sl" if effective_trigger < fixed_trigger else "fixed"),
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
            try:
                await self._notifier.hedge_cancelled(
                    trade=trade,
                    reason="Ingen SL tillgänglig för hedge TP — hedge avbruten.",
                )
            except Exception:
                log.exception("hedge.notify_cancelled_failed", trade_id=trade.id)
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
            order_result = await self._bybit.place_market_order(
                symbol=symbol,
                side=hedge_side,
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
            try:
                await self._notifier.hedge_cancelled(
                    trade=trade,
                    reason="Kunde inte öppna hedge-position på Bybit. Se loggar.",
                )
            except Exception:
                log.exception("hedge.notify_error_failed", trade_id=trade.id)
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
            try:
                await self._notifier.hedge_cancelled(
                    trade=trade,
                    reason=(
                        "Hedge öppnad men TP/SL kunde inte sättas! "
                        "Manuell åtgärd krävs."
                    ),
                )
            except Exception:
                log.exception("hedge.notify_tp_sl_failed", trade_id=trade.id)

        # --- Schedule delayed SL adjustment on original trade ---
        # Client requirement (2026-04-23): do NOT move the original
        # trade's SL immediately when the hedge opens. Wait for price
        # to either:
        #   (a) move a further 0.5% in the hedge direction (confirms
        #       the adverse move is real, not a spike), OR
        #   (b) 90 seconds pass as time-based fallback.
        # This prevents premature stop-outs from volatility spikes.
        import time as _time
        trade._hedge_sl_move_at_price = current_price  # baseline price
        trade._hedge_sl_move_deadline = _time.monotonic() + 90
        trade._hedge_sl_move_pending = True
        log.info(
            "hedge.original_sl_move_scheduled",
            trade_id=trade.id,
            symbol=symbol,
            baseline_price=current_price,
            time_fallback_sec=90,
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

        # --- Notify (use the proper Swedish template) ---
        try:
            await self._notifier.hedge_activated(
                trade=trade,
                hedge_entry=current_price,
                hedge_sl=hedge_sl,
                hedge_tp=hedge_tp,
                leverage=trade.leverage or 0.0,
                im=trade.margin or 0.0,
            )
        except Exception:
            log.exception(
                "hedge.notify_activated_failed",
                trade_id=trade.id, symbol=symbol,
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
    # Phase 3: pre-arm hedge as Bybit conditional market order
    # ------------------------------------------------------------------

    async def pre_arm_on_bybit(
        self,
        trade: Trade,
    ) -> Optional[str]:
        """Pre-arm the hedge as a Bybit conditional market order.

        Once placed on Bybit, the hedge fires autonomously when price
        crosses the trigger — the bot does NOT need to be online for
        the hedge to open. Bot-side check_and_activate remains as a
        backup path, but in normal operation Bybit's conditional
        engine does the work.

        Called once after a trade opens (after SL/TPs are armed).
        Returns the Bybit orderId of the conditional, or None if the
        arm failed for any reason (bot-side path will still fire).
        """
        if not self._settings.enabled:
            return None
        if trade.hedge_conditional_order_id is not None:
            return trade.hedge_conditional_order_id  # already armed
        if trade.hedge_trade_id is not None:
            return None  # hedge already opened, nothing to pre-arm
        if trade.signal is None or not trade.avg_entry or not trade.quantity:
            return None

        avg_entry = trade.avg_entry
        direction = trade.signal.direction
        symbol = trade.signal.symbol

        # Client 2026-04-30 production-stable spec: hedge always fires
        # at a fixed adverse move (default -1.5%). Earlier dynamic
        # SL-buffer logic created varying triggers per signal; the
        # client wants ONE deterministic value across all trades.
        effective_trigger = abs(self._settings.trigger_pct)

        # Trigger PRICE: how far is effective_trigger% from avg_entry,
        # in the adverse direction?
        if direction == "LONG":
            trigger_price = avg_entry * (1 - effective_trigger / 100.0)
            trigger_direction = 2  # rising-to-falling: fires when price falls to trigger
            hedge_side = "Sell"   # opening a SHORT hedge
            hedge_position_idx = 2
        else:
            trigger_price = avg_entry * (1 + effective_trigger / 100.0)
            trigger_direction = 1  # falling-to-rising: fires when price rises to trigger
            hedge_side = "Buy"    # opening a LONG hedge
            hedge_position_idx = 1

        try:
            result = await self._bybit.place_conditional_open(
                symbol=symbol,
                side=hedge_side,
                qty=trade.quantity,
                trigger_price=trigger_price,
                position_idx=hedge_position_idx,
                trigger_direction=trigger_direction,
                trigger_by="LastPrice",
            )
            order_id = result.get("orderId", "")
            if order_id:
                trade.hedge_conditional_order_id = order_id
                try:
                    await self._db.update_trade(
                        int(trade.id),
                        hedge_conditional_order_id=order_id,
                    )
                except Exception:
                    pass
                # Phase 4 (client 2026-05-01) — explicit HEDGE_ARMED
                # state once the pre-arm conditional is on Bybit.
                # The transition is best-effort: if the trade is in
                # an unexpected state (e.g. already ORIGINAL_FORCE_
                # CLOSED), the matrix will block it and warn.
                try:
                    from core.models import TradeState as _TS
                    trade.transition(_TS.HEDGE_ARMED)
                except Exception:
                    pass
                log.info(
                    "hedge.pre_armed_on_bybit",
                    trade_id=trade.id, symbol=symbol,
                    trigger_price=round(trigger_price, 8),
                    trigger_pct=round(effective_trigger, 4),
                    order_id=order_id,
                )
                return order_id
        except Exception:
            log.exception(
                "hedge.pre_arm_failed",
                trade_id=trade.id, symbol=symbol,
            )
        return None

    async def cancel_pre_armed(
        self,
        trade: Trade,
    ) -> None:
        """Cancel the pre-armed hedge conditional on Bybit.

        Called when the main trade closes before the hedge trigger
        fires — leaves the conditional armed otherwise it would
        eventually fire and open an unwanted position.
        """
        oid = trade.hedge_conditional_order_id
        if not oid or trade.signal is None:
            return
        try:
            await self._bybit.cancel_order(trade.signal.symbol, oid)
            log.info(
                "hedge.pre_armed_cancelled",
                trade_id=trade.id, symbol=trade.signal.symbol,
                order_id=oid,
            )
        except Exception:
            log.exception(
                "hedge.pre_arm_cancel_failed",
                trade_id=trade.id, symbol=trade.signal.symbol,
                order_id=oid,
            )
        trade.hedge_conditional_order_id = None
        try:
            await self._db.update_trade(
                int(trade.id),
                hedge_conditional_order_id=None,
            )
        except Exception:
            pass

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
            await self._bybit.place_market_order(
                symbol=symbol,
                side=close_side,
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
            try:
                await self._notifier.hedge_cancelled(
                    trade=trade,
                    reason=(
                        "Kunde inte stänga hedge-position på Bybit. "
                        "Manuell åtgärd krävs."
                    ),
                )
            except Exception:
                log.exception(
                    "hedge.notify_close_error_failed",
                    trade_id=trade.id, symbol=symbol,
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

        # Use the proper hedge_completed Swedish template. Without
        # qty/PnL details from Bybit we pass the values we have on
        # hand — full PnL still reaches the operator from the parent
        # close report. Client 2026-04-29: no more plain-text.
        try:
            await self._notifier.hedge_completed(
                trade=trade,
                exit_price=exit_price,
                qty=trade.quantity or 0.0,
                pct_of_position=100.0,
                result_pct=0.0,
                result_usdt=0.0,
            )
        except Exception:
            log.exception(
                "hedge.notify_completed_failed",
                trade_id=trade.id, symbol=symbol,
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
            await self._notifier._send_notify(message)
        except Exception:
            log.exception("hedge.notify_error", message=message[:80])
