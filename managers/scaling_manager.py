"""
Stratos1 - Pyramid / Scaling Manager
--------------------------------------
Adds margin and/or changes leverage at predefined profit milestones.

Each ``ScalingStep`` in the configuration specifies:
    - ``trigger_pct``  : profit % from avg_entry that activates this step
    - ``add_margin``   : additional USDT margin to inject (optional)
    - ``set_leverage``  : new leverage to apply (optional)

Steps are executed in order (``trade.scaling_step`` tracks progress).
After each step the manager verifies that the position is still safe
by checking the liquidation distance.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from config.settings import LeverageSettings, ScalingSettings
    from core.models import Trade

log = structlog.get_logger(__name__)

# Minimum acceptable distance between current price and liquidation price
# expressed as a fraction.  1 % = 0.01.
_MIN_LIQ_DISTANCE_PCT = 1.0


class ScalingManager:
    """Execute pyramid / scaling steps as the trade moves into profit.

    Parameters
    ----------
    settings:
        ``ScalingSettings`` containing the enabled flag and step list.
    leverage_settings:
        ``LeverageSettings`` for leverage bounds (used for validation).
    bybit:
        Exchange adapter with ``set_leverage``, ``add_margin``,
        ``get_position`` methods.
    notifier:
        Telegram notifier.
    db:
        Database instance.
    """

    def __init__(
        self,
        settings: ScalingSettings,
        leverage_settings: LeverageSettings,
        bybit: Any,
        notifier: Any,
        db: Any,
    ) -> None:
        self._settings = settings
        self._leverage_settings = leverage_settings
        self._bybit = bybit
        self._notifier = notifier
        self._db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def check_and_apply(
        self,
        trade: Trade,
        current_price: float,
    ) -> bool:
        """Check whether the next scaling step should fire for *trade*.

        Returns ``True`` if a step was executed on this call, ``False``
        otherwise.  Idempotent -- tracks progress via ``trade.scaling_step``.
        """
        if not self._settings.enabled:
            return False

        if trade.avg_entry is None or trade.avg_entry <= 0:
            return False

        if trade.signal is None:
            return False

        steps = self._settings.steps
        next_step_idx = trade.scaling_step  # 0-based index into steps list

        # All steps done?
        if next_step_idx >= len(steps):
            return False

        step = steps[next_step_idx]
        direction = trade.signal.direction
        avg_entry = trade.avg_entry
        symbol = trade.signal.symbol

        # Calculate favourable price movement %.
        move_pct = self._calculate_move_pct(direction, avg_entry, current_price)

        if move_pct < step.trigger_pct:
            return False

        log.info(
            "scaling.trigger_reached",
            trade_id=trade.id,
            symbol=symbol,
            step_index=next_step_idx,
            trigger_pct=step.trigger_pct,
            move_pct=round(move_pct, 4),
            set_leverage=step.set_leverage,
            add_margin=step.add_margin,
        )

        # positionIdx: 1 = Buy/Long, 2 = Sell/Short (hedge mode)
        position_idx = 1 if direction == "LONG" else 2

        # --- Set leverage (if specified in this step) ---
        if step.set_leverage is not None:
            try:
                side = "Buy" if direction == "LONG" else "Sell"
                await self._bybit.set_leverage(
                    symbol=symbol,
                    leverage=step.set_leverage,
                    side=side,
                )
                log.info(
                    "scaling.leverage_set",
                    trade_id=trade.id,
                    symbol=symbol,
                    leverage=step.set_leverage,
                )
            except Exception:
                log.exception(
                    "scaling.leverage_error",
                    trade_id=trade.id,
                    symbol=symbol,
                    leverage=step.set_leverage,
                )
                await self._safe_notify(
                    f"[SCALING ERROR] {symbol}: kunde inte andra leverage "
                    f"till x{step.set_leverage}. Se loggar."
                )
                return False

        # --- Add margin (if specified in this step) ---
        if step.add_margin is not None:
            try:
                await self._bybit.add_margin(
                    symbol=symbol,
                    margin=step.add_margin,
                    position_idx=position_idx,
                )
                log.info(
                    "scaling.margin_added",
                    trade_id=trade.id,
                    symbol=symbol,
                    margin=step.add_margin,
                )
            except Exception:
                log.exception(
                    "scaling.margin_error",
                    trade_id=trade.id,
                    symbol=symbol,
                    margin=step.add_margin,
                )
                await self._safe_notify(
                    f"[SCALING ERROR] {symbol}: kunde inte lagga till "
                    f"{step.add_margin} USDT margin. Se loggar."
                )
                return False

        # --- Validate position safety after changes ---
        is_safe = await self.validate_position_safety(trade, symbol)
        if not is_safe:
            log.warning(
                "scaling.unsafe_after_step",
                trade_id=trade.id,
                symbol=symbol,
                step_index=next_step_idx,
            )
            await self._safe_notify(
                f"[SCALING VARNING] {symbol}: likvidationspris for nara "
                f"efter steg {next_step_idx + 1}. Kontrollera positionen!"
            )
            # We still record the step as executed -- rolling back leverage /
            # margin on-exchange would be more dangerous than the warning.

        # --- Update trade state ---
        trade.scaling_step = next_step_idx + 1
        if step.set_leverage is not None:
            trade.leverage = step.set_leverage

        # Map scaling_step (1-4) to the corresponding TradeState.
        from core.models import TradeState
        _SCALING_STATES = {
            1: TradeState.SCALING_STEP_1,
            2: TradeState.SCALING_STEP_2,
            3: TradeState.SCALING_STEP_3,
            4: TradeState.SCALING_STEP_4,
        }
        new_state = _SCALING_STATES.get(trade.scaling_step, TradeState.POSITION_OPEN)
        trade.transition(new_state)

        # Persist.
        try:
            update_fields: dict[str, Any] = {
                "state": trade.state.value,
                "scaling_step": trade.scaling_step,
            }
            if step.set_leverage is not None:
                update_fields["leverage"] = step.set_leverage

            await self._db.update_trade(int(trade.id), **update_fields)
            await self._db.log_event(
                trade_id=int(trade.id),
                event_type="scaling_step",
                details={
                    "step_index": next_step_idx,
                    "trigger_pct": step.trigger_pct,
                    "move_pct": round(move_pct, 4),
                    "set_leverage": step.set_leverage,
                    "add_margin": step.add_margin,
                    "is_safe": is_safe,
                },
            )
        except Exception:
            log.exception("scaling.db_error", trade_id=trade.id)

        # --- Notify ---
        margin_note = (
            f"Margin +{step.add_margin} USDT" if step.add_margin else ""
        )
        leverage_note = (
            f"Leverage -> x{step.set_leverage}" if step.set_leverage else ""
        )
        details = " | ".join(filter(None, [margin_note, leverage_note]))

        await self._safe_notify(
            f"[PYRAMID] {symbol} {direction} - Steg {next_step_idx + 1}\n"
            f"Trigger: +{step.trigger_pct}% (nu +{move_pct:.2f}%)\n"
            f"{details}"
        )

        log.info(
            "scaling.applied",
            trade_id=trade.id,
            symbol=symbol,
            step=next_step_idx + 1,
        )
        return True

    # ------------------------------------------------------------------
    # Position safety validation
    # ------------------------------------------------------------------

    async def validate_position_safety(
        self,
        trade: Trade,
        symbol: str,
    ) -> bool:
        """Verify that the liquidation price is at a safe distance.

        Returns ``True`` if the position is safe, ``False`` if the
        liquidation price is dangerously close (< 1 % from current mark
        price).
        """
        try:
            position = await self._bybit.get_position(symbol=symbol)
        except Exception:
            log.exception(
                "scaling.position_fetch_error",
                trade_id=trade.id,
                symbol=symbol,
            )
            # Cannot verify -> treat as safe to avoid blocking the step.
            return True

        if position is None:
            log.warning(
                "scaling.no_position_found",
                trade_id=trade.id,
                symbol=symbol,
            )
            return True

        liq_price = float(position.get("liqPrice", 0) or 0)
        mark_price = float(position.get("markPrice", 0) or 0)

        if liq_price <= 0 or mark_price <= 0:
            # Insufficient data -- assume safe.
            return True

        distance_pct = abs(mark_price - liq_price) / mark_price * 100.0

        log.debug(
            "scaling.liq_check",
            trade_id=trade.id,
            symbol=symbol,
            liq_price=liq_price,
            mark_price=mark_price,
            distance_pct=round(distance_pct, 4),
        )

        if distance_pct < _MIN_LIQ_DISTANCE_PCT:
            log.warning(
                "scaling.liq_too_close",
                trade_id=trade.id,
                symbol=symbol,
                distance_pct=round(distance_pct, 4),
            )
            return False

        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _calculate_move_pct(
        direction: str,
        avg_entry: float,
        current_price: float,
    ) -> float:
        """Return favourable price movement % from avg_entry."""
        if direction == "LONG":
            return (current_price - avg_entry) / avg_entry * 100.0
        else:
            return (avg_entry - current_price) / avg_entry * 100.0

    async def _safe_notify(self, message: str) -> None:
        """Send a Telegram notification, swallowing errors."""
        try:
            await self._notifier._send_notify(message)
        except Exception:
            log.exception("scaling.notify_error", message=message[:80])
