"""
Stratos1 - Trailing Stop Manager
----------------------------------
Activates a trailing stop on Bybit once the position has moved far
enough into profit.

Activation level logic:
    activation_price = avg_entry * (1 +/- activation_pct / 100)

If the highest TP target is *lower* than the computed activation price,
the highest TP is used instead (so the trailing stop can still fire on
signals with modest TP targets).

Once activated the exchange tracks the trailing stop autonomously; the
bot only needs to set it once via ``set_trading_stop``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from config.settings import TrailingStopSettings
    from core.models import Trade

log = structlog.get_logger(__name__)


class TrailingManager:
    """Activate a trailing stop when price reaches the activation level.

    Parameters
    ----------
    settings:
        ``TrailingStopSettings`` with ``activation_pct``,
        ``trailing_distance_pct``, and ``trigger_type``.
    bybit:
        Exchange adapter exposing ``set_trading_stop``.
    notifier:
        Telegram notifier.
    db:
        Database instance.
    """

    def __init__(
        self,
        settings: TrailingStopSettings,
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
        """Check whether the trailing stop should be activated for *trade*.

        Returns ``True`` if the trailing stop was set on this call,
        ``False`` otherwise.  Idempotent -- once ``trade.trailing_sl``
        is set, subsequent calls are no-ops.
        """
        # Guard: already activated or trade not ready.
        if trade.trailing_sl is not None:
            return False

        if trade.avg_entry is None or trade.avg_entry <= 0:
            return False

        if trade.signal is None:
            return False

        avg_entry = trade.avg_entry
        direction = trade.signal.direction
        symbol = trade.signal.symbol

        # --- Determine activation price ---
        activation_price = self._compute_activation_price(
            direction=direction,
            avg_entry=avg_entry,
            tp_list=trade.signal.tps if hasattr(trade.signal, "tps") else (
                trade.signal.tp_list if hasattr(trade.signal, "tp_list") else []
            ),
        )

        # --- Check if current price has reached the activation level ---
        if direction == "LONG" and current_price < activation_price:
            return False
        if direction == "SHORT" and current_price > activation_price:
            return False

        # --- Calculate trailing distance in price terms ---
        trailing_distance = round(
            avg_entry * (self._settings.trailing_distance_pct / 100.0), 8
        )

        log.info(
            "trailing.trigger_reached",
            trade_id=trade.id,
            symbol=symbol,
            direction=direction,
            activation_price=activation_price,
            trailing_distance=trailing_distance,
            current_price=current_price,
        )

        # --- Set trailing stop on exchange ---
        position_idx = 1 if direction == "LONG" else 2

        try:
            await self._bybit.set_trading_stop(
                symbol=symbol,
                trailing_stop=trailing_distance,
                active_price=activation_price,
                position_idx=position_idx,
            )
        except Exception:
            log.exception(
                "trailing.exchange_error",
                trade_id=trade.id,
                symbol=symbol,
            )
            await self._safe_notify(
                f"[TRAILING ERROR] {symbol}: kunde inte aktivera trailing "
                f"stop. Se loggar."
            )
            return False

        # --- Update trade state ---
        trade.trailing_sl = trailing_distance
        from core.models import TradeState
        trade.transition(TradeState.TRAILING_ACTIVE)

        # Persist.
        try:
            await self._db.update_trade(
                int(trade.id),
                state=trade.state.value,
                trailing_sl=trailing_distance,
            )
            await self._db.log_event(
                trade_id=int(trade.id),
                event_type="trailing_activated",
                details={
                    "activation_price": activation_price,
                    "trailing_distance": trailing_distance,
                    "current_price": current_price,
                    "trigger_type": self._settings.trigger_type,
                },
            )
        except Exception:
            log.exception("trailing.db_error", trade_id=trade.id)

        # --- Notify ---
        await self._safe_notify(
            f"[TRAILING] {symbol} {direction}\n"
            f"Trailing stop aktiverad!\n"
            f"Aktivering: {activation_price} | Avstand: {trailing_distance}\n"
            f"Trigger: {self._settings.trigger_type}"
        )

        log.info(
            "trailing.activated",
            trade_id=trade.id,
            symbol=symbol,
            activation_price=activation_price,
            trailing_distance=trailing_distance,
        )
        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _compute_activation_price(
        self,
        direction: str,
        avg_entry: float,
        tp_list: list[float],
    ) -> float:
        """Determine the activation price for the trailing stop.

        Uses the lesser of:
            1. avg_entry * (1 +/- activation_pct / 100)
            2. The highest TP target

        This ensures that the trailing stop can still activate even when
        the TP targets are modest (below the default activation %).
        """
        pct = self._settings.activation_pct

        if direction == "LONG":
            pct_price = avg_entry * (1.0 + pct / 100.0)
            if tp_list:
                highest_tp = max(tp_list)
                # Use the lower of the two so the trailing can fire sooner.
                activation = min(pct_price, highest_tp)
            else:
                activation = pct_price
        else:
            pct_price = avg_entry * (1.0 - pct / 100.0)
            if tp_list:
                lowest_tp = min(tp_list)
                # For SHORT, "lower" means a higher price => use max.
                activation = max(pct_price, lowest_tp)
            else:
                activation = pct_price

        return round(activation, 8)

    async def _safe_notify(self, message: str) -> None:
        """Send a Telegram notification, swallowing errors."""
        try:
            await self._notifier._send_notify(message)
        except Exception:
            log.exception("trailing.notify_error", message=message[:80])
