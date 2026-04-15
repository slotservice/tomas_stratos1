"""
Stratos1 - Break-even Manager
------------------------------
Monitors open trades and moves the stop-loss to break-even (plus a small
safety buffer) once the position has moved sufficiently in profit.

Trigger rule:
    LONG  -> move_pct = (current_price - avg_entry) / avg_entry * 100
    SHORT -> move_pct = (avg_entry - current_price) / avg_entry * 100

When ``move_pct >= trigger_pct`` (default 2.3 %):
    BE price = avg_entry + buffer   (LONG)
    BE price = avg_entry - buffer   (SHORT)

The buffer is ``avg_entry * buffer_pct / 100`` so the SL sits slightly
inside profit rather than exactly at break-even.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from config.settings import BreakevenSettings
    from core.models import Trade

log = structlog.get_logger(__name__)


class BreakevenManager:
    """Move the stop-loss to break-even once a configured profit % is reached.

    Parameters
    ----------
    settings:
        ``BreakevenSettings`` with ``trigger_pct`` and ``buffer_pct``.
    bybit:
        Exchange adapter exposing ``set_trading_stop``.
    notifier:
        Telegram notifier with a ``send`` / ``notify`` coroutine.
    db:
        ``Database`` instance for persisting state changes.
    """

    def __init__(
        self,
        settings: BreakevenSettings,
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

    async def check_and_apply(
        self,
        trade: Trade,
        current_price: float,
    ) -> bool:
        """Check whether *trade* qualifies for break-even adjustment.

        Returns ``True`` if the SL was moved to BE on this call, ``False``
        otherwise.  Safe to call repeatedly -- once BE is set the trade's
        ``be_price`` is non-None and subsequent calls are no-ops.
        """
        # Guard: already applied or trade has no avg_entry yet.
        if trade.be_price is not None:
            return False

        if trade.avg_entry is None or trade.avg_entry <= 0:
            return False

        if trade.signal is None:
            return False

        avg_entry = trade.avg_entry
        direction = trade.signal.direction

        # Calculate how far price has moved in the favourable direction.
        move_pct = self._calculate_move_pct(direction, avg_entry, current_price)

        if move_pct < self._settings.trigger_pct:
            return False

        # --- Trigger reached: compute BE price ---
        buffer_abs = avg_entry * (self._settings.buffer_pct / 100.0)

        if direction == "LONG":
            be_price = round(avg_entry + buffer_abs, 8)
        else:
            be_price = round(avg_entry - buffer_abs, 8)

        symbol = trade.signal.symbol

        log.info(
            "breakeven.trigger_reached",
            trade_id=trade.id,
            symbol=symbol,
            direction=direction,
            avg_entry=avg_entry,
            current_price=current_price,
            move_pct=round(move_pct, 4),
            be_price=be_price,
        )

        # --- Place the BE stop on the exchange ---
        try:
            # positionIdx: 1 = Buy/Long, 2 = Sell/Short (hedge mode)
            position_idx = 1 if direction == "LONG" else 2
            await self._bybit.set_trading_stop(
                symbol=symbol,
                stop_loss=be_price,
                position_idx=position_idx,
            )
        except Exception:
            log.exception(
                "breakeven.exchange_error",
                trade_id=trade.id,
                symbol=symbol,
                be_price=be_price,
            )
            await self._safe_notify(
                f"[BE ERROR] {symbol}: kunde inte flytta SL till BE "
                f"({be_price}). Se loggar."
            )
            return False

        # --- Update trade state ---
        trade.be_price = be_price
        trade.sl_price = be_price
        from core.models import TradeState
        trade.transition(TradeState.BREAKEVEN_ACTIVE)

        # Persist to DB.
        try:
            await self._db.update_trade(
                int(trade.id),
                state=trade.state.value,
                be_price=be_price,
                sl_price=be_price,
            )
            await self._db.log_event(
                trade_id=int(trade.id),
                event_type="breakeven_adjusted",
                details={
                    "avg_entry": avg_entry,
                    "be_price": be_price,
                    "move_pct": round(move_pct, 4),
                    "current_price": current_price,
                },
            )
        except Exception:
            log.exception(
                "breakeven.db_error",
                trade_id=trade.id,
            )

        # --- Notify via Telegram ---
        await self._safe_notify(
            f"[BE] {symbol} {direction}\n"
            f"SL flyttad till break-even: {be_price}\n"
            f"Avg entry: {avg_entry} | Rorelse: +{move_pct:.2f}%"
        )

        log.info(
            "breakeven.applied",
            trade_id=trade.id,
            symbol=symbol,
            be_price=be_price,
        )
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
        """Return price movement % in the favourable direction."""
        if direction == "LONG":
            return (current_price - avg_entry) / avg_entry * 100.0
        else:
            return (avg_entry - current_price) / avg_entry * 100.0

    async def _safe_notify(self, message: str) -> None:
        """Send a Telegram notification, swallowing errors."""
        try:
            await self._notifier.send(message)
        except Exception:
            log.exception("breakeven.notify_error", message=message[:80])
