"""
Stratos1 - Position Manager (Central Trade State Machine)
-----------------------------------------------------------
Orchestrates the complete lifecycle of every trade from signal reception
through entry, management (BE / scaling / trailing / hedge / re-entry),
and eventual close.

Key responsibilities:
    1. Validate and gate incoming signals (duplicate, capacity, staleness).
    2. Calculate leverage and order sizing.
    3. Place entry orders (two Market orders, split quantity).
    4. Track fills via WebSocket callbacks.
    5. Set TP/SL on the exchange once both entries are filled.
    6. Delegate ongoing management to sub-managers (breakeven, scaling,
       trailing, hedge, re-entry).
    7. Close trades and compute PnL.
    8. Clean up timed-out unfilled orders.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import structlog

from core.leverage import calculate_leverage
from core.models import OrderRecord, Trade, TradeState
from managers.breakeven_manager import BreakevenManager
from managers.hedge_manager import HedgeManager
from managers.reentry_manager import ReentryManager
from managers.scaling_manager import ScalingManager
from managers.trailing_manager import TrailingManager

if TYPE_CHECKING:
    from config.settings import AppSettings
    from core.duplicate_detector import DuplicateDetector
    from persistence.database import Database

log = structlog.get_logger(__name__)


class PositionManager:
    """Central trade management state machine.

    Parameters
    ----------
    settings:
        Full ``AppSettings`` object.
    db:
        Async SQLite database.
    bybit:
        Exchange adapter for Bybit API calls.
    notifier:
        Telegram notification sender.
    duplicate_detector:
        ``DuplicateDetector`` instance.
    """

    def __init__(
        self,
        settings: AppSettings,
        db: Database,
        bybit: Any,
        notifier: Any,
        duplicate_detector: DuplicateDetector,
    ) -> None:
        self._settings = settings
        self._db = db
        self._bybit = bybit
        self._notifier = notifier
        self._dup = duplicate_detector

        # Active trades indexed by trade.id for fast lookup.
        self._active_trades: Dict[str, Trade] = {}

        # Mapping from Bybit order ID -> trade ID for fill tracking.
        self._order_to_trade: Dict[str, str] = {}

        # Fill events received from WS, keyed by Bybit order ID.
        self._fill_events: Dict[str, asyncio.Event] = {}
        self._fill_data: Dict[str, dict] = {}

        # In-flight signal locks per symbol to prevent race conditions where
        # the same signal arrives multiple times before the first one has
        # saved its trade to the DB. Key: symbol, Value: asyncio.Lock
        self._symbol_locks: Dict[str, asyncio.Lock] = {}

        # --- Sub-managers ---
        self._be_mgr = BreakevenManager(
            settings=settings.breakeven,
            bybit=bybit,
            notifier=notifier,
            db=db,
        )
        self._scaling_mgr = ScalingManager(
            settings=settings.scaling,
            leverage_settings=settings.leverage,
            bybit=bybit,
            notifier=notifier,
            db=db,
        )
        self._trailing_mgr = TrailingManager(
            settings=settings.trailing_stop,
            bybit=bybit,
            notifier=notifier,
            db=db,
        )
        self._hedge_mgr = HedgeManager(
            settings=settings.hedge,
            bybit=bybit,
            notifier=notifier,
            db=db,
        )
        self._reentry_mgr = ReentryManager(
            settings=settings.reentry,
            bybit=bybit,
            notifier=notifier,
            db=db,
            position_manager=self,
        )

    # ==================================================================
    # Signal processing -- full entry pipeline
    # ==================================================================

    async def process_signal(
        self,
        signal: Any,
        *,
        is_reentry: bool = False,
        parent_reentry_count: int = 0,
    ) -> Optional[Trade]:
        """Process an incoming parsed signal through the full entry pipeline.

        Returns the created ``Trade`` on success, or ``None`` if the
        signal was rejected or entry failed.
        """
        symbol = signal.symbol
        direction = signal.direction

        # Serialize processing per symbol so the duplicate check sees any
        # in-flight trade that's already being placed.
        if symbol not in self._symbol_locks:
            self._symbol_locks[symbol] = asyncio.Lock()
        symbol_lock = self._symbol_locks[symbol]

        async with symbol_lock:
            return await self._process_signal_locked(
                signal,
                is_reentry=is_reentry,
                parent_reentry_count=parent_reentry_count,
            )

    async def _process_signal_locked(
        self,
        signal: Any,
        *,
        is_reentry: bool = False,
        parent_reentry_count: int = 0,
    ) -> Optional[Trade]:
        """Inner signal processing (runs while holding the symbol lock)."""
        symbol = signal.symbol
        direction = signal.direction

        log.info(
            "signal.processing",
            symbol=symbol,
            direction=direction,
            is_reentry=is_reentry,
        )

        # ----------------------------------------------------------
        # 1. Duplicate / update check (skip for re-entries).
        # ----------------------------------------------------------
        if not is_reentry:
            from core.duplicate_detector import DuplicateCheckResult
            dup_result = await self._dup.check(signal)

            if dup_result.is_blocked:
                # Within 5%: block entirely
                log.info("signal.duplicate_blocked", symbol=symbol,
                         reason=dup_result.reason)
                from core.time_utils import format_time, now_utc
                channel_name = (
                    getattr(signal, "channel_name", "")
                    or getattr(signal, "source_channel_name", "")
                    or "Unknown"
                )
                await self._safe_notify(
                    f"❌ SIGNAL BLOCKERAD (Dubblett ≤{self._settings.duplicate.threshold_pct}%) ❌\n"
                    f"🕒 Time: {format_time(now_utc())}\n"
                    f"📢 From channel: {channel_name}\n"
                    f"📊 Symbol: #{symbol}\n"
                    f"📈 Riktning: {direction}\n"
                    f"📍 {dup_result.reason}"
                )
                try:
                    await self._db.increment_report_stat(
                        0, "ALL", datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        "blocked_count",
                    )
                except Exception:
                    pass
                return None

            if dup_result.is_update:
                # Beyond 5%: update existing trade's TP/SL instead
                log.info("signal.update_existing", symbol=symbol,
                         reason=dup_result.reason)
                await self._update_existing_trade(signal, dup_result.existing_trade)
                return None

        # ----------------------------------------------------------
        # 2. Capacity check.
        # ----------------------------------------------------------
        max_trades = self._settings.capacity.max_active_trades
        if len(self._active_trades) >= max_trades:
            log.warning(
                "signal.capacity_full",
                symbol=symbol,
                active=len(self._active_trades),
                max=max_trades,
            )
            await self._safe_notify(
                f"[BLOCKERAD] {symbol}: maxkapacitet ({max_trades} trades) nadd."
            )
            return None

        # ----------------------------------------------------------
        # 3. Stale signal check (skip for re-entries).
        # ----------------------------------------------------------
        if not is_reentry:
            max_age = self._settings.stale_signal.max_age_seconds
            if self._is_stale(signal, max_age):
                log.info(
                    "signal.stale",
                    symbol=symbol,
                    max_age=max_age,
                )
                await self._safe_notify(
                    f"[BLOCKERAD] {symbol}: signal for gammal "
                    f"(> {max_age}s)."
                )
                return None

        # ----------------------------------------------------------
        # 4. Auto-SL fallback if SL is missing.
        # ----------------------------------------------------------
        entry_price = signal.entry
        sl_price = signal.sl if hasattr(signal, "sl") else None
        auto_sl_applied = False

        if sl_price is None:
            auto_sl_applied = True
            fallback_pct = self._settings.auto_sl.fallback_pct
            if direction == "LONG":
                sl_price = round(entry_price * (1.0 - fallback_pct / 100.0), 8)
            else:
                sl_price = round(entry_price * (1.0 + fallback_pct / 100.0), 8)
            log.info(
                "signal.auto_sl",
                symbol=symbol,
                sl_price=sl_price,
                fallback_pct=fallback_pct,
            )

        # ----------------------------------------------------------
        # 5. Calculate dynamic leverage.
        # ----------------------------------------------------------
        if auto_sl_applied:
            # Lock leverage when auto-SL is used.
            leverage = self._settings.auto_sl.fallback_leverage
        else:
            leverage = calculate_leverage(
                entry=entry_price,
                sl=sl_price,
                settings=(self._settings.wallet, self._settings.leverage),
            )

        # ----------------------------------------------------------
        # 6. Calculate order quantity (rounded to exchange precision).
        # ----------------------------------------------------------
        initial_margin = self._settings.wallet.initial_margin
        raw_quantity = (initial_margin * leverage) / entry_price

        # Ensure instrument info is cached for this symbol.
        # If the symbol doesn't exist on Bybit, reject the signal early.
        try:
            instrument_info = await self._bybit.get_instrument_info(symbol)
            if not instrument_info:
                log.warning(
                    "signal.symbol_not_on_bybit",
                    symbol=symbol,
                )
                await self._safe_notify(
                    f"⚠️ SYMBOL EJ TILLGANGLIG\n"
                    f"📊 Symbol: #{symbol}\n"
                    f"📍 {symbol} finns inte som perpetual pa Bybit. Signal hoppas over."
                )
                return None
        except Exception:
            log.warning(
                "signal.instrument_info_fetch_failed",
                symbol=symbol,
            )
            await self._safe_notify(
                f"⚠️ SYMBOL VALIDERING MISSLYCKADES\n"
                f"📊 Symbol: #{symbol}\n"
                f"📍 Kunde inte kontrollera {symbol} pa Bybit. Signal hoppas over."
            )
            return None

        # Round to the symbol's precision using Bybit instrument info.
        try:
            quantity = self._bybit.calculate_order_qty(
                margin=initial_margin,
                leverage=leverage,
                price=entry_price,
                symbol=symbol,
            )
        except Exception:
            # Fallback: floor to 3 decimals for most symbols.
            # For high-price assets like BTC, use smaller precision.
            import math
            if entry_price > 10000:
                step = 0.001  # BTC-like
            elif entry_price > 100:
                step = 0.01   # ETH-like
            elif entry_price > 1:
                step = 0.1    # SOL-like
            else:
                step = 1.0    # Low-price tokens
            quantity = math.floor(raw_quantity / step) * step
            log.warning(
                "signal.qty_precision_fallback",
                symbol=symbol,
                raw_qty=raw_quantity,
                step=step,
                rounded_qty=quantity,
            )

        # Split into 2 equal parts for 2 entry orders,
        # rounded down to exchange step size.
        try:
            qty_per_order = self._bybit.round_qty(quantity / 2.0, symbol)
        except Exception:
            import math
            if entry_price > 10000:
                step = 0.001
            elif entry_price > 100:
                step = 0.01
            elif entry_price > 1:
                step = 0.1
            else:
                step = 1.0
            qty_per_order = math.floor((quantity / 2.0) / step) * step

        # Ensure qty meets minimum order size.
        try:
            min_qty = await self._bybit.get_min_order_qty(symbol)
            if qty_per_order < min_qty:
                log.warning(
                    "signal.qty_below_minimum",
                    symbol=symbol,
                    qty=qty_per_order,
                    min_qty=min_qty,
                )
                qty_per_order = min_qty
                quantity = min_qty * 2
        except Exception:
            pass

        if qty_per_order <= 0:
            log.error(
                "signal.zero_quantity",
                symbol=symbol,
                leverage=leverage,
                entry_price=entry_price,
            )
            return None

        # ----------------------------------------------------------
        # 7. Save signal and create trade in DB.
        # ----------------------------------------------------------
        tp_list = (
            signal.tps if hasattr(signal, "tps")
            else signal.tp_list if hasattr(signal, "tp_list")
            else []
        )

        signal_db_id: Optional[int] = None
        try:
            signal_db_id = await self._db.save_signal({
                "symbol": symbol,
                "direction": direction,
                "entry_price": entry_price,
                "sl_price": sl_price,
                "tp_prices": tp_list,
                "source_channel_id": getattr(signal, "channel_id", None)
                    or getattr(signal, "source_channel_id", None),
                "source_channel_name": getattr(signal, "channel_name", None)
                    or getattr(signal, "source_channel_name", None),
                "signal_type": getattr(signal, "signal_type", "dynamic"),
                "raw_text": getattr(signal, "raw_text", ""),
                "received_at": (
                    signal.received_at.isoformat()
                    if hasattr(signal, "received_at")
                        and isinstance(signal.received_at, datetime)
                    else None
                ),
            })
        except Exception:
            log.exception("signal.db_save_error", symbol=symbol)

        trade = Trade(
            signal=signal,
            state=TradeState.PENDING,
            quantity=quantity,
            leverage=leverage,
            margin=initial_margin,
            sl_price=sl_price,
            reentry_count=parent_reentry_count,
        )

        trade_db_id: Optional[int] = None
        try:
            trade_db_id = await self._db.save_trade({
                "signal_id": signal_db_id,
                "state": trade.state.value,
                "quantity": quantity,
                "leverage": leverage,
                "margin": initial_margin,
                "sl_price": sl_price,
                "reentry_count": parent_reentry_count,
            })
            if trade_db_id is not None:
                # Use the DB row ID as the canonical trade ID for consistency.
                trade.id = str(trade_db_id)
        except Exception:
            log.exception("trade.db_save_error", symbol=symbol)

        # ----------------------------------------------------------
        # 8. Place entry order 1 (Market, hedge-mode positionIdx).
        # ----------------------------------------------------------
        side = "Buy" if direction == "LONG" else "Sell"
        position_idx = 1 if direction == "LONG" else 2

        order1_result = await self._place_entry_order(
            trade=trade,
            symbol=symbol,
            side=side,
            qty=qty_per_order,
            position_idx=position_idx,
            order_label="entry1",
        )
        if order1_result is None:
            trade.transition(TradeState.ERROR)
            await self._persist_trade_state(trade)
            return None

        order1_bybit_id = order1_result.get("orderId", "")
        trade.entry1_order_id = order1_bybit_id
        trade.bybit_order_ids.append(order1_bybit_id)
        trade.transition(TradeState.ENTRY1_PLACED)
        await self._persist_trade_state(trade, entry1_order_id_bybit=order1_bybit_id)

        # ----------------------------------------------------------
        # 9. Send "Signal mottagen" notification.
        # ----------------------------------------------------------
        from core.time_utils import format_time, now_utc
        channel_name = (
            getattr(signal, "channel_name", "")
            or getattr(signal, "source_channel_name", "")
            or "Unknown"
        )
        auto_sl_note = " (Auto-SL)" if auto_sl_applied else ""

        # Build TP lines - only real TPs (skip zeros/empty)
        tp_lines = []
        for i, tp in enumerate(tp_list, start=1):
            if tp and tp > 0:
                if entry_price > 0:
                    if direction == "LONG":
                        pct = (tp - entry_price) / entry_price * 100
                    else:
                        pct = (entry_price - tp) / entry_price * 100
                    tp_lines.append(f"🎯 TP{i}: {tp} ({pct:+.2f}%)")
                else:
                    tp_lines.append(f"🎯 TP{i}: {tp}")
        tp_text = "\n".join(tp_lines) if tp_lines else "🎯 TP: (none)"

        # SL with percentage
        if sl_price and entry_price > 0:
            if direction == "LONG":
                sl_pct = (sl_price - entry_price) / entry_price * 100
            else:
                sl_pct = (entry_price - sl_price) / entry_price * 100
            sl_text = f"🚩 SL: {sl_price} ({sl_pct:+.2f}%)"
        else:
            sl_text = f"🚩 SL: {sl_price}"

        await self._safe_notify(
            f"✅ [SIGNAL MOTTAGEN] {symbol} {direction}{auto_sl_note}\n"
            f"🕒 Time: {format_time(now_utc())}\n"
            f"📢 From channel: {channel_name}\n"
            f"📊 Symbol: #{symbol}\n"
            f"📈 Riktning: {direction}\n"
            f"\n"
            f"💥 Entry: {entry_price}\n"
            f"{tp_text}\n"
            f"{sl_text}\n"
            f"\n"
            f"⚙️ Leverage: x{leverage}\n"
            f"💰 IM: {initial_margin} USDT\n"
            f"💵 Qty: {quantity}\n"
            f"\n"
            f"Entry 1 placerad..."
        )

        # ----------------------------------------------------------
        # 10. Wait for entry 1 fill confirmation.
        # ----------------------------------------------------------
        fill1 = await self._wait_for_fill(
            order1_bybit_id,
            timeout=self._settings.entry.entry_timeout_seconds,
            order_result=order1_result,
            symbol=symbol,
        )
        if fill1 is None:
            log.warning(
                "entry1.fill_timeout",
                trade_id=trade.id,
                symbol=symbol,
            )
            await self._abort_trade(trade, "Entry 1 fylldes inte inom timeout.")
            return None

        trade.entry1_fill_price = float(fill1.get("avgPrice", 0) or fill1.get("price", 0) or entry_price)
        trade.transition(TradeState.ENTRY1_FILLED)
        await self._persist_trade_state(trade, entry1_fill_price=trade.entry1_fill_price)

        # ----------------------------------------------------------
        # 11. Place entry order 2.
        # ----------------------------------------------------------
        order2_result = await self._place_entry_order(
            trade=trade,
            symbol=symbol,
            side=side,
            qty=qty_per_order,
            position_idx=position_idx,
            order_label="entry2",
        )
        if order2_result is None:
            # Entry 1 filled but entry 2 failed -- close partial.
            await self._abort_trade(
                trade,
                "Entry 2 kunde inte placeras. Stanger partiell position.",
                close_partial=True,
            )
            return None

        order2_bybit_id = order2_result.get("orderId", "")
        trade.entry2_order_id = order2_bybit_id
        trade.bybit_order_ids.append(order2_bybit_id)
        trade.transition(TradeState.ENTRY2_PLACED)
        await self._persist_trade_state(trade, entry2_order_id_bybit=order2_bybit_id)

        # ----------------------------------------------------------
        # 12. Wait for entry 2 fill confirmation.
        # ----------------------------------------------------------
        fill2 = await self._wait_for_fill(
            order2_bybit_id,
            timeout=self._settings.entry.entry_timeout_seconds,
            order_result=order2_result,
            symbol=symbol,
        )
        if fill2 is None:
            log.warning(
                "entry2.fill_timeout",
                trade_id=trade.id,
                symbol=symbol,
            )
            await self._abort_trade(
                trade,
                "Entry 2 fylldes inte inom timeout. Stanger partiell position.",
                close_partial=True,
            )
            return None

        trade.entry2_fill_price = float(fill2.get("avgPrice", 0) or fill2.get("price", 0) or entry_price)
        trade.transition(TradeState.ENTRY2_FILLED)

        # ----------------------------------------------------------
        # 13. Both filled: calculate avg_entry.
        # ----------------------------------------------------------
        avg_entry = round(
            (trade.entry1_fill_price + trade.entry2_fill_price) / 2.0, 8
        )
        trade.avg_entry = avg_entry

        await self._safe_notify(
            f"[ENTRIES MERGED] {symbol} {direction}\n"
            f"Fill 1: {trade.entry1_fill_price} | Fill 2: {trade.entry2_fill_price}\n"
            f"Snittpris: {avg_entry}"
        )

        # ----------------------------------------------------------
        # 14. Set TP and SL via set_trading_stop.
        # ----------------------------------------------------------
        # Get current market price to validate TPs against actual
        # mark price (not just avg_entry).
        current_mark = avg_entry
        try:
            ticker = await self._bybit.get_ticker(symbol)
            if ticker:
                mp = float(ticker.get("markPrice", 0) or 0)
                if mp > 0:
                    current_mark = mp
        except Exception:
            pass

        # Find a valid TP that hasn't been passed by current mark price.
        # For LONG: TP must be ABOVE current mark price
        # For SHORT: TP must be BELOW current mark price
        tp_price = None
        for tp in tp_list:
            if tp and tp > 0:
                if direction == "LONG" and tp > current_mark:
                    tp_price = tp
                    break
                elif direction == "SHORT" and tp < current_mark:
                    tp_price = tp
                    break

        if tp_price is None and tp_list:
            log.warning(
                "trade.all_tps_already_passed",
                trade_id=trade.id,
                symbol=symbol,
                avg_entry=avg_entry,
                current_mark=current_mark,
                tps=tp_list,
            )

        # Validate SL direction against current mark price.
        # For LONG: SL must be BELOW current mark
        # For SHORT: SL must be ABOVE current mark
        valid_sl = sl_price
        if valid_sl:
            if direction == "LONG" and valid_sl >= current_mark:
                log.warning("trade.sl_above_mark_long",
                            sl=valid_sl, mark=current_mark)
                valid_sl = None
            elif direction == "SHORT" and valid_sl <= current_mark:
                log.warning("trade.sl_below_mark_short",
                            sl=valid_sl, mark=current_mark)
                valid_sl = None

        # Set TP and/or SL (only if we have valid values)
        if tp_price or valid_sl:
            try:
                kwargs = {"symbol": symbol, "position_idx": position_idx}
                if tp_price:
                    kwargs["take_profit"] = tp_price
                if valid_sl:
                    kwargs["stop_loss"] = valid_sl

                await self._bybit.set_trading_stop(**kwargs)
                log.info(
                    "trade.tp_sl_set",
                    trade_id=trade.id,
                    symbol=symbol,
                    tp=tp_price,
                    sl=valid_sl,
                )
            except Exception:
                log.exception(
                    "trade.tp_sl_error",
                    trade_id=trade.id,
                    symbol=symbol,
                )
                await self._safe_notify(
                    f"[VARNING] {symbol}: TP/SL kunde inte sattas pa borsen. "
                    f"Manuell atgard kan kravas."
                )
        else:
            log.warning(
                "trade.no_valid_tp_sl",
                trade_id=trade.id,
                symbol=symbol,
            )

        # ----------------------------------------------------------
        # 15. Transition to POSITION_OPEN.
        # ----------------------------------------------------------
        trade.transition(TradeState.POSITION_OPEN)

        await self._persist_trade_state(
            trade,
            entry2_fill_price=trade.entry2_fill_price,
            avg_entry=avg_entry,
        )

        await self._db.log_event(
            trade_id=int(trade.id),
            event_type="position_opened",
            details={
                "avg_entry": avg_entry,
                "quantity": quantity,
                "leverage": leverage,
                "sl": sl_price,
                "tp": tp_price,
                "auto_sl": auto_sl_applied,
                "is_reentry": is_reentry,
            },
        )

        # Register in the active trades map.
        self._active_trades[trade.id] = trade

        log.info(
            "trade.opened",
            trade_id=trade.id,
            symbol=symbol,
            direction=direction,
            avg_entry=avg_entry,
            leverage=leverage,
        )

        return trade

    # ==================================================================
    # WebSocket event handlers
    # ==================================================================

    async def on_order_update(self, data: dict) -> None:
        """Handle a WebSocket order update.

        Tracks order status changes and triggers fill events for the
        entry pipeline.
        """
        order_id = data.get("orderId", "")
        status = data.get("orderStatus", "")
        symbol = data.get("symbol", "")

        log.debug(
            "ws.order_update",
            order_id=order_id,
            status=status,
            symbol=symbol,
        )

        # Persist order status.
        try:
            bot_id = data.get("orderLinkId", order_id)
            await self._db.update_order(
                bot_id,
                status=status,
                fill_price=data.get("avgPrice"),
                fill_qty=data.get("cumExecQty"),
            )
        except Exception:
            log.exception("ws.order_update_db_error", order_id=order_id)

        # Always store fill data for filled orders so _wait_for_fill
        # can find it even if the WS event arrives before the wait starts.
        if status == "Filled":
            self._fill_data[order_id] = data
            # Signal the event if someone is already waiting.
            if order_id in self._fill_events:
                self._fill_events[order_id].set()

    async def on_position_update(self, data: dict) -> None:
        """Handle a WebSocket position update.

        Detects position closures (e.g. from TP/SL hit on the exchange)
        that the bot did not initiate directly.
        """
        symbol = data.get("symbol", "")
        size = float(data.get("size", 0) or 0)
        side = data.get("side", "")

        log.debug(
            "ws.position_update",
            symbol=symbol,
            size=size,
            side=side,
        )

        if size == 0:
            # Position fully closed -- find the matching active trade.
            trade = self._find_trade_by_symbol_side(symbol, side)
            if trade is not None:
                exit_price = float(data.get("markPrice", 0) or 0)
                close_reason = self._infer_close_reason(trade, exit_price)
                await self.close_trade(
                    trade_id=trade.id,
                    reason=close_reason,
                    exit_price=exit_price,
                )

    async def on_execution_update(self, data: dict) -> None:
        """Handle a WebSocket execution / fill update.

        An execution is a partial or full fill of an order. We use this
        as an alternative fill-detection path in addition to order
        status updates.
        """
        order_id = data.get("orderId", "")
        exec_type = data.get("execType", "")
        symbol = data.get("symbol", "")

        log.debug(
            "ws.execution_update",
            order_id=order_id,
            exec_type=exec_type,
            symbol=symbol,
        )

        if exec_type == "Trade":
            self._fill_data[order_id] = data
            if order_id in self._fill_events:
                self._fill_events[order_id].set()

    # ==================================================================
    # Price update handler -- delegates to sub-managers
    # ==================================================================

    async def handle_price_update(
        self,
        symbol: str,
        price: float,
    ) -> None:
        """Check all active trades for management triggers.

        Called on every relevant price tick (mark price / last price).
        Delegates to the appropriate sub-manager for each trade.
        """
        for trade in list(self._active_trades.values()):
            if trade.signal is None:
                continue
            if trade.signal.symbol != symbol:
                continue
            if trade.is_terminal:
                continue

            try:
                await self._check_trade_triggers(trade, price)
            except Exception:
                log.exception(
                    "price_update.error",
                    trade_id=trade.id,
                    symbol=symbol,
                    price=price,
                )

    async def _check_trade_triggers(
        self,
        trade: Trade,
        current_price: float,
    ) -> None:
        """Run all management checks on a single trade at the given price."""
        state = trade.state

        # Skip trades that are not yet fully open.
        if state in (
            TradeState.PENDING,
            TradeState.ENTRY1_PLACED,
            TradeState.ENTRY1_FILLED,
            TradeState.ENTRY2_PLACED,
            TradeState.ENTRY2_FILLED,
        ):
            return

        # --- Re-entry monitoring (REENTRY_WAITING state) ---
        if state == TradeState.REENTRY_WAITING:
            await self._reentry_mgr.check_and_activate(trade, current_price)
            return

        # --- Break-even check (only if not yet applied) ---
        if trade.be_price is None:
            applied = await self._be_mgr.check_and_apply(trade, current_price)
            if applied:
                return  # Give the trade a tick before checking further.

        # --- Scaling check (next pending step) ---
        if trade.scaling_step < len(self._settings.scaling.steps):
            applied = await self._scaling_mgr.check_and_apply(
                trade, current_price
            )
            if applied:
                return

        # --- Trailing stop activation ---
        if trade.trailing_sl is None:
            applied = await self._trailing_mgr.check_and_activate(
                trade, current_price
            )
            if applied:
                return

        # --- Hedge trigger ---
        if trade.hedge_trade_id is None:
            await self._hedge_mgr.check_and_activate(trade, current_price)

    # ==================================================================
    # Trade closure
    # ==================================================================

    async def close_trade(
        self,
        trade_id: str,
        reason: str,
        exit_price: float,
    ) -> None:
        """Close a trade, update DB, remove from active map, and notify.

        Parameters
        ----------
        trade_id:
            The trade's ID (string, matching ``Trade.id``).
        reason:
            Human-readable close reason (e.g. ``"tp_hit"``, ``"sl_hit"``).
        exit_price:
            The price at which the position was closed.
        """
        trade = self._active_trades.get(trade_id)
        if trade is None:
            log.warning("close_trade.not_found", trade_id=trade_id)
            return

        if trade.is_terminal:
            log.debug("close_trade.already_closed", trade_id=trade_id)
            return

        symbol = trade.signal.symbol if trade.signal else "UNKNOWN"
        direction = trade.signal.direction if trade.signal else "?"

        # --- Close any open hedge ---
        if trade.hedge_trade_id is not None:
            await self._hedge_mgr.close_hedge(trade, exit_price)

        # --- Compute PnL ---
        pnl_pct: Optional[float] = None
        pnl_usdt: Optional[float] = None

        if trade.avg_entry and trade.avg_entry > 0 and trade.quantity:
            if direction == "LONG":
                pnl_pct = round(
                    (exit_price - trade.avg_entry) / trade.avg_entry * 100.0, 4
                )
            else:
                pnl_pct = round(
                    (trade.avg_entry - exit_price) / trade.avg_entry * 100.0, 4
                )
            pnl_usdt = round(
                pnl_pct / 100.0 * (trade.margin or 0) * (trade.leverage or 1), 4
            )

        trade.pnl_pct = pnl_pct
        trade.pnl_usdt = pnl_usdt
        trade.close_reason = reason
        trade.closed_at = datetime.now(timezone.utc)
        trade.transition(TradeState.CLOSED)

        # Persist.
        try:
            await self._db.update_trade(
                int(trade.id),
                state=trade.state.value,
                close_reason=reason,
                pnl_pct=pnl_pct,
                pnl_usdt=pnl_usdt,
                closed_at=trade.closed_at.isoformat(),
            )
            await self._db.log_event(
                trade_id=int(trade.id),
                event_type="trade_closed",
                details={
                    "reason": reason,
                    "exit_price": exit_price,
                    "pnl_pct": pnl_pct,
                    "pnl_usdt": pnl_usdt,
                },
            )
        except Exception:
            log.exception("close_trade.db_error", trade_id=trade_id)

        # Update report stats.
        await self._update_report_stats(trade, pnl_pct, pnl_usdt, reason)

        # Remove from active map.
        self._active_trades.pop(trade_id, None)

        # Notify.
        pnl_str = f"{pnl_pct:+.2f}%" if pnl_pct is not None else "N/A"
        usdt_str = f"{pnl_usdt:+.2f} USDT" if pnl_usdt is not None else ""
        await self._safe_notify(
            f"[STANGD] {symbol} {direction}\n"
            f"Anledning: {reason}\n"
            f"Exit: {exit_price}\n"
            f"PnL: {pnl_str} {usdt_str}"
        )

        log.info(
            "trade.closed",
            trade_id=trade_id,
            symbol=symbol,
            reason=reason,
            pnl_pct=pnl_pct,
        )

        # --- Check re-entry eligibility ---
        if reason in ("sl_hit", "be_hit", "breakeven_hit", "stop_loss"):
            if trade.reentry_count < self._settings.reentry.max_reentries:
                trade.transition(TradeState.REENTRY_WAITING)
                self._active_trades[trade.id] = trade  # Re-add for monitoring.
                try:
                    await self._db.update_trade(
                        int(trade.id),
                        state=trade.state.value,
                    )
                except Exception:
                    log.exception("close_trade.reentry_state_error", trade_id=trade_id)

    # ==================================================================
    # Active trades accessor
    # ==================================================================

    async def get_active_trades(self) -> List[Trade]:
        """Return a snapshot of all currently active trades."""
        return list(self._active_trades.values())

    # ==================================================================
    # Cleanup stale orders
    # ==================================================================

    async def cleanup_timeout_orders(self) -> None:
        """Cancel and delete unfilled orders older than the configured timeout.

        Should be called periodically (e.g. every hour) by the main loop.
        """
        timeout_hours = self._settings.timeout.unfilled_order_hours

        try:
            stale_orders = await self._db.get_unfilled_orders(
                older_than_hours=timeout_hours
            )
        except Exception:
            log.exception("cleanup.db_error")
            return

        if not stale_orders:
            return

        log.info("cleanup.stale_orders_found", count=len(stale_orders))

        for order in stale_orders:
            bybit_id = order.get("order_id_bybit")
            bot_id = order.get("order_id_bot", "")
            symbol = order.get("symbol", "")

            # Try to cancel on exchange.
            if bybit_id:
                try:
                    await self._bybit.cancel_order(
                        symbol=symbol,
                        order_id=bybit_id,
                    )
                    log.info(
                        "cleanup.order_cancelled",
                        order_id_bybit=bybit_id,
                        symbol=symbol,
                    )
                except Exception:
                    log.exception(
                        "cleanup.cancel_error",
                        order_id_bybit=bybit_id,
                        symbol=symbol,
                    )

            # Delete from local DB.
            try:
                await self._db.delete_order(bot_id)
            except Exception:
                log.exception("cleanup.delete_error", order_id_bot=bot_id)

        await self._safe_notify(
            f"[CLEANUP] {len(stale_orders)} ofyllda order(s) aldre an "
            f"{timeout_hours}h raderade."
        )

    # ==================================================================
    # Internal helpers
    # ==================================================================

    async def _place_entry_order(
        self,
        trade: Trade,
        symbol: str,
        side: str,
        qty: float,
        position_idx: int,
        order_label: str,
    ) -> Optional[dict]:
        """Place a single entry order and save it to the DB.

        Returns the Bybit order result dict, or None on failure.
        """
        order = OrderRecord(symbol=symbol, side=side, qty=qty)

        try:
            result = await self._bybit.place_market_order(
                symbol=symbol,
                side=side,
                qty=qty,
                position_idx=position_idx,
            )
        except Exception:
            log.exception(
                f"{order_label}.place_error",
                trade_id=trade.id,
                symbol=symbol,
            )
            await self._safe_notify(
                f"[ORDER ERROR] {symbol}: {order_label} kunde inte placeras. "
                f"Se loggar."
            )
            return None

        order_id_bybit = result.get("orderId", "")
        order.order_id_bybit = order_id_bybit

        # Register for fill tracking.
        self._fill_events[order_id_bybit] = asyncio.Event()
        self._order_to_trade[order_id_bybit] = trade.id

        # Save order to DB.
        try:
            await self._db.save_order({
                "trade_id": int(trade.id),
                "order_id_bot": order.order_id_bot,
                "order_id_bybit": order_id_bybit,
                "symbol": symbol,
                "side": side,
                "order_type": "Market",
                "qty": qty,
                "status": "New",
            })
        except Exception:
            log.exception(f"{order_label}.db_save_error", trade_id=trade.id)

        log.info(
            f"{order_label}.placed",
            trade_id=trade.id,
            symbol=symbol,
            order_id_bybit=order_id_bybit,
            qty=qty,
        )
        return result

    def _register_fill_event(self, order_id: str) -> None:
        """Pre-register an asyncio.Event for an order BEFORE placing it."""
        if order_id and order_id not in self._fill_events:
            self._fill_events[order_id] = asyncio.Event()

    async def _wait_for_fill(
        self,
        order_id: str,
        timeout: int = 30,
        order_result: Optional[dict] = None,
        symbol: Optional[str] = None,
    ) -> Optional[dict]:
        """Wait for a fill event for the given Bybit order ID.

        Returns the fill data dict, or None on timeout.

        If *order_result* is provided (the REST response from placing
        the order), checks whether the order was already filled
        immediately (common for Market orders).
        """
        # Check 1: Did the order already fill in the REST response?
        if order_result:
            status = order_result.get("orderStatus", "")
            if status == "Filled":
                log.info("fill.immediate_from_rest", order_id=order_id)
                return order_result

        # Check 2: Did the fill event already arrive via WebSocket
        # before we started waiting?
        if order_id in self._fill_data:
            log.info("fill.already_received", order_id=order_id)
            data = self._fill_data.pop(order_id, None)
            self._fill_events.pop(order_id, None)
            return data

        # Check 3: Wait for the WS fill event.
        event = self._fill_events.get(order_id)
        if event is None:
            event = asyncio.Event()
            self._fill_events[order_id] = event

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return self._fill_data.get(order_id)
        except asyncio.TimeoutError:
            # Last resort: check order status via REST API.
            if symbol:
                try:
                    order_info = await self._bybit.get_order(
                        symbol=symbol,
                        order_id=order_id,
                    )
                    if order_info and order_info.get("orderStatus") == "Filled":
                        log.info("fill.found_via_rest_poll",
                                 order_id=order_id, symbol=symbol)
                        return order_info
                except Exception:
                    log.exception("fill.rest_poll_error",
                                  order_id=order_id, symbol=symbol)
            return None
        finally:
            self._fill_events.pop(order_id, None)
            self._fill_data.pop(order_id, None)

    async def _abort_trade(
        self,
        trade: Trade,
        message: str,
        close_partial: bool = False,
    ) -> None:
        """Abort a trade that failed during entry.

        If ``close_partial`` is True, attempts to close any partially
        filled position on the exchange.
        """
        symbol = trade.signal.symbol if trade.signal else "UNKNOWN"
        direction = trade.signal.direction if trade.signal else "?"

        log.warning(
            "trade.abort",
            trade_id=trade.id,
            symbol=symbol,
            reason=message,
        )

        if close_partial and trade.entry1_fill_price is not None:
            # Close the partial position.
            close_side = "Sell" if direction == "LONG" else "Buy"
            position_idx = 1 if direction == "LONG" else 2
            qty = round((trade.quantity or 0) / 2.0, 8)

            try:
                await self._bybit.place_market_order(
                    symbol=symbol,
                    side=close_side,
                    qty=qty,
                    position_idx=position_idx,
                    reduce_only=True,
                )
            except Exception:
                log.exception(
                    "trade.abort_close_error",
                    trade_id=trade.id,
                    symbol=symbol,
                )

        # Cancel any pending orders.
        for oid in trade.bybit_order_ids:
            try:
                await self._bybit.cancel_order(symbol=symbol, order_id=oid)
            except Exception:
                pass  # Best effort -- order may already be filled/cancelled.

        trade.transition(TradeState.ERROR)
        trade.close_reason = message

        await self._persist_trade_state(trade, close_reason=message)

        await self._safe_notify(
            f"[AVBRUTEN] {symbol} {direction}\n{message}"
        )

    async def _persist_trade_state(self, trade: Trade, **extra: Any) -> None:
        """Persist the current trade state and any extra fields to DB."""
        try:
            await self._db.update_trade(
                int(trade.id),
                state=trade.state.value,
                **extra,
            )
        except Exception:
            log.exception("trade.persist_error", trade_id=trade.id)

    def _is_stale(self, signal: Any, max_age_seconds: int) -> bool:
        """Return True if the signal is older than *max_age_seconds*."""
        # Check parsed_at (unix timestamp from signal_parser).
        if hasattr(signal, "parsed_at") and signal.parsed_at > 0:
            age = time.time() - signal.parsed_at
            return age > max_age_seconds

        # Fallback: check received_at (datetime).
        if hasattr(signal, "received_at") and isinstance(
            signal.received_at, datetime
        ):
            age = (
                datetime.now(timezone.utc) - signal.received_at
            ).total_seconds()
            return age > max_age_seconds

        # Cannot determine age -> allow through.
        return False

    def _find_trade_by_symbol_side(
        self,
        symbol: str,
        side: str,
    ) -> Optional[Trade]:
        """Find an active trade matching *symbol* and position *side*."""
        for trade in self._active_trades.values():
            if trade.signal is None:
                continue
            if trade.signal.symbol != symbol:
                continue
            trade_side = "Buy" if trade.signal.direction == "LONG" else "Sell"
            if trade_side == side or not side:
                return trade
        return None

    def _infer_close_reason(self, trade: Trade, exit_price: float) -> str:
        """Infer why a position was closed based on exit price vs SL/TP."""
        if trade.signal is None:
            return "unknown"

        # Check if exit matches SL.
        sl = trade.sl_price or trade.be_price
        if sl and exit_price > 0:
            sl_dist = abs(exit_price - sl) / sl * 100.0
            if sl_dist < 0.5:
                if trade.be_price is not None and abs(sl - trade.be_price) < 0.01:
                    return "be_hit"
                return "sl_hit"

        # Check if exit matches any TP.
        tp_list = (
            trade.signal.tps if hasattr(trade.signal, "tps")
            else trade.signal.tp_list if hasattr(trade.signal, "tp_list")
            else []
        )
        for tp in tp_list:
            if tp > 0:
                tp_dist = abs(exit_price - tp) / tp * 100.0
                if tp_dist < 0.5:
                    return "tp_hit"

        # Check if trailing stop.
        if trade.trailing_sl is not None:
            return "trailing_stop"

        return "unknown"

    async def _update_report_stats(
        self,
        trade: Trade,
        pnl_pct: Optional[float],
        pnl_usdt: Optional[float],
        reason: str,
    ) -> None:
        """Increment the relevant report_stats counters for a closed trade."""
        channel_id = 0
        channel_name = ""
        if trade.signal:
            channel_id = getattr(trade.signal, "channel_id", 0) or getattr(
                trade.signal, "source_channel_id", 0
            )
            channel_name = getattr(trade.signal, "channel_name", "") or getattr(
                trade.signal, "source_channel_name", ""
            )

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        try:
            # Win/loss counter.
            if pnl_pct is not None:
                field = "wins" if pnl_pct > 0 else "losses"
                await self._db.increment_report_stat(
                    channel_id, channel_name, today, field
                )

            # Reason-specific counter.
            reason_map = {
                "tp_hit": "tp_count",
                "sl_hit": "sl_count",
                "be_hit": "sl_count",
                "trailing_stop": "trailing_stop_count",
            }
            stat_field = reason_map.get(reason)
            if stat_field:
                await self._db.increment_report_stat(
                    channel_id, channel_name, today, stat_field
                )

            # Profit tracking.
            if pnl_usdt is not None:
                await self._db.update_report_profit(
                    channel_id, channel_name, today, pnl_usdt
                )
        except Exception:
            log.exception("report_stats.update_error", trade_id=trade.id)

    # ------------------------------------------------------------------
    # Update existing trade TP/SL (signal >5% entry diff)
    # ------------------------------------------------------------------

    async def _update_existing_trade(
        self,
        signal: Any,
        existing_trade_row: dict,
    ) -> None:
        """
        When a new signal for the same symbol arrives with entry >5%
        different, update the existing trade's TP and SL levels instead
        of opening a duplicate position.

        Also performs a liquidation safety check after the update.
        """
        trade_id = existing_trade_row.get("id")
        symbol = signal.symbol
        direction = signal.direction

        # Get new TP/SL from the incoming signal.
        new_tps = signal.tps if hasattr(signal, "tps") else (
            signal.tp_list if hasattr(signal, "tp_list") else []
        )
        new_sl = signal.sl if hasattr(signal, "sl") else None

        log.info(
            "trade.updating_tp_sl",
            trade_id=trade_id,
            symbol=symbol,
            new_tps=new_tps,
            new_sl=new_sl,
        )

        # Determine positionIdx for Bybit hedge mode.
        position_idx = 1 if direction == "LONG" else 2

        # --- Update TP on exchange ---
        try:
            highest_tp = max(new_tps) if new_tps else None
            update_params = {}
            if highest_tp and highest_tp > 0:
                update_params["take_profit"] = highest_tp
            if new_sl and new_sl > 0:
                update_params["stop_loss"] = new_sl

            if update_params:
                await self._bybit.set_trading_stop(
                    symbol=symbol,
                    position_idx=position_idx,
                    **update_params,
                    tp_trigger_by=self._settings.tp_sl.trigger_type,
                    sl_trigger_by=self._settings.tp_sl.trigger_type,
                )
                log.info(
                    "trade.tp_sl_updated",
                    trade_id=trade_id,
                    symbol=symbol,
                    **update_params,
                )
        except Exception:
            log.exception(
                "trade.tp_sl_update_failed",
                trade_id=trade_id,
                symbol=symbol,
            )
            await self._safe_notify(
                f"❌ TP/SL UPPDATERING MISSLYCKADES ❌\n"
                f"📊 Symbol: #{symbol}\n"
                f"📍 Kunde inte uppdatera TP/SL pa befintlig trade"
            )
            return

        # --- Liquidation safety check ---
        try:
            position = await self._bybit.get_position(symbol, "Buy" if direction == "LONG" else "Sell")
            if position:
                liq_price = float(position.get("liqPrice", 0) or 0)
                mark_price = float(position.get("markPrice", 0) or 0)

                if liq_price > 0 and mark_price > 0:
                    liq_distance_pct = abs(mark_price - liq_price) / mark_price * 100

                    if liq_distance_pct < 2.0:
                        log.warning(
                            "trade.liquidation_risk_after_update",
                            trade_id=trade_id,
                            symbol=symbol,
                            liq_price=liq_price,
                            mark_price=mark_price,
                            liq_distance_pct=round(liq_distance_pct, 2),
                        )
                        await self._safe_notify(
                            f"⚠️ LIKVIDATIONSVARNING ⚠️\n"
                            f"📊 Symbol: #{symbol}\n"
                            f"📍 Likvidationspris: {liq_price}\n"
                            f"📍 Marknadspris: {mark_price}\n"
                            f"📍 Avstand: {liq_distance_pct:.2f}%\n"
                            f"📍 Kontrollera positionen manuellt!"
                        )
                    else:
                        log.info(
                            "trade.liquidation_check_ok",
                            trade_id=trade_id,
                            liq_distance_pct=round(liq_distance_pct, 2),
                        )
        except Exception:
            log.exception(
                "trade.liquidation_check_failed",
                trade_id=trade_id,
                symbol=symbol,
            )

        # --- Update DB ---
        try:
            import json
            update_fields = {}
            if new_sl:
                update_fields["sl_price"] = new_sl
            if new_tps:
                update_fields["tp_hits"] = json.dumps([])  # Reset TP tracking

            if update_fields:
                await self._db.update_trade(int(trade_id), **update_fields)

            await self._db.log_event(
                trade_id=int(trade_id),
                event_type="tp_sl_updated_from_signal",
                details={
                    "new_tps": new_tps,
                    "new_sl": new_sl,
                    "source_channel": getattr(signal, "channel_name", "unknown"),
                },
            )
        except Exception:
            log.exception("trade.db_update_failed", trade_id=trade_id)

        # --- Notify ---
        tp_str = ", ".join(str(t) for t in new_tps if t and t > 0)
        await self._safe_notify(
            f"🔄 TP/SL UPPDATERAD (signal >5% avvikelse)\n"
            f"📊 Symbol: #{symbol}\n"
            f"📈 Riktning: {direction}\n"
            f"🎯 Nya TP: {tp_str}\n"
            f"🚩 Ny SL: {new_sl or 'oforandrad'}\n"
            f"📍 Befintlig trade uppdaterad (ingen ny position)"
        )

    async def _safe_notify(self, message: str) -> None:
        """Send a Telegram notification, swallowing errors."""
        try:
            await self._notifier._send_notify(message)
        except Exception:
            log.exception("position_manager.notify_error", message=message[:80])
