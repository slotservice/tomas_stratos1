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
        # Give hedge manager a way to force-close the main leg when the
        # combined-loss cap is hit (no-double-loss contract).
        self._hedge_mgr.bind_close_main_callback(self.close_trade)
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
                existing = dup_result.existing_trade or {}
                try:
                    await self._notifier.signal_blocked_duplicate(
                        signal=signal,
                        existing_entry=existing.get("entry_price", 0),
                        reason=dup_result.reason,
                    )
                except Exception:
                    log.exception("notify.signal_blocked_duplicate_failed")
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

        # Slippage guard: reject signal if current market price is too
        # far from the signal's entry price. Prevents placing orders on
        # stale signals where the market has already moved through the
        # TP or SL zone - the main cause of the PROMUSDT liquidation.
        try:
            ticker = await self._bybit.get_ticker(symbol)
            if ticker:
                current = float(ticker.get("markPrice", 0) or ticker.get("lastPrice", 0) or 0)
                if current > 0 and entry_price > 0:
                    price_diff_pct = abs(current - entry_price) / entry_price * 100
                    max_slippage = 3.0  # reject if market moved >3% from signal
                    if price_diff_pct > max_slippage:
                        log.warning(
                            "signal.stale_price",
                            symbol=symbol,
                            signal_entry=entry_price,
                            current_mark=current,
                            diff_pct=round(price_diff_pct, 2),
                        )
                        chan = getattr(signal, "channel_name", "") or "okand"
                        await self._safe_notify(
                            f"⚠️ SIGNAL AVVISAD (pris for langt fran entry)\n"
                            f"📢 Fran kanal: {chan}\n"
                            f"📊 Symbol: #{symbol}\n"
                            f"📈 Riktning: {direction}\n"
                            f"💥 Signal entry: {entry_price}\n"
                            f"📍 Marknadspris: {current}\n"
                            f"📍 Diff: {price_diff_pct:.2f}% (max {max_slippage}%)\n"
                            f"📍 Signalen ar for gammal / marknad har redan flyttat."
                        )
                        return None
        except Exception:
            log.exception("signal.slippage_check_error", symbol=symbol)

        # Round leverage to symbol's leverage step (keeps e.g. 12.34 precision).
        try:
            # Ensure instrument info is cached so round_leverage works.
            await self._bybit.get_instrument_info(symbol)
            leverage = self._bybit.round_leverage(leverage, symbol)
        except Exception:
            leverage = round(leverage, 2)

        # Also set the leverage on Bybit BEFORE placing the order.
        try:
            side_tmp = "Buy" if direction == "LONG" else "Sell"
            await self._bybit.set_leverage(symbol, leverage, side_tmp)
        except Exception:
            log.exception("trade.set_leverage_failed",
                          symbol=symbol, leverage=leverage)

        # ----------------------------------------------------------
        # 6. Calculate order quantity (rounded to exchange precision).
        # ----------------------------------------------------------
        initial_margin = self._settings.wallet.initial_margin
        raw_quantity = (initial_margin * leverage) / entry_price

        # Ensure instrument info is cached for this symbol.
        # If the symbol doesn't exist on Bybit, probe common Bybit
        # multiplier variants before rejecting — small-price meme tokens
        # are listed on Bybit as either <prefix><BASE>USDT (1000PEPE,
        # 1000NEIROCTO, 10000SATS) OR <BASE><suffix>USDT (SHIB1000,
        # BONK1000, FLOKI1000) — the convention depends on when the
        # pair was listed. We also handle signal symbols that embed
        # the multiplier on the opposite side (signal says
        # 1000SHIBUSDT but Bybit lists SHIB1000USDT).
        # We do NOT auto-trade the variant because prices in the
        # signal are per-1-token and would be off by the multiplier;
        # we only surface the resolved symbol to the operator.
        try:
            instrument_info = await self._bybit.get_instrument_info(symbol)
            resolved_prefix: Optional[str] = None
            if not instrument_info:
                # Strip the USDT suffix for manipulation.
                base = symbol
                if base.endswith("USDT"):
                    base = base[: -len("USDT")]
                # Also strip any leading 1000/10000/1000000 to get the
                # pure base (e.g. "1000SHIB" -> "SHIB") so we can try
                # the suffix variant.
                import re as _re
                m = _re.match(r"^(1000000|10000|1000)(.+)$", base)
                core = m.group(2) if m else base
                candidates: list[str] = []
                for mult in ("1000", "10000", "1000000"):
                    candidates.append(f"{mult}{core}USDT")  # prefix
                    candidates.append(f"{core}{mult}USDT")  # suffix
                # De-duplicate while preserving order.
                seen: set[str] = set()
                candidates = [c for c in candidates if c != symbol and not (c in seen or seen.add(c))]
                for alt in candidates:
                    try:
                        alt_info = await self._bybit.get_instrument_info(alt)
                    except Exception:
                        alt_info = None
                    if alt_info:
                        resolved_prefix = alt
                        break
                log.warning(
                    "signal.symbol_not_on_bybit",
                    symbol=symbol,
                    suggestion=resolved_prefix,
                )
                try:
                    await self._notifier.symbol_not_on_bybit(
                        signal, suggestion=resolved_prefix,
                    )
                except Exception:
                    log.exception("notify.symbol_not_on_bybit_failed")
                return None
        except Exception:
            log.warning(
                "signal.instrument_info_fetch_failed",
                symbol=symbol,
            )
            try:
                await self._notifier.symbol_not_on_bybit(signal)
            except Exception:
                log.exception("notify.symbol_not_on_bybit_failed")
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
        num_orders = max(1, self._settings.entry.num_entry_orders)
        try:
            qty_per_order = self._bybit.round_qty(quantity / num_orders, symbol)
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

        # Pre-trade guard: if a Bybit position for this symbol/side
        # already has size > 0, a new market order would MERGE with it
        # and stack the margin (40 USDT IM instead of 20 USDT — the
        # BOMEUSDT / AKEUSDT cases on 2026-04-24). This happens when a
        # residual position from a previous session wasn't closed
        # manually. Reject the signal in that case so the client can
        # clean up Bybit state explicitly instead of accumulating IM.
        # Re-entries intentionally skip this check — a re-entry is the
        # valid case where we want to re-open after a close.
        if not is_reentry:
            try:
                existing = await self._bybit.get_position(symbol, side)
                existing_size = 0.0
                if existing:
                    existing_size = float(existing.get("size", 0) or 0)
                if existing_size > 0:
                    log.warning(
                        "signal.existing_position_on_bybit",
                        symbol=symbol,
                        side=side,
                        existing_size=existing_size,
                    )
                    chan = getattr(signal, "channel_name", "") or "okand"
                    await self._safe_notify(
                        f"⚠️ SIGNAL BLOCKERAD (position finns redan på Bybit)\n"
                        f"📢 Fran kanal: {chan}\n"
                        f"📊 Symbol: #{symbol}\n"
                        f"📈 Riktning: {direction}\n"
                        f"📍 Bybit har redan storlek {existing_size} på "
                        f"{side}-sidan.\n"
                        f"📍 Sta'ng positionen manuellt pa Bybit, "
                        f"sedan kan boten ta nya signaler."
                    )
                    return None
            except Exception:
                log.exception(
                    "signal.existing_position_check_failed", symbol=symbol,
                )

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
        # 9. Send "Signal mottagen & kopierad" notification.
        # ----------------------------------------------------------
        # Effective IM = post-rounding quantity * entry / leverage.
        # This differs from the nominal initial_margin because the
        # exchange's lot-size rounding shifts the qty slightly, which
        # in turn shifts the real IM (20.12 instead of flat 20.00).
        # Client IZZU 2026-04-24: "the problem is many templates only
        # show 20 USDT as hardcoded values". Pre-fill notifications
        # can't read Bybit positionIM yet, so we surface the
        # better-than-nominal estimate here and let position_opened
        # overwrite with the exact Bybit value post-fill.
        effective_im = initial_margin
        if quantity and quantity > 0 and leverage and leverage > 0 and entry_price > 0:
            effective_im = round(quantity * entry_price / leverage, 4)

        try:
            await self._notifier.signal_received(
                signal=signal,
                leverage=leverage,
                im=effective_im,
                bot_order_id=str(trade.id),
                bybit_order_id=order1_bybit_id,
            )
        except Exception:
            log.exception("notify.signal_received_failed")

        # Send "Order placerad" - the order has been placed on Bybit.
        try:
            await self._notifier.order_placed(
                signal=signal,
                leverage=leverage,
                im=effective_im,
                entry1=entry_price,
                entry2=entry_price,
                bot_id=str(trade.id),
                bybit_id=order1_bybit_id,
            )
        except Exception:
            log.exception("notify.order_placed_failed")

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

        # Send "ENTRY 1 TAGEN" notification.
        # IM must be the ACTUAL value reported by Bybit (e.g. 19.47 USDT),
        # not our calculated estimate. Fetch from position endpoint.
        try:
            fill1_qty = float(fill1.get("cumExecQty", qty_per_order) or qty_per_order)
            pos_side = "Buy" if direction == "LONG" else "Sell"
            fill1_im = None
            try:
                pos = await self._bybit.get_position(symbol, pos_side)
                if pos:
                    fill1_im = float(pos.get("positionIM", 0) or 0)
            except Exception:
                pass
            if not fill1_im or fill1_im <= 0:
                fill1_im = (fill1_qty * trade.entry1_fill_price) / max(leverage, 1)

            # In single-order mode the full "POSITION OPPNAD" message
            # is sent later via position_opened() with TPs/SL/leverage
            # already finalized. Sending entry1_filled here would only
            # duplicate and contradict it (different IM reading, etc.).
            if num_orders > 1:
                await self._notifier.entry1_filled(
                    trade=trade,
                    qty=fill1_qty,
                    im=fill1_im,
                    im_total=fill1_im,
                    bot_id=str(trade.id),
                    bybit_id=order1_bybit_id,
                    single_order=False,
                )
        except Exception:
            log.exception("notify.entry1_filled_failed")

        # ----------------------------------------------------------
        # 11-12. Place entry order 2 (only if num_entry_orders > 1).
        # For single-order mode, skip to merge step with entry2 = entry1.
        # ----------------------------------------------------------
        if num_orders > 1:
            order2_result = await self._place_entry_order(
                trade=trade,
                symbol=symbol,
                side=side,
                qty=qty_per_order,
                position_idx=position_idx,
                order_label="entry2",
            )
            if order2_result is None:
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
        else:
            # Single-order mode: fill2 mirrors fill1 for downstream logic.
            order2_bybit_id = order1_bybit_id
            fill2 = fill1

        trade.entry2_fill_price = float(fill2.get("avgPrice", 0) or fill2.get("price", 0) or entry_price)
        trade.transition(TradeState.ENTRY2_FILLED)

        fill1_qty = float(fill1.get("cumExecQty", qty_per_order) or qty_per_order)
        fill2_qty = float(fill2.get("cumExecQty", qty_per_order) or qty_per_order)

        # Fetch actual IM from Bybit position (not calculated).
        # After entry2 the position is complete, so positionIM reflects
        # the total IM. We split proportionally for entry1 vs entry2.
        pos_side = "Buy" if direction == "LONG" else "Sell"
        total_im_actual = None
        try:
            pos = await self._bybit.get_position(symbol, pos_side)
            if pos:
                total_im_actual = float(pos.get("positionIM", 0) or 0)
        except Exception:
            pass

        if total_im_actual and total_im_actual > 0 and (fill1_qty + fill2_qty) > 0:
            total_qty = fill1_qty + fill2_qty
            fill1_im = total_im_actual * (fill1_qty / total_qty)
            fill2_im = total_im_actual * (fill2_qty / total_qty)
            # Overwrite trade.margin with the ACTUAL IM Bybit is
            # charging for this position so the "Position oppnad"
            # notification (and downstream PnL maths) reflect reality
            # instead of the configured 20.00 nominal (client IZZU
            # 2026-04-24: "money = bybit, not 20 usdt").
            trade.margin = round(total_im_actual, 4)
        else:
            # Fallback to calculated value if position query failed
            fill1_im = (fill1_qty * trade.entry1_fill_price) / max(leverage, 1)
            fill2_im = (fill2_qty * trade.entry2_fill_price) / max(leverage, 1)
            trade.margin = round(fill1_im + fill2_im, 4)

        # Send Entry 2 + Merged notifications only when actually using
        # 2 orders. Single-order mode already notified via entry1_filled.
        if num_orders > 1:
            try:
                await self._notifier.entry2_filled(
                    trade=trade,
                    qty=fill2_qty,
                    im=fill2_im,
                    im_total=fill1_im + fill2_im,
                    bot_id=str(trade.id),
                    bybit_id=order2_bybit_id,
                )
            except Exception:
                log.exception("notify.entry2_filled_failed")

        # ----------------------------------------------------------
        # 13. Calculate avg_entry and total qty.
        # For 1-order mode, both fills are the same order.
        # ----------------------------------------------------------
        if num_orders > 1:
            avg_entry = round(
                (trade.entry1_fill_price + trade.entry2_fill_price) / 2.0, 8
            )
            trade.quantity = fill1_qty + fill2_qty
        else:
            avg_entry = trade.entry1_fill_price
            trade.quantity = fill1_qty
        trade.avg_entry = avg_entry

        # Send "Sammanslagning" only in 2-order mode.
        if num_orders > 1:
            try:
                await self._notifier.entries_merged(
                    trade=trade,
                    entry1=trade.entry1_fill_price,
                    qty1=fill1_qty,
                    im1=fill1_im,
                    entry2=trade.entry2_fill_price,
                    qty2=fill2_qty,
                    im2=fill2_im,
                    avg_entry=avg_entry,
                    total_qty=fill1_qty + fill2_qty,
                    im_total=fill1_im + fill2_im,
                )
            except Exception:
                log.exception("notify.entries_merged_failed")

        # ----------------------------------------------------------
        # 14. Set TP and SL via set_trading_stop.
        # ----------------------------------------------------------
        # Get current market price + liquidation price to validate
        # the SL against actual position state (prevents liquidation
        # before SL triggers - the scenario where fill slippage puts
        # the SL inside the liquidation zone).
        current_mark = avg_entry
        liq_price = None
        try:
            ticker = await self._bybit.get_ticker(symbol)
            if ticker:
                mp = float(ticker.get("markPrice", 0) or 0)
                if mp > 0:
                    current_mark = mp
        except Exception:
            pass

        # Fetch actual position to get liquidation price from Bybit.
        try:
            pos_side = "Buy" if direction == "LONG" else "Sell"
            pos = await self._bybit.get_position(symbol, pos_side)
            if pos:
                lp = float(pos.get("liqPrice", 0) or 0)
                if lp > 0:
                    liq_price = lp
        except Exception:
            log.exception("trade.liq_price_fetch_failed", symbol=symbol)

        # Safety adjust SL so it fires BEFORE liquidation.
        # Bybit's reported liq_price is the bankruptcy price - actual
        # liquidation happens ~2-3% earlier due to maintenance margin.
        # So we need a 3% buffer to ensure SL triggers first.
        if sl_price and liq_price:
            buffer_pct = 0.03  # 3% safety buffer before liq
            if direction == "LONG":
                # LONG: liq is below entry, SL must be even higher than liq+buffer
                min_safe_sl = liq_price * (1 + buffer_pct)
                if sl_price <= min_safe_sl:
                    log.warning(
                        "trade.sl_inside_liq_zone.adjusting",
                        symbol=symbol, old_sl=sl_price,
                        new_sl=min_safe_sl, liq_price=liq_price,
                    )
                    sl_price = round(min_safe_sl, 8)
                    trade.sl_price = sl_price
            else:
                # SHORT: liq is above entry, SL must be even lower than liq-buffer
                max_safe_sl = liq_price * (1 - buffer_pct)
                if sl_price >= max_safe_sl:
                    log.warning(
                        "trade.sl_inside_liq_zone.adjusting",
                        symbol=symbol, old_sl=sl_price,
                        new_sl=max_safe_sl, liq_price=liq_price,
                    )
                    sl_price = round(max_safe_sl, 8)
                    trade.sl_price = sl_price

        # Filter TPs that are still valid (not already passed by market).
        # For LONG: TP must be ABOVE current mark.
        # For SHORT: TP must be BELOW current mark.
        # Also drop TPs whose profit distance from entry is <2% — partial
        # closes below that threshold eat too much into the SL+buffer safety
        # margin for minimal profit (client rule 2026-04-23).
        min_tp_distance_pct = 2.0
        valid_tps: list[float] = []
        skipped_too_close: list[float] = []
        for tp in tp_list:
            if not tp or tp <= 0:
                continue
            # Direction sanity vs current mark.
            if direction == "LONG" and tp <= current_mark:
                continue
            if direction == "SHORT" and tp >= current_mark:
                continue
            # Distance from avg entry in %.
            if avg_entry and avg_entry > 0:
                if direction == "LONG":
                    dist_pct = (tp - avg_entry) / avg_entry * 100.0
                else:
                    dist_pct = (avg_entry - tp) / avg_entry * 100.0
                if dist_pct < min_tp_distance_pct:
                    skipped_too_close.append(tp)
                    continue
            valid_tps.append(tp)

        if skipped_too_close:
            log.info(
                "trade.tps_below_min_distance.skipped",
                trade_id=trade.id, symbol=symbol,
                skipped=skipped_too_close,
                min_pct=min_tp_distance_pct,
            )

        if not valid_tps and tp_list:
            log.warning(
                "trade.all_tps_already_passed",
                trade_id=trade.id,
                symbol=symbol,
                avg_entry=avg_entry,
                current_mark=current_mark,
                tps=tp_list,
            )

        # Validate SL direction against current mark price.
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

        # ---------- Place the SL via set_trading_stop ----------
        # SL must always use the position-wide trading-stop (not a
        # conditional order) because it applies to the whole position.
        trigger_src = self._settings.tp_sl.trigger_type  # e.g. "LastPrice"
        if valid_sl:
            try:
                await self._bybit.set_trading_stop(
                    symbol=symbol,
                    position_idx=position_idx,
                    stop_loss=valid_sl,
                    sl_trigger_by=trigger_src,
                )
                log.info("trade.sl_set",
                         trade_id=trade.id, symbol=symbol, sl=valid_sl)
            except Exception:
                log.exception(
                    "trade.sl_error", trade_id=trade.id, symbol=symbol,
                )
                await self._safe_notify(
                    f"[VARNING] {symbol}: SL kunde inte sattas pa borsen. "
                    f"Manuell atgard kan kravas."
                )

        # ---------- Place PARTIAL TPs — one reduce-only conditional
        # order per TP level (from TP2 onwards). Client IZZU
        # 2026-04-24: "TP1 'block' — no move, no profit." TP1 is
        # informational only; no order placed, no quantity slice
        # reserved. For a 3-TP signal that means 50/50 at TP2/TP3;
        # a 5-TP signal -> 25% at TP2/TP3/TP4/TP5. SL progression
        # continues to use all TP levels (TP-2 offset) — TP1 is
        # simply not closed.
        # Trailing-stop merge rule (client IZZU 2026-04-27):
        #   TPs whose distance from entry is BELOW the trailing
        #   activation level get individual partial-close orders.
        #   TPs at/above the trailing activation are MERGED into
        #   the trailing — no individual order is placed for them;
        #   the trailing manages that slice of the position.
        # Example with 4 TPs (avg_entry move): TP1 +2.5%, TP2 +4%,
        # TP3 +8%, TP4 +12%, trailing activation 6.1%:
        #   TP1, TP2 -> individual orders (25% each)
        #   TP3, TP4 -> merged into trailing (50% combined)
        # Slice sizing distributes the position equally across the
        # TOTAL slice count (individual closes + 1 trailing slice
        # if any TPs were merged).
        trailing_activation_pct = self._settings.trailing_stop.activation_pct
        below_trailing_tps: list[float] = []
        merged_above_trailing_count = 0
        for tp_price in valid_tps:
            if avg_entry and avg_entry > 0:
                if direction == "LONG":
                    dist_pct = (tp_price - avg_entry) / avg_entry * 100.0
                else:
                    dist_pct = (avg_entry - tp_price) / avg_entry * 100.0
                if dist_pct >= trailing_activation_pct:
                    merged_above_trailing_count += 1
                    continue
            below_trailing_tps.append(tp_price)

        # Total slice count = individual TPs below trailing + (1 if
        # any TPs were merged into trailing else 0). The trailing's
        # slice is intentionally NOT placed as a partial-close order
        # here — the trailing manager's set_trading_stop call covers
        # whatever quantity is still open at activation time.
        merged_slot = 1 if merged_above_trailing_count > 0 else 0
        num_slices = len(below_trailing_tps) + merged_slot

        tp_order_ids: list[str] = []
        if below_trailing_tps and trade.quantity and trade.quantity > 0 and num_slices > 0:
            num_individual = len(below_trailing_tps)
            total_qty = trade.quantity
            base_qty = total_qty / num_slices
            close_side = "Sell" if direction == "LONG" else "Buy"

            placed_qty = 0.0
            for i, tp_price in enumerate(below_trailing_tps):
                this_qty = base_qty
                try:
                    this_qty_rounded = self._bybit.round_qty(this_qty, symbol)
                    if this_qty_rounded <= 0:
                        continue
                    result = await self._bybit.place_conditional_close(
                        symbol=symbol,
                        side=close_side,
                        qty=this_qty_rounded,
                        trigger_price=tp_price,
                        position_idx=position_idx,
                        trigger_by=trigger_src,
                    )
                    oid = result.get("orderId", "")
                    if oid:
                        tp_order_ids.append(oid)
                    placed_qty += this_qty_rounded
                    log.info(
                        "trade.partial_tp_placed",
                        trade_id=trade.id, symbol=symbol,
                        tp_index=i + 1, tp_price=tp_price,
                        qty=this_qty_rounded, order_id=oid,
                    )
                except Exception:
                    log.exception(
                        "trade.partial_tp_error",
                        trade_id=trade.id, symbol=symbol,
                        tp_index=i + 1, tp_price=tp_price,
                    )

            trade.tp_order_ids = tp_order_ids
            log.info(
                "trade.partial_tps_summary",
                trade_id=trade.id, symbol=symbol,
                placed=len(tp_order_ids),
                individual=num_individual,
                merged_into_trailing=merged_above_trailing_count,
                trailing_activation_pct=trailing_activation_pct,
            )
        elif merged_above_trailing_count > 0:
            log.info(
                "trade.all_tps_merged_into_trailing",
                trade_id=trade.id, symbol=symbol,
                tps_merged=merged_above_trailing_count,
                trailing_activation_pct=trailing_activation_pct,
            )

        if not valid_tps and not valid_sl:
            log.warning(
                "trade.no_valid_tp_sl",
                trade_id=trade.id, symbol=symbol,
            )

        # ----------------------------------------------------------
        # 15. Transition to POSITION_OPEN.
        # ----------------------------------------------------------
        trade.transition(TradeState.POSITION_OPEN)

        await self._persist_trade_state(
            trade,
            entry2_fill_price=trade.entry2_fill_price,
            avg_entry=avg_entry,
            margin=trade.margin,
            quantity=trade.quantity,
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

        # Send "Position öppnad" notification
        try:
            await self._notifier.position_opened(
                trade=trade,
                signal=signal,
            )
        except Exception:
            log.exception("notify.position_opened_failed")

        log.info(
            "trade.opened",
            trade_id=trade.id,
            symbol=symbol,
            direction=direction,
            avg_entry=avg_entry,
            leverage=leverage,
        )

        # Phase 3 — pre-arm the hedge on Bybit as a conditional market
        # order so the hedge fires autonomously even if the bot is
        # offline at the moment price crosses the trigger. Failure
        # here is non-fatal: bot-side check_and_activate remains as a
        # backup path.
        try:
            await self._hedge_mgr.pre_arm_on_bybit(trade)
        except Exception:
            log.exception(
                "trade.hedge_pre_arm_failed",
                trade_id=trade.id, symbol=symbol,
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
        # Bybit hedge mode: positionIdx 1 = Long side, 2 = Short side.
        # The hedge side carries size=0 + side="" whenever no position
        # exists there — including transient events emitted while the
        # Phase 3 hedge pre-arm conditional is being placed and cancelled.
        # Without an idx check, every such empty event matched the main
        # trade and closed it ~1s after open (see incident 2026-04-27).
        try:
            position_idx = int(data.get("positionIdx") or 0)
        except (TypeError, ValueError):
            position_idx = 0

        log.debug(
            "ws.position_update",
            symbol=symbol,
            size=size,
            side=side,
            position_idx=position_idx,
        )

        if size != 0 or position_idx not in (1, 2):
            return

        direction_for_idx = "LONG" if position_idx == 1 else "SHORT"
        trade = self._find_trade_by_symbol_direction(symbol, direction_for_idx)
        if trade is None:
            return

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

        # --- Absolute max-loss cap (client Option 4, 2026-04-24) ---
        # Hard ceiling on worst-case per-trade loss regardless of hedge
        # state or signal SL distance. Fires if unrealized PnL in USDT
        # falls to or below -max_loss_usdt. Catches the cases where
        # hedge didn't/couldn't open AND SL is still far away AND the
        # trade is bleeding. Runs before every other manager so it
        # short-circuits the whole pipeline.
        closed = await self._check_max_loss_cap(trade, current_price)
        if closed:
            return

        # --- Verify position still exists on Bybit (every 30s) ---
        # Prevents error spam when the position was closed externally
        # (TP hit on exchange / liquidation) but the bot didn't catch
        # the close event because public WS is down.
        import time as _time
        now = _time.monotonic()
        last_check = getattr(trade, "_last_pos_check", 0)
        if now - last_check > 30:
            trade._last_pos_check = now
            try:
                if trade.signal:
                    side = "Buy" if trade.signal.direction == "LONG" else "Sell"
                    pos = await self._bybit.get_position(
                        trade.signal.symbol, side,
                    )
                    pos_size = float(pos.get("size", 0) or 0) if pos else 0
                    if pos_size == 0:
                        # Position no longer exists on Bybit - mark closed.
                        log.info(
                            "trade.position_closed_externally",
                            trade_id=trade.id,
                            symbol=trade.signal.symbol,
                        )
                        await self.close_trade(
                            trade.id, "external_close", current_price,
                        )
                        return
            except Exception:
                log.exception(
                    "trade.position_check_error", trade_id=trade.id,
                )

        # SL-management runs in priority order. The first rule that
        # advances SL wins; later rules only kick in if earlier ones
        # didn't protect enough. The "SL only moves towards profit"
        # invariant is enforced at every step so a later rule can
        # never relax a tighter stop set by an earlier rule.
        #
        #   1. TP progression (TP-2 offset, client IZZU rule):
        #        TP2 hit  -> SL moves to entry + 0.15 % buffer (BE)
        #        TP3 hit  -> SL moves to TP1
        #        TP4 hit  -> SL moves to TP2
        #        TP5 hit  -> SL moves to TP3
        #        ...TPn hit (n>=2) -> SL moves to TP(n-2), or
        #                              entry+buffer if n==2.
        #   2. Break-even fallback at +2.3 % (only if no TP fired
        #      and trade has no TP hits yet).
        #   3. Safety ladder fallback (only if neither of the above
        #      already moved SL further into profit):
        #        +4 % move -> SL locked at +1.5 %
        #        +5 % move -> SL locked at +2.5 %
        await self._check_tp_progression(trade, current_price)

        if trade.be_price is None and len(trade.tp_hits) == 0:
            applied = await self._be_mgr.check_and_apply(trade, current_price)
            if applied:
                return

        try:
            await self._be_mgr.check_safety_ladder(trade, current_price)
        except Exception:
            log.exception(
                "trade.safety_ladder_error",
                trade_id=trade.id,
                symbol=trade.signal.symbol if trade.signal else "?",
            )

        # --- Scaling check (next pending step). Gated by the
        # [scaling].enabled config flag — when disabled the entire
        # pyramid pipeline is bypassed (basic-bot mode). ---
        if (
            self._settings.scaling.enabled
            and trade.scaling_step < len(self._settings.scaling.steps)
        ):
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
        else:
            # Pending delayed SL move after a hedge has opened?
            await self._hedge_mgr.check_pending_sl_move(trade, current_price)
            # No-double-loss guard: reconcile hedge lifecycle + cap
            # combined unrealized loss. Returns True if main was closed.
            closed = await self._hedge_mgr.check_linked_closure(
                trade, current_price,
            )
            if closed:
                return

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
        # Phase 3 — if the hedge was pre-armed on Bybit but never
        # fired (main trade closed via TP / SL / manual / max-loss
        # cap before the hedge trigger was reached), cancel the
        # pending conditional so it doesn't fire after the main
        # trade is gone and open an unwanted position.
        if trade.hedge_conditional_order_id is not None:
            try:
                await self._hedge_mgr.cancel_pre_armed(trade)
            except Exception:
                log.exception(
                    "close_trade.hedge_pre_arm_cancel_failed",
                    trade_id=trade.id,
                )

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

        # Notify — pick the right structured template per close reason.
        # Client Meddelande telegram.docx spec (2026-04-24): STOP LOSS
        # TRÄFFAD gets its own template; everything else uses
        # POSITION STÄNGD. Fall back to a raw message on any error.
        try:
            sl_reasons = {"sl_hit"}
            qty_for_msg = trade.quantity or 0
            if reason in sl_reasons:
                await self._notifier.stop_loss_hit(
                    trade=trade,
                    sl_price=trade.sl_price or exit_price,
                    qty=qty_for_msg,
                    result_pct=pnl_pct if pnl_pct is not None else 0.0,
                    result_usdt=pnl_usdt if pnl_usdt is not None else 0.0,
                )
            else:
                await self._notifier.position_closed(
                    trade=trade,
                    exit_price=exit_price,
                    qty=qty_for_msg,
                    result_pct_total=pnl_pct if pnl_pct is not None else 0.0,
                    result_usdt_total=pnl_usdt if pnl_usdt is not None else 0.0,
                )
        except Exception:
            log.exception(
                "close_trade.template_failed", trade_id=trade_id, reason=reason,
            )
            # Fall back to raw message so operator still gets a close notification.
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
        except Exception as exc:
            log.exception(
                f"{order_label}.place_error",
                trade_id=trade.id,
                symbol=symbol,
            )
            err_str = str(exc)
            err_lower = err_str.lower()
            # 110007 = "available balance not enough" -> use SLUT PA PENGAR
            # template and throttle to once per 10 minutes.
            if "110007" in err_str or "not enough" in err_lower:
                import time as _time
                last = getattr(self, "_last_no_money_notify", 0)
                if _time.monotonic() - last > 600:
                    self._last_no_money_notify = _time.monotonic()
                    try:
                        await self._notifier._send_notify(
                            f"❌ SLUT PÅ PENGAR ❌\n"
                            f"📍 SYSTEM BYBIT\n"
                            f"📍 Fel: Inga medel kvar på kontot för att öppna eller fylla på position"
                        )
                    except Exception:
                        log.exception("notify.no_money_failed")
            elif "110074" in err_str or "not live" in err_lower or "delist" in err_lower:
                # Contract delisted / not tradable — give a human
                # reason instead of the raw API error.
                try:
                    if trade.signal:
                        await self._notifier.order_place_failed(
                            signal=trade.signal,
                            order_label=order_label,
                            reason="Kontraktet är inte aktivt på Bybit (delistat eller ej handelbart).",
                        )
                except Exception:
                    log.exception("notify.order_place_failed_failed")
            else:
                try:
                    if trade.signal:
                        await self._notifier.order_place_failed(
                            signal=trade.signal,
                            order_label=order_label,
                            reason=err_str[:80],
                        )
                except Exception:
                    log.exception("notify.order_place_failed_failed")
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

    async def _check_max_loss_cap(
        self,
        trade: Trade,
        current_price: float,
    ) -> bool:
        """Force-close trade if unrealized PnL USDT ≤ -max_loss_usdt.

        Client Option 4, 2026-04-24: absolute safety ceiling that
        fires even when no hedge is open. Returns True if the cap
        fired (caller should short-circuit other checks).
        """
        cap = self._settings.wallet.max_loss_usdt
        if not cap or cap <= 0:
            return False
        if trade.avg_entry is None or trade.avg_entry <= 0:
            return False
        if not trade.quantity or trade.quantity <= 0:
            return False
        if trade.signal is None:
            return False

        direction = trade.signal.direction
        qty = trade.quantity
        avg_entry = trade.avg_entry

        # Main-leg unrealized USDT PnL (no leverage multiplier: qty
        # IS already the leveraged notional quantity, so price * qty
        # is the real USDT value exposed).
        if direction == "LONG":
            main_pnl = (current_price - avg_entry) * qty
        else:
            main_pnl = (avg_entry - current_price) * qty

        # If a hedge is open, include its unrealized PnL too — the
        # cap applies to the combined exposure, not just one leg.
        hedge_pnl = 0.0
        if trade.hedge_trade_id:
            try:
                hedge_row = await self._db.get_trade(int(trade.hedge_trade_id))
            except (ValueError, Exception):
                hedge_row = None
            if hedge_row:
                h_entry = float(hedge_row.get("avg_entry") or 0)
                h_qty = float(hedge_row.get("quantity") or qty)
                if h_entry > 0:
                    if direction == "LONG":
                        # Hedge is SHORT (opposite direction).
                        hedge_pnl = (h_entry - current_price) * h_qty
                    else:
                        hedge_pnl = (current_price - h_entry) * h_qty

        combined = main_pnl + hedge_pnl
        if combined > -abs(cap):
            return False

        log.warning(
            "trade.max_loss_cap_hit",
            trade_id=trade.id,
            symbol=trade.signal.symbol,
            main_pnl=round(main_pnl, 4),
            hedge_pnl=round(hedge_pnl, 4),
            combined=round(combined, 4),
            cap=cap,
            current_price=current_price,
        )
        await self._safe_notify(
            f"[MAX LOSS CAP] {trade.signal.symbol} {direction}\n"
            f"Forlust {combined:.2f} USDT har natt granssen "
            f"-{cap:.2f} USDT.\n"
            f"Stanger positionen for att begransa forlusten."
        )
        try:
            await self.close_trade(
                trade_id=str(trade.id),
                reason="max_loss_cap",
                exit_price=current_price,
            )
        except Exception:
            log.exception(
                "trade.max_loss_cap_close_failed", trade_id=trade.id,
            )
        return True

    async def _check_tp_progression(
        self,
        trade: Trade,
        current_price: float,
    ) -> None:
        """Detect TP fills by price crossing and advance SL.

        TP-2 offset progression (client spec 2026-04-24):
          TP1 hit -> no SL change (too early to lock)
          TP2 hit -> SL moves to entry + buffer (zero loss locked)
          TP3 hit -> SL moves to TP1
          TP4 hit -> SL moves to TP2
          TP5 hit -> SL moves to TP3
          ...TPn hit (n >= 2) -> SL moves to TP(n-2), or entry+buffer if n == 2.
        """
        if trade.signal is None or trade.avg_entry is None:
            return
        tp_list = getattr(trade.signal, "tps", None) or []
        if not tp_list:
            return

        direction = trade.signal.direction
        entry = trade.avg_entry

        # Determine how many TPs have now been touched by the market.
        touched_count = 0
        if direction == "LONG":
            for tp in tp_list:
                if tp and current_price >= tp:
                    touched_count += 1
        else:
            for tp in tp_list:
                if tp and 0 < tp and current_price <= tp:
                    touched_count += 1

        # Compare against how many we've already recorded.
        prev_hits = len(trade.tp_hits)
        if touched_count <= prev_hits:
            return  # No new TP hit yet.

        # New TP(s) hit. Update tp_hits for bookkeeping and fire the
        # dedicated "TAKE PROFIT N TAGEN" template for each one so the
        # client sees every partial close in real time.
        num_valid_tps = len(tp_list)
        qty_total = trade.quantity or 0
        # Position-close share per placed TP — TP1 is NOT closed
        # (client IZZU rule), so the live slices are TP2 onwards.
        placed_slices = max(1, num_valid_tps - 1) if num_valid_tps > 1 else 1
        for i in range(prev_hits, touched_count):
            tp_price = tp_list[i]
            tp_num_local = i + 1
            trade.tp_hits.append(tp_price)
            log.info(
                "trade.tp_hit",
                trade_id=trade.id,
                symbol=trade.signal.symbol,
                tp_index=tp_num_local,
                tp_price=tp_price,
            )
            # Per-TP notification: TP1 gets no partial close order
            # but still fires as "informational" so the client sees
            # the price reached TP1. For TP2+, closed_qty reflects
            # the actual slice that filled on Bybit.
            try:
                if tp_num_local == 1:
                    closed_qty = 0
                    closed_pct = 0.0
                else:
                    closed_qty = (qty_total / placed_slices) if placed_slices else 0
                    closed_pct = (100.0 / placed_slices) if placed_slices else 0.0
                if direction == "LONG":
                    tp_pct = (tp_price - entry) / entry * 100.0 if entry else 0
                else:
                    tp_pct = (entry - tp_price) / entry * 100.0 if entry else 0
                leverage = trade.leverage or 1.0
                result_pct = tp_pct * leverage
                # Slice of margin this TP represents.
                slice_fraction = closed_pct / 100.0 if closed_pct else 0.0
                slice_margin = (trade.margin or 0) * slice_fraction
                result_usdt = slice_margin * (result_pct / 100.0)
                await self._notifier.take_profit_hit(
                    trade=trade,
                    tp_level=tp_num_local,
                    tp_price=tp_price,
                    tp_pct=tp_pct,
                    closed_qty=closed_qty,
                    closed_pct=closed_pct,
                    result_pct=result_pct,
                    result_usdt=result_usdt,
                )
            except Exception:
                log.exception(
                    "notify.take_profit_hit_failed",
                    trade_id=trade.id, tp_level=tp_num_local,
                )

        # Determine new SL from the latest-hit TP (TP-2 offset).
        latest_hit_idx = touched_count - 1  # zero-based index
        tp_num = latest_hit_idx + 1          # 1-based TP number

        if tp_num < 2:
            # TP1 hit alone - do not move SL yet.
            return
        elif tp_num == 2:
            # TP2 hit -> SL moves to entry + buffer.
            buffer_pct = self._settings.breakeven.buffer_pct / 100.0
            if direction == "LONG":
                new_sl = round(entry * (1 + buffer_pct), 8)
            else:
                new_sl = round(entry * (1 - buffer_pct), 8)
        else:
            # TPn hit (n >= 3) -> SL moves to TP(n-2).
            new_sl = tp_list[latest_hit_idx - 2]

        # Only move SL if the new level is MORE protective than current.
        current_sl = trade.sl_price or 0
        better = False
        if direction == "LONG" and new_sl > current_sl:
            better = True
        if direction == "SHORT" and (current_sl == 0 or new_sl < current_sl):
            better = True
        if not better:
            return

        try:
            position_idx = 1 if direction == "LONG" else 2
            await self._bybit.set_trading_stop(
                symbol=trade.signal.symbol,
                stop_loss=new_sl,
                position_idx=position_idx,
            )
            trade.sl_price = new_sl
            if tp_num == 2:
                # SL now at break-even after TP2 hit.
                trade.be_price = new_sl
            try:
                await self._db.update_trade(
                    int(trade.id), sl_price=new_sl, be_price=trade.be_price,
                )
            except Exception:
                pass

            # Pick the right template: TP2 (SL -> entry + buffer) =
            # BREAK-EVEN JUSTERAD per the client's spec; TP3+ move SL
            # to an earlier TP level, stays as the generic "SL
            # advanced after TP" notice.
            try:
                if tp_num == 2:
                    if direction == "LONG":
                        move_pct = (current_price - entry) / entry * 100.0
                    else:
                        move_pct = (entry - current_price) / entry * 100.0
                    await self._notifier.break_even_adjusted(
                        trade=trade,
                        new_sl=new_sl,
                        current_move_pct=move_pct,
                    )
                else:
                    # TP3+ progression — use the structured
                    # STOP LOSS FLYTTAD template (client IZZU
                    # 2026-04-27 update from yesterday's templates).
                    if direction == "LONG":
                        move_pct_now = (current_price - entry) / entry * 100.0 if entry else 0
                    else:
                        move_pct_now = (entry - current_price) / entry * 100.0 if entry else 0
                    await self._notifier.sl_moved(
                        trade=trade,
                        new_sl=new_sl,
                        reason=(
                            f"TP{tp_num} träffad — SL flyttad till "
                            f"TP{tp_num - 2} ({tp_list[tp_num - 3]})"
                        ),
                        move_pct=move_pct_now,
                    )
            except Exception:
                log.exception(
                    "notify.sl_progress_failed",
                    trade_id=trade.id,
                )
            log.info(
                "trade.sl_advanced_after_tp",
                trade_id=trade.id,
                symbol=trade.signal.symbol,
                tp_index=latest_hit_idx + 1,
                new_sl=new_sl,
            )
        except Exception:
            log.exception(
                "trade.sl_advance_error",
                trade_id=trade.id,
            )

    def _now_fmt(self) -> str:
        from core.time_utils import format_time, now_utc
        return format_time(now_utc())

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

    def _find_trade_by_symbol_direction(
        self,
        symbol: str,
        direction: str,
    ) -> Optional[Trade]:
        """Find an active (non-terminal) trade for *symbol* + *direction*.

        Used to translate a Bybit position-side close event back to the
        corresponding bot trade. Direction must be ``"LONG"`` or ``"SHORT"``.
        """
        for trade in self._active_trades.values():
            if trade.is_terminal:
                continue
            if trade.signal is None:
                continue
            if trade.signal.symbol != symbol:
                continue
            if trade.signal.direction != direction:
                continue
            return trade
        return None

    def _infer_close_reason(self, trade: Trade, exit_price: float) -> str:
        """Infer why a position was closed based on exit price vs SL/TP.

        Tolerance is widened to ~1% because LastPrice-triggered SL/TP
        closes routinely slip a few basis points past the trigger level,
        and partial-TP progression moves the SL to earlier TP prices —
        so 'near a moved SL' is a far more common exit than 'exactly at
        the original SL'.
        """
        if trade.signal is None:
            return "external_close"

        tol_pct = 1.0  # match window in %

        def _near(a: float, b: float) -> bool:
            return b > 0 and abs(a - b) / b * 100.0 <= tol_pct

        # 1) Trailing stop takes priority — once armed, SL is the trail.
        if trade.trailing_sl is not None and _near(exit_price, trade.trailing_sl):
            return "trailing_stop"

        # 2) Break-even: SL was moved to entry + buffer.
        if trade.be_price is not None and _near(exit_price, trade.be_price):
            return "be_hit"

        # 3) TP progression: SL was advanced to a previously-hit TP
        #    level (TP2 hit -> SL moved to TP0 reference, etc.). If the
        #    exit is near any hit TP, call it a progressed SL.
        for tp in trade.tp_hits or []:
            if _near(exit_price, tp):
                return "tp_progression_sl_hit"

        # 4) Current active SL on the trade record.
        if trade.sl_price and _near(exit_price, trade.sl_price):
            return "sl_hit"

        # 5) Any signal TP level hit.
        tp_list = (
            trade.signal.tps if hasattr(trade.signal, "tps")
            else trade.signal.tp_list if hasattr(trade.signal, "tp_list")
            else []
        )
        for tp in tp_list:
            if tp and _near(exit_price, tp):
                return "tp_hit"

        # 6) Compare against avg_entry — near-entry exits are almost
        #    always BE/progressed-SL fills that drifted past tolerance.
        if trade.avg_entry and _near(exit_price, trade.avg_entry):
            return "near_entry_close"

        # 7) Last resort: flag as external close rather than "unknown"
        #    so the operator knows the bot didn't originate it.
        return "external_close"

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
            try:
                await self._notifier.tp_sl_update_failed(signal)
            except Exception:
                log.exception("notify.tp_sl_update_failed_notify_error")
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

        # --- Notify (matches client's "Signal updated - difference above 5%" template) ---
        try:
            # Best-effort: pull leverage/IM from existing trade if available
            lev = float(existing_trade_row.get("leverage", 0) or 0) or 10.0
            im_val = float(existing_trade_row.get("margin", 0) or 0) or \
                     self._settings.wallet.initial_margin
            await self._notifier.signal_updated_tp_sl(
                signal=signal,
                leverage=lev,
                im=im_val,
                bot_order_id=str(trade_id),
                bybit_order_id=existing_trade_row.get("entry1_order_id_bybit", ""),
            )
        except Exception:
            log.exception("notify.signal_updated_tp_sl_failed")

    async def _safe_notify(self, message: str) -> None:
        """Send a Telegram notification, swallowing errors."""
        try:
            await self._notifier._send_notify(message)
        except Exception:
            log.exception("position_manager.notify_error", message=message[:80])
