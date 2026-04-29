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
from managers.hedge_manager import HedgeManager
from managers.reentry_manager import ReentryManager
from managers.scaling_manager import ScalingManager

if TYPE_CHECKING:
    from config.settings import AppSettings
    from core.duplicate_detector import DuplicateDetector
    from persistence.database import Database

log = structlog.get_logger(__name__)


def _format_close_source(reason: str) -> str:
    """Map an internal close-reason string (set by Bybit's order-fill
    classifier) to the human-readable suffix shown in the
    "POSITION CLOSED - X" Telegram header.
    """
    if reason == "stop_loss":
        return "stop loss"
    if reason == "trailing_stop":
        return "trailing stop"
    if reason == "liquidation":
        return "liquidation"
    if reason == "external_close":
        return "external close"
    if reason and reason.startswith("tp_"):
        try:
            return f"TP{int(reason.split('_', 1)[1])}"
        except (IndexError, ValueError):
            return reason
    return reason or "unknown"


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

        # TP-fill notification dedup: Bybit may re-send a Filled event,
        # we only fire the per-TP Telegram notification once per order.
        self._tp_notified: set[str] = set()

        # Last-price cache per symbol, fed from the ticker stream via
        # ``handle_price_update``. Client 2026-04-29: trailing-stop
        # activation must gate on Last price, not Mark — Bybit fires
        # the trailing on Last, so the bot's notification has to use
        # the same source to avoid skew.
        self._last_price_by_symbol: Dict[str, float] = {}

        # Per-symbol-direction dedup for repeat rejection notifications
        # (client 2026-04-30). Same signal posted by 3 different
        # Telegram channels within seconds was firing 3 identical
        # ``⚠️ Finns inte på bybit ⚠️`` (and similar rejection)
        # messages. Key: f"{kind}:{symbol}:{direction}". Value: monotonic
        # timestamp of the last fire. Same key within ``_dedup_window_s``
        # is silenced.
        self._reject_notify_last: Dict[str, float] = {}
        self._dedup_window_s: float = 300.0  # 5 minutes

        # --- Sub-managers ---
        # Strict architecture (client IZZU 2026-04-28): the bot must not
        # think, assume, guess, or infer anything — every close decision
        # comes from Bybit. We removed BreakevenManager and TrailingManager
        # (bot-side polling that decided when to move SL or activate
        # trailing) in favour of Bybit's native trailing stop set once
        # at trade open. Same applies to the loss caps and the close-
        # reason inferrer — see the gutted close path below.
        self._scaling_mgr = ScalingManager(
            settings=settings.scaling,
            leverage_settings=settings.leverage,
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
    # Reject-notification dedup helper
    # ==================================================================

    def _should_send_reject_notify(
        self,
        kind: str,
        symbol: str,
        direction: str = "",
    ) -> bool:
        """Return True if this rejection notification should fire.

        Client 2026-04-30: dedupe ANY rejection for the same
        (symbol, direction) within ``_dedup_window_s`` (default 5
        min), regardless of WHY rejected. Earlier behaviour keyed
        on (kind, symbol, direction) so the same signal could fire
        ``Entre saknas`` + ``pris för långt från entry`` + ``Finns
        inte på bybit`` separately. Operator just needs to see
        "this symbol was rejected once" — the first reason wins.

        ``kind`` is still passed in so the log line can identify
        which rejection actually fired vs which were silenced.
        """
        key = f"{symbol}:{direction}"
        now = time.monotonic()
        last = self._reject_notify_last.get(key)
        if last is not None and (now - last) < self._dedup_window_s:
            log.debug(
                "reject_notify.deduped",
                kind=kind, symbol=symbol, direction=direction,
                window_s=self._dedup_window_s,
            )
            return False
        self._reject_notify_last[key] = now
        # Best-effort GC: prune entries older than 2x the window.
        if len(self._reject_notify_last) > 500:
            cutoff = now - 2 * self._dedup_window_s
            stale = [
                k for k, t in self._reject_notify_last.items()
                if t < cutoff
            ]
            for k in stale:
                self._reject_notify_last.pop(k, None)
        return True

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
                # Persist the blocked signal so the per-channel group
                # analysis report can count "copies / blocked signals"
                # against the source channel (client request 2026-04-27).
                try:
                    tp_list_blocked = (
                        signal.tps if hasattr(signal, "tps")
                        else signal.tp_list if hasattr(signal, "tp_list")
                        else []
                    )
                    await self._db.save_signal({
                        "symbol": symbol,
                        "direction": direction,
                        "entry_price": getattr(signal, "entry", 0),
                        "sl_price": getattr(signal, "sl", None),
                        "tp_prices": tp_list_blocked,
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
                        "status": "blocked_duplicate",
                    })
                except Exception:
                    log.exception("signal.blocked_save_failed", symbol=symbol)
                if self._should_send_reject_notify(
                    "duplicate", symbol, direction,
                ):
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
        # 4. SL is taken from the signal AS-IS — no auto-fallback,
        #    no liquidation-zone adjustment, no bot-side decision.
        #    Strict architecture (client 2026-04-28): the bot must
        #    never invent or adjust the SL. If the signal carries no
        #    SL, the trade is rejected — there is no safe trade
        #    without one.
        # ----------------------------------------------------------
        entry_price = signal.entry
        sl_price = signal.sl if hasattr(signal, "sl") else None
        auto_sl_applied = False  # retained for legacy log fields; always False

        if sl_price is None:
            log.warning(
                "signal.no_sl_rejected",
                symbol=symbol, channel_name=channel_name,
            )
            if self._should_send_reject_notify(
                "no_sl", symbol, direction,
            ):
                try:
                    from telegram.notifier import _chan, _ts
                    await self._safe_notify(
                        f"⚠️ SIGNAL AVVISAD (saknar stop-loss)\n"
                        f"🕒 Tid: {_ts()}\n"
                        f"📢 Från kanal: {_chan(channel_name)}\n"
                        f"📊 Symbol: #{symbol}\n"
                        f"📈 Riktning: {direction}\n"
                        f"📍 Anledning: signalen saknar stop-loss — "
                        f"ingen säker handel utan SL."
                    )
                except Exception:
                    pass
            return None

        # ----------------------------------------------------------
        # 5. Dynamic leverage from the signal's actual SL distance.
        # ----------------------------------------------------------
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
                        if self._should_send_reject_notify(
                            "stale_price", symbol, direction,
                        ):
                            # Channel name needs the #Hashtag form for
                            # Telegram clickable history (2026-04-29).
                            from telegram.notifier import _chan
                            chan = _chan(getattr(signal, "channel_name", ""))
                            await self._safe_notify(
                                f"⚠️ SIGNAL AVVISAD (pris för långt från entry)\n"
                                f"📢 Från kanal: {chan}\n"
                                f"📊 Symbol: #{symbol}\n"
                                f"📈 Riktning: {direction}\n"
                                f"💥 Signal entry: {entry_price}\n"
                                f"📍 Marknadspris: {current}\n"
                                f"📍 Diff: {price_diff_pct:.2f}% (max {max_slippage}%)\n"
                                f"📍 Signalen är för gammal / marknad har redan flyttat."
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
                if self._should_send_reject_notify(
                    "not_on_bybit", symbol, direction,
                ):
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
            if self._should_send_reject_notify(
                "not_on_bybit", symbol, direction,
            ):
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
                    from telegram.notifier import _chan, _ts
                    await self._safe_notify(
                        f"⚠️ SIGNAL BLOCKERAD (position finns redan på Bybit)\n"
                        f"🕒 Tid: {_ts()}\n"
                        f"📢 Från kanal: "
                        f"{_chan(getattr(signal, 'channel_name', ''))}\n"
                        f"📊 Symbol: #{symbol}\n"
                        f"📈 Riktning: {direction}\n"
                        f"📍 Bybit har redan storlek {existing_size} på "
                        f"{side}-sidan.\n"
                        f"📍 Stäng positionen manuellt på Bybit, "
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

        # Liquidation-zone SL adjustment REMOVED 2026-04-28.
        # Strict architecture (client): the bot must never adjust the
        # signal's SL. If the SL sits inside the liquidation zone,
        # Bybit will reject the set_trading_stop call below and the
        # operator will see a [VARNING] in the channel — that is the
        # correct surface for the issue, not a silent bot adjustment.

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

        # SL direction validation REMOVED 2026-04-28. The signal's SL
        # goes to Bybit unmodified. If the SL is on the wrong side of
        # the current mark, Bybit will reject set_trading_stop and the
        # [VARNING] notification will fire — the operator decides, not
        # the bot.
        valid_sl = sl_price

        # ---------- Place the SL via set_trading_stop ----------
        # SL must always use the position-wide trading-stop (not a
        # conditional order) because it applies to the whole position.
        # Client 2026-04-29: initial SL fires on MarkPrice; everything
        # else (TP, hedge, trailing, BE/scaling moved SLs) uses
        # LastPrice. The split is intentional — Mark is steadier than
        # Last and prevents wick-driven SL hits on the fresh position,
        # while moved SLs follow Last so they react to actual fills.
        trigger_src = self._settings.tp_sl.trigger_type  # LastPrice (TP/conditional/moved-SL)
        sl_initial_trigger = getattr(
            self._settings.tp_sl,
            "sl_initial_trigger_type",
            "MarkPrice",
        )
        if valid_sl:
            try:
                await self._bybit.set_trading_stop(
                    symbol=symbol,
                    position_idx=position_idx,
                    stop_loss=valid_sl,
                    sl_trigger_by=sl_initial_trigger,
                )
                log.info("trade.sl_set",
                         trade_id=trade.id, symbol=symbol, sl=valid_sl)
            except Exception:
                log.exception(
                    "trade.sl_error", trade_id=trade.id, symbol=symbol,
                )
                try:
                    await self._notifier.error_sl_not_executed(
                        signal=trade.signal,
                        error_detail=(
                            "SL kunde inte sättas på börsen. "
                            "Manuell åtgärd kan krävas."
                        ),
                    )
                except Exception:
                    log.exception(
                        "notify.sl_error_failed",
                        trade_id=trade.id, symbol=symbol,
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
        # Client 2026-04-29: TPs closer than 2% from avg_entry are
        # ignored — they leave too little headroom for fees+slippage
        # and effectively scratch the position. The dropped quantity
        # rolls into the remaining slices automatically (smaller
        # num_slices = larger per-slice qty).
        MIN_TP_DISTANCE_PCT = 2.0
        below_trailing_tps: list[float] = []
        merged_above_trailing_count = 0
        ignored_too_close_count = 0
        for tp_price in valid_tps:
            if avg_entry and avg_entry > 0:
                if direction == "LONG":
                    dist_pct = (tp_price - avg_entry) / avg_entry * 100.0
                else:
                    dist_pct = (avg_entry - tp_price) / avg_entry * 100.0
                if dist_pct < MIN_TP_DISTANCE_PCT:
                    ignored_too_close_count += 1
                    log.info(
                        "trade.tp_ignored_too_close",
                        trade_id=trade.id, symbol=symbol,
                        tp_price=tp_price,
                        dist_pct=round(dist_pct, 3),
                        threshold_pct=MIN_TP_DISTANCE_PCT,
                    )
                    continue
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
        # 14b. Bybit-native trailing stop, set ONCE at trade open.
        # ----------------------------------------------------------
        # Strict architecture (client IZZU 2026-04-28): the bot does
        # not poll prices to decide when to activate a trailing or
        # move SL to break-even. Instead we hand the entire post-
        # entry management to Bybit by setting `trailingStop` +
        # `activePrice` on the position now.
        #
        # Activation rule (client 2026-04-28): "Highest TP first,
        # 6.1% second." All TP levels above 6.1% merge into the
        # single trailing stop.
        #   LONG:  activation = min(avg_entry × 1.061, highest TP)
        #   SHORT: activation = max(avg_entry × 0.939, lowest TP)
        # When the highest TP sits below 6.1% from entry, trailing
        # activates at the highest TP instead of waiting for 6.1%.
        try:
            ts_settings = self._settings.trailing_stop
            activation_pct = ts_settings.activation_pct / 100.0
            distance_pct = ts_settings.trailing_distance_pct / 100.0
            if avg_entry and avg_entry > 0 and activation_pct > 0 and distance_pct > 0:
                if direction == "LONG":
                    pct_price = avg_entry * (1 + activation_pct)
                    if valid_tps:
                        activation_price = round(min(pct_price, max(valid_tps)), 8)
                    else:
                        activation_price = round(pct_price, 8)
                else:
                    pct_price = avg_entry * (1 - activation_pct)
                    if valid_tps:
                        activation_price = round(max(pct_price, min(valid_tps)), 8)
                    else:
                        activation_price = round(pct_price, 8)
                trailing_distance = round(avg_entry * distance_pct, 8)
                await self._bybit.set_trading_stop(
                    symbol=symbol,
                    position_idx=position_idx,
                    trailing_stop=trailing_distance,
                    active_price=activation_price,
                )
                trade.trailing_sl = trailing_distance
                # Stash activation params on the trade — the Telegram
                # notification fires later, when Bybit *actually* starts
                # trailing (price crosses activation_price). Client
                # 2026-04-28: "the message should be sent when the
                # trailing stop starts, not when it is placed."
                trade.trailing_activation_price = activation_price
                trade.trailing_distance = trailing_distance
                trade.trailing_activation_pct = ts_settings.activation_pct
                trade.trailing_distance_pct = ts_settings.trailing_distance_pct
                trade.trailing_activated_notified = False
                log.info(
                    "trade.trailing_armed_at_open",
                    trade_id=trade.id, symbol=symbol,
                    activation_price=activation_price,
                    trailing_distance=trailing_distance,
                    activation_pct=ts_settings.activation_pct,
                    distance_pct=ts_settings.trailing_distance_pct,
                    highest_tp_used=(
                        valid_tps and (
                            (direction == "LONG" and max(valid_tps) < pct_price)
                            or (direction == "SHORT" and min(valid_tps) > pct_price)
                        )
                    ),
                )
        except Exception:
            log.exception(
                "trade.trailing_arm_failed",
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
            # Strict architecture (client 2026-04-28): every close
            # notification and every close decision is driven by Bybit's
            # order-fill event, never by bot inference. Classify this
            # fill below — if it is a position-closing event (SL fire,
            # trailing fire, partial-TP fill, liquidation), record the
            # close reason on the trade. on_position_update will read
            # that reason when Bybit reports size=0 and call close_trade.
            await self._classify_bybit_close_fill(order_id, data)

    async def _classify_bybit_close_fill(
        self, order_id: str, data: dict,
    ) -> None:
        """Bybit's order-fill event is the single source of close decisions.

        Strict architecture (client 2026-04-28): one Bybit event triggers
        ``close_trade`` directly; on_position_update no longer fires
        closes. This eliminates the race where two paths would emit two
        POSITION CLOSED notifications for the same trade.

        Mapping (from ``stopOrderType`` / ``execType`` on the fill):
          * ``StopLoss``     reduce-only fill → ``close_trade(stop_loss)``
          * ``TrailingStop`` reduce-only fill → ``close_trade(trailing_stop)``
          * ``execType=Liquidation``         → ``close_trade(liquidation)``
          * ``Stop`` + matches a recorded ``tp_order_id`` → emit the
            TAKE PROFIT N TAGEN notification only. The position closes
            only when its remaining size hits zero, which Bybit reports
            as a separate fill event.

        ``close_trade`` carries an internal ``is_terminal`` guard so any
        repeat WS event for the same close is a safe no-op.
        """
        try:
            position_idx = int(data.get("positionIdx") or 0)
        except (TypeError, ValueError):
            position_idx = 0
        if position_idx not in (1, 2):
            return

        symbol = data.get("symbol", "")
        if not symbol:
            return

        sot = data.get("stopOrderType", "") or ""
        exec_type = data.get("execType", "") or ""
        reduce_only = bool(data.get("reduceOnly", False))

        direction = "LONG" if position_idx == 1 else "SHORT"
        trade = self._find_trade_by_symbol_direction(symbol, direction)
        if trade is None or trade.is_terminal or trade.signal is None:
            return

        # Position-closing event types: fire close_trade directly.
        def _exit_price() -> float:
            try:
                ap = float(data.get("avgPrice") or 0)
                if ap > 0:
                    return ap
            except (TypeError, ValueError):
                pass
            try:
                tp = float(data.get("triggerPrice") or 0)
                if tp > 0:
                    return tp
            except (TypeError, ValueError):
                pass
            return trade.avg_entry or 0.0

        if reduce_only and sot == "StopLoss":
            log.info(
                "ws.close_event",
                trade_id=trade.id, symbol=symbol,
                kind="stop_loss", order_id=order_id,
            )
            await self.close_trade(trade.id, "stop_loss", _exit_price())
            return

        if reduce_only and sot == "TrailingStop":
            log.info(
                "ws.close_event",
                trade_id=trade.id, symbol=symbol,
                kind="trailing_stop", order_id=order_id,
            )
            await self.close_trade(trade.id, "trailing_stop", _exit_price())
            return

        if exec_type == "Liquidation":
            log.warning(
                "ws.close_event",
                trade_id=trade.id, symbol=symbol,
                kind="liquidation", order_id=order_id,
            )
            await self.close_trade(trade.id, "liquidation", _exit_price())
            return

        # Partial-TP fill — notify only.
        if reduce_only and sot == "Stop" and order_id in (trade.tp_order_ids or []):
            await self._notify_tp_filled(trade, order_id, data)
            return

        # Manual close on the Bybit UI surfaces as a reduce-only Filled
        # order with no stopOrderType. Treat as an external close —
        # Bybit closed the position, the bot just records that.
        if reduce_only and not sot:
            log.info(
                "ws.close_event",
                trade_id=trade.id, symbol=symbol,
                kind="external_close", order_id=order_id,
            )
            await self.close_trade(trade.id, "external_close", _exit_price())

    async def _notify_tp_filled(
        self, trade: "Trade", order_id: str, data: dict,
    ) -> None:
        """Emit the TAKE PROFIT N TAGEN message and record ``tp_N`` as the
        pending close reason in case this fill happens to take the position
        to size=0 (i.e. the last TP closes everything)."""
        if order_id in self._tp_notified:
            return

        trigger_price_str = data.get("triggerPrice", "")
        try:
            trigger_price = float(trigger_price_str)
        except (TypeError, ValueError):
            return
        if trigger_price <= 0:
            return

        tp_list = (
            trade.signal.tps if hasattr(trade.signal, "tps")
            else getattr(trade.signal, "tp_list", []) or []
        )
        tp_level: Optional[int] = None
        for i, tp in enumerate(tp_list):
            if not tp:
                continue
            if abs(tp - trigger_price) / max(abs(tp), 1e-12) < 1e-4:
                tp_level = i + 1
                break
        if tp_level is None:
            return

        self._tp_notified.add(order_id)

        avg_entry = trade.avg_entry or 0.0
        direction = trade.signal.direction
        if avg_entry > 0:
            if direction == "LONG":
                tp_pct = (trigger_price - avg_entry) / avg_entry * 100.0
            else:
                tp_pct = (avg_entry - trigger_price) / avg_entry * 100.0
        else:
            tp_pct = 0.0

        try:
            closed_qty = float(data.get("cumExecQty") or 0)
        except (TypeError, ValueError):
            closed_qty = 0.0
        total_qty = trade.quantity or 0.0
        closed_pct = (closed_qty / total_qty * 100.0) if total_qty > 0 else 0.0
        leverage = trade.leverage or 1.0
        result_pct = tp_pct * leverage
        slice_margin = (trade.margin or 0.0) * (closed_pct / 100.0)
        result_usdt = slice_margin * (result_pct / 100.0)

        try:
            await self._notifier.take_profit_hit(
                trade=trade,
                tp_level=tp_level,
                tp_price=trigger_price,
                tp_pct=tp_pct,
                closed_qty=closed_qty,
                closed_pct=closed_pct,
                result_pct=result_pct,
                result_usdt=result_usdt,
            )
            trade.tp_hits.append(trigger_price)
            # Record as pending close reason so if this fill takes the
            # position to size=0 (last TP), the POSITION CLOSED message
            # reads "POSITION CLOSED - TP{N}".
            trade._pending_close_reason = f"tp_{tp_level}"
            log.info(
                "trade.tp_hit",
                trade_id=trade.id,
                symbol=trade.signal.symbol,
                tp_level=tp_level,
                tp_price=trigger_price,
                closed_qty=closed_qty,
            )
        except Exception:
            log.exception(
                "notify.take_profit_hit_failed",
                trade_id=trade.id, tp_level=tp_level,
            )

    async def on_position_update(self, data: dict) -> None:
        """Position-update events are observational only after 2026-04-28.

        The single Bybit-driven close path lives in ``on_order_update``
        (via ``_classify_bybit_close_fill``). When Bybit fires SL /
        Trailing / Liquidation, we close from that fill event with the
        right reason. Position-update size=0 used to trigger a close
        too — that produced duplicate POSITION CLOSED messages on
        VINEUSDT and others (one labelled "external close", one
        labelled "stop loss"). Removed.

        Manual closes via the Bybit UI also surface as a reduce-only
        Filled order — they are still caught by on_order_update and
        labelled "external close" (no stopOrderType).

        One observational side-effect remains: detect when Bybit's
        trailing stop has actually started trailing (mark price crossed
        the activation price we set at trade open) and fire the
        TRAILING STOP AKTIVERAD Telegram message exactly once. Client
        2026-04-28: "the message should be sent when the trailing stop
        starts, not when it is placed."
        """
        try:
            position_idx = int(data.get("positionIdx") or 0)
        except (TypeError, ValueError):
            position_idx = 0
        log.debug(
            "ws.position_update",
            symbol=data.get("symbol", ""),
            size=data.get("size", "0"),
            side=data.get("side", ""),
            position_idx=position_idx,
        )
        # Trailing AKTIVERAD gating moved to handle_price_update so it
        # follows Last price (client 2026-04-29). The position-event
        # path stays for UPPDATERAD only — that one watches stopLoss
        # changes Bybit pushes after the trailing has already started.
        await self._maybe_notify_trailing_updated(data)

    async def _maybe_notify_trailing_activated(self, data: dict) -> None:
        """Fire TRAILING STOP AKTIVERAD when Bybit's trailing has
        actually started moving.

        We detect activation by comparing the position's mark price
        against ``trade.trailing_activation_price`` (set when Bybit
        accepted set_trading_stop at trade open):
          * LONG  -> activated when mark_price >= activation_price
          * SHORT -> activated when mark_price <= activation_price

        Once fired, we set ``trailing_activated_notified=True`` so the
        notification doesn't repeat on every subsequent position event.
        Pure observation of Bybit-pushed data — no polling, no inference.
        """
        symbol = data.get("symbol", "")
        if not symbol:
            return
        try:
            mark_price = float(data.get("markPrice", 0) or 0)
        except (TypeError, ValueError):
            return
        if mark_price <= 0:
            return

        for trade in list(self._active_trades.values()):
            if trade.signal is None or trade.signal.symbol != symbol:
                continue
            if trade.trailing_activated_notified:
                continue
            activation = trade.trailing_activation_price
            if not activation or activation <= 0:
                continue
            direction = (trade.signal.direction or "").upper()
            crossed = (
                (direction == "LONG" and mark_price >= activation)
                or (direction == "SHORT" and mark_price <= activation)
            )
            if not crossed:
                continue

            trade.trailing_activated_notified = True
            # Pull the live position fields off the same WS event.
            # Bybit-confirmed values only — never compute locally
            # (client 2026-04-28: "Värdet måste bekräftas från Bybit").
            try:
                bybit_trailing_sl = float(data.get("stopLoss", 0) or 0) or None
            except (TypeError, ValueError):
                bybit_trailing_sl = None
            try:
                quantity = float(data.get("size", 0) or 0)
            except (TypeError, ValueError):
                quantity = 0.0
            try:
                unrealised_pnl = float(data.get("unrealisedPnl", 0) or 0)
            except (TypeError, ValueError):
                unrealised_pnl = 0.0
            # Seed the last-known trailing-stop price so subsequent
            # moves are detected as deltas in trailing_updated.
            trade.last_trailing_stop_price = bybit_trailing_sl
            log.info(
                "trade.trailing_activated",
                trade_id=trade.id, symbol=symbol,
                mark_price=mark_price,
                activation_price=activation,
                direction=direction,
                bybit_trailing_sl=bybit_trailing_sl,
            )
            try:
                await self._notifier.trailing_stop_activated(
                    trade=trade,
                    activation_price=activation,
                    trailing_distance=trade.trailing_distance or 0.0,
                    activation_pct=trade.trailing_activation_pct or 0.0,
                    distance_pct=trade.trailing_distance_pct or 0.0,
                    trailing_stop_price=bybit_trailing_sl,
                    mark_price=mark_price,
                    quantity=quantity,
                    unrealised_pnl=unrealised_pnl,
                )
            except Exception:
                log.exception(
                    "trade.trailing_activate_notify_failed",
                    trade_id=trade.id, symbol=symbol,
                )

    async def _maybe_notify_trailing_updated(self, data: dict) -> None:
        """Fire TRAILING STOP UPPDATERAD every time Bybit moves the
        trailing stop to lock in a better profit level.

        Detected by watching the position event's ``stopLoss`` field
        AFTER the trailing has activated:
          * LONG  -> updated when stopLoss > last_trailing_stop_price
          * SHORT -> updated when stopLoss < last_trailing_stop_price

        Bybit moves stopLoss in only one direction (favourable) once
        the trailing is active, so the strict inequality is enough.
        Client 2026-04-28: "Värdet måste bekräftas från Bybit, inte
        beräknas lokalt" — the trailing-stop value passed to the
        notifier comes from Bybit's WS event, never from a bot
        calculation.
        """
        symbol = data.get("symbol", "")
        if not symbol:
            return
        try:
            stop_loss = float(data.get("stopLoss", 0) or 0)
            mark_price = float(data.get("markPrice", 0) or 0)
            quantity = float(data.get("size", 0) or 0)
            unrealised_pnl = float(data.get("unrealisedPnl", 0) or 0)
        except (TypeError, ValueError):
            return
        if stop_loss <= 0 or mark_price <= 0:
            return

        for trade in list(self._active_trades.values()):
            if trade.signal is None or trade.signal.symbol != symbol:
                continue
            # Only after the trailing has actually activated — before
            # then any stopLoss change is the static SL Tomas set, not
            # the trailing leg. We must not confuse the two.
            if not trade.trailing_activated_notified:
                continue

            direction = (trade.signal.direction or "").upper()
            last = trade.last_trailing_stop_price
            moved = (
                last is None
                or (direction == "LONG" and stop_loss > last)
                or (direction == "SHORT" and stop_loss < last)
            )
            if not moved:
                continue

            trade.last_trailing_stop_price = stop_loss
            log.info(
                "trade.trailing_updated",
                trade_id=trade.id, symbol=symbol,
                trailing_stop_price=stop_loss,
                mark_price=mark_price,
                previous=last,
            )
            try:
                await self._notifier.trailing_stop_updated(
                    trade=trade,
                    trailing_stop_price=stop_loss,
                    mark_price=mark_price,
                    quantity=quantity,
                    unrealised_pnl=unrealised_pnl,
                )
            except Exception:
                log.exception(
                    "trade.trailing_update_notify_failed",
                    trade_id=trade.id, symbol=symbol,
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

        Called on every relevant price tick (lastPrice from the public
        ticker WS or REST poll). The price is also cached for use by
        the trailing-stop activation gate (client 2026-04-29: must
        track Bybit's Last price, not Mark).
        """
        if symbol and price > 0:
            self._last_price_by_symbol[symbol] = price

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

            # Trailing-stop activation also checked on every Last
            # tick — gating must follow Bybit's trailing trigger
            # source (Last), not Mark. The richer notification body
            # (qty / unrealisedPnl / Bybit-confirmed stopLoss) still
            # comes from on_position_update, but the GATE fires here.
            try:
                await self._check_trailing_activation_on_tick(trade, price)
            except Exception:
                log.exception(
                    "price_update.trailing_check_error",
                    trade_id=trade.id, symbol=symbol,
                )

    async def _check_trailing_activation_on_tick(
        self,
        trade: Trade,
        last_price: float,
    ) -> None:
        """Fire TRAILING STOP AKTIVERAD when Last price crosses the
        activation level. Mirrors the gate previously housed in
        ``_maybe_notify_trailing_activated`` but driven by Last
        ticks instead of Mark from the position event.
        """
        if trade.trailing_activated_notified:
            return
        activation = trade.trailing_activation_price
        if not activation or activation <= 0:
            return
        if trade.signal is None:
            return
        direction = (trade.signal.direction or "").upper()
        crossed = (
            (direction == "LONG" and last_price >= activation)
            or (direction == "SHORT" and last_price <= activation)
        )
        if not crossed:
            return

        trade.trailing_activated_notified = True
        log.info(
            "trade.trailing_activated_via_last",
            trade_id=trade.id, symbol=trade.signal.symbol,
            last_price=last_price,
            activation_price=activation,
            direction=direction,
        )
        # Notification body uses 0 placeholders for Bybit-confirmed
        # stopLoss / qty / unrealisedPnl; the next position event
        # will arrive within ~1s and the trailing_updated path takes
        # over showing the live values.
        try:
            await self._notifier.trailing_stop_activated(
                trade=trade,
                activation_price=activation,
                trailing_distance=trade.trailing_distance or 0.0,
                activation_pct=trade.trailing_activation_pct or 0.0,
                distance_pct=trade.trailing_distance_pct or 0.0,
                trailing_stop_price=None,
                mark_price=last_price,
                quantity=trade.quantity or 0.0,
                unrealised_pnl=0.0,
            )
        except Exception:
            log.exception(
                "trade.trailing_activate_notify_failed",
                trade_id=trade.id, symbol=trade.signal.symbol,
            )

    # ==================================================================
    # Orphan adoption
    # ==================================================================

    async def adopt_orphan_position(self, p: dict) -> Optional[Trade]:
        """Adopt a Bybit position that exists outside the bot's DB.

        Client 2026-04-29 spec: when reverse_reconcile finds a
        profitable orphan, do NOT close it. Instead create a managed
        Trade record so the bot tracks it like any other:

          * Synthesise a minimal ParsedSignal (channel="Orphan/Manual",
            entry=Bybit avgPrice, direction inferred from side).
          * Reuse Bybit's stopLoss/takeProfit if set; otherwise place
            a -3 % auto-SL so the position can never bleed unbounded.
          * Register the Trade in ``_active_trades`` and persist to DB.

        Returns the created Trade or None on failure. Failures are
        logged + flagged via Telegram so the operator can intervene
        manually.
        """
        try:
            sym = p.get("symbol", "")
            side = p.get("side", "")
            size = float(p.get("size") or 0)
            avg_price = float(p.get("avgPrice") or 0)
            position_idx = int(p.get("positionIdx") or 0)
            unreal = float(p.get("unrealisedPnl") or 0)
            try:
                bybit_sl = float(p.get("stopLoss") or 0) or None
            except (TypeError, ValueError):
                bybit_sl = None
            try:
                bybit_tp = float(p.get("takeProfit") or 0) or None
            except (TypeError, ValueError):
                bybit_tp = None
            try:
                leverage = float(p.get("leverage") or 0) or None
            except (TypeError, ValueError):
                leverage = None
            try:
                position_im = float(p.get("positionIM") or 0) or None
            except (TypeError, ValueError):
                position_im = None
        except Exception:
            log.exception("orphan_adopt.parse_error")
            return None

        if not sym or not side or size <= 0 or avg_price <= 0:
            log.warning(
                "orphan_adopt.skip_unparseable",
                symbol=sym, side=side, size=size, avg_price=avg_price,
            )
            return None

        direction = "LONG" if side == "Buy" else "SHORT"

        # If Bybit has no SL on this position, set an auto-SL at -3 %
        # from the avg entry. Without a protective SL the orphan is
        # technically unmanageable; this guarantees a floor before we
        # adopt it.
        sl_price = bybit_sl
        if not sl_price:
            fallback_pct = self._settings.auto_sl.fallback_pct / 100.0
            if direction == "LONG":
                sl_price = round(avg_price * (1 - fallback_pct), 8)
            else:
                sl_price = round(avg_price * (1 + fallback_pct), 8)
            try:
                await self._bybit.set_trading_stop(
                    symbol=sym,
                    position_idx=position_idx,
                    stop_loss=sl_price,
                    sl_trigger_by=getattr(
                        self._settings.tp_sl,
                        "sl_initial_trigger_type",
                        "MarkPrice",
                    ),
                )
                log.info(
                    "orphan_adopt.auto_sl_set",
                    symbol=sym, side=side, sl=sl_price,
                )
            except Exception:
                log.exception(
                    "orphan_adopt.auto_sl_failed",
                    symbol=sym, side=side, sl=sl_price,
                )

        # Synthesised signal. signal_type "dynamic" — orphans aren't
        # classified by SL distance; the bot treats them as a normal
        # managed position from this point on.
        from core.signal_parser import ParsedSignal as _PS
        synth_signal = _PS(
            symbol=sym,
            direction=direction,
            entry=avg_price,
            tp_list=[bybit_tp] if bybit_tp else [],
            sl=sl_price,
            source_channel_id=0,
            source_channel_name="Orphan / Manual",
            raw_text="(orphan position adopted from Bybit)",
            signal_type="dynamic",
        )
        # The notifier reads ``channel_name`` (not source_channel_name).
        try:
            synth_signal.channel_name = "Orphan / Manual"
        except Exception:
            pass
        try:
            synth_signal.tps = list(synth_signal.tp_list)
        except Exception:
            pass

        # Build the managed Trade.
        trade = Trade(
            signal=synth_signal,
            state=TradeState.POSITION_OPEN,
            entry1_fill_price=avg_price,
            avg_entry=avg_price,
            quantity=size,
            leverage=leverage,
            margin=position_im,
            sl_price=sl_price,
        )
        self._active_trades[trade.id] = trade

        # Persist to DB. Failures here don't unwind the in-memory
        # registration — the bot still manages the position; we just
        # lose a row in the audit log.
        try:
            db_id = await self._db.save_trade({
                "signal_id": None,
                "state": trade.state.value,
                "avg_entry": avg_price,
                "quantity": size,
                "leverage": leverage,
                "margin": position_im,
                "sl_price": sl_price,
                "source_channel_id": 0,
                "source_channel_name": "Orphan / Manual",
            })
            if db_id is not None:
                # Switch the trade ID to the DB row's primary key so
                # all subsequent updates (close_trade etc.) hit the
                # right row.
                self._active_trades.pop(trade.id, None)
                trade.id = str(db_id)
                self._active_trades[trade.id] = trade
        except Exception:
            log.exception("orphan_adopt.db_save_failed", symbol=sym)

        log.info(
            "orphan_adopt.completed",
            trade_id=trade.id, symbol=sym, side=side, size=size,
            avg_price=avg_price, sl=sl_price, tp=bybit_tp,
            unrealised_pnl=unreal,
        )

        # Notify the operator with the proper Swedish template.
        try:
            from telegram.notifier import _chan, _ts, _sym, _pnl_sign
            sl_str = f"{sl_price}"
            tp_str = f"{bybit_tp}" if bybit_tp else "ej satt"
            lev_str = f"x{leverage}" if leverage else "okänd"
            im_str = f"{position_im:.2f} USDT" if position_im else "okänd"
            await self._notifier._send_notify(
                f"♻️ ORPHAN POSITION ADOPTED\n"
                f"🕒 Tid: {_ts()}\n"
                f"📢 Från kanal: {_chan('Orphan Manual')}\n"
                f"📊 Symbol: {_sym(sym)}\n"
                f"📈 Riktning: {direction}\n"
                f"\n"
                f"💥 Entry: {avg_price}\n"
                f"💵 Storlek: {size}\n"
                f"🚩 SL: {sl_str}\n"
                f"🎯 TP: {tp_str}\n"
                f"\n"
                f"⚙️ Hävstång: {lev_str}\n"
                f"💰 IM: {im_str}\n"
                f"💰 Aktuell PnL: {_pnl_sign(unreal)} USDT\n"
                f"🔑 Order-ID BOT: {trade.id}\n"
                f"\n"
                f"📍 Status: positionen fanns på Bybit utan att vara "
                f"länkad i botens DB. Den är nu adopterad och hanteras "
                f"som en vanlig position."
            )
        except Exception:
            log.exception("orphan_adopt.notify_failed", symbol=sym)

        return trade

    async def _check_trade_triggers(
        self,
        trade: Trade,
        current_price: float,
    ) -> None:
        """Run the only remaining tick-driven action: hedge pre-arm.

        Strict architecture (client IZZU 2026-04-28): every close decision
        and every Telegram notification (except "signal copied") is
        triggered by a verified Bybit event. The bot does not poll,
        infer, guess, or decide. The price-tick loop only places the
        hedge pre-arm conditional once at the configured trigger — Bybit
        then fires it autonomously, just like SL / TP / trailing.

        Removed (one path, one solution):
          * 30-second position poll — duplicate of WS on_position_update
          * 15-second hedge poll    — duplicate of WS on_position_update
          * BE / Trailing / TP-progression bot-side decisions (deleted
            in the strict refactor)
          * max_loss_cap, combined_loss_cap (bot deciding to close)
          * Re-entry polling — re-entry now fires from close_trade when
            Bybit reports the SL fill.
        """
        state = trade.state

        # Skip trades that are not yet fully open or are awaiting re-entry
        # (the re-entry trigger is Bybit-event-driven inside close_trade).
        if state in (
            TradeState.PENDING,
            TradeState.ENTRY1_PLACED,
            TradeState.ENTRY1_FILLED,
            TradeState.ENTRY2_PLACED,
            TradeState.ENTRY2_FILLED,
            TradeState.REENTRY_WAITING,
        ):
            return

        # --- Scaling check (gated by [scaling].enabled; off in M1). ---
        if (
            self._settings.scaling.enabled
            and trade.scaling_step < len(self._settings.scaling.steps)
        ):
            applied = await self._scaling_mgr.check_and_apply(
                trade, current_price
            )
            if applied:
                return

        # --- Original trade -2% force-close (client 2026-04-30) ---
        # Independent of the signal's SL: when adverse move on the
        # ORIGINAL trade reaches the configured threshold (default -2%),
        # close it with Market reduce-only. Pairs with the hedge's
        # -1.5% trigger so the original trade stops bleeding while the
        # hedge runs alone.
        await self._maybe_force_close_original(trade, current_price)

        # --- Hedge: bot-side check_and_activate is DISABLED 2026-04-30.
        # The Bybit pre-arm conditional placed at trade open is the
        # single source of truth for hedge opening.

        # --- Hedge timeout: close hedge if no meaningful move in
        # configured window (default 20 min, threshold 0.5%). The
        # parent trade carries hedge_filled_at + hedge_entry_price.
        await self._maybe_timeout_hedge(trade, current_price)

    async def _maybe_force_close_original(
        self,
        trade: Trade,
        current_price: float,
    ) -> None:
        """Force-close the original trade at -2 % adverse move.

        Client 2026-04-30: pairs with the -1.5 % hedge trigger so the
        original stops bleeding while the hedge runs alone after.
        Independent of the signal's SL — runs even if SL is further
        away. Idempotent via ``trade.original_force_closed``.
        """
        if getattr(trade, "original_force_closed", False):
            return
        if trade.signal is None or trade.avg_entry is None:
            return
        if trade.avg_entry <= 0 or current_price <= 0:
            return
        threshold_pct = getattr(
            self._settings.hedge, "original_force_close_pct", 2.0,
        )
        direction = (trade.signal.direction or "").upper()
        if direction == "LONG":
            adverse_pct = (trade.avg_entry - current_price) / trade.avg_entry * 100.0
        else:
            adverse_pct = (current_price - trade.avg_entry) / trade.avg_entry * 100.0
        if adverse_pct < threshold_pct:
            return

        # Place a Market reduce-only close on the original leg only.
        # The hedge leg (opposite positionIdx) is left open by design.
        symbol = trade.signal.symbol
        position_idx = 1 if direction == "LONG" else 2
        close_side = "Sell" if direction == "LONG" else "Buy"
        qty = trade.quantity or 0
        if qty <= 0:
            return

        log.info(
            "trade.original_force_close",
            trade_id=trade.id, symbol=symbol,
            avg_entry=trade.avg_entry, current_price=current_price,
            adverse_pct=round(adverse_pct, 4),
            threshold_pct=threshold_pct,
        )
        trade.original_force_closed = True
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
                "trade.original_force_close_failed",
                trade_id=trade.id, symbol=symbol,
            )
            # Don't latch original_force_closed — let it retry next tick.
            trade.original_force_closed = False
            return

        # The Bybit fill-event path (on_order_update -> close_trade)
        # will issue the POSITION CLOSED notification. We just log the
        # decision here and let the unified close path handle the rest.

    async def _maybe_timeout_hedge(
        self,
        trade: Trade,
        current_price: float,
    ) -> None:
        """Close the hedge if it hasn't made a meaningful move in the
        configured timeout window.

        Client 2026-04-30: hedges that stall (price within
        ``no_move_threshold_pct`` of hedge entry for more than
        ``timeout_minutes``) are closed to free capital. Bot-side
        timer — the only bot decision left in the hedge flow.
        """
        if not getattr(trade, "hedge_trade_id", None):
            return
        hedge_entry = getattr(trade, "hedge_entry_price", None)
        hedge_filled_at = getattr(trade, "hedge_filled_at", None)
        if not hedge_entry or hedge_entry <= 0 or hedge_filled_at is None:
            return
        if current_price <= 0:
            return
        if trade.signal is None:
            return

        timeout_minutes = getattr(
            self._settings.hedge, "timeout_minutes", 20,
        )
        no_move_pct = getattr(
            self._settings.hedge, "no_move_threshold_pct", 0.5,
        )

        elapsed = (
            datetime.now(timezone.utc) - hedge_filled_at
        ).total_seconds() / 60.0
        if elapsed < timeout_minutes:
            return

        move_pct = abs(current_price - hedge_entry) / hedge_entry * 100.0
        if move_pct >= no_move_pct:
            return

        # Close the hedge leg — opposite position_idx of the parent.
        symbol = trade.signal.symbol
        parent_dir = (trade.signal.direction or "").upper()
        hedge_position_idx = 2 if parent_dir == "LONG" else 1
        # SHORT hedge -> close with Buy; LONG hedge -> close with Sell.
        close_side = "Buy" if parent_dir == "LONG" else "Sell"

        log.info(
            "hedge.timeout_close",
            trade_id=trade.id, symbol=symbol,
            hedge_entry=hedge_entry, current_price=current_price,
            move_pct=round(move_pct, 4),
            elapsed_minutes=round(elapsed, 2),
            timeout_minutes=timeout_minutes,
        )
        # Latch so we don't keep firing — Bybit's fill event will
        # mark the hedge closed and reset hedge_trade_id elsewhere.
        trade.hedge_filled_at = None
        try:
            await self._bybit.place_market_order(
                symbol=symbol,
                side=close_side,
                qty=trade.quantity or 0,
                position_idx=hedge_position_idx,
                reduce_only=True,
            )
            try:
                from telegram.notifier import _chan, _ts, _sym
                await self._notifier._send_notify(
                    f"⏰ HEDGE TIMEOUT — STÄNGD\n"
                    f"🕒 Tid: {_ts()}\n"
                    f"📢 Från kanal: {_chan(trade.signal.channel_name)}\n"
                    f"📊 Symbol: {_sym(symbol)}\n"
                    f"\n"
                    f"💥 Hedge entry: {hedge_entry}\n"
                    f"📍 Aktuellt pris: {current_price}\n"
                    f"📍 Rörelse: {move_pct:.2f}% "
                    f"(<{no_move_pct}%)\n"
                    f"📍 Tid sedan fill: {elapsed:.1f} min "
                    f"(>{timeout_minutes} min)\n"
                    f"\n"
                    f"📍 Skäl: ingen meningsfull rörelse — "
                    f"capital control."
                )
            except Exception:
                log.exception(
                    "hedge.timeout_notify_failed",
                    trade_id=trade.id, symbol=symbol,
                )
        except Exception:
            log.exception(
                "hedge.timeout_close_failed",
                trade_id=trade.id, symbol=symbol,
            )

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

        # Cancel the pre-armed hedge conditional if it never fired —
        # otherwise it would open an unwanted hedge position after the
        # main trade is already gone.
        # We do NOT force-close an already-filled hedge here. Per the
        # strict architecture, the hedge has its own SL/TP set on Bybit;
        # Bybit closes it autonomously and the bot mirrors that close
        # via on_order_update. Force-closing from the bot was the source
        # of the [HEDGE CLOSE ERROR] notifications (2026-04-28).
        if trade.hedge_conditional_order_id is not None:
            try:
                await self._hedge_mgr.cancel_pre_armed(trade)
            except Exception:
                log.exception(
                    "close_trade.hedge_pre_arm_cancel_failed",
                    trade_id=trade.id,
                )

        # Sweep any leftover bot-placed conditional orders on this
        # symbol/side. Bybit doesn't auto-cancel untriggered Stop /
        # conditional orders when the position closes via trailing or
        # native TP/SL — without this sweep they accumulate (see
        # incident 2026-04-28 where 29 orphan orders piled up across
        # closed trades). In hedge mode there is exactly one position
        # per (symbol, positionIdx), so cancelling all orders matching
        # this trade's positionIdx is safe.
        if trade.signal:
            try:
                close_position_idx = 1 if direction == "LONG" else 2
                leftover = await self._bybit.get_open_orders(symbol=symbol)
                cancelled = 0
                for od in leftover:
                    try:
                        o_pidx = int(od.get("positionIdx") or 0)
                    except (TypeError, ValueError):
                        continue
                    if o_pidx != close_position_idx:
                        continue
                    oid = od.get("orderId")
                    if not oid:
                        continue
                    try:
                        await self._bybit.cancel_order(
                            symbol=symbol, order_id=oid,
                        )
                        cancelled += 1
                    except Exception:
                        log.exception(
                            "close_trade.order_cancel_failed",
                            trade_id=trade.id, symbol=symbol, order_id=oid,
                        )
                if cancelled:
                    log.info(
                        "close_trade.leftover_orders_swept",
                        trade_id=trade.id, symbol=symbol,
                        position_idx=close_position_idx, cancelled=cancelled,
                    )
            except Exception:
                log.exception(
                    "close_trade.order_sweep_failed",
                    trade_id=trade.id, symbol=symbol,
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

        # Notify — single template, with the close source appended to the
        # header so the operator sees exactly what Bybit did:
        #   POSITION CLOSED - stop loss
        #   POSITION CLOSED - trailing stop
        #   POSITION CLOSED - TP3
        #   POSITION CLOSED - liquidation
        #   POSITION CLOSED - external close
        try:
            qty_for_msg = trade.quantity or 0
            await self._notifier.position_closed(
                trade=trade,
                exit_price=exit_price,
                qty=qty_for_msg,
                result_pct_total=pnl_pct if pnl_pct is not None else 0.0,
                result_usdt_total=pnl_usdt if pnl_usdt is not None else 0.0,
                close_source=_format_close_source(reason),
            )
        except Exception:
            log.exception(
                "close_trade.template_failed", trade_id=trade_id, reason=reason,
            )

        log.info(
            "trade.closed",
            trade_id=trade_id,
            symbol=symbol,
            reason=reason,
            pnl_pct=pnl_pct,
        )

        # --- Re-entry: triggered ONLY by a Bybit-classified SL fill,
        # never by polling. close_trade fires re-entry directly so
        # Bybit's order-fill event drives the new trade. ---
        if reason == "stop_loss":
            if trade.reentry_count < self._settings.reentry.max_reentries:
                try:
                    await self._reentry_mgr.activate_after_sl(trade)
                except Exception:
                    log.exception(
                        "close_trade.reentry_activate_failed",
                        trade_id=trade.id,
                    )

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
                # Contract delisted / not tradable on Bybit — same
                # operational meaning as "Finns inte på bybit"
                # (the symbol is not tradable here), so use the same
                # warning template for consistency (client 2026-04-28).
                if trade.signal and self._should_send_reject_notify(
                    "not_on_bybit",
                    trade.signal.symbol,
                    trade.signal.direction,
                ):
                    try:
                        await self._notifier.symbol_not_on_bybit(trade.signal)
                    except Exception:
                        log.exception("notify.symbol_not_on_bybit_failed")
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

        # Client 2026-04-30: only update TP if the new TP is BETTER
        # than the existing one (further upside for LONG, further
        # downside for SHORT). Never downgrade. SL only changes after
        # the liquidation check (existing behaviour). This collapses
        # the recurring `❌ TP/SL UPPDATERING MISSLYCKADES` spam:
        # repeat signals from other channels with similar/worse TPs
        # are silently skipped instead of fired into Bybit and
        # rejected.
        existing_tp_raw = existing_trade_row.get("highest_tp_price")
        try:
            existing_tp = (
                float(existing_tp_raw) if existing_tp_raw else None
            )
        except (TypeError, ValueError):
            existing_tp = None
        if existing_tp is None:
            # Fallback: pull from JSON tp_list column if the schema
            # doesn't have a denormalised highest_tp_price column.
            try:
                import json as _json
                raw_list = existing_trade_row.get("tp_list")
                if raw_list:
                    parsed = (
                        _json.loads(raw_list)
                        if isinstance(raw_list, str) else raw_list
                    )
                    if parsed:
                        existing_tp = (
                            max(float(x) for x in parsed if x)
                            if direction == "LONG"
                            else min(float(x) for x in parsed if x)
                        )
            except Exception:
                existing_tp = None

        existing_sl_raw = existing_trade_row.get("sl_price")
        try:
            existing_sl = (
                float(existing_sl_raw) if existing_sl_raw else None
            )
        except (TypeError, ValueError):
            existing_sl = None

        # --- Decide whether the new TP is an improvement ---
        if direction == "LONG":
            new_best_tp = max(new_tps) if new_tps else None
        else:
            new_best_tp = min((tp for tp in new_tps if tp), default=None)

        tp_improved = False
        if new_best_tp and new_best_tp > 0:
            if existing_tp is None:
                tp_improved = True
            elif direction == "LONG" and new_best_tp > existing_tp * 1.001:
                tp_improved = True
            elif direction == "SHORT" and new_best_tp < existing_tp * 0.999:
                tp_improved = True

        # --- Decide whether SL needs an update ---
        # Only push a new SL if it's materially different from the
        # current one (≥0.1% delta). Identical-value updates are the
        # primary cause of Bybit "not modified" rejections and the
        # repeat ❌ TP/SL UPPDATERING MISSLYCKADES messages.
        sl_changed = False
        if new_sl and new_sl > 0:
            if existing_sl is None:
                sl_changed = True
            elif abs(new_sl - existing_sl) / max(existing_sl, 1e-9) >= 0.001:
                sl_changed = True

        if not tp_improved and not sl_changed:
            log.info(
                "trade.tp_sl_update_skipped_no_improvement",
                trade_id=trade_id,
                symbol=symbol,
                new_best_tp=new_best_tp,
                existing_tp=existing_tp,
                new_sl=new_sl,
                existing_sl=existing_sl,
            )
            return

        # --- Update TP / SL on exchange ---
        try:
            update_params = {}
            if tp_improved:
                update_params["take_profit"] = new_best_tp
            if sl_changed:
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
        except Exception as exc:
            # Bybit code 34040 = "not modified" — values already
            # match. Treat as no-op, no operator notification.
            ret_code = getattr(exc, "ret_code", None)
            if ret_code == 34040:
                log.info(
                    "trade.tp_sl_update_unchanged",
                    trade_id=trade_id, symbol=symbol,
                )
                return
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
