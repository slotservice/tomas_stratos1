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

        # Phase 6 audit-snapshot counter (client 2026-05-02 audit #11):
        # incremented in close_trade after a successful close; once it
        # reaches reporting.audit_snapshot_every_n_trades the snapshot
        # fires and the counter resets. 0 in config disables the
        # snapshot entirely.
        self._closes_since_snapshot: int = 0

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
            if self._should_send_reject_notify(
                "max_capacity", symbol, direction,
            ):
                from telegram.notifier import _chan, _ts
                await self._safe_notify(
                    f"⚠️ SIGNAL BLOCKERAD (maxkapacitet nådd)\n"
                    f"🕒 Tid: {_ts()}\n"
                    f"📢 Från kanal: "
                    f"{_chan(getattr(signal, 'channel_name', ''))}\n"
                    f"📊 Symbol: #{symbol}\n"
                    f"📈 Riktning: {direction}\n"
                    f"📍 Aktiva trades: {max_trades} (max nått). "
                    f"Stäng en position innan boten kan ta nya signaler."
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
                if self._should_send_reject_notify(
                    "stale_signal_age", symbol, direction,
                ):
                    from telegram.notifier import _chan, _ts
                    await self._safe_notify(
                        f"⚠️ SIGNAL BLOCKERAD (för gammal)\n"
                        f"🕒 Tid: {_ts()}\n"
                        f"📢 Från kanal: "
                        f"{_chan(getattr(signal, 'channel_name', ''))}\n"
                        f"📊 Symbol: #{symbol}\n"
                        f"📈 Riktning: {direction}\n"
                        f"📍 Anledning: signalen är äldre än {max_age}s — "
                        f"marknaden kan ha flyttat."
                    )
                return None

        # ----------------------------------------------------------
        # 4. SL handling. Client 2026-05-02 spec (revised):
        #    - Signal HAS SL  -> use it as-is, dynamic leverage from
        #      its distance.
        #    - Signal MISSING SL -> apply auto-SL at -auto_sl.fallback_pct
        #      (default -3%) AND force fixed leverage at
        #      auto_sl.fallback_leverage (default x10). The trade
        #      proceeds normally; the Phase 1 PROTECTION_FAILED gate
        #      will verify the SL actually landed on Bybit and force-
        #      close the position if not.
        # ----------------------------------------------------------
        entry_price = signal.entry
        sl_price = signal.sl if hasattr(signal, "sl") else None
        auto_sl_applied = False

        if sl_price is None:
            fallback_pct = self._settings.auto_sl.fallback_pct / 100.0
            if direction == "LONG":
                sl_price = round(entry_price * (1 - fallback_pct), 8)
            else:
                sl_price = round(entry_price * (1 + fallback_pct), 8)
            # Mutate the signal so downstream code (notifier templates,
            # _move_sl_to safety predicates, audit log) sees the auto-SL.
            try:
                signal.sl = sl_price
                signal.signal_type = "fixed"
            except Exception:
                pass
            auto_sl_applied = True
            log.info(
                "signal.auto_sl_applied",
                symbol=symbol,
                direction=direction,
                entry=entry_price,
                auto_sl=sl_price,
                fallback_pct=self._settings.auto_sl.fallback_pct,
                fallback_leverage=self._settings.auto_sl.fallback_leverage,
                channel_name=channel_name,
            )

        # ----------------------------------------------------------
        # 5. Leverage. Auto-SL signals get a FIXED leverage (per
        #    client spec 2026-05-02); real-SL signals get the
        #    dynamic formula bucketed by SL distance.
        # ----------------------------------------------------------
        if auto_sl_applied:
            leverage = float(self._settings.auto_sl.fallback_leverage)
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
        # Also functions as the "symbol delisted" check — if get_ticker
        # raises 'No ticker data', the symbol isn't tradable on Bybit
        # (e.g. FETUSDT was delisted after the FET+AGIX+OCEAN merger
        # — Bybit returns ErrCode 110074 'contract is not live').
        # We early-reject in that case with the deduped 'Finns inte
        # på bybit' notification, instead of silently swallowing the
        # error and letting the failure cascade through set_leverage
        # + place_market_order (each emitting its own notification).
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
        except Exception as exc:
            err_str = str(exc).lower()
            # Detect "symbol not on Bybit" specifically. Two signal
            # patterns from Bybit / our adapter:
            #   - "No ticker data for X"  (adapter raises this when
            #     get_ticker returns empty/None)
            #   - "contract is not live"   (Bybit ErrCode 110074)
            #   - "symbol is not exists"   (some Bybit error variants)
            if (
                "no ticker data" in err_str
                or "not live" in err_str
                or "not exists" in err_str
                or "110074" in err_str
            ):
                log.info(
                    "signal.symbol_not_on_bybit",
                    symbol=symbol,
                    direction=direction,
                    channel_name=channel_name,
                    error=err_str[:120],
                )
                if self._should_send_reject_notify(
                    "not_on_bybit", symbol, direction,
                ):
                    try:
                        from telegram.notifier import _chan, _ts
                        await self._safe_notify(
                            f"⚠️ Finns inte på bybit ⚠️\n"
                            f"🕒 Tid: {_ts()}\n"
                            f"📢 Från kanal: {_chan(channel_name)}\n"
                            f"📊 Symbol: #{symbol}\n"
                            f"📈 Riktning: {direction}\n"
                            f"📍 Fel: Kontrolera manuellt"
                        )
                    except Exception:
                        log.exception("notify.not_on_bybit_failed")
                return None
            log.exception("signal.slippage_check_error", symbol=symbol)

        # Round leverage to symbol's leverage step (keeps e.g. 12.34 precision).
        try:
            # Ensure instrument info is cached so round_leverage works.
            await self._bybit.get_instrument_info(symbol)
            leverage = self._bybit.round_leverage(leverage, symbol)
        except Exception:
            leverage = round(leverage, 2)

        # SL-vs-liquidation guard. Reject signals where the SL sits
        # outside the position's liquidation distance — there is no
        # safe way to honour such an SL because Bybit will liquidate
        # the position before the SL can fire. BSBUSDT trade 3340
        # incident 2026-04-30: SHORT entry 0.3313, signal SL 0.44
        # (+33 % adverse) with 6x leverage (liq distance ~16.7 %) —
        # liquidated at -16.6 USDT before the SL had any chance.
        # Estimate liq distance as ~1/leverage minus a small
        # maintenance-margin buffer (Bybit linear typical ~0.5 %).
        # Reject if signal_sl_distance > 0.85 * liq_distance.
        try:
            sl_distance_pct = abs(entry_price - sl_price) / entry_price
            est_liq_distance = max(1.0 / leverage - 0.005, 0.005)
            if sl_distance_pct > est_liq_distance * 0.85:
                log.warning(
                    "signal.sl_beyond_liq_rejected",
                    symbol=symbol,
                    entry=entry_price,
                    sl=sl_price,
                    sl_distance_pct=round(sl_distance_pct * 100, 4),
                    est_liq_distance_pct=round(est_liq_distance * 100, 4),
                    leverage=leverage,
                    channel_name=channel_name,
                )
                if self._should_send_reject_notify(
                    "sl_beyond_liq", symbol, direction,
                ):
                    try:
                        from telegram.notifier import _chan, _ts
                        await self._safe_notify(
                            f"⚠️ SIGNAL AVVISAD (SL bortom likvidationsavstånd)\n"
                            f"🕒 Tid: {_ts()}\n"
                            f"📢 Från kanal: {_chan(channel_name)}\n"
                            f"📊 Symbol: #{symbol}\n"
                            f"📈 Riktning: {direction}\n"
                            f"💥 Entry: {entry_price}\n"
                            f"🚩 Signal-SL: {sl_price} "
                            f"({sl_distance_pct*100:.2f}% från entry)\n"
                            f"⚙️ Hävstång: x{leverage} "
                            f"(likvidation ~{est_liq_distance*100:.2f}% från entry)\n"
                            f"📍 Anledning: SL ligger längre bort än "
                            f"likvidationsavståndet — Bybit hade likviderat "
                            f"positionen innan SL hann triggra."
                        )
                    except Exception:
                        log.exception("notify.sl_beyond_liq_failed")
                return None
        except Exception:
            log.exception("signal.sl_liq_check_error", symbol=symbol)

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

        # Protection-failure tracker (Phase 1, client 2026-05-01 audit
        # point #18). Each protection step (SL, TP, trailing) appends
        # its name on Bybit-side failure. After all steps run, if this
        # list is non-empty, the trade is marked PROTECTION_FAILED, the
        # position is force-closed via Market reduce-only, and the
        # normal "POSITION ÖPPNAD" notification is SUPPRESSED.
        protection_failures: list[str] = []

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
                protection_failures.append("SL")

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
                    if "TP" not in protection_failures:
                        protection_failures.append("TP")

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
            protection_failures.append("trailing")

        # ----------------------------------------------------------
        # 14c. PROTECTION GATE (Phase 1, client 2026-05-01 audit #18).
        # If any protection step (SL, TP, trailing) failed to verify
        # on Bybit, the position is unsafe. Force-close it via
        # Market reduce-only and emit a single PROTECTION_FAILED
        # error in place of POSITION ÖPPNAD.
        # ----------------------------------------------------------
        if protection_failures:
            log.error(
                "trade.protection_failed",
                trade_id=trade.id, symbol=symbol,
                failed_steps=protection_failures,
            )
            close_side = "Sell" if direction == "LONG" else "Buy"
            close_action = "force-stängd"
            try:
                if trade.quantity and trade.quantity > 0:
                    await self._bybit.place_market_order(
                        symbol=symbol,
                        side=close_side,
                        qty=trade.quantity,
                        position_idx=position_idx,
                        reduce_only=True,
                    )
                    log.info(
                        "trade.protection_failed.force_closed",
                        trade_id=trade.id, symbol=symbol,
                        qty=trade.quantity,
                    )
            except Exception as exc:
                ret_code = getattr(exc, "ret_code", None)
                if ret_code == 110017:
                    log.warning(
                        "trade.protection_failed.position_already_zero",
                        trade_id=trade.id, symbol=symbol,
                    )
                    close_action = "redan stängd på Bybit"
                else:
                    log.exception(
                        "trade.protection_failed.force_close_failed",
                        trade_id=trade.id, symbol=symbol,
                    )
                    close_action = "stängning MISSLYCKADES — manuell åtgärd"
            trade.transition(TradeState.PROTECTION_FAILED)
            await self._persist_trade_state(
                trade,
                avg_entry=avg_entry,
                margin=trade.margin,
                quantity=trade.quantity,
            )
            try:
                await self._db.log_event(
                    trade_id=int(trade.id),
                    event_type="protection_failed",
                    details={
                        "failed_steps": protection_failures,
                        "close_action": close_action,
                    },
                )
            except Exception:
                pass
            if self._should_send_reject_notify(
                "protection_failed", symbol, direction,
            ):
                try:
                    await self._notifier.protection_failed(
                        trade=trade,
                        signal=signal,
                        failed_steps=protection_failures,
                        action=close_action,
                    )
                except Exception:
                    log.exception(
                        "notify.protection_failed_failed",
                        trade_id=trade.id, symbol=symbol,
                    )
            self._active_trades.pop(trade.id, None)
            return None

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

        # Audit fields (client 2026-05-01 audit point #10): every trade
        # log line must include signal_id (DB row), db_id (= trade.id
        # after save_trade), bybit order IDs, leverage, state. structlog
        # adds the timestamp automatically.
        log.info(
            "trade.opened",
            trade_id=trade.id,
            db_id=trade.id,
            signal_id=signal_db_id,
            symbol=symbol,
            direction=direction,
            avg_entry=avg_entry,
            leverage=leverage,
            sl_price=trade.sl_price,
            tp_count=len(valid_tps),
            bybit_order_ids=trade.bybit_order_ids,
            tp_order_ids=trade.tp_order_ids,
            state=trade.state.value,
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

        # Phase 2 (client 2026-05-01) — original-trade emergency close
        # is a Bybit conditional, not a bot-poll decision. Place ONE
        # reduce-only Market conditional at entry ± original_force_
        # close_pct (LastPrice). Bybit fires it autonomously if price
        # ever reaches the threshold. The bot's only job is to react
        # to the WS fill event and cancel the conditional when the
        # trade closes via any other path (TP, trailing, manual).
        try:
            force_close_pct = getattr(
                self._settings.hedge,
                "original_force_close_pct",
                2.0,
            ) / 100.0
            if avg_entry and avg_entry > 0 and force_close_pct > 0:
                if direction == "LONG":
                    fc_trigger_price = round(
                        avg_entry * (1 - force_close_pct), 8,
                    )
                    fc_side = "Sell"
                else:
                    fc_trigger_price = round(
                        avg_entry * (1 + force_close_pct), 8,
                    )
                    fc_side = "Buy"
                fc_qty = trade.quantity or 0.0
                if fc_qty > 0:
                    fc_resp = await self._bybit.place_conditional_stop(
                        symbol=symbol,
                        side=fc_side,
                        qty=fc_qty,
                        trigger_price=fc_trigger_price,
                        position_idx=position_idx,
                        trigger_by="LastPrice",
                    )
                    fc_order_id = fc_resp.get("orderId", "") or None
                    trade.original_force_close_order_id = fc_order_id
                    log.info(
                        "trade.original_force_close.armed",
                        trade_id=trade.id,
                        symbol=symbol,
                        trigger_price=fc_trigger_price,
                        side=fc_side,
                        qty=fc_qty,
                        order_id=fc_order_id,
                        force_close_pct=force_close_pct * 100,
                    )
                    try:
                        await self._db.update_trade(
                            int(trade.id),
                            original_force_close_order_id=fc_order_id,
                        )
                    except Exception:
                        pass
        except Exception:
            log.exception(
                "trade.original_force_close.arm_failed",
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
            # Phase 3 (client 2026-05-01) — hedge conditional fill is
            # now handled atomically here, NOT by reverse_reconcile
            # polling. If this filled order is a tracked hedge
            # conditional, activate the hedge in one path: read the
            # Bybit fill, set SL+trailing on the hedge, persist to DB,
            # send Telegram. No race, no orphan, single source of truth.
            await self._maybe_activate_hedge_from_fill(order_id, data)
            # Phase 4 (client 2026-05-01) — record ORIGINAL_FORCE_CLOSED
            # state when the -2% conditional fires. The actual close
            # bookkeeping (PnL + Telegram) still happens via
            # _classify_bybit_close_fill -> close_trade below; this
            # transition is purely for the audit trail.
            await self._maybe_record_force_close_fill(order_id)
            # Strict architecture (client 2026-04-28): every close
            # notification and every close decision is driven by Bybit's
            # order-fill event, never by bot inference. Classify this
            # fill below — if it is a position-closing event (SL fire,
            # trailing fire, partial-TP fill, liquidation), record the
            # close reason on the trade. on_position_update will read
            # that reason when Bybit reports size=0 and call close_trade.
            await self._classify_bybit_close_fill(order_id, data)

    async def _maybe_record_force_close_fill(self, order_id: str) -> None:
        """Mark a trade as ORIGINAL_FORCE_CLOSED when its -2% Bybit
        conditional fires. Audit-trail only — close_trade is still
        the path that finalises PnL + Telegram.
        """
        if not order_id:
            return
        for tr in self._active_trades.values():
            if (
                getattr(tr, "original_force_close_order_id", None) == order_id
                and not tr.is_terminal
            ):
                tr.transition(TradeState.ORIGINAL_FORCE_CLOSED)
                log.info(
                    "trade.original_force_close.fired",
                    trade_id=tr.id,
                    symbol=tr.signal.symbol if tr.signal else "",
                    order_id=order_id,
                )
                # Clear the order id — once it has fired, it's no
                # longer a valid Bybit reference.
                tr.original_force_close_order_id = None
                try:
                    await self._db.update_trade(
                        int(tr.id),
                        original_force_close_order_id=None,
                    )
                except Exception:
                    pass
                return

    async def _maybe_activate_hedge_from_fill(
        self, order_id: str, data: dict,
    ) -> None:
        """Atomically activate a hedge whose pre-armed conditional just fired.

        Phase 3 of the Bybit-verified-state-machine refactor (client
        2026-05-01). Replaces the polling-based pre_armed_hedges arming
        block in main._periodic_reverse_reconciliation. Single path,
        WS-driven, all values verified from Bybit's fill event.

        Match rule: ``order_id`` equals some active trade's
        ``hedge_conditional_order_id``. When matched, this method:
          1. Reads the Bybit-verified avg fill price + qty.
          2. Computes hedge hard SL (configured % from hedge entry,
             BACKUP only — trailing is the primary exit) and
             trailing distance (configured %).
          3. Calls set_trading_stop with stop_loss + trailing_stop in
             ONE API call (active_price OMITTED so Bybit defaults to
             immediate activation, avoiding the activePrice == entry
             rejection that hit the earlier polling path).
          4. Saves the hedge as a separate trade row in DB and links
             it on the parent trade via hedge_trade_id.
          5. Records hedge_entry_price + hedge_filled_at for the
             20-min timeout watcher.
          6. Transitions the parent trade to HEDGE_ACTIVE.
          7. Sends the HEDGE AKTIVERAD Telegram — only after the
             Bybit ack confirms SL+trailing are on the hedge position.
        """
        if not order_id:
            return
        # Find the parent trade whose hedge conditional just filled.
        parent: Optional[Trade] = None
        for tr in self._active_trades.values():
            if tr.hedge_conditional_order_id == order_id and not tr.hedge_trade_id:
                parent = tr
                break
        if parent is None:
            return
        if parent.signal is None:
            log.warning(
                "hedge.fill.parent_has_no_signal",
                order_id=order_id, parent_trade_id=parent.id,
            )
            return

        symbol = parent.signal.symbol
        parent_dir = (parent.signal.direction or "").upper()
        # Hedge position lives in the OPPOSITE positionIdx slot.
        hedge_position_idx = 2 if parent_dir == "LONG" else 1
        hedge_side = "Sell" if parent_dir == "LONG" else "Buy"
        hedge_dir = "SHORT" if parent_dir == "LONG" else "LONG"

        # Read Bybit-verified fill data. avgPrice on the conditional's
        # fill event is the actual entry. Fall back to triggerPrice
        # only if avgPrice is missing.
        try:
            avg_price = float(data.get("avgPrice") or 0)
        except (TypeError, ValueError):
            avg_price = 0.0
        if avg_price <= 0:
            try:
                avg_price = float(data.get("triggerPrice") or 0)
            except (TypeError, ValueError):
                avg_price = 0.0
        try:
            fill_qty = float(data.get("cumExecQty") or 0)
        except (TypeError, ValueError):
            fill_qty = 0.0
        if avg_price <= 0 or fill_qty <= 0:
            log.warning(
                "hedge.fill.unparseable",
                order_id=order_id, parent_trade_id=parent.id,
                avg_price=avg_price, fill_qty=fill_qty,
            )
            return

        hedge_settings = self._settings.hedge
        hard_sl_pct = hedge_settings.hard_sl_pct / 100.0
        trailing_pct = hedge_settings.trailing_pct / 100.0
        # Hedge SHORT: SL above entry (loses when price rises).
        # Hedge LONG: SL below entry (loses when price falls).
        if hedge_side == "Sell":
            hedge_sl = round(avg_price * (1 + hard_sl_pct), 8)
        else:
            hedge_sl = round(avg_price * (1 - hard_sl_pct), 8)
        trailing_distance = round(avg_price * trailing_pct, 8)

        # ONE Bybit call to set both SL and trailing on the hedge.
        # active_price is OMITTED so Bybit defaults to immediate
        # activation at current price (correct for a hedge that fires
        # AFTER -1.5 % adverse — the hedge is already in profit
        # territory at fill time; setting active_price == entry
        # triggers Bybit error 10001).
        try:
            await self._bybit.set_trading_stop(
                symbol=symbol,
                position_idx=hedge_position_idx,
                stop_loss=hedge_sl,
                trailing_stop=trailing_distance,
                sl_trigger_by="LastPrice",
            )
        except Exception:
            log.exception(
                "hedge.fill.set_trading_stop_failed",
                parent_trade_id=parent.id, symbol=symbol,
            )
            # Without SL+trailing the hedge is unprotected. This is
            # a Phase-1 PROTECTION_FAILED scenario — but for the
            # HEDGE leg specifically. We do NOT force-close the
            # hedge here (Bybit can still manage it manually); we
            # surface a deduped error and let the operator handle.
            try:
                if self._should_send_reject_notify(
                    "hedge_protection_failed", symbol, hedge_dir,
                ):
                    from telegram.notifier import _chan, _sym, _ts
                    chan = _chan(parent.signal.channel_name)
                    await self._safe_notify(
                        f"❌ HEDGE OFÖRSVARAD (PROTECTION FAILED)\n"
                        f"🕒 Tid: {_ts()}\n"
                        f"📢 Från kanal: {chan}\n"
                        f"📊 Symbol: {_sym(symbol)}\n"
                        f"📈 Riktning: {hedge_dir} "
                        f"(motrikt mot {parent_dir})\n"
                        f"💥 Hedge entry: {avg_price}\n"
                        f"📍 SL/trailing kunde inte sättas på Bybit "
                        f"— manuell åtgärd krävs."
                    )
            except Exception:
                log.exception(
                    "hedge.fill.notify_protection_failed",
                    parent_trade_id=parent.id, symbol=symbol,
                )
            return

        # Persist hedge as its own trade row.
        hedge_trade_db_id = None
        try:
            hedge_trade_db_id = await self._db.save_trade({
                "signal_id": None,
                "state": TradeState.HEDGE_ACTIVE.value,
                "avg_entry": avg_price,
                "quantity": fill_qty,
                "leverage": parent.leverage,
                "margin": parent.margin,
                "sl_price": hedge_sl,
            })
        except Exception:
            log.exception(
                "hedge.fill.db_save_failed",
                parent_trade_id=parent.id, symbol=symbol,
            )

        parent.hedge_trade_id = (
            str(hedge_trade_db_id)
            if hedge_trade_db_id is not None
            else "fired"
        )
        parent.hedge_conditional_order_id = None
        parent.hedge_entry_price = avg_price
        parent.hedge_filled_at = datetime.now(timezone.utc)
        try:
            await self._db.update_trade(
                int(parent.id),
                hedge_trade_id=parent.hedge_trade_id,
                hedge_conditional_order_id=None,
            )
        except Exception:
            pass
        parent.transition(TradeState.HEDGE_ACTIVE)

        log.info(
            "hedge.fill.activated",
            parent_trade_id=parent.id,
            symbol=symbol,
            hedge_trade_id=parent.hedge_trade_id,
            hedge_entry=avg_price,
            hedge_qty=fill_qty,
            hedge_sl=hedge_sl,
            trailing_distance=trailing_distance,
            hard_sl_pct=hedge_settings.hard_sl_pct,
            trailing_pct=hedge_settings.trailing_pct,
        )

        # Telegram — only AFTER Bybit ack on the SL+trailing set call.
        try:
            from telegram.notifier import _chan, _sym, _ts
            chan = _chan(parent.signal.channel_name)
            lev_type = parent.signal.signal_type or "dynamic"
            leverage_val = parent.leverage or 0.0
            margin_val = parent.margin or 0.0
            await self._safe_notify(
                f"🛡️ HEDGE AKTIVERAD (Bybit-conditional)\n"
                f"🕒 Tid: {_ts()}\n"
                f"📢 Från kanal: {chan}\n"
                f"📊 Symbol: {_sym(symbol)}\n"
                f"📈 Riktning: {hedge_dir} (motrikt mot {parent_dir})\n"
                f"📍 Typ: {lev_type}\n"
                f"\n"
                f"💥 Entry (hedge): {avg_price}\n"
                f"🚩 Hard SL: {hedge_sl} "
                f"(-{hedge_settings.hard_sl_pct}% från hedge-entry, BACKUP)\n"
                f"🔄 Trailing: {hedge_settings.trailing_pct}% "
                f"(Last price, primär exit, ingen fast TP)\n"
                f"\n"
                f"⚙️ Hävstång ({lev_type}): x{leverage_val}\n"
                f"💰 IM: {margin_val:.2f} USDT (Bybit confirmed)\n"
                f"🔑 Order-ID BOT: {parent.id}\n"
                f"🔑 Order-ID Bybit: {parent.hedge_trade_id or 'N/A'}"
            )
        except Exception:
            log.exception(
                "hedge.fill.notify_failed",
                parent_trade_id=parent.id, symbol=symbol,
            )

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

        # Phase 5c (client 2026-05-02 audit findings) — hedge-child
        # close detection. When a hedge SL/Trailing fires, the closing
        # WS event lands on the OPPOSITE slot from the parent
        # (LONG parent -> hedge fires at idx=2 SHORT; SHORT parent ->
        # hedge fires at idx=1 LONG). The lookup above returns None
        # because no signal-direction trade lives in that slot — only
        # a hedge child does. Find the parent via opposite-direction
        # lookup with hedge_trade_id set, and route to a dedicated
        # hedge-close handler that updates the child trade row.
        is_hedge_close = False
        parent_for_hedge_close = None
        if trade is None:
            parent_direction = "SHORT" if position_idx == 2 else "LONG"
            # Wait: hedge of LONG is at idx=2. So if WS event is on
            # idx=2, parent could be LONG (and we're closing the hedge).
            # Reverse: if event on idx=1, parent could be SHORT.
            parent_direction = "LONG" if position_idx == 2 else "SHORT"
            candidate = self._find_trade_by_symbol_direction(
                symbol, parent_direction,
            )
            if (
                candidate is not None
                and not candidate.is_terminal
                and candidate.signal is not None
                and candidate.hedge_trade_id
            ):
                parent_for_hedge_close = candidate
                is_hedge_close = True
                trade = candidate  # so _exit_price() below works

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
                is_hedge_close=is_hedge_close,
            )
            if is_hedge_close:
                await self._close_hedge_child(
                    parent_for_hedge_close, "hedge_stop_loss",
                    _exit_price(), position_idx,
                )
            else:
                await self.close_trade(trade.id, "stop_loss", _exit_price())
            return

        if reduce_only and sot == "TrailingStop":
            log.info(
                "ws.close_event",
                trade_id=trade.id, symbol=symbol,
                kind="trailing_stop", order_id=order_id,
                is_hedge_close=is_hedge_close,
            )
            if is_hedge_close:
                await self._close_hedge_child(
                    parent_for_hedge_close, "hedge_trailing_stop",
                    _exit_price(), position_idx,
                )
            else:
                await self.close_trade(trade.id, "trailing_stop", _exit_price())
            return

        if exec_type == "Liquidation":
            log.warning(
                "ws.close_event",
                trade_id=trade.id, symbol=symbol,
                kind="liquidation", order_id=order_id,
                is_hedge_close=is_hedge_close,
            )
            if is_hedge_close:
                await self._close_hedge_child(
                    parent_for_hedge_close, "hedge_liquidation",
                    _exit_price(), position_idx,
                )
            else:
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
                is_hedge_close=is_hedge_close,
            )
            if is_hedge_close:
                await self._close_hedge_child(
                    parent_for_hedge_close, "hedge_external_close",
                    _exit_price(), position_idx,
                )
            else:
                await self.close_trade(trade.id, "external_close", _exit_price())

    async def _close_hedge_child(
        self,
        parent: "Trade",
        reason: str,
        exit_price: float,
        position_idx: int,
    ) -> None:
        """Close the hedge child trade row when the hedge position closes
        on Bybit.

        Phase 5c (client 2026-05-02 audit findings): the diagnostic
        showed +3 ghost rows in DB vs Bybit because the hedge child
        Trade row created by ``_maybe_activate_hedge_from_fill``
        never got marked CLOSED when its underlying Bybit position
        was closed by trailing or hard SL. Without this handler:
          - hedge child row stays POSITION_OPEN forever
          - leftover Bybit conditionals don't get swept
          - state_recovery.orphan_check on next restart gets confused

        This method:
          1. Looks up the hedge child trade by parent.hedge_trade_id.
          2. Computes hedge PnL from hedge_entry_price + exit price.
          3. Marks the child trade CLOSED in DB.
          4. Sweeps any leftover orders on the hedge slot.
          5. Sends a "🛡️ HEDGE STÄNGD" Telegram (Bybit-verified).

        The PARENT trade is NOT closed — it may still hold the
        original position. The parent transitions to ORIGINAL_FORCE_
        CLOSED if its own -2% conditional fired earlier; otherwise
        stays in HEDGE_ACTIVE (the hedge ran its course but the
        original may still be open).
        """
        if parent is None or parent.signal is None:
            return
        symbol = parent.signal.symbol
        parent_dir = parent.signal.direction
        hedge_dir = "SHORT" if parent_dir == "LONG" else "LONG"

        hedge_child_id = parent.hedge_trade_id
        if not hedge_child_id or hedge_child_id == "fired":
            log.warning(
                "hedge.close.no_child_trade",
                parent_trade_id=parent.id, symbol=symbol,
                hedge_trade_id=hedge_child_id,
            )
            # Still sweep leftover orders + send Telegram so the
            # operator sees something.
            hedge_child_id = None

        # PnL on the hedge leg.
        hedge_entry = parent.hedge_entry_price or 0.0
        hedge_qty = 0.0
        try:
            # The hedge child's quantity was saved at fill time.
            hedge_pnl_pct = None
            hedge_pnl_usdt = None
            if hedge_entry > 0 and exit_price > 0:
                if hedge_dir == "LONG":
                    hedge_pnl_pct = (
                        (exit_price - hedge_entry) / hedge_entry * 100.0
                    )
                else:
                    hedge_pnl_pct = (
                        (hedge_entry - exit_price) / hedge_entry * 100.0
                    )
        except Exception:
            hedge_pnl_pct = None
            hedge_pnl_usdt = None

        # Mark the child trade row CLOSED in DB.
        if hedge_child_id is not None:
            try:
                from datetime import datetime as _dt
                from datetime import timezone as _tz
                await self._db.update_trade(
                    int(hedge_child_id),
                    state=TradeState.CLOSED.value,
                    close_reason=reason,
                    pnl_pct=(
                        round(hedge_pnl_pct, 4)
                        if hedge_pnl_pct is not None else None
                    ),
                    closed_at=_dt.now(_tz.utc).isoformat(),
                )
            except Exception:
                log.exception(
                    "hedge.close.db_update_failed",
                    parent_trade_id=parent.id,
                    hedge_trade_id=hedge_child_id,
                )

        # Sweep any leftover Bybit orders on the hedge slot — without
        # this, the original-trade force-close conditional or the
        # hedge pre-arm leftover would linger as orphan orders
        # (diagnostic 2026-05-02 found 13 such leftovers).
        try:
            leftover = await self._bybit.get_open_orders(symbol=symbol)
            cancelled = 0
            for od in leftover:
                try:
                    o_pidx = int(od.get("positionIdx") or 0)
                except (TypeError, ValueError):
                    continue
                if o_pidx != position_idx:
                    continue
                oid = od.get("orderId")
                if not oid:
                    continue
                try:
                    await self._bybit.cancel_order(symbol=symbol, order_id=oid)
                    cancelled += 1
                except Exception:
                    log.exception(
                        "hedge.close.order_cancel_failed",
                        symbol=symbol, order_id=oid,
                    )
            if cancelled:
                log.info(
                    "hedge.close.leftover_orders_swept",
                    parent_trade_id=parent.id, symbol=symbol,
                    position_idx=position_idx, cancelled=cancelled,
                )
        except Exception:
            log.exception(
                "hedge.close.order_sweep_failed",
                parent_trade_id=parent.id, symbol=symbol,
            )

        log.info(
            "hedge.close.completed",
            parent_trade_id=parent.id, symbol=symbol,
            hedge_trade_id=hedge_child_id,
            reason=reason,
            hedge_entry=hedge_entry,
            exit_price=exit_price,
            hedge_pnl_pct=(
                round(hedge_pnl_pct, 4)
                if hedge_pnl_pct is not None else None
            ),
        )

        # Telegram — Bybit-verified hedge close.
        try:
            from telegram.notifier import _chan, _sym, _ts
            chan = _chan(parent.signal.channel_name)
            reason_label = {
                "hedge_stop_loss":     "Hard SL (-2%)",
                "hedge_trailing_stop": "Trailing 1.2% (primär exit)",
                "hedge_liquidation":   "Likvidation",
                "hedge_external_close": "Manuell stängning på Bybit",
            }.get(reason, reason)
            pnl_str = (
                f"{hedge_pnl_pct:+.2f} %"
                if hedge_pnl_pct is not None else "?"
            )
            await self._safe_notify(
                f"🛡️ HEDGE STÄNGD\n"
                f"🕒 Tid: {_ts()}\n"
                f"📢 Från kanal: {chan}\n"
                f"📊 Symbol: {_sym(symbol)}\n"
                f"📈 Riktning (hedge): {hedge_dir} (motrikt mot {parent_dir})\n"
                f"\n"
                f"💥 Hedge entry: {hedge_entry}\n"
                f"💥 Exit: {exit_price}\n"
                f"📊 Resultat: {pnl_str}\n"
                f"\n"
                f"📍 Skäl: {reason_label}\n"
                f"🔑 Order-ID BOT (parent): {parent.id}\n"
                f"🔑 Order-ID BOT (hedge): {hedge_child_id or 'N/A'}"
            )
        except Exception:
            log.exception(
                "hedge.close.notify_failed",
                parent_trade_id=parent.id, symbol=symbol,
            )

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

        # Phase 5 (client 2026-05-01) — TP-cascade SL movement.
        # TP2 hit -> SL=BE, TP3 -> SL=TP1, TP4 -> SL=TP2, etc.
        # Single function path; runs only after the TP fill is
        # acknowledged by Bybit.
        try:
            await self._maybe_cascade_sl_on_tp_hit(trade, tp_level, tp_list)
        except Exception:
            log.exception(
                "trade.sl_cascade_failed",
                trade_id=trade.id, tp_level=tp_level,
            )

    # ------------------------------------------------------------------
    # Phase 5: SL-management (single function, Bybit-verified, dedup)
    # ------------------------------------------------------------------

    async def _move_sl_to(
        self,
        trade: "Trade",
        new_sl_price: float,
        target_state: TradeState,
        reason: str,
    ) -> bool:
        """Single canonical path for moving an active position's SL.

        Client 2026-05-01 audit point #4: "There must be one single
        SL-management function and one state-machine transition path."
        Every BE / profit-lock / TP-cascade SL move goes through here.

        Steps:
          1. Validate the new SL is on the correct side of entry (LONG
             SL must be below entry; SHORT SL above) — defence against
             a logic error trying to set the SL to a price that would
             instantly trigger.
          2. Skip if the new SL is not strictly better than the current
             SL (LONG: new > current; SHORT: new < current). SL never
             moves backwards.
          3. Call set_trading_stop with sl_trigger_by="LastPrice"
             (Tomas's Phase 5 spec: all SL movement uses Last).
          4. On Bybit ack: update trade.sl_price, transition trade
             state, append to sl_movement_history, fire one Telegram.

        Returns True when the move went through, False when skipped /
        rejected.
        """
        if trade.signal is None or trade.is_terminal:
            return False
        symbol = trade.signal.symbol
        direction = (trade.signal.direction or "").upper()
        position_idx = 1 if direction == "LONG" else 2
        avg_entry = trade.avg_entry or trade.signal.entry or 0
        if avg_entry <= 0 or new_sl_price <= 0:
            return False

        # Side validation.
        if direction == "LONG" and new_sl_price >= avg_entry * 1.5:
            log.warning(
                "trade.sl_move_rejected.too_high_for_long",
                trade_id=trade.id, symbol=symbol,
                new_sl=new_sl_price, avg_entry=avg_entry,
            )
            return False
        if direction == "SHORT" and new_sl_price <= avg_entry * 0.5:
            log.warning(
                "trade.sl_move_rejected.too_low_for_short",
                trade_id=trade.id, symbol=symbol,
                new_sl=new_sl_price, avg_entry=avg_entry,
            )
            return False

        # Never move backwards.
        current_sl = trade.sl_price
        if current_sl is not None and current_sl > 0:
            if direction == "LONG" and new_sl_price <= current_sl:
                return False
            if direction == "SHORT" and new_sl_price >= current_sl:
                return False

        # Bybit call. set_trading_stop is idempotent on the Bybit side
        # (returns 34040 if unchanged, swallowed by the adapter).
        try:
            await self._bybit.set_trading_stop(
                symbol=symbol,
                position_idx=position_idx,
                stop_loss=round(new_sl_price, 8),
                sl_trigger_by="LastPrice",
            )
        except Exception:
            log.exception(
                "trade.sl_move_failed",
                trade_id=trade.id, symbol=symbol,
                new_sl=new_sl_price, reason=reason,
            )
            return False

        # Bybit ack received — update state.
        from datetime import datetime as _dt
        from datetime import timezone as _tz
        old_sl = current_sl
        trade.sl_price = round(new_sl_price, 8)
        trade.sl_movement_history.append({
            "ts": _dt.now(_tz.utc).isoformat(),
            "from_sl": old_sl,
            "to_sl": trade.sl_price,
            "reason": reason,
            "to_state": target_state.value,
        })
        # State transition is best-effort — already-deeper states will
        # block via the matrix and that's fine; the SL move itself is
        # the authoritative action.
        try:
            trade.transition(target_state)
        except Exception:
            pass

        log.info(
            "trade.sl_moved",
            trade_id=trade.id, symbol=symbol,
            from_sl=old_sl, to_sl=trade.sl_price,
            reason=reason, state=trade.state.value,
        )

        try:
            await self._db.update_trade(
                int(trade.id),
                sl_price=trade.sl_price,
                state=trade.state.value,
            )
        except Exception:
            pass
        try:
            await self._db.log_event(
                trade_id=int(trade.id),
                event_type="sl_moved",
                details={
                    "from_sl": old_sl,
                    "to_sl": trade.sl_price,
                    "reason": reason,
                    "state": trade.state.value,
                },
            )
        except Exception:
            pass

        # Telegram — only after Bybit ack (audit #6: Bybit-verified
        # before any Telegram message).
        try:
            await self._notifier.sl_moved(
                trade=trade,
                old_sl=old_sl,
                new_sl=trade.sl_price,
                reason=reason,
            )
        except Exception:
            log.exception(
                "notify.sl_moved_failed",
                trade_id=trade.id, symbol=symbol,
            )

        return True

    async def _maybe_cascade_sl_on_tp_hit(
        self,
        trade: "Trade",
        tp_level: int,
        tp_list: list,
    ) -> None:
        """TP-cascade SL movement (client 2026-05-01 spec, confirmed
        via Tomas Telegram 2026-05-01 22:58 "tp2= move to sl, tp3
        move sl tp tp1, and so on, this is fallback").

        Rules:
          TP2 hit -> SL = entry (BE)
          TP3 hit -> SL = TP1
          TP4 hit -> SL = TP2
          TP5 hit -> SL = TP3
          TP_n hit (n >= 2) -> SL = TP_{n-2} (or entry if n == 2)

        Only fires when ``tp_level >= 2`` and the cascade for that
        level hasn't already been applied (idempotent via
        ``trade.sl_moved_to_tp_index`` and ``trade.sl_moved_to_be``).
        """
        if tp_level < 2 or trade.signal is None:
            return
        avg_entry = trade.avg_entry or trade.signal.entry or 0
        if avg_entry <= 0:
            return

        if tp_level == 2:
            if trade.sl_moved_to_be:
                return
            new_sl = avg_entry
            target_state = TradeState.BREAKEVEN_ACTIVE
            reason = "tp2_hit_sl_to_breakeven"
            applied = await self._move_sl_to(
                trade, new_sl, target_state, reason,
            )
            if applied:
                trade.sl_moved_to_be = True
            return

        # tp_level >= 3 — SL moves to TP_{level-2} (1-indexed in spec,
        # 0-indexed in tp_list).
        target_tp_idx = tp_level - 2  # 1-indexed
        if target_tp_idx <= trade.sl_moved_to_tp_index:
            return  # already moved to this or further
        target_tp_zero_idx = target_tp_idx - 1
        if target_tp_zero_idx < 0 or target_tp_zero_idx >= len(tp_list):
            return
        new_sl = tp_list[target_tp_zero_idx]
        if not new_sl or new_sl <= 0:
            return

        # State name: SL_MOVED_TO_TP1..TP4 are defined in the enum.
        # Beyond TP4 we still execute the SL move but skip the named
        # transition (state stays at SL_MOVED_TO_TP4).
        state_name = f"SL_MOVED_TO_TP{target_tp_idx}"
        target_state = getattr(
            TradeState, state_name, TradeState.SL_MOVED_TO_TP4,
        )
        reason = f"tp{tp_level}_hit_sl_to_tp{target_tp_idx}"
        applied = await self._move_sl_to(
            trade, float(new_sl), target_state, reason,
        )
        if applied:
            trade.sl_moved_to_tp_index = target_tp_idx

    async def _maybe_apply_profit_locks(
        self,
        trade: "Trade",
        last_price: float,
    ) -> None:
        """Profit-lock SL moves — FALLBACK ONLY when TP-cascade isn't
        available (client 2026-05-02 clarification: "Move SL based on
        TP levels first. As a fallback, if TP levels are not
        available, use fixed thresholds: 4%, 5%, and 2%, or 2% to
        break-even plus buffer. Both should not apply at the same
        time.").

        Mutual-exclusion rule: if the trade has at least one
        partial-TP conditional placed on Bybit, the TP-cascade is the
        SL-management path for this trade and profit-lock stays off.
        Profit-lock fires only for trades where no TP orders made it
        to Bybit (signal had no TPs, all TPs were merged into trailing,
        all were below the 2% min-distance, or a direction mismatch).

        Fallback ladder when active:
          +2 % favorable -> SL = entry +/- 0.2 % (BE + small buffer)
          +4 % favorable -> SL = entry +/- 1.5 %
          +5 % favorable -> SL = entry +/- 2.5 %

        Each step idempotent via flags on the trade. Higher steps
        override lower ones (5% implies 4% implies 2% were already
        reached and the SL keeps marching forward — never backwards).
        """
        if trade.signal is None or trade.is_terminal:
            return

        # Mutual exclusion: cascade-only when TPs are on Bybit.
        if trade.tp_order_ids:
            return

        avg_entry = trade.avg_entry or trade.signal.entry or 0
        if avg_entry <= 0 or last_price <= 0:
            return
        direction = (trade.signal.direction or "").upper()
        if direction == "LONG":
            favorable_pct = (last_price - avg_entry) / avg_entry * 100.0
        else:
            favorable_pct = (avg_entry - last_price) / avg_entry * 100.0
        if favorable_pct <= 0:
            return

        # FALLBACK STEP 0: +2% trade movement -> SL = entry +/- 0.2%
        # (BE + small buffer for fees/slippage). Reuses sl_moved_to_be
        # so the cascade BE flag also blocks this if the cascade ever
        # somehow ran (defence against future code drift).
        if favorable_pct >= 2.0 and not trade.sl_moved_to_be:
            buffer_pct = 0.002  # 0.2% buffer past entry
            if direction == "LONG":
                new_sl = avg_entry * (1 + buffer_pct)
            else:
                new_sl = avg_entry * (1 - buffer_pct)
            applied = await self._move_sl_to(
                trade,
                new_sl,
                TradeState.BREAKEVEN_ACTIVE,
                "fallback_be_buffer_at_2pct",
            )
            if applied:
                trade.sl_moved_to_be = True

        # FALLBACK STEP 1: +4% trade movement -> SL at entry +/- 1.5%
        if favorable_pct >= 4.0 and not trade.profit_lock_1_active:
            if direction == "LONG":
                new_sl = avg_entry * 1.015
            else:
                new_sl = avg_entry * 0.985
            applied = await self._move_sl_to(
                trade,
                new_sl,
                TradeState.PROFIT_LOCK_1_ACTIVE,
                "fallback_profit_lock_1_at_4pct",
            )
            if applied:
                trade.profit_lock_1_active = True

        # FALLBACK STEP 2: +5% trade movement -> SL at entry +/- 2.5%
        if favorable_pct >= 5.0 and not trade.profit_lock_2_active:
            if direction == "LONG":
                new_sl = avg_entry * 1.025
            else:
                new_sl = avg_entry * 0.975
            applied = await self._move_sl_to(
                trade,
                new_sl,
                TradeState.PROFIT_LOCK_2_ACTIVE,
                "fallback_profit_lock_2_at_5pct",
            )
            if applied:
                trade.profit_lock_2_active = True

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
            # Phase 4 (client 2026-05-01) — explicit TRAILING_ACTIVE
            # state once Bybit's MarkPrice crosses activation. The
            # state-machine transition is best-effort: if the trade is
            # already in a deeper state (e.g. HEDGE_ACTIVE), the matrix
            # will reject it and warn — that's fine, the activation is
            # still reflected on the trade via trailing_activated_notified.
            try:
                trade.transition(TradeState.TRAILING_ACTIVE)
            except Exception:
                pass
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
            # Phase 4 — explicit TRAILING_UPDATED state on every
            # Bybit-driven trailing-SL move. Self-loops (multiple
            # consecutive updates) are allowed by the matrix.
            try:
                trade.transition(TradeState.TRAILING_UPDATED)
            except Exception:
                pass
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
            # 2026-05-02 hedge-fill detection bug fix: when a hedge
            # CONDITIONAL order fires, Bybit sends order_update
            # status=Triggered + execution_update execType=Trade —
            # but no order_update with status=Filled. The hedge
            # detection in on_order_update is gated on Filled, so
            # it never fires for conditional patterns. Result:
            # hedge position lives on Bybit but the bot doesn't
            # know it fired -> reverse_reconcile labels it as
            # OBEVAKAD POSITION every cycle (TRBUSDT incident
            # 2026-05-02 22:04 CEST).
            #
            # Fix: also try the hedge-activation path here. The
            # _maybe_activate_hedge_from_fill function is
            # idempotent — it bails out if the parent already has
            # hedge_trade_id set, so calling it from both
            # on_order_update AND on_execution_update is safe.
            #
            # We pass the execution `data` directly; it contains
            # symbol, side, execPrice, execQty fields that the
            # handler reads (it falls back to triggerPrice if
            # avgPrice is missing, which it always is on
            # execution events).
            #
            # Adapt the data shape: the handler reads
            # data.get("avgPrice") and data.get("cumExecQty"),
            # but execution events use execPrice/execQty. Build a
            # compatible dict.
            try:
                fill_view = dict(data)
                fill_view.setdefault("avgPrice", data.get("execPrice"))
                fill_view.setdefault("cumExecQty", data.get("execQty"))
                await self._maybe_activate_hedge_from_fill(
                    order_id, fill_view,
                )
            except Exception:
                log.exception(
                    "on_execution.hedge_fill_detection_failed",
                    order_id=order_id, symbol=symbol,
                )

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
    # Orphan adoption — REMOVED in Phase 3 (client 2026-05-01 audit).
    # ==================================================================
    #
    # The previous adopt_orphan_position() flow synthesised a fake
    # signal and adopted untracked Bybit positions. That was a
    # fallback path Tomas explicitly forbade ("no fallback magic, no
    # auto-correct"). Hedge fills are now handled atomically by
    # _maybe_activate_hedge_from_fill() in the WS handler — single
    # path, Bybit-verified. Truly untracked positions (e.g. operator
    # opened manually on the Bybit UI) are surfaced as deduped
    # warnings by main._periodic_reverse_reconciliation; the bot
    # does not adopt or close them.

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

        # --- Original trade -2% force-close: handled by the Bybit
        # conditional armed at trade open (Phase 2, client 2026-05-01).
        # NO bot-side polling here. When the conditional fires on
        # Bybit, on_order_update -> close_trade picks up the fill.
        # This satisfies "single path per function": one place arms
        # the conditional (process_signal), one place reacts to it
        # (on_order_update -> close_trade), no duplicate logic.

        # --- Hedge: bot-side check_and_activate is DISABLED 2026-04-30.
        # The Bybit pre-arm conditional placed at trade open is the
        # single source of truth for hedge opening.

        # --- Phase 5 profit-lock (client 2026-05-01 spec):
        #     +4% favorable -> SL = entry +/- 1.5%
        #     +5% favorable -> SL = entry +/- 2.5%
        # Single SL-management function (_move_sl_to) handles both,
        # idempotent via flags on the trade.
        await self._maybe_apply_profit_locks(trade, current_price)

        # --- Hedge timeout: close hedge if no meaningful move in
        # configured window (default 20 min, threshold 0.5%). The
        # parent trade carries hedge_filled_at + hedge_entry_price.
        await self._maybe_timeout_hedge(trade, current_price)

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

        # --- Phase 6 audit-snapshot trigger (client 2026-05-02
        # audit #11 + #26). Increment the post-close counter and, if
        # we've reached the configured threshold, fire the snapshot
        # to Telegram. Failures here are NEVER allowed to break the
        # close path (audit reporting must not affect trading). ---
        try:
            every_n = int(getattr(
                self._settings.reporting,
                "audit_snapshot_every_n_trades",
                0,
            ) or 0)
            if every_n > 0:
                self._closes_since_snapshot += 1
                if self._closes_since_snapshot >= every_n:
                    self._closes_since_snapshot = 0
                    await self._fire_audit_snapshot(every_n)
        except Exception:
            log.exception(
                "close_trade.audit_snapshot_trigger_failed",
                trade_id=trade.id,
            )

    async def _fire_audit_snapshot(self, window_n: int) -> None:
        """Collect + post the periodic audit snapshot.

        Single function path (audit #4: "There must be only one
        function and one execution path"). Never raises; failures
        are logged but the trading flow continues unaffected.
        """
        try:
            from health.audit_snapshot import (
                collect_snapshot, render_snapshot_text,
            )
            db_path = getattr(self._db, "db_path", None) or (
                self._settings.general.db_path
            )
            snap = await collect_snapshot(
                db_path=str(db_path),
                bybit_adapter=self._bybit,
                window_n=window_n,
            )
            text = render_snapshot_text(snap)
            await self._notifier.audit_snapshot(text)
            log.info(
                "audit_snapshot.posted",
                window_n=window_n,
                bybit_positions=snap.bybit_position_count,
                db_active=snap.db_active_count,
                drift=snap.drift,
                leftover=snap.leftover_orders,
                unprotected=snap.unprotected_positions,
                total_pnl_window=snap.total_pnl,
            )
        except Exception:
            log.exception("audit_snapshot.fire_failed", window_n=window_n)

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

        from telegram.notifier import _ts
        await self._safe_notify(
            f"🧹 ORDERSTÄDNING UTFÖRD\n"
            f"🕒 Tid: {_ts()}\n"
            f"📍 Antal ofyllda order raderade: {len(stale_orders)}\n"
            f"📍 Tröskel: äldre än {timeout_hours} timmar."
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

        from telegram.notifier import _chan, _ts
        chan = (
            _chan(trade.signal.channel_name)
            if trade.signal else "#Unknown"
        )
        await self._safe_notify(
            f"❌ TRADE AVBRUTEN\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {chan}\n"
            f"📊 Symbol: #{symbol}\n"
            f"📈 Riktning: {direction}\n"
            f"📍 Anledning: {message}"
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
            # Client 2026-04-30: dedupe by (symbol, direction) so 3
            # channels echoing the same signal don't fire 3 identical
            # ❌ TP/SL UPPDATERING MISSLYCKADES messages.
            if self._should_send_reject_notify(
                "tp_sl_update_failed", symbol, direction,
            ):
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
                        from telegram.notifier import _chan, _ts
                        chan = _chan(getattr(signal, "channel_name", ""))
                        await self._safe_notify(
                            f"⚠️ LIKVIDATIONSVARNING ⚠️\n"
                            f"🕒 Tid: {_ts()}\n"
                            f"📢 Från kanal: {chan}\n"
                            f"📊 Symbol: #{symbol}\n"
                            f"📈 Riktning: {direction}\n"
                            f"📍 Likvidationspris: {liq_price}\n"
                            f"📍 Marknadspris: {mark_price}\n"
                            f"📍 Avstånd: {liq_distance_pct:.2f}%\n"
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
