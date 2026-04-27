"""
Stratos1 - Health Checker & Watchdog
--------------------------------------
Performs startup system health checks, reconciles state on restart, and
runs periodic monitoring to detect and recover from connection failures,
stuck orders, and other runtime issues.

Also includes the OrderLoopProtector: a per-symbol rate limiter that
prevents runaway order placement caused by logic bugs or exchange
feedback loops.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import structlog

if TYPE_CHECKING:
    from config.settings import AppSettings
    from exchange.bybit_adapter import BybitAdapter
    from persistence.database import Database
    from telegram.listener import TelegramListener
    from telegram.notifier import TelegramNotifier

log = structlog.get_logger(__name__)

# Version identifier used in health-check error reports.
_BOT_VERSION = "1.0.0"
_BOT_ENV = "production"


# ===================================================================
# OrderLoopProtector
# ===================================================================

class OrderLoopProtector:
    """
    Per-symbol rate limiter that prevents runaway order placement.

    Tracks the number of orders placed for each symbol within a sliding
    time window.  If the count exceeds ``max_count``, subsequent orders
    for that symbol are blocked until the window expires.

    Parameters
    ----------
    max_count:
        Maximum number of orders allowed per symbol within the window.
    window_seconds:
        Length of the sliding time window in seconds.
    """

    def __init__(
        self,
        max_count: int = 10,
        window_seconds: int = 60,
    ) -> None:
        self._max_count = max_count
        self._window_seconds = window_seconds
        # symbol -> list of timestamps (order placement times)
        self._orders: Dict[str, list[float]] = defaultdict(list)
        # symbol -> block-expiry timestamp (0 = not blocked)
        self._blocked_until: Dict[str, float] = defaultdict(float)

    def check(self, symbol: str) -> bool:
        """
        Record an order attempt for *symbol* and return True if it is
        allowed, False if blocked by the rate limiter.

        Call this BEFORE placing each order.  If it returns False, the
        order must not be sent.
        """
        now = time.monotonic()

        # If the symbol is currently blocked, check whether the block
        # has expired.
        if self._blocked_until[symbol] > now:
            log.warning(
                "order_loop.blocked",
                symbol=symbol,
                remaining=round(self._blocked_until[symbol] - now, 1),
            )
            return False

        # Remove expired timestamps outside the window.
        self._cleanup_symbol(symbol, now)

        # Record the new order attempt.
        self._orders[symbol].append(now)

        # Check if the count exceeds the limit.
        if len(self._orders[symbol]) > self._max_count:
            # Block the symbol for the duration of one window.
            self._blocked_until[symbol] = now + self._window_seconds
            log.error(
                "order_loop.protection_triggered",
                symbol=symbol,
                count=len(self._orders[symbol]),
                max_count=self._max_count,
                block_seconds=self._window_seconds,
            )
            return False

        return True

    def _cleanup_symbol(self, symbol: str, now: float) -> None:
        """Remove timestamps older than the sliding window for a single symbol."""
        cutoff = now - self._window_seconds
        self._orders[symbol] = [
            ts for ts in self._orders[symbol] if ts > cutoff
        ]

    def _cleanup(self) -> None:
        """Remove all expired entries across every symbol."""
        now = time.monotonic()
        symbols_to_remove: list[str] = []

        for symbol in list(self._orders.keys()):
            self._cleanup_symbol(symbol, now)
            # If both the order list and block have expired, remove.
            if (
                not self._orders[symbol]
                and self._blocked_until.get(symbol, 0) <= now
            ):
                symbols_to_remove.append(symbol)

        for symbol in symbols_to_remove:
            del self._orders[symbol]
            self._blocked_until.pop(symbol, None)


# ===================================================================
# HealthChecker
# ===================================================================

class HealthChecker:
    """
    Performs component health checks at startup and periodically during
    runtime.  Handles state recovery after a restart by reconciling the
    database with live exchange positions.

    Parameters
    ----------
    settings:
        Full ``AppSettings`` object.
    db:
        Async SQLite database instance.
    bybit:
        Exchange adapter (``BybitAdapter``).
    tg_listener:
        Telegram user-session listener.
    tg_notifier:
        Telegram bot notifier.
    """

    def __init__(
        self,
        settings: "AppSettings",
        db: "Database",
        bybit: "BybitAdapter",
        tg_listener: "TelegramListener",
        tg_notifier: "TelegramNotifier",
    ) -> None:
        self._settings = settings
        self._db = db
        self._bybit = bybit
        self._tg_listener = tg_listener
        self._tg_notifier = tg_notifier

        # Unique identifiers for this session (used in error reports).
        self._session_id = uuid.uuid4().hex[:12]
        self._trace_id = uuid.uuid4().hex[:12]

    # ------------------------------------------------------------------
    # Startup health checks
    # ------------------------------------------------------------------

    async def run_startup_checks(
        self,
    ) -> List[Tuple[str, bool, str]]:
        """
        Execute all startup health checks and return a summary list.

        Each entry is a tuple of ``(check_name, passed, message)``.
        If any critical check fails, an ``error_health_check``
        notification is sent via Telegram.

        Returns
        -------
        list of (str, bool, str)
            Results for every check executed.
        """
        results: List[Tuple[str, bool, str]] = []

        # 1. Bybit API connectivity
        results.append(await self._check_bybit_connectivity())

        # 2. Bybit account mode (hedge capability)
        results.append(await self._check_bybit_account_mode())

        # 3. Telegram user session
        results.append(await self._check_telegram_session())

        # 4. Telegram bot messaging
        results.append(await self._check_telegram_bot())

        # 5. Database
        results.append(await self._check_database())

        # 6. NTP / time sync
        results.append(await self._check_time_sync())

        # Log and notify on failures.
        failed = [(name, msg) for name, ok, msg in results if not ok]

        if failed:
            for check_name, error_msg in failed:
                log.error(
                    "startup_check_failed",
                    check=check_name,
                    error=error_msg,
                )
                try:
                    await self._tg_notifier.error_health_check(
                        component="Stratos1",
                        check_name=check_name,
                        error_msg=error_msg,
                        env=_BOT_ENV,
                        version=_BOT_VERSION,
                        trace_id=self._trace_id,
                        session_id=self._session_id,
                    )
                except Exception:
                    log.exception(
                        "health_check_notify_failed",
                        check=check_name,
                    )
        else:
            log.info(
                "startup_checks_all_passed",
                total=len(results),
                session_id=self._session_id,
            )

        return results

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    async def _check_bybit_connectivity(
        self,
    ) -> Tuple[str, bool, str]:
        """Verify Bybit REST API is reachable by fetching wallet balance."""
        name = "Bybit API Connectivity"
        try:
            balance = await self._bybit.get_wallet_balance()
            equity_raw = balance.get("totalEquity", "0") or "0"
            equity = float(equity_raw) if equity_raw else 0.0
            msg = f"Connected. Equity: {equity:.2f} USDT"
            log.info("health.bybit_connected", equity=equity)
            return (name, True, msg)
        except Exception as exc:
            # Try a simpler check - just get ticker
            try:
                ticker = await self._bybit.get_ticker("BTCUSDT")
                if ticker:
                    price = ticker.get("lastPrice", "?")
                    msg = f"Connected (ticker OK). BTCUSDT: {price}"
                    log.info("health.bybit_connected_via_ticker", price=price)
                    return (name, True, msg)
            except Exception:
                pass
            msg = f"Failed to connect to Bybit API: {exc}"
            return (name, False, msg)

    async def _check_bybit_account_mode(
        self,
    ) -> Tuple[str, bool, str]:
        """Verify the account supports hedge-mode positions."""
        name = "Bybit Account Mode (Hedge)"
        try:
            await self._bybit.setup_hedge_mode("BTCUSDT")
            msg = "Hedge mode verified on BTCUSDT"
            log.info("health.hedge_mode_ok")
            return (name, True, msg)
        except Exception as exc:
            err_str = str(exc)
            # Error 110025 means "Position mode is not modified" which
            # means hedge mode is ALREADY active - that's a PASS.
            if "110025" in err_str or "not modified" in err_str.lower():
                msg = "Hedge mode already active on BTCUSDT"
                log.info("health.hedge_mode_already_active")
                return (name, True, msg)
            msg = f"Hedge mode check failed: {exc}"
            return (name, False, msg)

    async def _check_telegram_session(
        self,
    ) -> Tuple[str, bool, str]:
        """Verify the Telegram user session is connected and authorised."""
        name = "Telegram User Session"
        try:
            client = self._tg_listener._client
            if client is None:
                return (name, False, "Telethon client is None (not started)")

            if not client.is_connected():
                return (name, False, "Telethon client not connected")

            me = await client.get_me()
            if me is None:
                return (name, False, "User session is not authorised")

            msg = f"Authenticated as {me.username or me.id}"
            log.info("health.telegram_session_ok", user=me.username)
            return (name, True, msg)
        except Exception as exc:
            return (name, False, f"Telegram session check failed: {exc}")

    async def _check_telegram_bot(
        self,
    ) -> Tuple[str, bool, str]:
        """Verify the Telegram bot can send messages."""
        name = "Telegram Bot Messaging"
        try:
            client = self._tg_notifier._client
            if client is None:
                return (name, False, "Bot client is None (not started)")

            if not client.is_connected():
                return (name, False, "Bot client not connected")

            me = await client.get_me()
            if me is None:
                return (name, False, "Bot is not authenticated")

            msg = f"Bot ready: @{me.username}"
            log.info("health.telegram_bot_ok", bot=me.username)
            return (name, True, msg)
        except Exception as exc:
            return (name, False, f"Bot check failed: {exc}")

    async def _check_database(
        self,
    ) -> Tuple[str, bool, str]:
        """Verify database is accessible by running a simple query."""
        name = "Database"
        try:
            # Use the internal connection to run a lightweight probe.
            cursor = await self._db._conn.execute("SELECT 1")
            row = await cursor.fetchone()
            if row is None:
                return (name, False, "SELECT 1 returned no rows")

            msg = f"Database accessible at {self._db.db_path}"
            log.info("health.database_ok", path=self._db.db_path)
            return (name, True, msg)
        except Exception as exc:
            return (name, False, f"Database check failed: {exc}")

    async def _check_time_sync(
        self,
    ) -> Tuple[str, bool, str]:
        """
        Check that local system time is reasonably close to exchange time.

        Compares local UTC with the Bybit server time (derived from
        REST response).  Allows up to 30 seconds of drift before
        flagging a warning.
        """
        name = "NTP / Time Sync"
        try:
            # Use a lightweight call that returns quickly.
            local_ts = time.time()
            resp = await self._bybit._call_with_retry(
                self._bybit._rest.get_server_time,
            )
            server_ts_ms = int(
                resp.get("result", {}).get("timeSecond", "0")
            )
            # Bybit returns seconds as string in timeSecond.
            if server_ts_ms == 0:
                # Fallback: try timeNano field.
                server_ts_ns = int(
                    resp.get("result", {}).get("timeNano", "0")
                )
                server_ts = server_ts_ns / 1_000_000_000
            else:
                server_ts = float(server_ts_ms)

            drift = abs(local_ts - server_ts)

            # If the Bybit adapter has a clock patch applied, the
            # actual API calls use corrected timestamps. Report the
            # drift as informational but don't fail the check.
            clock_patched = getattr(self._bybit, "_time_offset", 0) != 0

            if drift > 30 and not clock_patched:
                msg = (
                    f"Clock drift is {drift:.1f}s (max 30s). "
                    f"Check NTP service."
                )
                log.warning("health.time_drift_high", drift=drift)
                return (name, False, msg)

            if drift > 30 and clock_patched:
                msg = (
                    f"Clock drift is {drift:.1f}s but auto-corrected "
                    f"(offset patch active: {self._bybit._time_offset}s)"
                )
                log.info("health.time_drift_patched", drift=round(drift, 1),
                         offset=self._bybit._time_offset)
                return (name, True, msg)

            msg = f"Time sync OK (drift: {drift:.1f}s)"
            log.info("health.time_sync_ok", drift=round(drift, 2))
            return (name, True, msg)
        except Exception as exc:
            # Time sync is non-critical; warn but don't block startup.
            msg = f"Could not verify time sync: {exc}"
            log.warning("health.time_sync_error", error=str(exc))
            return (name, True, msg)

    # ------------------------------------------------------------------
    # State recovery after restart
    # ------------------------------------------------------------------

    async def recover_state(
        self,
        position_manager: Any,
    ) -> Dict[str, int]:
        """
        Reconcile the database with live exchange positions after a
        restart.

        Steps:
        1. Load all active trades from DB.
        2. Fetch live positions from Bybit.
        3. For each DB trade, verify the position still exists.
        4. Restore SL/TP/trailing state if missing.
        5. For trades in DB but not on exchange: mark as closed.
        6. For positions on exchange not in DB: log warning.
        7. Send state_restored notification.
        8. Add recovered trades to the position manager.

        Parameters
        ----------
        position_manager:
            ``PositionManager`` instance to receive recovered trades.

        Returns
        -------
        dict
            Summary counts: ``positions_verified``, ``sl_tp_restored``,
            ``orphan_db_closed``, ``orphan_exchange_found``.
        """
        counts = {
            "positions_verified": 0,
            "sl_tp_restored": 0,
            "orphan_db_closed": 0,
            "orphan_exchange_found": 0,
        }

        log.info("state_recovery.started")

        # 1. Load active trades from DB.
        try:
            db_trades = await self._db.get_active_trades()
        except Exception:
            log.exception("state_recovery.db_load_failed")
            return counts

        if not db_trades:
            log.info("state_recovery.no_active_trades")
            try:
                await self._tg_notifier.state_restored(
                    positions_verified=0,
                    sl_tp_restored=0,
                )
            except Exception:
                log.exception("state_recovery.notify_failed")
            return counts

        log.info(
            "state_recovery.loaded_trades",
            count=len(db_trades),
        )

        # 1b. Sweep any "stuck" pre-fill states — PENDING, ENTRY1_PLACED,
        # ENTRY2_PLACED — that have lingered across a restart. These
        # represent trades that never made it to POSITION_OPEN; the
        # entry order either never fired, timed out, or errored and
        # the cleanup path failed. They can't be "recovered" because
        # there's no live position to attach to, so mark them
        # CANCELLED and remove from the active set so the rest of the
        # recovery loop processes only real positions.
        stuck_pre_fill_states = {
            "PENDING", "ENTRY1_PLACED", "ENTRY2_PLACED",
            "ENTRY1_FILLED", "ENTRY2_FILLED",
        }
        pending_cleaned = 0
        filtered_trades = []
        for trade in db_trades:
            state = (trade.get("state") or "").upper()
            if state in stuck_pre_fill_states:
                pending_cleaned += 1
                tid = trade.get("id")
                try:
                    now_iso = datetime.now(timezone.utc).isoformat()
                    await self._db.update_trade(
                        tid,
                        state="CANCELLED",
                        close_reason="stuck_pre_fill_on_restart",
                        closed_at=now_iso,
                    )
                    await self._db.log_event(
                        trade_id=tid,
                        event_type="stuck_pre_fill_cancelled",
                        details={"previous_state": state},
                    )
                    log.warning(
                        "state_recovery.stuck_pre_fill_cancelled",
                        trade_id=tid,
                        previous_state=state,
                    )
                except Exception:
                    log.exception(
                        "state_recovery.pre_fill_cleanup_failed",
                        trade_id=tid,
                    )
            else:
                filtered_trades.append(trade)
        db_trades = filtered_trades
        if pending_cleaned:
            counts["stuck_pre_fill_cleaned"] = pending_cleaned
            log.info(
                "state_recovery.stuck_pre_fill_sweep",
                cleaned=pending_cleaned,
                remaining=len(db_trades),
            )

        # 2. Build a set of live exchange positions (keyed by symbol+side).
        live_positions: Dict[str, dict] = {}
        symbols_seen: set[str] = set()

        for trade in db_trades:
            # Resolve the symbol from the signal.
            signal_id = trade.get("signal_id")
            if signal_id is None:
                continue

            # We need the symbol -- fetch from DB if not embedded.
            symbol = trade.get("symbol")
            if not symbol and signal_id:
                try:
                    cursor = await self._db._conn.execute(
                        "SELECT symbol FROM signals WHERE id = ?",
                        (signal_id,),
                    )
                    row = await cursor.fetchone()
                    symbol = dict(row)["symbol"] if row else None
                except Exception:
                    log.exception(
                        "state_recovery.symbol_lookup_failed",
                        signal_id=signal_id,
                    )
                    continue

            if not symbol:
                continue

            if symbol not in symbols_seen:
                symbols_seen.add(symbol)
                # Fetch both buy and sell positions for this symbol.
                for side in ("Buy", "Sell"):
                    try:
                        pos = await self._bybit.get_position(symbol, side)
                        if pos and pos.get("size", 0) > 0:
                            key = f"{symbol}:{side}"
                            live_positions[key] = pos
                    except Exception:
                        log.exception(
                            "state_recovery.position_fetch_failed",
                            symbol=symbol,
                            side=side,
                        )

        # 3. Reconcile each DB trade against live positions.
        for trade in db_trades:
            trade_id = trade.get("id")
            signal_id = trade.get("signal_id")
            state = trade.get("state", "")

            # Resolve symbol and direction.
            symbol = trade.get("symbol")
            if not symbol and signal_id:
                try:
                    cursor = await self._db._conn.execute(
                        "SELECT symbol, direction FROM signals WHERE id = ?",
                        (signal_id,),
                    )
                    row = await cursor.fetchone()
                    if row:
                        row_dict = dict(row)
                        symbol = row_dict.get("symbol")
                        direction = row_dict.get("direction")
                    else:
                        continue
                except Exception:
                    continue
            else:
                # Try to infer direction from state or side fields.
                direction = trade.get("direction")
                if not direction and signal_id:
                    try:
                        cursor = await self._db._conn.execute(
                            "SELECT direction FROM signals WHERE id = ?",
                            (signal_id,),
                        )
                        row = await cursor.fetchone()
                        direction = dict(row)["direction"] if row else None
                    except Exception:
                        direction = None

            if not symbol or not direction:
                continue

            side = "Buy" if direction == "LONG" else "Sell"
            key = f"{symbol}:{side}"

            if key in live_positions:
                # Position exists on exchange -- verified.
                counts["positions_verified"] += 1
                pos = live_positions[key]

                # Check if SL/TP needs restoring.
                sl_on_exchange = pos.get("stopLoss", "")
                tp_on_exchange = pos.get("takeProfit", "")
                sl_in_db = trade.get("sl_price")
                trailing_in_db = trade.get("trailing_sl")

                needs_restore = False
                if sl_in_db and (not sl_on_exchange or sl_on_exchange == "0"):
                    needs_restore = True
                if trailing_in_db and (not sl_on_exchange or sl_on_exchange == "0"):
                    needs_restore = True

                if needs_restore:
                    try:
                        position_idx = 1 if direction == "LONG" else 2
                        sl_to_set = trailing_in_db or sl_in_db
                        await self._bybit.set_trading_stop(
                            symbol=symbol,
                            stop_loss=float(sl_to_set),
                            position_idx=position_idx,
                        )
                        counts["sl_tp_restored"] += 1
                        log.info(
                            "state_recovery.sl_restored",
                            trade_id=trade_id,
                            symbol=symbol,
                            sl=sl_to_set,
                        )
                    except Exception:
                        log.exception(
                            "state_recovery.sl_restore_failed",
                            trade_id=trade_id,
                            symbol=symbol,
                        )

                # Re-hydrate this trade into the position manager's
                # active_trades map so runtime managers (BE, scaling,
                # trailing, hedge, re-entry, max-loss cap) can act on
                # it again. Without this step, _active_trades is empty
                # after every restart and no manager runs on existing
                # trades — the reverse-reconcile loop would even
                # mistake them for orphans and close them.
                try:
                    from core.models import Trade, TradeState, ParsedSignal
                    import json as _json
                    # Build a minimal ParsedSignal from the linked DB
                    # signals row so downstream managers have their
                    # symbol, direction, tps, sl. Anything we can't
                    # rebuild is fine — managers tolerate None.
                    sig_row = None
                    if signal_id is not None:
                        try:
                            cursor = await self._db._conn.execute(
                                "SELECT * FROM signals WHERE id = ?",
                                (signal_id,),
                            )
                            r = await cursor.fetchone()
                            sig_row = dict(r) if r else None
                        except Exception:
                            sig_row = None
                    tps_raw = (sig_row or {}).get("tps", "[]")
                    try:
                        tps = _json.loads(tps_raw) if isinstance(tps_raw, str) else (tps_raw or [])
                    except Exception:
                        tps = []
                    signal_obj = ParsedSignal(
                        symbol=symbol,
                        direction=direction,
                        entry=float((sig_row or {}).get("entry") or trade.get("avg_entry") or 0),
                        tp_list=[float(t) for t in tps if t],
                        sl=float((sig_row or {}).get("sl") or 0) or None,
                        source_channel_id=int((sig_row or {}).get("source_channel_id") or 0),
                        source_channel_name=(sig_row or {}).get("source_channel_name") or "",
                        raw_text=(sig_row or {}).get("raw_text") or "",
                        signal_type=(sig_row or {}).get("signal_type") or "dynamic",
                    )
                    # Restore the Trade itself.
                    state_str = (trade.get("state") or "POSITION_OPEN").upper()
                    try:
                        state_enum = TradeState[state_str]
                    except KeyError:
                        state_enum = TradeState.POSITION_OPEN
                    tp_hits_raw = trade.get("tp_hits") or "[]"
                    try:
                        tp_hits = _json.loads(tp_hits_raw) if isinstance(tp_hits_raw, str) else (tp_hits_raw or [])
                    except Exception:
                        tp_hits = []
                    restored_trade = Trade(
                        signal=signal_obj,
                        state=state_enum,
                        entry1_fill_price=trade.get("entry1_fill_price"),
                        entry2_fill_price=trade.get("entry2_fill_price"),
                        avg_entry=trade.get("avg_entry"),
                        quantity=trade.get("quantity"),
                        leverage=trade.get("leverage"),
                        margin=trade.get("margin"),
                        sl_price=trade.get("sl_price"),
                        be_price=trade.get("be_price"),
                        trailing_sl=trade.get("trailing_sl"),
                        hedge_trade_id=trade.get("hedge_trade_id"),
                        reentry_count=int(trade.get("reentry_count") or 0),
                        scaling_step=int(trade.get("scaling_step") or 0),
                        tp_hits=[float(t) for t in tp_hits if t],
                    )
                    restored_trade.id = str(trade_id)
                    position_manager._active_trades[restored_trade.id] = restored_trade
                    counts.setdefault("trades_rehydrated", 0)
                    counts["trades_rehydrated"] += 1
                except Exception:
                    log.exception(
                        "state_recovery.rehydrate_failed",
                        trade_id=trade_id,
                        symbol=symbol,
                    )

                # Remove from live_positions so we can detect orphans.
                del live_positions[key]

            else:
                # Trade in DB but not in the bulk live_positions snapshot.
                # IMPORTANT (client IZZU 2026-04-27): we no longer mark
                # the trade CLOSED here. Doing so on a racy bulk-fetch
                # response wrongly closed still-open positions in the
                # past, accumulating 48 orphan positions on Bybit. The
                # earlier "re-confirm with extra API calls" approach
                # was correct in principle but made startup glacial
                # (1 trade/sec → 10+ min on a busy DB).
                #
                # Cleaner architecture: state_recovery only LOADS DB
                # trades into memory; it does NOT make close decisions.
                # The runtime forward-reconciliation loop (30s, in
                # main._periodic_position_reconciliation) closes any
                # tracked trade whose position has size=0 on Bybit.
                # The runtime reverse-reconciliation loop (60s, in
                # main._periodic_reverse_reconciliation) closes any
                # Bybit position the bot doesn't track. Together those
                # two loops catch drift in both directions within
                # ~60s of startup, without blocking startup itself.
                counts.setdefault("orphan_close_skipped", 0)
                counts["orphan_close_skipped"] += 1
                log.info(
                    "state_recovery.orphan_check_deferred_to_runtime",
                    trade_id=trade_id,
                    symbol=symbol,
                    direction=direction,
                    state=state,
                )

        # 6. Remaining live_positions entries are exchange positions not
        #    tracked in the DB -- log warnings.
        for key, pos in live_positions.items():
            counts["orphan_exchange_found"] += 1
            log.warning(
                "state_recovery.orphan_exchange_position",
                key=key,
                size=pos.get("size"),
                avg_price=pos.get("avgPrice"),
                unrealised_pnl=pos.get("unrealisedPnl"),
            )

        # 7. Send notification.
        try:
            await self._tg_notifier.state_restored(
                positions_verified=counts["positions_verified"],
                sl_tp_restored=counts["sl_tp_restored"],
            )
        except Exception:
            log.exception("state_recovery.notify_failed")

        log.info("state_recovery.complete", **counts)
        return counts

    # ------------------------------------------------------------------
    # Periodic health check
    # ------------------------------------------------------------------

    async def periodic_health_check(self) -> None:
        """
        Run a lightweight health check every 5 minutes.

        Verifies:
        - WebSocket connections are alive.
        - Telegram client connection is alive.
        - No stuck orders exist.

        If issues are found, attempts automatic recovery.
        """
        log.debug("periodic_health_check.running")

        # 1. Check WebSocket connections.
        if not self._bybit.has_private_ws:
            log.warning("health.private_ws_down")
            try:
                await self._bybit.stop()
                await self._bybit.start()
                log.info("health.bybit_reconnected")
            except Exception:
                log.exception("health.bybit_reconnect_failed")

        if not self._bybit.has_public_ws:
            log.warning("health.public_ws_down")
            # Public WS is non-critical; just log.

        # 2. Check Telegram client.
        try:
            client = self._tg_listener._client
            if client and not client.is_connected():
                log.warning("health.telegram_disconnected")
                try:
                    await client.connect()
                    log.info("health.telegram_reconnected")
                except Exception:
                    log.exception("health.telegram_reconnect_failed")
        except Exception:
            log.exception("health.telegram_check_failed")

        # 3. Check for stuck orders (orders pending > 1 hour).
        try:
            stuck = await self._db.get_unfilled_orders(older_than_hours=1.0)
            if stuck:
                log.warning(
                    "health.stuck_orders_found",
                    count=len(stuck),
                )
                for order in stuck:
                    log.info(
                        "health.stuck_order",
                        order_id_bot=order.get("order_id_bot"),
                        symbol=order.get("symbol"),
                        created_at=order.get("created_at"),
                    )
        except Exception:
            log.exception("health.stuck_order_check_failed")

        log.debug("periodic_health_check.complete")
