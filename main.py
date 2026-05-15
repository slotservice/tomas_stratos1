"""
Stratos1 - Main Bot Orchestrator & Entry Point
------------------------------------------------
Ties every subsystem together: configuration, database, exchange adapter,
Telegram listener + notifier, all trade managers, health checks, and
periodic maintenance tasks.

Run with:
    python main.py

Or as a built executable:
    stratos1.exe
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Optional

import structlog
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Project root (directory containing this file).
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent

# Load .env into os.environ early so all components can read secrets.
load_dotenv(PROJECT_ROOT / ".env", override=True)

# ---------------------------------------------------------------------------
# Structured logging setup
# ---------------------------------------------------------------------------

def _setup_logging(log_level: str = "INFO", log_file: str = "stratos1.log") -> None:
    """
    Configure structlog + stdlib logging with both console and file
    output.  The file handler receives JSON, the console gets coloured
    human-readable output.
    """
    log_level_int = getattr(logging, log_level.upper(), logging.INFO)
    log_path = PROJECT_ROOT / log_file

    # --- stdlib root logger: file handler (JSON) ---
    file_handler = logging.FileHandler(str(log_path), encoding="utf-8")
    file_handler.setLevel(log_level_int)

    # --- stdlib root logger: console handler (human-readable) ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level_int)

    logging.basicConfig(
        format="%(message)s",
        level=log_level_int,
        handlers=[file_handler, console_handler],
        force=True,
    )

    # --- structlog processors ---
    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Attach structlog-aware formatters to both handlers.
    json_formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
    )
    console_formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty()),
        ],
    )

    file_handler.setFormatter(json_formatter)
    console_handler.setFormatter(console_formatter)


log = structlog.get_logger("stratos1.main")


# ---------------------------------------------------------------------------
# Imports (deferred so structlog is configured before any module-level
# logger calls).
# ---------------------------------------------------------------------------

def _import_components():
    """
    Import all subsystems.  Done as a function so we can call it after
    logging is configured, avoiding import-time log messages that bypass
    our formatter.
    """
    # Config
    from config.settings import load_settings

    # Persistence
    from persistence.database import Database

    # Exchange
    from exchange.bybit_adapter import BybitAdapter

    # Telegram
    from telegram.listener import TelegramListener
    from telegram.notifier import TelegramNotifier

    # Core
    from core.duplicate_detector import DuplicateDetector
    from core.signal_parser import parse_signal, parse_signal_detailed

    # Managers
    from managers.position_manager import PositionManager

    # Health
    from health.watchdog import HealthChecker, OrderLoopProtector

    return {
        "load_settings": load_settings,
        "Database": Database,
        "BybitAdapter": BybitAdapter,
        "TelegramListener": TelegramListener,
        "TelegramNotifier": TelegramNotifier,
        "DuplicateDetector": DuplicateDetector,
        "parse_signal": parse_signal,
        "parse_signal_detailed": parse_signal_detailed,
        "PositionManager": PositionManager,
        "HealthChecker": HealthChecker,
        "OrderLoopProtector": OrderLoopProtector,
    }


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

class _GracefulShutdown:
    """
    Coordinates graceful shutdown of all components when the bot
    receives SIGINT (Ctrl+C) or SIGTERM.
    """

    def __init__(self) -> None:
        self._shutdown_event = asyncio.Event()
        self._components: list = []

    def register(self, component) -> None:
        """Register a component that has an async ``stop()`` method."""
        self._components.append(component)

    @property
    def is_shutting_down(self) -> bool:
        return self._shutdown_event.is_set()

    def trigger(self) -> None:
        """Signal the shutdown event."""
        self._shutdown_event.set()

    async def wait(self) -> None:
        """Block until shutdown is triggered."""
        await self._shutdown_event.wait()

    async def shutdown_all(self) -> None:
        """Stop every registered component in reverse order."""
        log.info("shutdown.starting", components=len(self._components))
        for comp in reversed(self._components):
            name = type(comp).__name__
            try:
                log.info("shutdown.stopping", component=name)
                await comp.stop()
                log.info("shutdown.stopped", component=name)
            except Exception:
                log.exception("shutdown.error", component=name)
        log.info("shutdown.complete")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def main() -> None:
    """
    Main entry point.  Initialises every subsystem in the correct order,
    wires up callbacks, starts periodic tasks, and runs until shutdown.
    """
    # ---------------------------------------------------------------
    # 1. Load settings
    # ---------------------------------------------------------------
    # Import early so we know the log level before configuring logging.
    from config.settings import load_settings

    settings = load_settings(PROJECT_ROOT)

    # ---------------------------------------------------------------
    # 2. Structured logging
    # ---------------------------------------------------------------
    _setup_logging(
        log_level=settings.general.log_level,
        log_file=settings.general.log_file,
    )
    from core.build_info import get_build_hash
    build_hash = get_build_hash()
    log.info(
        "stratos1.starting",
        build=build_hash,
        timezone=settings.general.timezone,
        demo=settings.bybit.demo,
        groups=len(settings.telegram_groups),
    )

    # ---------------------------------------------------------------
    # Import all components (after logging is configured)
    # ---------------------------------------------------------------
    C = _import_components()

    # ---------------------------------------------------------------
    # Graceful shutdown handler
    # ---------------------------------------------------------------
    shutdown = _GracefulShutdown()

    loop = asyncio.get_running_loop()

    def _signal_handler() -> None:
        log.info("shutdown.signal_received")
        shutdown.trigger()

    # Register signal handlers (works on Unix; on Windows we use
    # try/except KeyboardInterrupt as fallback).
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _signal_handler)
    except NotImplementedError:
        # Windows does not support add_signal_handler; rely on
        # KeyboardInterrupt.
        pass

    # ---------------------------------------------------------------
    # 3. Initialize database
    # ---------------------------------------------------------------
    db = C["Database"](str(PROJECT_ROOT / settings.general.db_path))
    await db.initialize()
    shutdown.register(db)

    # ---------------------------------------------------------------
    # 3b. Load operator-managed symbol overrides (Tomas 2026-05-03).
    # symbol_overrides.toml lets the operator skip non-tradable
    # symbols, rename to Bybit's canonical form, and attach notes
    # that surface in warning notifications. Failure to load is
    # non-fatal (logged warning, bot continues with no overrides).
    # ---------------------------------------------------------------
    try:
        from core.symbol_overrides import load_overrides
        load_overrides(PROJECT_ROOT / "symbol_overrides.toml")
    except Exception:
        log.exception("symbol_overrides.startup_load_failed")

    # ---------------------------------------------------------------
    # 4. Initialize Bybit adapter
    # ---------------------------------------------------------------
    bybit = C["BybitAdapter"](settings.bybit)
    await bybit.start()
    shutdown.register(bybit)

    # ---------------------------------------------------------------
    # 4b. Clean-start-on-restart (Tomas 2026-05-02).
    # Flatten Bybit + reset DB-active trades to a clean slate every
    # restart. Eliminates the entire orphan / drift / state-recovery
    # class of bugs by making "clean DB matches clean Bybit" the
    # definition of startup. Best-effort — never blocks startup.
    # Runs BEFORE the Telegram listener so no signals can be consumed
    # while we're flattening.
    # ---------------------------------------------------------------
    clean_start_ran = False
    if settings.general.clean_start_on_restart:
        try:
            from health.clean_start import run_clean_start
            cs_summary = await run_clean_start(
                bybit, str(PROJECT_ROOT / settings.general.db_path),
            )
            clean_start_ran = True
            log.info("startup.clean_start_done", **cs_summary)
        except Exception:
            log.exception("startup.clean_start_failed")

    # ---------------------------------------------------------------
    # 5. Initialize Telegram listener
    # ---------------------------------------------------------------
    # Session string from environment (generated by generate_session.py).
    session_string = os.environ.get("TG_SESSION_STRING", "")

    # Create a placeholder callback -- we'll wire it up after the
    # position manager is ready.
    signal_callback_holder: dict = {"fn": None}

    async def _signal_router(
        raw_text: str,
        channel_id: int,
        channel_name: str,
    ) -> None:
        """Routes incoming Telegram messages to the signal pipeline."""
        fn = signal_callback_holder.get("fn")
        if fn is not None:
            await fn(raw_text, channel_id, channel_name)

    tg_listener = C["TelegramListener"](
        settings=settings.telegram,
        groups=settings.telegram_groups,
        on_signal_callback=_signal_router,
        session_string=session_string,
    )
    await tg_listener.start()
    shutdown.register(tg_listener)

    # ---------------------------------------------------------------
    # 6. Initialize Telegram notifier
    # ---------------------------------------------------------------
    tg_notifier = C["TelegramNotifier"](settings.telegram)
    await tg_notifier.start()
    shutdown.register(tg_notifier)

    # ---------------------------------------------------------------
    # 6b. Initialize report scheduler (daily + weekly group reports).
    # Client IZZU 2026-04-24: wants the daily/weekly per-group
    # reports from Meddelande telegram.docx active. The
    # ReportScheduler class has existed but was never started from
    # main. Hook it up here.
    # ---------------------------------------------------------------
    try:
        from reporting.scheduler import ReportScheduler
        report_scheduler = ReportScheduler(
            settings=settings.reporting,
            db=db,
            notifier=tg_notifier,
            groups=settings.telegram_groups,
            timezone=settings.general.timezone,
        )
        await report_scheduler.start()
        shutdown.register(report_scheduler)
        log.info(
            "report_scheduler.wired",
            daily_hour=settings.reporting.daily_report_hour,
            weekly_day=settings.reporting.weekly_report_day,
            weekly_hour=settings.reporting.weekly_report_hour,
        )
    except Exception:
        log.exception("report_scheduler.startup_failed")

    # ---------------------------------------------------------------
    # 7. Initialize duplicate detector
    # ---------------------------------------------------------------
    dup_detector = C["DuplicateDetector"](
        db=db,
        threshold_pct=settings.duplicate.threshold_pct,
        lookback_hours=settings.duplicate.lookback_hours,
    )

    # ---------------------------------------------------------------
    # 8. Initialize position manager (wires up sub-managers internally)
    # ---------------------------------------------------------------
    position_mgr = C["PositionManager"](
        settings=settings,
        db=db,
        bybit=bybit,
        notifier=tg_notifier,
        duplicate_detector=dup_detector,
    )

    # ---------------------------------------------------------------
    # 9. Initialize order loop protector
    # ---------------------------------------------------------------
    order_protector = C["OrderLoopProtector"](
        max_count=settings.capacity.order_loop_max_count,
        window_seconds=settings.capacity.order_loop_window_seconds,
    )

    # ---------------------------------------------------------------
    # 10. Initialize health checker
    # ---------------------------------------------------------------
    health_checker = C["HealthChecker"](
        settings=settings,
        db=db,
        bybit=bybit,
        tg_listener=tg_listener,
        tg_notifier=tg_notifier,
    )

    # ---------------------------------------------------------------
    # 10b. Purge WishingBell notification channel on every restart.
    # Client IZZU 2026-04-24: clean slate for the channel so the
    # operator isn't staring at stale history from a previous run.
    # Runs only when both env flags are satisfied:
    #   TG_SESSION_STRING set (user session with admin rights), AND
    #   STRATOS1_PURGE_ON_RESTART not explicitly "0" / "false"
    # Errors are swallowed — purge is best-effort and must never
    # block startup.
    # ---------------------------------------------------------------
    purge_enabled = os.environ.get(
        "STRATOS1_PURGE_ON_RESTART", "1",
    ).strip().lower() not in ("0", "false", "no", "off")
    if purge_enabled and session_string:
        try:
            from telethon import TelegramClient as _TC
            from telethon.sessions import StringSession as _SS
            _purge_client = _TC(
                _SS(session_string),
                int(settings.telegram.api_id),
                settings.telegram.api_hash,
            )
            await _purge_client.connect()
            if await _purge_client.is_user_authorized():
                try:
                    _entity = await _purge_client.get_entity(
                        settings.telegram.notify_channel_id,
                    )
                    _batch: list[int] = []
                    _total = 0
                    async for _msg in _purge_client.iter_messages(
                        _entity, limit=None,
                    ):
                        _batch.append(_msg.id)
                        if len(_batch) >= 100:
                            try:
                                await _purge_client.delete_messages(
                                    _entity, _batch, revoke=True,
                                )
                                _total += len(_batch)
                            except Exception:
                                log.exception("purge.batch_error")
                            _batch.clear()
                    if _batch:
                        try:
                            await _purge_client.delete_messages(
                                _entity, _batch, revoke=True,
                            )
                            _total += len(_batch)
                        except Exception:
                            log.exception("purge.final_batch_error")
                    log.info(
                        "startup.channel_purged",
                        channel_id=settings.telegram.notify_channel_id,
                        messages_deleted=_total,
                    )
                except Exception:
                    log.exception("startup.channel_purge_failed")
            await _purge_client.disconnect()
        except Exception:
            log.exception("startup.channel_purge_setup_failed")

    # ---------------------------------------------------------------
    # 11. Run startup health checks
    # ---------------------------------------------------------------
    results = await health_checker.run_startup_checks()
    critical_failures = [
        name for name, ok, msg in results
        if not ok and name in (
            "Bybit API Connectivity",
            "Database",
        )
    ]
    if critical_failures:
        log.error(
            "startup.critical_failure",
            failed_checks=critical_failures,
        )
        # Still continue -- the operator can fix while the bot retries.
        # A truly hard failure (DB not opening) would have thrown above.

    # ---------------------------------------------------------------
    # 12. State recovery
    # Skip when clean_start_on_restart wiped the DB — there's nothing
    # to recover by definition, and running recovery would only
    # reintroduce the orphan/drift class of bugs we just eliminated.
    # ---------------------------------------------------------------
    if clean_start_ran:
        log.info("startup.recovery_skipped_clean_start")
    else:
        recovery_counts = await health_checker.recover_state(position_mgr)
        log.info("startup.recovery_complete", **recovery_counts)

    # ---------------------------------------------------------------
    # 13. Wire up callbacks
    # ---------------------------------------------------------------
    # a) TG listener -> signal parser -> position manager
    parse_signal_detailed_fn = C["parse_signal_detailed"]
    from core.signal_parser import (
        is_status_update,
        is_trade_update_message,
        parse_trade_update,
    )

    async def _on_signal_message(
        raw_text: str,
        channel_id: int,
        channel_name: str,
    ) -> None:
        """Full signal processing pipeline for each Telegram message."""
        try:
            # Status-update guard (Tomas 2026-05-03). Trade-result
            # messages like "ALL TARGETS DONE" / "TP1 taken" / "Stopped
            # out" must NOT be parsed as new signals — when they are,
            # the bot fabricates partial signals and emits "entry/TP
            # missing" warnings to the operator. Drop them silently
            # BEFORE the parser runs.
            if is_status_update(raw_text):
                log.info(
                    "status_update_skipped",
                    channel_id=channel_id,
                    channel_name=channel_name,
                    text_preview=raw_text[:80],
                )
                return

            # Trade-update guard (Tomas 2026-05-15). "OPEN ENTRY / NEW
            # TP" follow-up messages carry no direction word, so the
            # parser would drop them at no_direction. Route them to the
            # position manager's trade-update handler instead — it
            # updates a running trade, or (if none) opens a fresh
            # fixed-mode trade with the direction inferred from
            # TP-vs-entry. Wrapped so a handler error can't kill the
            # listener.
            if is_trade_update_message(raw_text):
                update = parse_trade_update(raw_text)
                if update is not None:
                    try:
                        await position_mgr.handle_trade_update(
                            update, channel_name
                        )
                    except Exception:
                        log.exception(
                            "trade_update.handler_failed",
                            channel_name=channel_name,
                        )
                    return
                # is_trade_update_message True but no symbol — fall
                # through; parse_signal_detailed will exit silently.

            result = parse_signal_detailed_fn(
                text=raw_text,
                channel_id=channel_id,
                channel_name=channel_name,
            )

            # On rejection AFTER symbol+direction were detected, notify
            # the operator. Tomas 2026-05-12 spec: "Nothing should happen
            # silently inside the bot. If the bot blocks or skips
            # something, the exact reason must be visible in Telegram."
            # Pure-chatter rejections ("no_symbol" / "no_direction" /
            # "empty") still stay silent — those are not signal-shaped
            # and notifying them would mirror every chat message into
            # the operator channel.
            if result.signal is None:
                # skipped_by_override (symbol_overrides.toml said
                # skip = true) is intentionally NOT notified. Tomas
                # 2026-05-15: a per-skip message just duplicates the
                # "not on Bybit" concept and clutters the channel —
                # the skip list only holds symbols already known to be
                # untradeable. The skip is still recorded in the file
                # log (signal_parse.skipped_by_override) for the audit
                # trail; only the operator-channel message is dropped.
                if (result.reason == "no_entry" and result.symbol
                        and result.direction and (result.tps or result.sl)):
                    # Notify only for SIGNAL-SHAPED rejections: symbol +
                    # direction + at least one of SL/TP. A real signal
                    # missing only its entry price still has SL and/or
                    # TP lines and still fires "Blokerad, Entre saknas"
                    # (Tomas 2026-05-12 spec: every signal-shaped
                    # rejection notifies).
                    #
                    # 2026-05-14: the `(result.tps or result.sl)` guard
                    # was added. A message with a ticker + LONG/SHORT
                    # word but NO entry, NO SL and NO TP is news /
                    # chatter / a target-update post — the parser logs
                    # it as signal_parse_no_entry_chatter and it exits
                    # silently here. Tomas 2026-05-14: "bot classifies
                    # news/updates as signals and sends error messages
                    # for them."
                    try:
                        await tg_notifier.signal_blocked_no_entry(
                            symbol=result.symbol,
                            direction=result.direction,
                            channel_name=channel_name,
                        )
                    except Exception:
                        log.exception("notify.signal_blocked_no_entry_failed")
                elif (result.reason in ("no_tps", "invalid")
                      and result.symbol and result.direction):
                    # ``invalid`` covers TP-direction mismatch (LONG with
                    # TP below entry, SHORT with TP above) AND the SL
                    # sanity-check failures from validate_signal. The
                    # parser's specific reason string (result.detail)
                    # tells us which field actually failed: route SL
                    # failures to the SL-specific wording, otherwise
                    # use the TP wording (Tomas 2026-05-15 msg 54706
                    # CryptoPasta BTC Stop:8230 typo — the rejection
                    # was correct but said "TP är fel angiva" for an
                    # SL-side failure).
                    detail = (result.detail or "").upper()
                    sl_failure = (
                        result.reason == "invalid"
                        and "SL" in detail
                        and "TP" not in detail
                    )
                    try:
                        if sl_failure:
                            await tg_notifier.signal_blocked_invalid_sl(
                                symbol=result.symbol,
                                direction=result.direction,
                                channel_name=channel_name,
                            )
                        else:
                            await tg_notifier.signal_blocked_invalid_tps(
                                symbol=result.symbol,
                                direction=result.direction,
                                channel_name=channel_name,
                            )
                    except Exception:
                        log.exception("notify.signal_blocked_invalid_failed")
                return

            signal = result.signal

            # Check order-loop protection.
            if not order_protector.check(signal.symbol):
                log.warning(
                    "signal.order_loop_blocked",
                    symbol=signal.symbol,
                    channel_name=channel_name,
                )
                try:
                    await tg_notifier.orderloop_protection(
                        symbol=signal.symbol,
                        block_window=settings.capacity.order_loop_window_seconds,
                    )
                except Exception:
                    pass
                return

            # Hand off to position manager.
            await position_mgr.process_signal(signal)

        except Exception:
            log.exception(
                "signal_pipeline.error",
                channel_id=channel_id,
                channel_name=channel_name,
                text_preview=raw_text[:120] if raw_text else "",
            )

    signal_callback_holder["fn"] = _on_signal_message

    # b) Bybit WS callbacks -> position manager
    #    The BybitAdapter dispatches from pybit's background threads,
    #    so we schedule coroutines on the event loop.
    def _make_ws_callback(coro_fn):
        """Create a thread-safe wrapper that schedules a coroutine."""
        def _wrapper(data: dict) -> None:
            try:
                loop.call_soon_threadsafe(
                    asyncio.ensure_future, coro_fn(data)
                )
            except Exception:
                pass
        return _wrapper

    # Patch in WS callbacks (order, position, execution).
    if hasattr(position_mgr, "on_order_update"):
        bybit._on_order_update = _make_ws_callback(
            position_mgr.on_order_update
        )
    if hasattr(position_mgr, "on_position_update"):
        bybit._on_position_update = _make_ws_callback(
            position_mgr.on_position_update
        )
    if hasattr(position_mgr, "on_execution_update"):
        bybit._on_execution_update = _make_ws_callback(
            position_mgr.on_execution_update
        )

    # c) Price updates via Bybit public WS ticker subscription.
    #    Subscribe to tickers for all symbols that have active trades.
    _subscribed_symbols: set = set()

    def _on_ticker(message: dict) -> None:
        """Public WS ticker callback (runs in pybit thread)."""
        try:
            data = message.get("data", {})
            symbol = data.get("symbol", "")
            last_price = data.get("lastPrice")
            if symbol and last_price:
                loop.call_soon_threadsafe(
                    asyncio.ensure_future,
                    position_mgr.handle_price_update(
                        symbol, float(last_price)
                    ),
                )
        except Exception:
            pass

    async def subscribe_ticker(symbol: str) -> None:
        """Subscribe to public ticker for a symbol (idempotent)."""
        if symbol in _subscribed_symbols:
            return
        if bybit._ws_public is None:
            return
        try:
            bybit._ws_public.ticker_stream(
                symbol=symbol,
                callback=_on_ticker,
            )
            _subscribed_symbols.add(symbol)
            log.info("ticker_subscribed", symbol=symbol)
        except Exception:
            log.exception("ticker_subscribe_failed", symbol=symbol)

    # Subscribe to tickers for any symbols from recovered trades.
    for trade_id, trade in getattr(position_mgr, "_active_trades", {}).items():
        if trade.signal:
            await subscribe_ticker(trade.signal.symbol)

    # ---------------------------------------------------------------
    # 14. Periodic tasks
    # ---------------------------------------------------------------
    background_tasks: set[asyncio.Task] = set()

    async def _periodic_health() -> None:
        """Run health check every 5 minutes."""
        while not shutdown.is_shutting_down:
            try:
                await asyncio.sleep(300)
                if shutdown.is_shutting_down:
                    break
                await health_checker.periodic_health_check()
                # Cleanup order protector expired entries.
                order_protector._cleanup()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("periodic_health.error")

    async def _periodic_price_poll() -> None:
        """
        Poll Bybit REST for current prices of all active trades' symbols
        every 5 seconds. This is the fallback path when the public
        WebSocket is down (VPN / geo-blocking issues). Without this,
        BE / scaling / trailing / hedge / re-entry managers never fire
        because they all depend on price updates.
        """
        poll_interval = 5  # seconds between polls
        while not shutdown.is_shutting_down:
            try:
                await asyncio.sleep(poll_interval)
                if shutdown.is_shutting_down:
                    break

                # Get unique symbols from active trades.
                active = getattr(position_mgr, "_active_trades", {})
                symbols = set()
                for trade in active.values():
                    if trade.signal and trade.signal.symbol:
                        symbols.add(trade.signal.symbol)

                if not symbols:
                    continue

                # Poll ticker for each symbol.
                for symbol in symbols:
                    try:
                        ticker = await bybit.get_ticker(symbol)
                        if ticker:
                            price = float(
                                ticker.get("markPrice") or
                                ticker.get("lastPrice") or
                                0
                            )
                            if price > 0:
                                await position_mgr.handle_price_update(
                                    symbol, price,
                                )
                    except Exception:
                        log.exception(
                            "price_poll.ticker_error", symbol=symbol,
                        )
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("price_poll.error")

    async def _periodic_reverse_reconciliation() -> None:
        """Detect Bybit-side orphan positions and auto-close them.

        Inverse of _periodic_position_reconciliation. That loop catches
        "DB has it but Bybit doesn't" and marks the DB closed. THIS
        loop catches the OPPOSITE drift — "Bybit has an open position
        but the DB has no active trade" — by paginating Bybit's full
        position list every 60 seconds and closing anything the bot
        doesn't manage.

        Why orphans accumulate in this direction: state recovery on
        restart used to mark DB trades CLOSED on a single missed
        Bybit lookup (rate-limited / racy / transient). The position
        was actually still open; the DB just thought it wasn't.
        Without this loop, those orphans would bleed indefinitely
        because the -4 USDT cap and every other safety check runs
        ONLY on trades the bot tracks. Discovered 2026-04-27 with 48
        orphan positions on Bybit summing to -77 USDT unrealised.

        Auto-closing matches the safety-net philosophy. If a position
        has no managing trade in the DB, the bot has no SL/TP/hedge
        plan for it — best to flatten and report rather than leave it
        bleeding.
        """
        reconcile_interval = 60
        while not shutdown.is_shutting_down:
            try:
                await asyncio.sleep(reconcile_interval)
                if shutdown.is_shutting_down:
                    break

                # Pull every open position on Bybit, paginated.
                all_positions: list = []
                cursor = ""
                page_attempts = 0
                while page_attempts < 10:
                    page_attempts += 1
                    try:
                        kwargs = {
                            "category": "linear",
                            "settleCoin": "USDT",
                            "limit": 200,
                        }
                        if cursor:
                            kwargs["cursor"] = cursor
                        resp = await bybit._call_with_retry(
                            bybit._rest.get_positions, **kwargs,
                        )
                        result = resp.get("result", {})
                        chunk = result.get("list") or []
                        all_positions.extend(chunk)
                        cursor = result.get("nextPageCursor", "") or ""
                        if not cursor or not chunk:
                            break
                    except Exception:
                        log.exception("reverse_reconcile.page_fetch_failed")
                        break

                # Phase 3 (client 2026-05-01) — reverse_reconcile is now
                # a SAFETY-NET DETECTOR ONLY. It does NOT arm hedges,
                # does NOT adopt orphans, does NOT close orphans. The
                # hedge fill is handled atomically by
                # PositionManager._maybe_activate_hedge_from_fill via
                # WS, which is the single source of truth. Orphan
                # positions are surfaced as a deduped warning so the
                # operator can investigate manually — no silent
                # auto-correction (Tomas explicit rule: "no fallback
                # paths, no auto-correct, no parallel handlers").
                tracked: set[tuple[str, str]] = set()
                for tr in getattr(position_mgr, "_active_trades", {}).values():
                    if tr.signal and tr.signal.symbol and not tr.is_terminal:
                        tracked_side = "Buy" if tr.signal.direction == "LONG" else "Sell"
                        tracked.add((tr.signal.symbol, tracked_side))
                        # Hedge side: tracked when hedge is active OR
                        # when the pre-armed conditional is on Bybit.
                        # The WS handler clears
                        # ``hedge_conditional_order_id`` on fill, so a
                        # filled-but-not-yet-WS-acked hedge stays in
                        # ``tracked`` here too.
                        hedge_side = "Sell" if tr.signal.direction == "LONG" else "Buy"
                        if tr.hedge_trade_id or tr.hedge_conditional_order_id:
                            tracked.add((tr.signal.symbol, hedge_side))

                # Tomas 2026-05-13 CHIPUSDT race fix: when close_trade
                # fires, the trade leaves _active_trades INSTANTLY but
                # Bybit needs 1-3 seconds to actually reduce the
                # position size to zero. This reconcile worker runs
                # during that window and sees an "untracked" position
                # that is actually mid-close. Skip the orphan warning
                # for (symbol, side) pairs closed within 60s.
                recently_closed = getattr(
                    position_mgr, "_recently_closed", {},
                )
                _now_mono = time.monotonic()
                _RECENT_CLOSE_COOLDOWN = 60.0
                for p in all_positions:
                    try:
                        size = float(p.get("size") or 0)
                        if size <= 0:
                            continue
                        sym = p.get("symbol", "")
                        side = p.get("side", "")
                        position_idx = int(p.get("positionIdx") or 0)
                        if not sym or not side:
                            continue
                        if (sym, side) in tracked:
                            continue
                        closed_at = recently_closed.get((sym, side))
                        if (
                            closed_at is not None
                            and (_now_mono - closed_at) < _RECENT_CLOSE_COOLDOWN
                        ):
                            log.debug(
                                "reverse_reconcile.recently_closed_skip",
                                symbol=sym, side=side,
                                age_s=round(_now_mono - closed_at, 2),
                            )
                            continue
                        unreal = float(p.get("unrealisedPnl") or 0)
                        # Untracked Bybit position — log a deduped
                        # warning and surface to the operator. Do NOT
                        # adopt, do NOT close. Operator decides.
                        if position_mgr._should_send_reject_notify(
                            "untracked_bybit_position", sym, side,
                        ):
                            log.warning(
                                "reverse_reconcile.untracked_position",
                                symbol=sym, side=side, size=size,
                                position_idx=position_idx,
                                unrealised_pnl=unreal,
                            )
                            try:
                                await tg_notifier._send_notify(
                                    f"⚠️ Obevakad position på Bybit\n"
                                    f"📊 Symbol: #{sym}\n"
                                    f"📈 Riktning: {side}\n"
                                    f"💵 Storlek: {size}\n"
                                    f"💰 PnL: {unreal:+.2f} USDT\n"
                                    f"📍 Boten har ingen aktiv trade som "
                                    f"matchar denna position. Inga åtgärder "
                                    f"vidtas automatiskt — operatör måste "
                                    f"hantera manuellt (stänga via Bybit, "
                                    f"eller utreda var positionen kommer "
                                    f"ifrån)."
                                )
                            except Exception:
                                pass
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        log.exception("reverse_reconcile.position_error")
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("reverse_reconcile.loop_error")

    async def _periodic_order_cleanup() -> None:
        """Clean up timed-out unfilled orders every 1 hour."""
        while not shutdown.is_shutting_down:
            try:
                await asyncio.sleep(3600)
                if shutdown.is_shutting_down:
                    break
                log.info("order_cleanup.running")
                unfilled_hours = settings.timeout.unfilled_order_hours
                stale_orders = await db.get_unfilled_orders(
                    older_than_hours=unfilled_hours,
                )
                for order in stale_orders:
                    order_id_bot = order.get("order_id_bot")
                    order_id_bybit = order.get("order_id_bybit")
                    symbol = order.get("symbol", "")

                    # Cancel on exchange if we have a Bybit order ID.
                    if order_id_bybit and symbol:
                        try:
                            await bybit.cancel_order(symbol, order_id_bybit)
                        except Exception:
                            log.exception(
                                "order_cleanup.cancel_failed",
                                order_id_bybit=order_id_bybit,
                                symbol=symbol,
                            )

                    # Remove from local DB.
                    if order_id_bot:
                        try:
                            await db.delete_order(order_id_bot)
                        except Exception:
                            log.exception(
                                "order_cleanup.delete_failed",
                                order_id_bot=order_id_bot,
                            )

                if stale_orders:
                    log.info(
                        "order_cleanup.complete",
                        cleaned=len(stale_orders),
                    )
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("order_cleanup.error")

    async def _periodic_missed_signal_audit() -> None:
        """Tomas (client) 2026-05-04: scan the log every interval to
        flag messages from monitored channels that never reached the
        operator's Telegram channel. The audit posts a summary ONLY
        when silent_drops > 0 — healthy windows produce no spam.

        Silent drops are also appended to silent_drops.jsonl
        (deduped by signal_id) so the operator builds a permanent
        backlog of unhandled signal formats that survives restarts.
        """
        cfg = settings.missed_signal_audit
        interval_min = cfg.interval_minutes
        if interval_min <= 0:
            log.info("missed_signal_audit.disabled")
            return
        log_path = PROJECT_ROOT / settings.general.log_file
        persist_path = (
            PROJECT_ROOT / cfg.persist_path
            if cfg.persist_silent_drops else None
        )
        from health.missed_signal_audit import run_audit_and_notify
        while not shutdown.is_shutting_down:
            try:
                await asyncio.sleep(interval_min * 60)
                if shutdown.is_shutting_down:
                    break
                await run_audit_and_notify(
                    notifier=tg_notifier,
                    log_path=log_path,
                    window_minutes=interval_min,
                    persist_path=persist_path,
                    persist_max_entries=cfg.persist_max_entries,
                )
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("periodic_missed_signal_audit.error")

    # Start periodic tasks.
    health_task = asyncio.create_task(_periodic_health())
    background_tasks.add(health_task)
    health_task.add_done_callback(background_tasks.discard)

    price_poll_task = asyncio.create_task(_periodic_price_poll())
    background_tasks.add(price_poll_task)
    price_poll_task.add_done_callback(background_tasks.discard)

    cleanup_task = asyncio.create_task(_periodic_order_cleanup())
    background_tasks.add(cleanup_task)
    cleanup_task.add_done_callback(background_tasks.discard)

    reverse_reconcile_task = asyncio.create_task(_periodic_reverse_reconciliation())
    background_tasks.add(reverse_reconcile_task)
    reverse_reconcile_task.add_done_callback(background_tasks.discard)

    missed_signal_task = asyncio.create_task(_periodic_missed_signal_audit())
    background_tasks.add(missed_signal_task)
    missed_signal_task.add_done_callback(background_tasks.discard)

    # ---------------------------------------------------------------
    # 15. Run forever
    # ---------------------------------------------------------------
    log.info(
        "stratos1.running",
        active_trades=len(getattr(position_mgr, "_active_trades", {})),
        monitored_groups=len(settings.telegram_groups),
        demo=settings.bybit.demo,
    )

    # Tomas 2026-05-12: every bot start must be visible in the operator
    # channel as proof of life ("nothing silent" spec). Posted after
    # all background tasks are wired and the bot is fully ready, so the
    # operator's clock starts when the bot is truly accepting signals.
    try:
        from core.time_utils import format_time, now_utc
        await tg_notifier._send_notify(
            f"🤖 boten startad\n"
            f"🕒 Tid: {format_time(now_utc())}\n"
            f"📊 Övervakade grupper: {len(settings.telegram_groups)}\n"
            f"📍 Bybit-läge: {'DEMO' if settings.bybit.demo else 'LIVE'}\n"
            f"📍 Aktiva trades: {len(getattr(position_mgr, '_active_trades', {}))}"
        )
    except Exception:
        log.exception("startup.bot_started_notify_failed")

    try:
        await shutdown.wait()
    except (KeyboardInterrupt, SystemExit):
        log.info("shutdown.keyboard_interrupt")
        shutdown.trigger()

    # ---------------------------------------------------------------
    # 16. Graceful shutdown
    # ---------------------------------------------------------------
    log.info("stratos1.shutting_down")

    # Cancel background tasks.
    for task in background_tasks:
        task.cancel()
    if background_tasks:
        await asyncio.gather(*background_tasks, return_exceptions=True)

    # Stop all registered components.
    await shutdown.shutdown_all()

    log.info("stratos1.stopped")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    """Synchronous entry point for the bot."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception:
        logging.exception("stratos1.fatal_error")
        sys.exit(1)


if __name__ == "__main__":
    run()
