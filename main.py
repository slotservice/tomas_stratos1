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
    from core.signal_parser import parse_signal

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
    log.info(
        "stratos1.starting",
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
    # 4. Initialize Bybit adapter
    # ---------------------------------------------------------------
    bybit = C["BybitAdapter"](settings.bybit)
    await bybit.start()
    shutdown.register(bybit)

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
    # ---------------------------------------------------------------
    recovery_counts = await health_checker.recover_state(position_mgr)
    log.info("startup.recovery_complete", **recovery_counts)

    # ---------------------------------------------------------------
    # 13. Wire up callbacks
    # ---------------------------------------------------------------
    # a) TG listener -> signal parser -> position manager
    parse_signal_fn = C["parse_signal"]

    async def _on_signal_message(
        raw_text: str,
        channel_id: int,
        channel_name: str,
    ) -> None:
        """Full signal processing pipeline for each Telegram message."""
        try:
            signal = parse_signal_fn(
                text=raw_text,
                channel_id=channel_id,
                channel_name=channel_name,
            )
            if signal is None:
                return  # Not a valid signal -- silently ignore.

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

    # ---------------------------------------------------------------
    # 15. Run forever
    # ---------------------------------------------------------------
    log.info(
        "stratos1.running",
        active_trades=len(getattr(position_mgr, "_active_trades", {})),
        monitored_groups=len(settings.telegram_groups),
        demo=settings.bybit.demo,
    )

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
