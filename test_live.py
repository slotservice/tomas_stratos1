"""
Stratos1 - Live Integration Test Script
-----------------------------------------
Tests each component independently without needing the full bot running.
Run with:  python test_live.py

This script checks:
  1. Config loading
  2. Bybit API connection + wallet balance
  3. Bybit market data (ticker, instrument info)
  4. Leverage calculation with real market data
  5. Signal parsing with sample signals
  6. Database creation and operations
  7. Telegram bot connection (send test message)

Usage:
  python test_live.py              - Run all tests
  python test_live.py bybit        - Test Bybit only
  python test_live.py telegram     - Test Telegram only
  python test_live.py parser       - Test parser only
  python test_live.py db           - Test database only
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault("PYTHONIOENCODING", "utf-8")


def _header(title: str) -> None:
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)
    print()


def _ok(msg: str) -> None:
    print(f"  [OK] {msg}")


def _fail(msg: str) -> None:
    print(f"  [FAIL] {msg}")


def _skip(msg: str) -> None:
    print(f"  [SKIP] {msg}")


# ------------------------------------------------------------------
# Test 1: Config
# ------------------------------------------------------------------
def test_config():
    _header("TEST 1: Configuration Loading")
    from config.settings import load_settings

    settings = load_settings(PROJECT_ROOT)
    _ok(f"Wallet: {settings.wallet.bot_wallet} USDT")
    _ok(f"Risk: {settings.wallet.risk_pct * 100}%")
    _ok(f"Initial margin: {settings.wallet.initial_margin} USDT")
    _ok(f"Bybit demo: {settings.bybit.demo}")
    _ok(f"Telegram groups: {len(settings.telegram_groups)}")
    _ok(f"Timezone: {settings.general.timezone}")
    _ok(f"Trailing: {settings.trailing_stop.activation_pct}% / {settings.trailing_stop.trailing_distance_pct}%")
    _ok(f"Hedge: {settings.hedge.trigger_pct}%")
    _ok(f"Re-entry: max {settings.reentry.max_reentries}")
    _ok(f"Duplicate threshold: {settings.duplicate.threshold_pct}%")

    return settings


# ------------------------------------------------------------------
# Test 2: Bybit API
# ------------------------------------------------------------------
async def test_bybit(settings):
    _header("TEST 2: Bybit API Connection")
    from exchange.bybit_adapter import BybitAdapter

    adapter = BybitAdapter(settings.bybit)
    try:
        await adapter.start()
        _ok("Bybit adapter started (REST + WebSocket)")
    except Exception as e:
        _fail(f"Adapter start failed: {e}")
        return

    # Wallet balance
    try:
        balance = await adapter.get_wallet_balance()
        if balance:
            total = balance.get("totalEquity", "?")
            avail = balance.get("totalAvailableBalance", "?")
            _ok(f"Wallet - Total equity: {total} USDT")
            _ok(f"Wallet - Available balance: {avail} USDT")
        else:
            _ok("Wallet response received (empty - may be new demo account)")
    except Exception as e:
        _fail(f"Wallet balance: {e}")

    # Ticker
    try:
        ticker = await adapter.get_ticker("BTCUSDT")
        if ticker:
            last = ticker.get("lastPrice", "?")
            mark = ticker.get("markPrice", "?")
            _ok(f"BTCUSDT - Last: {last}, Mark: {mark}")
        else:
            _fail("Ticker returned empty")
    except Exception as e:
        _fail(f"Ticker: {e}")

    # Instrument info
    try:
        info = await adapter.get_instrument_info("BTCUSDT")
        if info:
            lot = info.get("lotSizeFilter", {})
            price_f = info.get("priceFilter", {})
            _ok(f"BTCUSDT - Min qty: {lot.get('minOrderQty', '?')}, "
                f"Tick size: {price_f.get('tickSize', '?')}")
        else:
            _fail("Instrument info returned empty")
    except Exception as e:
        _fail(f"Instrument info: {e}")

    # Position list
    try:
        pos = await adapter.get_position("BTCUSDT", "Buy")
        _ok(f"Position query OK (size: {pos.get('size', '0') if pos else '0'})")
    except Exception as e:
        _fail(f"Position query: {e}")

    await adapter.stop()
    _ok("Bybit adapter stopped cleanly")


# ------------------------------------------------------------------
# Test 3: Signal Parser
# ------------------------------------------------------------------
def test_parser():
    _header("TEST 3: Signal Parser")
    from core.signal_parser import parse_signal

    samples = [
        (
            "BTCUSDT LONG\nEntry: 65000\nTP1: 66000\nTP2: 67500\nSL: 63500",
            "Crypto Raketen",
        ),
        (
            "#ETHUSDT\n📈 Long\nEntry: 3200\nTargets: 3300/3400/3500\nStop Loss: 3100",
            "Smart Crypto",
        ),
        (
            "Coin: SOL/USDT\nDirection: SHORT\nEntry: 145.50\nTP1: 142\nTP2: 138\nSL: 148",
            "Zafir",
        ),
        (
            "🟢 LONG SIGNAL\nDOGEUSDT\nBuy: 0.1520\nT1: 0.1560\nT2: 0.1600\nSL: 0.1480",
            "Crypto Musk",
        ),
        (
            "Random chat message about crypto being cool today",
            "Noise Channel",
        ),
    ]

    for text, channel in samples:
        sig = parse_signal(text, -100, channel)
        if sig:
            _ok(f"{channel}: {sig.symbol} {sig.direction} entry={sig.entry} "
                f"TPs={sig.tps} SL={sig.sl} type={sig.signal_type}")
        else:
            _ok(f"{channel}: rejected (not a valid signal)")


# ------------------------------------------------------------------
# Test 4: Leverage Calculator
# ------------------------------------------------------------------
def test_leverage():
    _header("TEST 4: Leverage Calculator")
    from core.leverage import calculate_leverage, classify_leverage

    cases = [
        ("IOTA", 0.2215, 0.2149, "~x13.49 DYNAMIC"),
        ("Wide SL", 100.0, 90.0, "fixed x6"),
        ("Tight SL", 100.0, 99.5, "dynamic (high)"),
        ("Very tight", 100.0, 99.8, "capped x25"),
    ]
    for name, entry, sl, expected in cases:
        lev = calculate_leverage(entry, sl)
        cls = classify_leverage(lev)
        _ok(f"{name}: entry={entry}, SL={sl} -> x{lev} ({cls})  [expected: {expected}]")


# ------------------------------------------------------------------
# Test 5: Database
# ------------------------------------------------------------------
async def test_db():
    _header("TEST 5: Database")
    import json
    from persistence.database import Database

    db_path = str(PROJECT_ROOT / "test_stratos1.db")
    db = Database(db_path)
    await db.initialize()
    _ok("Database initialized (tables created)")

    # Save a signal
    sig_id = await db.save_signal({
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "entry_price": 65000.0,
        "sl_price": 63500.0,
        "tp_prices": json.dumps([66000.0, 67500.0]),
        "source_channel_name": "Test Channel",
        "signal_type": "dynamic",
        "raw_text": "test signal",
    })
    _ok(f"Signal saved with ID: {sig_id}")

    # Save a trade
    trade_id = await db.save_trade({
        "signal_id": sig_id,
        "state": "POSITION_OPEN",
        "avg_entry": 65000.0,
        "leverage": 13.49,
        "margin": 20.0,
    })
    _ok(f"Trade saved with ID: {trade_id}")

    # Query active trades
    active = await db.get_active_trades()
    _ok(f"Active trades: {len(active)}")

    # Log an event
    await db.log_event(trade_id, "test_event", {"note": "integration test"})
    _ok("Event logged")

    await db.close()
    _ok("Database closed")

    # Clean up test DB
    import os
    os.remove(db_path)
    _ok("Test database cleaned up")


# ------------------------------------------------------------------
# Test 6: Telegram Bot (send test message)
# ------------------------------------------------------------------
async def test_telegram(settings):
    _header("TEST 6: Telegram Bot")
    from telegram.notifier import TelegramNotifier

    notifier = TelegramNotifier(settings.telegram)
    try:
        await notifier.start()
        _ok("Telegram bot connected")

        # Send test message
        from core.time_utils import format_time, now_utc
        test_msg = (
            "🧪 <b>STRATOS1 TEST</b>\n"
            f"🕒 Tid: {format_time(now_utc())}\n"
            "📍 Status: Bot is connected and working\n"
            f"📊 Groups configured: {len(settings.telegram_groups)}\n"
            f"💰 Wallet: {settings.wallet.bot_wallet} USDT\n"
            f"⚙️ Demo mode: {settings.bybit.demo}"
        )
        await notifier._send_notify(test_msg)
        _ok(f"Test message sent to channel {settings.telegram.notify_channel_id}")

        await notifier.stop()
        _ok("Telegram bot disconnected")
    except Exception as e:
        _fail(f"Telegram: {e}")


# ------------------------------------------------------------------
# Test 7: Time formatting
# ------------------------------------------------------------------
def test_time():
    _header("TEST 7: Time Formatting")
    from core.time_utils import format_time, now_utc, now_local

    utc_now = now_utc()
    local_now = now_local("Europe/Stockholm")
    formatted = format_time(utc_now)
    _ok(f"UTC now:       {utc_now.isoformat()}")
    _ok(f"Stockholm now: {local_now.isoformat()}")
    _ok(f"Formatted:     {formatted}")


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
async def run_all(targets: set[str]):
    # Config always runs
    settings = test_config()

    if "parser" in targets or "all" in targets:
        test_parser()

    if "leverage" in targets or "all" in targets:
        test_leverage()

    if "time" in targets or "all" in targets:
        test_time()

    if "db" in targets or "all" in targets:
        await test_db()

    if "bybit" in targets or "all" in targets:
        await test_bybit(settings)

    if "telegram" in targets or "all" in targets:
        await test_telegram(settings)

    _header("ALL TESTS COMPLETE")


def main():
    targets = set(sys.argv[1:]) if len(sys.argv) > 1 else {"all"}
    try:
        asyncio.run(run_all(targets))
    except KeyboardInterrupt:
        print("\nCancelled.")


if __name__ == "__main__":
    main()
