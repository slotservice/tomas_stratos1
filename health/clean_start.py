"""
Stratos1 — clean-start-on-restart routine.

Client 2026-05-02 explicit: every bot restart should leave Bybit
flat and DB free of active trades. No state inheritance across
restarts. Eliminates the entire orphan / drift / state-recovery
class of bugs by making "clean DB matches clean Bybit" the
definition of startup.

The routine runs ONCE during main.py startup, after the Bybit
adapter is initialised but BEFORE the Telegram listener / position
manager start consuming signals.

Steps (idempotent, safe to re-run):
  1. Cancel every open Bybit order (orders + conditional / TP-SL /
     trailing stops). Paginated to avoid the limit=20 trap.
  2. Close every open Bybit position via Market reduce-only.
  3. Mark every DB trade in a non-terminal state as CLOSED with
     reason 'clean_start_on_restart'.

If any step fails the whole bot startup continues — the routine
never blocks the bot from coming up. Failures are logged.
"""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone
from typing import Any

import structlog

log = structlog.get_logger(__name__)


_NON_TERMINAL_STATES = (
    "PENDING", "ENTRY1_PLACED", "ENTRY1_FILLED",
    "ENTRY2_PLACED", "ENTRY2_FILLED", "POSITION_OPEN",
    "PROTECTION_FAILED",
    "HEDGE_ARMED", "HEDGE_ACTIVE",
    "ORIGINAL_FORCE_CLOSED",
    "BREAKEVEN_ACTIVE",
    "PROFIT_LOCK_1_ACTIVE", "PROFIT_LOCK_2_ACTIVE",
    "SL_MOVED_TO_TP1", "SL_MOVED_TO_TP2",
    "SL_MOVED_TO_TP3", "SL_MOVED_TO_TP4",
    "TRAILING_ARMED", "TRAILING_ACTIVE", "TRAILING_UPDATED",
    "SCALING_STEP_1", "SCALING_STEP_2",
    "SCALING_STEP_3", "SCALING_STEP_4",
    "REENTRY_WAITING",
)


async def _fetch_all_positions(rest: Any) -> list[dict]:
    """Paginated fetch of every non-zero position."""
    out: list[dict] = []
    cursor = ""
    for _ in range(20):
        kwargs = dict(category="linear", settleCoin="USDT", limit=200)
        if cursor:
            kwargs["cursor"] = cursor
        try:
            resp = await _run(rest.get_positions, **kwargs)
        except Exception:
            log.exception("clean_start.positions_fetch_failed")
            break
        result = resp.get("result", {}) or {}
        batch = result.get("list", []) or []
        out.extend(batch)
        cursor = result.get("nextPageCursor", "") or ""
        if not cursor or not batch:
            break
    return [p for p in out if float(p.get("size", 0) or 0) > 0]


async def _cancel_all_orders(rest: Any) -> int:
    """Cancel every open order on the account, by order-filter
    category. Bybit's cancel_all_orders takes a category and
    optional symbol; we issue it without a symbol but with each
    of the three relevant filter values."""
    cancelled = 0
    for order_filter in ("Order", "StopOrder", "tpslOrder"):
        try:
            resp = await _run(
                rest.cancel_all_orders,
                category="linear",
                settleCoin="USDT",
                orderFilter=order_filter,
            )
            ret_code = resp.get("retCode", 0)
            ret_msg = resp.get("retMsg", "")
            log.info(
                "clean_start.cancel_all_orders",
                order_filter=order_filter,
                ret_code=ret_code, ret_msg=ret_msg,
            )
            # Bybit returns 0 = success (with possibly an empty
            # cancelled list). Count is logged in the response.
            try:
                cancelled += len(
                    (resp.get("result", {}) or {}).get("list", []) or []
                )
            except Exception:
                pass
        except Exception:
            log.exception(
                "clean_start.cancel_all_failed",
                order_filter=order_filter,
            )
    return cancelled


async def _close_position(rest: Any, p: dict) -> bool:
    """Market reduce-only close of one position. Returns True on
    success."""
    sym = p.get("symbol")
    side = p.get("side")
    size = p.get("size")
    try:
        idx = int(p.get("positionIdx") or 0)
    except (TypeError, ValueError):
        idx = 0
    if not sym or not side or not size:
        return False
    close_side = "Sell" if side == "Buy" else "Buy"
    try:
        await _run(
            rest.place_order,
            category="linear",
            symbol=sym,
            side=close_side,
            orderType="Market",
            qty=str(size),
            positionIdx=idx,
            reduceOnly=True,
        )
        log.info(
            "clean_start.position_closed",
            symbol=sym, side=side, idx=idx, qty=size,
        )
        return True
    except Exception as exc:
        # 110017 ("position is zero") = race; treat as success.
        msg = str(exc).lower()
        if "110017" in msg or "position is zero" in msg:
            log.info(
                "clean_start.position_already_zero",
                symbol=sym, side=side, idx=idx,
            )
            return True
        log.exception(
            "clean_start.close_failed",
            symbol=sym, side=side, idx=idx,
        )
        return False


async def _run(func, *args, **kwargs):
    """Run a sync pybit method in the default executor."""
    import asyncio
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, lambda: func(*args, **kwargs),
    )


def _wipe_db_active_trades(db_path: str) -> int:
    """Mark every non-terminal DB trade as CLOSED with reason
    'clean_start_on_restart'. Returns the number of rows touched."""
    now_iso = datetime.now(timezone.utc).isoformat()
    placeholders = ",".join("?" * len(_NON_TERMINAL_STATES))
    try:
        conn = sqlite3.connect(db_path)
        try:
            cur = conn.execute(
                f"UPDATE trades SET state = 'CLOSED', "
                f"close_reason = 'clean_start_on_restart', "
                f"closed_at = ?, updated_at = ? "
                f"WHERE state IN ({placeholders})",
                (now_iso, now_iso, *_NON_TERMINAL_STATES),
            )
            n = cur.rowcount
            conn.commit()
        finally:
            conn.close()
    except Exception:
        log.exception("clean_start.db_wipe_failed", path=db_path)
        return 0
    return n


async def run_clean_start(bybit_adapter, db_path: str) -> dict:
    """Top-level entry point. Called from main.py during startup.

    Returns a summary dict for the startup log.
    """
    log.info("clean_start.starting")
    summary: dict = {"orders_cancelled": 0, "positions_closed": 0,
                     "positions_failed": 0, "db_rows_closed": 0}

    rest = bybit_adapter._rest
    if rest is None:
        log.warning("clean_start.no_rest_client")
        return summary

    # 1. Cancel every order. Do this BEFORE closing positions so
    #    none of the conditionals/TP-SLs auto-fire mid-close.
    summary["orders_cancelled"] = await _cancel_all_orders(rest)

    # Brief pause so Bybit settles cancellations before we read
    # positions (avoids race where a TP just filled).
    time.sleep(1.0)

    # 2. Close every open position.
    positions = await _fetch_all_positions(rest)
    log.info("clean_start.positions_to_close", count=len(positions))
    for p in positions:
        ok = await _close_position(rest, p)
        if ok:
            summary["positions_closed"] += 1
        else:
            summary["positions_failed"] += 1

    # 3. Wipe DB active trades.
    summary["db_rows_closed"] = _wipe_db_active_trades(db_path)

    log.info("clean_start.complete", **summary)
    return summary
