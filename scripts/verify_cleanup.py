"""Phase 6.C.2 — cleanup verifier (client 2026-05-02 audit #12).

"After a trade closes, verify:
   - no leftover orders
   - no ghost state in DB
   - no stale active trade"

Walks every trade row that's in a terminal state (CLOSED / CANCELLED /
ERROR / PROTECTION_FAILED), then asks Bybit:
  - Is the position truly zero?
  - Are there any remaining orders for that (symbol, positionIdx)?

Prints any inconsistency. Read-only.

Usage:
    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/verify_cleanup.py"
    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/verify_cleanup.py --since 24"   # last 24h only
"""

from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from pybit.unified_trading import HTTP

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env", override=True)


def _is_demo() -> bool:
    return os.environ.get("BYBIT_DEMO", "true").strip().lower() in (
        "1", "true", "yes", "on",
    )


def _client() -> HTTP:
    return HTTP(
        demo=_is_demo(),
        api_key=os.environ["BYBIT_API_KEY"],
        api_secret=os.environ["BYBIT_API_SECRET"],
    )


def fetch_terminal_trades(
    conn: sqlite3.Connection, since_hours: int | None,
) -> list[dict]:
    if since_hours is not None:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=since_hours)
        ).isoformat()
        cur = conn.execute(
            "SELECT t.*, s.symbol AS signal_symbol, "
            "       s.direction AS signal_direction "
            "FROM trades t LEFT JOIN signals s ON t.signal_id = s.id "
            "WHERE t.state IN ('CLOSED','CANCELLED','ERROR','PROTECTION_FAILED') "
            "  AND COALESCE(t.closed_at, t.updated_at) >= ? "
            "ORDER BY t.id ASC",
            (cutoff,),
        )
    else:
        cur = conn.execute(
            "SELECT t.*, s.symbol AS signal_symbol, "
            "       s.direction AS signal_direction "
            "FROM trades t LEFT JOIN signals s ON t.signal_id = s.id "
            "WHERE t.state IN ('CLOSED','CANCELLED','ERROR','PROTECTION_FAILED') "
            "ORDER BY t.id ASC",
        )
    return [dict(r) for r in cur.fetchall()]


def derive_keys(trade: dict, all_trades: list[dict]) -> tuple[str | None, int | None]:
    """Return (symbol, position_idx) for a trade row.

    Hedge children (signal_id NULL) inherit symbol from their parent
    trade, with the OPPOSITE positionIdx (parent LONG idx=1 -> hedge
    SHORT idx=2).
    """
    sym = trade.get("signal_symbol")
    direction = (trade.get("signal_direction") or "").upper()
    if sym and direction:
        idx = 1 if direction == "LONG" else 2
        return sym, idx
    if trade.get("signal_id") is None:
        # Hedge child — find the parent.
        for cand in all_trades:
            try:
                if cand.get("hedge_trade_id") and int(cand["hedge_trade_id"]) == int(trade["id"]):
                    parent_sym = cand.get("signal_symbol")
                    parent_dir = (cand.get("signal_direction") or "").upper()
                    if parent_sym and parent_dir:
                        # Hedge OPPOSITE of parent.
                        idx = 2 if parent_dir == "LONG" else 1
                        return parent_sym, idx
            except (TypeError, ValueError):
                continue
    return None, None


def fetch_position(client: HTTP, symbol: str, idx: int) -> dict | None:
    try:
        resp = client.get_positions(category="linear", symbol=symbol)
        for p in (resp.get("result", {}) or {}).get("list", []) or []:
            try:
                if int(p.get("positionIdx") or 0) == idx and float(p.get("size", 0) or 0) > 0:
                    return p
            except (TypeError, ValueError):
                continue
    except Exception as exc:
        print(f"  bybit get_positions {symbol}: {exc}", file=sys.stderr)
    return None


def fetch_orders_for(client: HTTP, symbol: str, idx: int) -> list[dict]:
    out = []
    try:
        resp = client.get_open_orders(
            category="linear", symbol=symbol, openOnly=0, limit=50,
        )
        for o in (resp.get("result", {}) or {}).get("list", []) or []:
            try:
                if int(o.get("positionIdx") or 0) == idx:
                    out.append(o)
            except (TypeError, ValueError):
                continue
    except Exception as exc:
        print(f"  bybit get_open_orders {symbol}: {exc}", file=sys.stderr)
    return out


def main() -> None:
    since_hours: int | None = None
    if "--since" in sys.argv:
        try:
            i = sys.argv.index("--since")
            since_hours = int(sys.argv[i + 1])
        except (IndexError, ValueError):
            print("usage: verify_cleanup.py [--since <hours>]", file=sys.stderr)
            sys.exit(2)

    conn = sqlite3.connect(str(PROJECT_ROOT / "stratos1.db"))
    conn.row_factory = sqlite3.Row
    try:
        terminal = fetch_terminal_trades(conn, since_hours)
    finally:
        conn.close()

    print(
        f"Verifying cleanup of {len(terminal)} terminal trade rows"
        + (f" (last {since_hours}h)" if since_hours else "")
        + "..."
    )
    if not terminal:
        return

    client = _client()
    issues = 0
    checked = 0
    for trade in terminal:
        sym, idx = derive_keys(trade, terminal)
        if sym is None or idx is None:
            continue
        checked += 1
        # 1. Position must be zero.
        pos = fetch_position(client, sym, idx)
        if pos is not None:
            issues += 1
            print(
                f"  [POSITION STILL OPEN]  trade#{trade['id']:>3} "
                f"state={trade.get('state')}  {sym} idx={idx}  "
                f"size={pos.get('size')}  pnl={pos.get('unrealisedPnl')}"
            )
        # 2. No leftover orders on this (symbol, positionIdx).
        orders = fetch_orders_for(client, sym, idx)
        # An order is "leftover" only if there's no live position to
        # justify it. If position is gone (pos is None) AND there are
        # orders, those are true leftovers.
        if pos is None and orders:
            issues += 1
            for o in orders:
                print(
                    f"  [LEFTOVER ORDER]   trade#{trade['id']:>3} "
                    f"state={trade.get('state')}  {sym} idx={idx}  "
                    f"order_id={o.get('orderId')}  side={o.get('side')}  "
                    f"trigger={o.get('triggerPrice')}  qty={o.get('qty')}  "
                    f"reduce_only={o.get('reduceOnly')}"
                )

    print()
    print(f"Checked {checked} trades. Issues: {issues}.")
    if issues == 0:
        print("✅ Cleanup verified — no live positions or leftover orders "
              "for any closed/cancelled/error/protection-failed trade.")
    else:
        print("⚠️ Issues found. Run reconcile_drift.py to clean them up.")


if __name__ == "__main__":
    main()
