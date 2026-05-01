"""Phase 6.C.1 — state-replay tool (client 2026-05-02 audit #2 + #10).

"Replay the full trade flow from logs:
   signal -> DB -> order -> fill -> TP/SL -> trailing -> close
It must be possible to reproduce the full flow exactly."

Plus audit #10 ("Logging audit"):
"Every trade must have:
   - signal_id, db_id, Bybit order ID, timestamps, state transitions
The full lifecycle must be traceable."

Usage:

    # Replay a trade by its DB id.
    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/replay_trade.py 12"

    # Replay every trade closed in the last 24h.
    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/replay_trade.py --recent 24"

The script reads the trades + events tables and renders a
chronological timeline. It is read-only.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "stratos1.db"


def fetch_trade(conn: sqlite3.Connection, trade_id: int) -> dict | None:
    cur = conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def fetch_signal(conn: sqlite3.Connection, signal_id: int) -> dict | None:
    cur = conn.execute("SELECT * FROM signals WHERE id = ?", (signal_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def fetch_events(conn: sqlite3.Connection, trade_id: int) -> list[dict]:
    cur = conn.execute(
        "SELECT id, event_type, details, created_at "
        "FROM events WHERE trade_id = ? ORDER BY id ASC",
        (trade_id,),
    )
    return [dict(r) for r in cur.fetchall()]


def fetch_orders(conn: sqlite3.Connection, trade_id: int) -> list[dict]:
    cur = conn.execute(
        "SELECT id, order_id_bot, order_id_bybit, side, order_type, "
        "       qty, price, fill_price, fill_qty, status, "
        "       created_at, updated_at "
        "FROM orders WHERE trade_id = ? ORDER BY id ASC",
        (trade_id,),
    )
    return [dict(r) for r in cur.fetchall()]


def fetch_recent_closed(
    conn: sqlite3.Connection, hours: int,
) -> list[int]:
    cutoff = (
        datetime.now(timezone.utc) - timedelta(hours=hours)
    ).isoformat()
    cur = conn.execute(
        "SELECT id FROM trades "
        "WHERE state IN ('CLOSED','CANCELLED','ERROR','PROTECTION_FAILED') "
        "  AND COALESCE(closed_at, updated_at) >= ? "
        "ORDER BY id ASC",
        (cutoff,),
    )
    return [int(r["id"]) for r in cur.fetchall()]


def render_replay(
    trade: dict,
    signal: dict | None,
    events: list[dict],
    orders: list[dict],
) -> str:
    out: list[str] = []
    tid = trade["id"]
    out.append(f"=" * 78)
    out.append(f"TRADE REPLAY  id={tid}  state={trade.get('state')}")
    out.append(f"=" * 78)
    out.append(
        f"signal_id   : {trade.get('signal_id')}  "
        f"hedge_trade_id: {trade.get('hedge_trade_id')}  "
        f"hedge_cond_id: {trade.get('hedge_conditional_order_id')}"
    )
    out.append(
        f"force_close_order_id: {trade.get('original_force_close_order_id')}"
    )
    out.append(
        f"created_at  : {trade.get('created_at')}  "
        f"updated_at: {trade.get('updated_at')}  "
        f"closed_at: {trade.get('closed_at')}"
    )
    out.append(
        f"close_reason: {trade.get('close_reason')}  "
        f"pnl_pct: {trade.get('pnl_pct')}  "
        f"pnl_usdt: {trade.get('pnl_usdt')}"
    )
    out.append(
        f"avg_entry   : {trade.get('avg_entry')}  "
        f"qty: {trade.get('quantity')}  "
        f"leverage: {trade.get('leverage')}  "
        f"margin: {trade.get('margin')}  "
        f"sl: {trade.get('sl_price')}"
    )
    out.append("")

    if signal:
        out.append("--- ORIGINATING SIGNAL ---")
        out.append(
            f"channel : {signal.get('source_channel_name') or signal.get('channel_name')}"
        )
        out.append(
            f"symbol  : {signal.get('symbol')}  direction: {signal.get('direction')}"
        )
        out.append(
            f"entry   : {signal.get('entry_price') or signal.get('entry')}  "
            f"sl: {signal.get('sl_price') or signal.get('sl')}  "
            f"tps: {signal.get('tp_list') or signal.get('tps')}"
        )
        out.append(f"received_at: {signal.get('received_at')}")
        out.append("")
    elif trade.get("signal_id") is None:
        out.append("--- HEDGE CHILD (no originating signal) ---")
        out.append("")

    out.append(f"--- ORDERS ({len(orders)}) ---")
    for o in orders:
        out.append(
            f"[{o.get('created_at')}]  "
            f"bot={o.get('order_id_bot')}  bybit={o.get('order_id_bybit')}  "
            f"{o.get('side')} {o.get('order_type')}  "
            f"qty={o.get('qty')} price={o.get('price')}  "
            f"status={o.get('status')}  fill_price={o.get('fill_price')}  "
            f"fill_qty={o.get('fill_qty')}"
        )
    out.append("")

    out.append(f"--- EVENT TIMELINE ({len(events)}) ---")
    for e in events:
        details_raw = e.get("details") or "{}"
        try:
            details = json.loads(details_raw)
            details_str = json.dumps(details, sort_keys=True)
        except Exception:
            details_str = details_raw
        out.append(
            f"[{e.get('created_at')}]  {e.get('event_type'):<28}  "
            f"{details_str}"
        )
    out.append("")
    return "\n".join(out)


def main() -> None:
    if "--recent" in sys.argv:
        try:
            i = sys.argv.index("--recent")
            hours = int(sys.argv[i + 1])
        except (IndexError, ValueError):
            print("usage: replay_trade.py --recent <hours>", file=sys.stderr)
            sys.exit(2)
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        try:
            ids = fetch_recent_closed(conn, hours)
            if not ids:
                print(f"No closed trades in the last {hours}h.")
                return
            for tid in ids:
                trade = fetch_trade(conn, tid)
                if trade is None:
                    continue
                sig = (
                    fetch_signal(conn, int(trade["signal_id"]))
                    if trade.get("signal_id") is not None else None
                )
                events = fetch_events(conn, tid)
                orders = fetch_orders(conn, tid)
                print(render_replay(trade, sig, events, orders))
        finally:
            conn.close()
        return

    if len(sys.argv) < 2:
        print(
            "usage:\n"
            "  replay_trade.py <trade_id>\n"
            "  replay_trade.py --recent <hours>",
            file=sys.stderr,
        )
        sys.exit(2)

    try:
        tid = int(sys.argv[1])
    except ValueError:
        print(f"Invalid trade_id: {sys.argv[1]}", file=sys.stderr)
        sys.exit(2)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        trade = fetch_trade(conn, tid)
        if trade is None:
            print(f"Trade #{tid} not found in DB.", file=sys.stderr)
            sys.exit(1)
        sig = (
            fetch_signal(conn, int(trade["signal_id"]))
            if trade.get("signal_id") is not None else None
        )
        events = fetch_events(conn, tid)
        orders = fetch_orders(conn, tid)
        print(render_replay(trade, sig, events, orders))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
