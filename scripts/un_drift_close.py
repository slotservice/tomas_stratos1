"""One-shot fix-up: reverse the buggy drift_cleanup_2026_05_02 closes.

Background: scripts/reconcile_drift.py was fetching only the first 20
positions from Bybit (default limit, no pagination). Accounts holding
more than 20 positions had their later positions look 'missing' to the
script, which then wrongly marked the corresponding DB trades as
CLOSED with close_reason='drift_cleanup_2026_05_02'.

This script:
  1. Reads every DB trade with close_reason='drift_cleanup_2026_05_02'.
  2. Pulls the FULL Bybit position set (paginated).
  3. For each closed trade whose underlying position is still alive on
     Bybit (matching symbol + side + positionIdx), restores the trade
     to POSITION_OPEN and clears the close fields.

Default: dry run. Pass --apply to actually update the DB.

Usage:
    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/un_drift_close.py"
    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/un_drift_close.py --apply"
"""

from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from pybit.unified_trading import HTTP

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env", override=True)
DB_PATH = PROJECT_ROOT / "stratos1.db"


def _client() -> HTTP:
    demo = os.environ.get("BYBIT_DEMO", "true").strip().lower() in (
        "1", "true", "yes", "on",
    )
    return HTTP(
        demo=demo,
        api_key=os.environ["BYBIT_API_KEY"],
        api_secret=os.environ["BYBIT_API_SECRET"],
    )


def fetch_all_positions(client: HTTP) -> list[dict]:
    out: list[dict] = []
    cursor = ""
    while True:
        kwargs = dict(category="linear", settleCoin="USDT", limit=200)
        if cursor:
            kwargs["cursor"] = cursor
        resp = client.get_positions(**kwargs)
        result = resp.get("result", {}) or {}
        batch = result.get("list", []) or []
        out.extend(batch)
        cursor = result.get("nextPageCursor", "") or ""
        if not cursor or not batch:
            break
    return [p for p in out if float(p.get("size", 0) or 0) > 0]


def main() -> None:
    apply = "--apply" in sys.argv

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    by_id_trade: dict[int, dict] = {}

    cur = conn.execute(
        "SELECT t.id, t.signal_id, t.hedge_trade_id, t.state, "
        "       t.close_reason, t.closed_at, "
        "       s.symbol AS sig_symbol, s.direction AS sig_direction "
        "FROM trades t LEFT JOIN signals s ON t.signal_id = s.id "
        "WHERE t.close_reason = 'drift_cleanup_2026_05_02'"
    )
    candidates = [dict(r) for r in cur.fetchall()]

    # Also load every trade for parent lookups (hedge children).
    cur2 = conn.execute(
        "SELECT t.id, t.signal_id, t.hedge_trade_id, "
        "       s.symbol AS sig_symbol, s.direction AS sig_direction "
        "FROM trades t LEFT JOIN signals s ON t.signal_id = s.id"
    )
    for r in cur2.fetchall():
        by_id_trade[int(r["id"])] = dict(r)

    client = _client()
    positions = fetch_all_positions(client)
    pos_keys = {
        (p.get("symbol"), p.get("side"), int(p.get("positionIdx") or 0))
        for p in positions
    }

    print(f"Bybit live positions: {len(positions)}")
    print(f"Drift-closed candidates: {len(candidates)}")
    print()

    to_resurrect: list[dict] = []
    for t in candidates:
        sym = t.get("sig_symbol")
        direction = (t.get("sig_direction") or "").upper()
        if sym and direction:
            side = "Buy" if direction == "LONG" else "Sell"
            idx = 1 if direction == "LONG" else 2
        else:
            # Hedge child — find parent.
            sym = side = None
            idx = None
            for cand in by_id_trade.values():
                try:
                    if cand.get("hedge_trade_id") and int(cand["hedge_trade_id"]) == int(t["id"]):
                        psym = cand.get("sig_symbol")
                        pdir = (cand.get("sig_direction") or "").upper()
                        if psym and pdir:
                            sym = psym
                            side = "Buy" if pdir == "SHORT" else "Sell"
                            idx = 2 if pdir == "LONG" else 1
                        break
                except (TypeError, ValueError):
                    continue
        if not sym or not side or idx is None:
            print(f"  trade #{t['id']}: SKIP — could not resolve sym/side/idx")
            continue
        key = (sym, side, idx)
        if key in pos_keys:
            print(f"  trade #{t['id']}: ALIVE on Bybit ({sym} {side} idx={idx}) — will resurrect")
            to_resurrect.append(t)
        else:
            print(f"  trade #{t['id']}: not on Bybit ({sym} {side} idx={idx}) — leave CLOSED")

    print()
    print(f"To resurrect: {len(to_resurrect)}")

    if not apply:
        print()
        print("DRY RUN. Re-run with --apply to mark these trades back to POSITION_OPEN.")
        conn.close()
        return

    print()
    print("APPLYING ...")
    for t in to_resurrect:
        try:
            conn.execute(
                "UPDATE trades SET state = ?, close_reason = NULL, "
                "closed_at = NULL, updated_at = ? WHERE id = ?",
                (
                    "POSITION_OPEN",
                    datetime.now(timezone.utc).isoformat(),
                    t["id"],
                ),
            )
            print(f"  resurrected trade #{t['id']}")
        except Exception as exc:
            print(f"  FAILED on trade #{t['id']}: {exc}", file=sys.stderr)
    conn.commit()
    conn.close()
    print()
    print(f"Done. Resurrected {len(to_resurrect)} trades.")


if __name__ == "__main__":
    main()
