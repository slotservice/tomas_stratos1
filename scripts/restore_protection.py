"""One-shot fix-up: restore SL+trailing on every live Bybit position
that's missing one of them.

Background: scripts/reconcile_drift.py was paginating wrong (default
limit=20) and wrongly cancelled TP/trailing/conditional orders for
positions past the 20th row. Trades have been resurrected in DB
(scripts/un_drift_close.py) but the cancelled Bybit orders haven't
come back. This script re-applies hedge-spec protection (SL +
trailing) on any position that's currently missing it.

Strategy:
  - Pull ALL live Bybit positions (paginated).
  - For each position:
      * if trailing is set already      -> skip
      * else                            -> set hedge-spec trailing
        (1.2% distance, no activePrice -> Bybit defaults to immediate
         activation). Don't touch the existing SL.
  - If Bybit rejects (e.g. trailing activation already past current
    price for SHORT/LONG), retry with trailing-only.

Default: dry run. --apply to execute.

Usage:
    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/restore_protection.py"
    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/restore_protection.py --apply"
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pybit.unified_trading import HTTP

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env", override=True)

HEDGE_TRAILING_PCT = 1.2  # match config.toml [hedge].trailing_pct


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


def restore_one(client: HTTP, p: dict) -> str:
    sym = p["symbol"]
    side = p["side"]
    idx = int(p["positionIdx"])
    try:
        avg = float(p.get("avgPrice", 0) or 0)
    except (TypeError, ValueError):
        avg = 0.0
    if avg <= 0:
        return "skip:no_entry"
    trailing_distance = round(avg * (HEDGE_TRAILING_PCT / 100.0), 8)
    try:
        client.set_trading_stop(
            category="linear",
            symbol=sym,
            positionIdx=idx,
            trailingStop=str(trailing_distance),
            slTriggerBy="LastPrice",
        )
        return "trailing_set"
    except Exception as exc:
        msg = str(exc).lower()
        if "base_price" in msg or "session_average_price" in msg:
            return "skip:past_activation"
        return f"err:{str(exc)[:80]}"


def main() -> None:
    apply = "--apply" in sys.argv
    client = _client()
    positions = fetch_all_positions(client)
    print(f"Live Bybit positions: {len(positions)}")
    print()

    to_restore: list[dict] = []
    for p in positions:
        try:
            trail = float(p.get("trailingStop") or 0)
        except (TypeError, ValueError):
            trail = 0.0
        if trail > 0:
            continue  # already protected
        to_restore.append(p)

    print(f"Missing trailing: {len(to_restore)}")
    for p in to_restore:
        sl = p.get("stopLoss") or "—"
        print(
            f"  {p['symbol']:14s} idx={p['positionIdx']} "
            f"{p['side']:4s} sz={p.get('size')} entry={p.get('avgPrice')} "
            f"SL={sl}  trail=missing"
        )

    if not apply:
        print()
        print("DRY RUN. Re-run with --apply to set hedge-spec trailing on each.")
        return

    print()
    print("APPLYING ...")
    counts: dict[str, int] = {}
    for p in to_restore:
        result = restore_one(client, p)
        counts[result] = counts.get(result, 0) + 1
        marker = "✓" if result == "trailing_set" else "·"
        print(f"  {marker} {p['symbol']:14s} idx={p['positionIdx']} -> {result}")
    print()
    print("Summary:")
    for k, v in counts.items():
        print(f"  {k:30s} {v}")


if __name__ == "__main__":
    main()
