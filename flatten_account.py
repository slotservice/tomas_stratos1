"""One-shot account flattener.

Stops the world: cancels every open order (limit / conditional / TP-SL /
trailing-stop) and closes every open position via Market reduce-only.

Used to wipe Bybit-side state before testing a new bot model so old
trades don't behave inconsistently with new rules.

Connects via the same .env credentials the bot uses. Demo / live is
controlled by BYBIT_DEMO. Print-only — no Telegram messages, no DB
writes.
"""

from __future__ import annotations

import asyncio
import io
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from pybit.unified_trading import HTTP


PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env", override=True)

# Force UTF-8 stdout so we can print Bybit's exception messages
# (which can contain Unicode arrows, em-dashes, etc.) on Windows
# without a UnicodeEncodeError.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  errors="replace", line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8",
                                  errors="replace", line_buffering=True)

CATEGORY = "linear"
SETTLE_COIN = "USDT"


def _is_demo() -> bool:
    return os.environ.get("BYBIT_DEMO", "true").strip().lower() in (
        "1", "true", "yes", "on",
    )


def _http() -> HTTP:
    api_key = os.environ["BYBIT_API_KEY"]
    api_secret = os.environ["BYBIT_API_SECRET"]
    return HTTP(
        demo=_is_demo(),
        api_key=api_key,
        api_secret=api_secret,
    )


def cancel_all_orders(client: HTTP) -> int:
    """Cancel every open order across all symbols. Returns total cancelled."""
    print("\n=== Cancelling open orders ===")
    total = 0

    # Bybit V5: cancel_all_orders accepts orderFilter to target each
    # bucket. "Order" covers active limit/market orders; "StopOrder"
    # covers conditional orders, TP/SL, and trailing stops. (tpslOrder
    # is documented for some endpoints but rejected by cancel-all-orders.)
    filters = ["Order", "StopOrder"]
    for f in filters:
        try:
            resp = client.cancel_all_orders(
                category=CATEGORY,
                settleCoin=SETTLE_COIN,
                orderFilter=f,
            )
            ret_code = resp.get("retCode", -1)
            ret_msg = resp.get("retMsg", "")
            result = resp.get("result", {})
            cancelled = result.get("list") or []
            print(f"  filter={f:>14}  retCode={ret_code}  "
                  f"cancelled={len(cancelled)}  msg={ret_msg}")
            total += len(cancelled)
        except Exception as exc:
            print(f"  filter={f:>14}  ERROR: {exc}")
        time.sleep(0.4)

    print(f"Total orders cancelled: {total}")
    return total


def list_open_positions(client: HTTP) -> list[dict]:
    """Paginate get_positions to return every open position."""
    out: list[dict] = []
    cursor = ""
    for _ in range(50):  # cap pagination
        kwargs = {
            "category": CATEGORY,
            "settleCoin": SETTLE_COIN,
            "limit": 200,
        }
        if cursor:
            kwargs["cursor"] = cursor
        try:
            resp = client.get_positions(**kwargs)
        except Exception as exc:
            print(f"  get_positions error: {exc}")
            break
        result = resp.get("result", {})
        chunk = result.get("list") or []
        for p in chunk:
            try:
                if float(p.get("size") or 0) > 0:
                    out.append(p)
            except (TypeError, ValueError):
                continue
        cursor = result.get("nextPageCursor", "") or ""
        if not cursor or not chunk:
            break
        time.sleep(0.2)
    return out


def close_all_positions(client: HTTP) -> int:
    """Close every open position with Market reduce-only. Returns count."""
    print("\n=== Closing open positions ===")
    positions = list_open_positions(client)
    print(f"Found {len(positions)} open positions")

    closed = 0
    for p in positions:
        try:
            sym = p.get("symbol", "")
            side = p.get("side", "")
            size = float(p.get("size") or 0)
            position_idx = int(p.get("positionIdx") or 0)
            if not sym or not side or size <= 0:
                continue
            close_side = "Sell" if side == "Buy" else "Buy"
            qty_str = f"{size:.10f}".rstrip("0").rstrip(".")
            resp = client.place_order(
                category=CATEGORY,
                symbol=sym,
                side=close_side,
                orderType="Market",
                qty=qty_str,
                positionIdx=position_idx,
                reduceOnly=True,
            )
            ret_code = resp.get("retCode", -1)
            ret_msg = resp.get("retMsg", "")
            if ret_code == 0:
                print(f"  closed: {sym:14} side={side:4} idx={position_idx} "
                      f"qty={qty_str}")
                closed += 1
            else:
                print(f"  FAIL  : {sym:14} side={side:4} idx={position_idx} "
                      f"code={ret_code} msg={ret_msg}")
        except Exception as exc:
            print(f"  ERROR : {p.get('symbol', '?')}: {exc}")
        time.sleep(0.15)

    print(f"Total positions closed: {closed} / {len(positions)}")
    return closed


def reset_local_db() -> None:
    """Move stratos1.db aside so the bot starts with a fresh DB."""
    db_path = PROJECT_ROOT / "stratos1.db"
    if not db_path.exists():
        print("\n=== Local DB ===\n  No stratos1.db to reset.")
        return
    backup = PROJECT_ROOT / f"stratos1.db.flatten_{int(time.time())}.bak"
    db_path.rename(backup)
    print(f"\n=== Local DB ===\n  Renamed {db_path.name} -> {backup.name}")
    print("  A fresh DB will be created on next bot start.")


def main() -> None:
    if not os.environ.get("BYBIT_API_KEY") or not os.environ.get("BYBIT_API_SECRET"):
        print("ERROR: BYBIT_API_KEY / BYBIT_API_SECRET not set in .env")
        sys.exit(1)

    print("Stratos1 — flatten_account.py")
    print(f"  mode  : {'DEMO' if _is_demo() else 'LIVE'}")
    print(f"  scope : category={CATEGORY}  settleCoin={SETTLE_COIN}")

    client = _http()

    cancel_all_orders(client)
    close_all_positions(client)

    # Verify nothing remains.
    print("\n=== Verification ===")
    leftover_positions = list_open_positions(client)
    print(f"  positions still open: {len(leftover_positions)}")
    if leftover_positions:
        for p in leftover_positions[:10]:
            print(f"    {p.get('symbol')} side={p.get('side')} "
                  f"size={p.get('size')}")

    # Optional DB reset (commented out so the operator opts in).
    # Uncomment the next line if you also want a fresh local DB.
    # reset_local_db()

    print("\nDone.")


if __name__ == "__main__":
    main()
