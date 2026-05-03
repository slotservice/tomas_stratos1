"""
Clean state — one-command reset of the bot environment.

Run this after every bot update / restart so the operator can verify
fresh behaviour from a known clean baseline (client request 2026-04-28).

What it does:
  1. Closes every open position on Bybit (reduce-only market orders).
  2. Cancels every open Bybit order (regular + stop/conditional).
  3. Marks any bot-DB trade in a non-terminal state as CLOSED with
     reason "manual_cleanup_post_update".
  4. Deletes every Telegram WishingBell message visible to the user
     session.

The script reads credentials from .env in the project root.

Default: DRY-RUN. Counts what would be touched and prints it. No
mutation happens until ``--apply`` is passed. Tomas (client)
2026-05-03: scripts that mutate state must default to dry-run; the
historical drift_cleanup_2026_05_02 incident was a script that ran
without preview and corrupted 26 live trades.

Usage:
  # Preview (default — safe, no mutation):
  cd /opt/stratos1
  venv/bin/python scripts/clean_state.py

  # Actually do it:
  venv/bin/python scripts/clean_state.py --apply
"""

from __future__ import annotations

import argparse
import asyncio
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import load_settings
from pybit.unified_trading import HTTP
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import DeleteMessagesRequest


def _read_env() -> dict[str, str]:
    env: dict[str, str] = {}
    with (ROOT / ".env").open() as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def clean_bybit(apply: bool = False) -> tuple[int, int]:
    """Close all positions + cancel all orders. Returns (closed, cancelled).

    With apply=False (default) the script only counts what would be
    touched — no place_order / cancel_all_orders calls go to Bybit.
    """
    s = load_settings()
    client = HTTP(
        testnet=False,
        demo=s.bybit.demo,
        api_key=s.bybit.api_key,
        api_secret=s.bybit.api_secret,
        recv_window=15000,
    )

    # Paginate — Bybit default limit=20 truncates accounts holding more.
    raw_positions: list = []
    cursor = ""
    while True:
        kwargs = dict(category="linear", settleCoin="USDT", limit=200)
        if cursor:
            kwargs["cursor"] = cursor
        pos_resp = client.get_positions(**kwargs)
        result = pos_resp.get("result", {}) or {}
        batch = result.get("list", []) or []
        raw_positions.extend(batch)
        cursor = result.get("nextPageCursor", "") or ""
        if not cursor or not batch:
            break
    active = [
        p for p in raw_positions
        if float(p.get("size", "0") or 0) > 0
    ]
    closed = 0
    for p in active:
        sym = p["symbol"]
        side = p["side"]
        size = p["size"]
        position_idx = int(p.get("positionIdx") or 0)
        close_side = "Sell" if side == "Buy" else "Buy"
        if not apply:
            print(f"  [dry-run] would close {sym} {side} size={size}")
            closed += 1
            continue
        try:
            client.place_order(
                category="linear",
                symbol=sym,
                side=close_side,
                orderType="Market",
                qty=size,
                reduceOnly=True,
                positionIdx=position_idx,
            )
            closed += 1
        except Exception as exc:
            print(f"  position close ERROR {sym}: {exc}")
        time.sleep(0.05)

    open_orders: list[dict] = []
    cursor = ""
    while True:
        kwargs = {"category": "linear", "settleCoin": "USDT", "limit": 50}
        if cursor:
            kwargs["cursor"] = cursor
        r = client.get_open_orders(**kwargs)
        open_orders.extend(r.get("result", {}).get("list", []))
        cursor = r.get("result", {}).get("nextPageCursor", "")
        if not cursor:
            break

    cancelled = 0
    for sym in sorted({o["symbol"] for o in open_orders}):
        if not apply:
            sym_orders = [o for o in open_orders if o["symbol"] == sym]
            print(f"  [dry-run] would cancel {len(sym_orders)} orders on {sym}")
            cancelled += len(sym_orders)
            continue
        for filt in ("Order", "StopOrder"):
            try:
                resp = client.cancel_all_orders(
                    category="linear", symbol=sym, orderFilter=filt,
                )
                cancelled += len(resp.get("result", {}).get("list", []))
            except Exception as exc:
                print(f"  cancel ERROR {sym} ({filt}): {exc}")
        time.sleep(0.04)

    return closed, cancelled


def clean_db(apply: bool = False) -> int:
    """Mark every non-terminal bot trade as CLOSED with reason
    'manual_cleanup_post_update'. Returns the row count.

    With apply=False (default) only counts the rows that would be
    updated — no DB write happens.
    """
    ghost_states = (
        "POSITION_OPEN", "HEDGE_ACTIVE",
        "SCALING_STEP_1", "SCALING_STEP_2", "SCALING_STEP_3", "SCALING_STEP_4",
        "PENDING",
        "ENTRY1_PLACED", "ENTRY1_FILLED",
        "ENTRY2_PLACED", "ENTRY2_FILLED",
        "BREAKEVEN_ACTIVE", "TRAILING_ACTIVE",
        "REENTRY_WAITING",
    )
    conn = sqlite3.connect(str(ROOT / "stratos1.db"))
    placeholders = ",".join("?" * len(ghost_states))
    rows = list(conn.execute(
        f"SELECT id FROM trades WHERE state IN ({placeholders})", ghost_states,
    ))
    if not apply:
        print(f"  [dry-run] would mark {len(rows)} trades CLOSED")
        conn.close()
        return len(rows)
    conn.execute(
        f"UPDATE trades SET state='CLOSED', "
        f"close_reason='manual_cleanup_post_update', "
        f"closed_at=datetime('now') WHERE state IN ({placeholders})",
        ghost_states,
    )
    conn.commit()
    conn.close()
    return len(rows)


async def clean_wishingbell(apply: bool = False) -> int:
    """Delete every WishingBell message visible to the user session.

    With apply=False (default) just counts the messages that would be
    deleted — no DeleteMessagesRequest is sent.
    """
    env = _read_env()
    api_id = int(env["TG_API_ID"])
    api_hash = env["TG_API_HASH"]
    sess = env["TG_SESSION_STRING"]
    chan_id = int(env["TG_NOTIFY_CHANNEL_ID"])

    client = TelegramClient(StringSession(sess), api_id, api_hash)
    await client.start()
    chan = await client.get_entity(chan_id)

    ids: list[int] = []
    async for msg in client.iter_messages(chan, limit=None):
        ids.append(msg.id)

    if not apply:
        print(f"  [dry-run] would delete {len(ids)} channel messages")
        await client.disconnect()
        return len(ids)

    deleted = 0
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        try:
            await client(DeleteMessagesRequest(channel=chan, id=chunk))
            deleted += len(chunk)
        except Exception as exc:
            print(f"  channel delete chunk error: {exc}")

    await client.disconnect()
    return deleted


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Reset Stratos1 to a clean baseline. "
            "Default: dry-run preview only. Pass --apply to mutate."
        ),
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually mutate state. Without this flag, only counts.",
    )
    args = parser.parse_args()
    apply = args.apply

    mode = "APPLYING" if apply else "DRY-RUN"
    print(f"== Stratos1 clean_state ({mode}) ==")
    if not apply:
        print("  (no mutation will happen — re-run with --apply to execute)")
    closed, cancelled = clean_bybit(apply=apply)
    print(f"Bybit: closed={closed} positions, cancelled={cancelled} orders")

    db_rows = clean_db(apply=apply)
    print(f"DB:    marked {db_rows} ghost trades CLOSED")

    deleted = await clean_wishingbell(apply=apply)
    print(f"WishingBell: deleted {deleted} messages")

    if not apply:
        print()
        print("DRY-RUN complete. Re-run with --apply to actually do it.")
    else:
        print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
