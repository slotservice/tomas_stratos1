"""Wallet-balance reconciliation (client 2026-05-02 question:
'why is my wallet down so much when the report says small losses?').

Bridges Bybit's actual wallet balance change against every contributor:

    start_equity
    + deposits/transfers in
    - withdrawals/transfers out
    + realized PnL  (closed_pnl, NET of trading fees)
    - funding payments (positions held across funding intervals)
    + unrealised PnL (currently open positions)
    + cash adjustments (Bybit demo top-ups, fee refunds, etc.)
    = end_equity (must match current wallet)

Pulls every line from Bybit and explains the gap. Read-only.

Usage:
    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/wallet_reconcile.py 24"   # last 24h
    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/wallet_reconcile.py"      # default 24h
"""

from __future__ import annotations

import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
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


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fmt_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(
        int(ts_ms) / 1000, tz=timezone.utc,
    ).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Pulls
# ---------------------------------------------------------------------------

def fetch_wallet_now(client: HTTP) -> dict:
    resp = client.get_wallet_balance(accountType="UNIFIED")
    items = (resp.get("result", {}) or {}).get("list", []) or []
    return items[0] if items else {}


def fetch_closed_pnl(client: HTTP, since_ms: int, until_ms: int) -> list:
    out: list = []
    cursor = ""
    while True:
        kwargs = {
            "category": "linear",
            "startTime": since_ms,
            "endTime": until_ms,
            "limit": 100,
        }
        if cursor:
            kwargs["cursor"] = cursor
        resp = client.get_closed_pnl(**kwargs)
        result = resp.get("result", {}) or {}
        items = result.get("list", []) or []
        out.extend(items)
        cursor = result.get("nextPageCursor", "") or ""
        if not cursor or not items:
            break
    return out


def fetch_executions(
    client: HTTP, since_ms: int, until_ms: int,
) -> list:
    """Trading + funding executions. cumExecFee on each row is the
    trading fee for that fill; execType=Funding rows are funding."""
    out: list = []
    cursor = ""
    while True:
        kwargs = {
            "category": "linear",
            "startTime": since_ms,
            "endTime": until_ms,
            "limit": 100,
        }
        if cursor:
            kwargs["cursor"] = cursor
        try:
            resp = client.get_executions(**kwargs)
        except Exception as exc:
            print(f"  get_executions failed: {exc}", file=sys.stderr)
            break
        result = resp.get("result", {}) or {}
        items = result.get("list", []) or []
        out.extend(items)
        cursor = result.get("nextPageCursor", "") or ""
        if not cursor or not items:
            break
    return out


def fetch_transactions(
    client: HTTP, since_ms: int, until_ms: int,
) -> list:
    """Wallet transaction log: deposits / withdrawals / fee
    deductions / funding / settlement / etc. The unified
    transaction-log endpoint covers all balance-impacting events."""
    out: list = []
    cursor = ""
    while True:
        kwargs = {
            "accountType": "UNIFIED",
            "category": "linear",
            "startTime": since_ms,
            "endTime": until_ms,
            "limit": 50,
        }
        if cursor:
            kwargs["cursor"] = cursor
        try:
            resp = client.get_transaction_log(**kwargs)
        except Exception as exc:
            print(f"  get_transaction_log failed: {exc}", file=sys.stderr)
            break
        result = resp.get("result", {}) or {}
        items = result.get("list", []) or []
        out.extend(items)
        cursor = result.get("nextPageCursor", "") or ""
        if not cursor or not items:
            break
    return out


def fetch_open_positions(client: HTTP) -> list:
    resp = client.get_positions(category="linear", settleCoin="USDT")
    raw = (resp.get("result", {}) or {}).get("list", []) or []
    return [p for p in raw if float(p.get("size", 0) or 0) > 0]


# ---------------------------------------------------------------------------
# Reconcile
# ---------------------------------------------------------------------------

def reconcile(
    wallet_now: dict,
    closed: list,
    executions: list,
    transactions: list,
    positions: list,
    since_ms: int,
) -> dict:
    """Aggregate every contributor + show the bridge from start->end."""
    # Closed PnL (already net of trading fees on Bybit's side).
    realized_pnl = 0.0
    for c in closed:
        try:
            realized_pnl += float(c.get("closedPnl", 0) or 0)
        except (TypeError, ValueError):
            pass

    # Trading fees (informational — already INCLUDED in realized PnL).
    trading_fees = 0.0
    for e in executions:
        if (e.get("execType") or "") == "Trade":
            try:
                trading_fees += float(e.get("execFee", 0) or 0)
            except (TypeError, ValueError):
                pass

    # Funding (NOT in realized PnL; charged separately every 8h).
    # On Bybit, funding is recorded both via execType=Funding in
    # executions AND via type=SETTLEMENT in transaction log. We use
    # the transaction log because it's the canonical balance impact.
    funding_total = 0.0
    settle_total = 0.0
    for tx in transactions:
        ttype = (tx.get("type") or "").upper()
        try:
            change = float(tx.get("change", 0) or 0)
        except (TypeError, ValueError):
            change = 0.0
        if ttype in ("SETTLEMENT", "FUNDING_FEE", "INTEREST"):
            funding_total += change
        elif ttype == "SETTLEMENT" and change == 0:
            pass
        # Track 'SETTLEMENT' as funding-related; a separate explicit
        # field would help if we want to separate maintenance interest
        # from periodic funding. Bybit lumps them under SETTLEMENT in
        # the unified transaction log.

    # Deposits / withdrawals / cash transfers / demo top-ups.
    deposits = 0.0
    withdrawals = 0.0
    cash_adjustments = 0.0
    for tx in transactions:
        ttype = (tx.get("type") or "").upper()
        try:
            change = float(tx.get("change", 0) or 0)
        except (TypeError, ValueError):
            change = 0.0
        if ttype in ("TRANSFER_IN", "DEPOSIT"):
            deposits += change
        elif ttype in ("TRANSFER_OUT", "WITHDRAW"):
            withdrawals += abs(change)
        elif ttype in ("BONUS", "AIRDROP", "ADJUSTMENT"):
            cash_adjustments += change

    # Unrealized PnL on open positions.
    unrealized = 0.0
    for p in positions:
        try:
            unrealized += float(p.get("unrealisedPnl", 0) or 0)
        except (TypeError, ValueError):
            pass

    # End equity (live).
    try:
        end_equity = float(wallet_now.get("totalEquity", 0) or 0)
    except (TypeError, ValueError):
        end_equity = 0.0

    # Start equity (back-derived). The bridge:
    #   start + deposits - withdrawals + realized + funding +
    #   cash_adjustments + (current unrealized) = end_equity
    # solve for start:
    start_equity = (
        end_equity
        - deposits
        + withdrawals
        - realized_pnl
        - funding_total
        - cash_adjustments
        - unrealized
    )

    return {
        "start_equity": start_equity,
        "deposits": deposits,
        "withdrawals": withdrawals,
        "realized_pnl": realized_pnl,
        "trading_fees": trading_fees,
        "funding_total": funding_total,
        "cash_adjustments": cash_adjustments,
        "unrealized": unrealized,
        "end_equity": end_equity,
        "executions_n": len(executions),
        "transactions_n": len(transactions),
        "closed_n": len(closed),
        "positions_n": len(positions),
    }


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

def render(rec: dict, hours: int) -> str:
    L = []
    L.append("# Wallet reconciliation")
    L.append("")
    L.append(f"Window: last {hours}h")
    L.append("")
    L.append("```")
    L.append(f"  Start equity (back-derived) : {rec['start_equity']:>+12.2f} USDT")
    L.append(f"+ Deposits / transfers in    : {rec['deposits']:>+12.2f} USDT")
    L.append(f"- Withdrawals / transfers out: {rec['withdrawals']:>+12.2f} USDT")
    L.append(f"+ Realized PnL (net of fees) : {rec['realized_pnl']:>+12.2f} USDT")
    L.append(f"+/- Funding payments         : {rec['funding_total']:>+12.2f} USDT")
    L.append(f"+/- Cash adjustments         : {rec['cash_adjustments']:>+12.2f} USDT")
    L.append(f"+ Unrealized PnL (open pos)  : {rec['unrealized']:>+12.2f} USDT")
    L.append(f"-----------------------------------------------")
    L.append(f"= End equity (live wallet)   : {rec['end_equity']:>+12.2f} USDT")
    L.append("```")
    L.append("")
    L.append("Notes:")
    L.append(
        "- Realized PnL is Bybit's `closedPnl` summed across the "
        "window. Bybit reports this NET of trading fees per trade."
    )
    L.append(
        f"- Trading fees this window: {rec['trading_fees']:.4f} USDT "
        "(informational — already deducted from Realized PnL)."
    )
    L.append(
        "- Funding is charged every 8h on open positions; not in "
        "Realized PnL. Negative = bot paid funding, positive = "
        "bot received funding."
    )
    L.append(
        "- Unrealized PnL is the live mark-to-market of currently "
        "open positions. Locks into Realized PnL only when each "
        "position closes."
    )
    L.append(
        "- Start equity is back-derived from end_equity minus every "
        "tracked contributor. If it looks wrong, it usually means a "
        "transaction type wasn't classified (check the raw "
        "transaction log)."
    )
    L.append("")
    L.append(
        f"Counts: {rec['closed_n']} closed records, "
        f"{rec['executions_n']} executions, "
        f"{rec['transactions_n']} transactions, "
        f"{rec['positions_n']} open positions."
    )
    return "\n".join(L)


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------

def main() -> None:
    hours = 24
    if len(sys.argv) >= 2 and sys.argv[1].isdigit():
        hours = int(sys.argv[1])
    until_ms = _now_ms()
    since_ms = until_ms - hours * 3600 * 1000

    client = _client()
    wallet_now = fetch_wallet_now(client)
    closed = fetch_closed_pnl(client, since_ms, until_ms)
    executions = fetch_executions(client, since_ms, until_ms)
    transactions = fetch_transactions(client, since_ms, until_ms)
    positions = fetch_open_positions(client)
    rec = reconcile(
        wallet_now=wallet_now,
        closed=closed,
        executions=executions,
        transactions=transactions,
        positions=positions,
        since_ms=since_ms,
    )
    print(render(rec, hours))


if __name__ == "__main__":
    main()
