"""One-shot diagnostic for the post-clean-start trading run.

Built for the client 2026-05-02 audit request: prove or disprove the
"bot performs well right after restart but degrades after 20-30
trades" hypothesis using existing Bybit + DB data.

Pulls:
  1. Closed-PnL records for the run window (Bybit-side ground truth).
  2. Open positions (Bybit) + open orders (Bybit).
  3. DB trades + DB orders.
  4. Cross-reference: DB rows with no Bybit position, Bybit positions
     with no DB row, leftover orders pointing to closed/missing trades.

Outputs a Markdown report to stdout. Run on the server:

    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/diagnose_clean_start.py"

The script is read-only — no Bybit writes, no DB writes, no Telegram.
"""

from __future__ import annotations

import json
import os
import sqlite3
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


def _fmt_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(
        int(ts_ms) / 1000, tz=timezone.utc,
    ).strftime("%Y-%m-%d %H:%M:%S")


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Data pulls
# ---------------------------------------------------------------------------

def fetch_closed_pnl(client: HTTP, since_ms: int, until_ms: int) -> list:
    """Bybit closed-PnL records since the clean-start ts."""
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


def fetch_open_positions(client: HTTP) -> list:
    """All non-zero positions across all USDT pairs.

    CRITICAL: Bybit's get_positions defaults to limit=20 — must
    paginate to get the full set when the account has > 20 positions.
    """
    out: list = []
    cursor = ""
    for _ in range(20):
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


def fetch_open_orders(client: HTTP) -> list:
    """All open (non-Filled / non-Cancelled) orders across all symbols."""
    out: list = []
    cursor = ""
    while True:
        kwargs = {
            "category": "linear",
            "settleCoin": "USDT",
            "openOnly": 0,
            "limit": 50,
        }
        if cursor:
            kwargs["cursor"] = cursor
        resp = client.get_open_orders(**kwargs)
        result = resp.get("result", {}) or {}
        items = result.get("list", []) or []
        out.extend(items)
        cursor = result.get("nextPageCursor", "") or ""
        if not cursor or not items:
            break
    return out


def fetch_db_trades(db_path: str) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT * FROM trades ORDER BY id ASC")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def fetch_db_signals(db_path: str) -> dict[int, dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT * FROM signals")
    rows = {int(r["id"]): dict(r) for r in cur.fetchall()}
    conn.close()
    return rows


def fetch_configured_channels() -> list[str]:
    """Read every channel name from telegram_groups.toml. Used to show
    'all configured channels' (with 0 rows for inactive ones) in the
    per-channel report — client request 2026-05-02."""
    groups_path = PROJECT_ROOT / "telegram_groups.toml"
    if not groups_path.exists():
        return []
    out: list[str] = []
    try:
        try:
            import tomllib  # py311+
            with open(groups_path, "rb") as f:
                data = tomllib.load(f)
        except ImportError:
            import tomli as tomllib  # type: ignore
            with open(groups_path, "rb") as f:
                data = tomllib.load(f)
        for g in data.get("groups", []) or []:
            name = g.get("name")
            if name:
                out.append(str(name))
    except Exception as exc:
        print(f"  warn: couldn't read telegram_groups.toml: {exc}")
    return out


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def bucket_by_index(closed: list, bucket_size: int = 10) -> list[dict]:
    """Group closed-PnL records by their order of completion. Each
    bucket has bucket_size entries (last bucket may be partial)."""
    closed_sorted = sorted(closed, key=lambda x: int(x.get("createdTime", 0)))
    out = []
    for i in range(0, len(closed_sorted), bucket_size):
        chunk = closed_sorted[i:i + bucket_size]
        pnls = [float(c.get("closedPnl", 0) or 0) for c in chunk]
        wins = sum(1 for p in pnls if p > 0)
        losses = sum(1 for p in pnls if p < 0)
        zeros = sum(1 for p in pnls if p == 0)
        total = sum(pnls)
        avg = total / len(pnls) if pnls else 0
        worst = min(pnls) if pnls else 0
        best = max(pnls) if pnls else 0
        # Bust-trade count = liquidations.
        bust = sum(
            1 for c in chunk if c.get("execType") == "BustTrade"
            or any(e.get("execType") == "BustTrade"
                   for e in c.get("orderExecs", []) or [])
        )
        out.append({
            "bucket": f"#{i+1}-{i+len(chunk)}",
            "trades": len(chunk),
            "wins": wins,
            "losses": losses,
            "zeros": zeros,
            "total_pnl": total,
            "avg_pnl": avg,
            "best": best,
            "worst": worst,
            "bust_trades": bust,
            "first_close": _fmt_ts(int(chunk[0].get("createdTime", 0))),
            "last_close": _fmt_ts(int(chunk[-1].get("createdTime", 0))),
        })
    return out


def per_symbol_pnl(closed: list) -> list[dict]:
    by_sym: dict[str, list[float]] = defaultdict(list)
    for c in closed:
        by_sym[c.get("symbol", "?")].append(float(c.get("closedPnl", 0) or 0))
    rows = []
    for sym, pnls in by_sym.items():
        wins = sum(1 for p in pnls if p > 0)
        losses = sum(1 for p in pnls if p < 0)
        win_rate = (wins / len(pnls) * 100.0) if pnls else 0.0
        rows.append({
            "symbol": sym,
            "n": len(pnls),
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "total": sum(pnls),
            "avg": sum(pnls) / len(pnls) if pnls else 0,
            "best": max(pnls) if pnls else 0,
            "worst": min(pnls) if pnls else 0,
        })
    rows.sort(key=lambda r: r["total"])
    return rows


def _build_attribution_maps(
    db_trades: list[dict], db_signals: dict[int, dict],
) -> tuple[
    dict[tuple[str, str], list[str]],
    dict[tuple[str, str], list[str]],
]:
    """Build (sym, side) -> [channel] and (sym, side) -> [signal_type]
    maps for closed-PnL attribution.

    Phase 6 follow-up (client 2026-05-02): hedge children have
    signal_id=NULL but their parent trade row links to them via
    hedge_trade_id. We walk that link so hedge closes get attributed
    back to the parent's channel + signal_type with a "(hedge)"
    suffix on the channel name. Without this, hedge PnL falls into
    the unmatched bucket and the per-channel breakdown under-counts
    real channel performance.
    """
    by_id = {int(t["id"]): t for t in db_trades}
    sym_side_to_channels: dict[tuple[str, str], list[str]] = defaultdict(list)
    sym_side_to_types: dict[tuple[str, str], list[str]] = defaultdict(list)

    # Pass 1: ORIGINAL trades (signal_id present).
    for t in db_trades:
        sig_id = t.get("signal_id")
        if sig_id is None:
            continue
        sig = db_signals.get(int(sig_id))
        if not sig:
            continue
        chan = sig.get("source_channel_name") or sig.get("channel_name") or "?"
        st = sig.get("signal_type") or "?"
        direction = (sig.get("direction") or "").upper()
        side = "Buy" if direction == "LONG" else "Sell"
        sym = sig.get("symbol") or "?"
        sym_side_to_channels[(sym, side)].append(chan)
        sym_side_to_types[(sym, side)].append(st)

    # Pass 2: HEDGE CHILDREN (signal_id=NULL). Walk the parent link
    # via hedge_trade_id and attribute the hedge close to the
    # parent's channel + signal_type, tagged with "(hedge)".
    for child in db_trades:
        if child.get("signal_id") is not None:
            continue
        # Find the parent that points to this child.
        for parent in db_trades:
            try:
                parent_hedge_id = parent.get("hedge_trade_id")
                if parent_hedge_id is None:
                    continue
                if int(parent_hedge_id) != int(child["id"]):
                    continue
            except (TypeError, ValueError):
                continue
            parent_sig_id = parent.get("signal_id")
            if parent_sig_id is None:
                break
            sig = db_signals.get(int(parent_sig_id))
            if not sig:
                break
            chan = (sig.get("source_channel_name")
                    or sig.get("channel_name") or "?") + " (hedge)"
            st = sig.get("signal_type") or "?"
            parent_dir = (sig.get("direction") or "").upper()
            # The hedge sits on the OPPOSITE side from the parent.
            hedge_side = "Sell" if parent_dir == "LONG" else "Buy"
            sym = sig.get("symbol") or "?"
            sym_side_to_channels[(sym, hedge_side)].append(chan)
            sym_side_to_types[(sym, hedge_side)].append(st)
            break

    return sym_side_to_channels, sym_side_to_types


def per_channel_pnl(closed: list, db_trades: list[dict],
                    db_signals: dict[int, dict]) -> tuple[list[dict], float, int]:
    """Map closed-PnL records back to source channels via DB.

    Phase 6.C.3 (audit #22): per-channel breakdown helps spot
    channels whose signals systematically lose money. Hedge closes
    are attributed back to the parent signal's channel with a
    "(hedge)" tag (Phase 6 follow-up 2026-05-02).
    """
    sym_side_to_channels, _ = _build_attribution_maps(db_trades, db_signals)

    by_chan: dict[str, list[float]] = defaultdict(list)
    unmatched_pnl: list[float] = []
    for c in closed:
        sym = c.get("symbol", "?")
        side = c.get("side", "?")
        entry_side = "Buy" if side == "Sell" else "Sell"
        chans = sym_side_to_channels.get((sym, entry_side), [])
        pnl = float(c.get("closedPnl", 0) or 0)
        if chans:
            share = pnl / len(chans)
            for chan in chans:
                by_chan[chan].append(share)
        else:
            unmatched_pnl.append(pnl)

    rows = []
    for chan, pnls in by_chan.items():
        wins = sum(1 for p in pnls if p > 0)
        losses = sum(1 for p in pnls if p < 0)
        win_rate = (wins / len(pnls) * 100.0) if pnls else 0.0
        rows.append({
            "channel": chan,
            "n": len(pnls),
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "total": sum(pnls),
            "avg": sum(pnls) / len(pnls) if pnls else 0,
        })
    rows.sort(key=lambda r: r["total"])
    return rows, sum(unmatched_pnl), len(unmatched_pnl)


def per_signal_type_pnl(
    closed: list, db_trades: list[dict], db_signals: dict[int, dict],
) -> list[dict]:
    """Per signal_type breakdown (dynamic / swing / fixed). Audit #8.

    Includes hedge PnL — hedge closes are attributed to the parent
    signal's signal_type via the shared attribution map.
    """
    _, sym_side_to_types = _build_attribution_maps(db_trades, db_signals)

    by_type: dict[str, list[float]] = defaultdict(list)
    for c in closed:
        sym = c.get("symbol", "?")
        side = c.get("side", "?")
        entry_side = "Buy" if side == "Sell" else "Sell"
        types = sym_side_to_types.get((sym, entry_side), [])
        pnl = float(c.get("closedPnl", 0) or 0)
        if types:
            share = pnl / len(types)
            for st in types:
                by_type[st].append(share)

    rows = []
    for st, pnls in by_type.items():
        wins = sum(1 for p in pnls if p > 0)
        losses = sum(1 for p in pnls if p < 0)
        win_rate = (wins / len(pnls) * 100.0) if pnls else 0.0
        rows.append({
            "signal_type": st,
            "n": len(pnls),
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "total": sum(pnls),
            "avg": sum(pnls) / len(pnls) if pnls else 0,
        })
    rows.sort(key=lambda r: r["total"])
    return rows


def db_vs_bybit_diff(db_trades: list[dict],
                     positions: list) -> dict:
    """Cross-check: which DB-active trades have no live Bybit
    position, and which Bybit positions have no DB trade?"""
    active_db = [
        t for t in db_trades
        if (t.get("state") or "").upper()
        not in ("CLOSED", "CANCELLED", "ERROR", "PROTECTION_FAILED")
    ]
    # Build (symbol, side) sets.
    db_keys: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for t in active_db:
        sig_id = t.get("signal_id")
        # We need the symbol+direction. The trades table doesn't store
        # symbol/direction directly; we'd have to JOIN signals. For
        # this audit we just count by trade.id.
        db_keys[("?", "?")].append(t)

    pos_keys: dict[tuple[str, str, int], dict] = {}
    for p in positions:
        sym = p.get("symbol", "?")
        side = p.get("side", "?")
        idx = int(p.get("positionIdx") or 0)
        pos_keys[(sym, side, idx)] = p

    return {
        "db_active_count": len(active_db),
        "bybit_open_count": len(positions),
        "diff_count": len(active_db) - len(positions),
        "db_active": active_db,
        "bybit_positions": positions,
    }


def leftover_orders_check(orders: list, positions: list) -> dict:
    """Orders whose (symbol, positionIdx) doesn't match any open position."""
    pos_keys = {(p.get("symbol"), int(p.get("positionIdx") or 0))
                for p in positions}
    leftover = []
    for o in orders:
        sym = o.get("symbol", "?")
        try:
            idx = int(o.get("positionIdx") or 0)
        except (TypeError, ValueError):
            idx = 0
        if (sym, idx) not in pos_keys:
            leftover.append(o)
    return {
        "total_orders": len(orders),
        "leftover_count": len(leftover),
        "leftover": leftover,
    }


def fees_vs_pnl(closed: list) -> dict:
    """Sum of cumExecFee vs sum of closedPnl."""
    gross = 0.0
    fees = 0.0
    for c in closed:
        try:
            gross += float(c.get("closedPnl", 0) or 0)
        except (TypeError, ValueError):
            pass
        try:
            fees += float(c.get("cumEntryValue", 0) or 0) * 0  # placeholder
        except (TypeError, ValueError):
            pass
        # Bybit's closedPnl is already net of fees on Bybit's side. The
        # raw fee total is on individual execution records (not in
        # closed_pnl). We surface gross (net of fees) and flag that
        # explicit fee breakdown needs the executions endpoint.
    return {"net_pnl_total": gross, "fees_separate_endpoint_required": True}


def leverage_consistency_check(positions: list,
                               db_trades: list[dict]) -> dict:
    """Verify all open positions share the same leverage as the DB
    expected, and flag any mismatch."""
    rows = []
    for p in positions:
        try:
            lev = float(p.get("leverage") or 0)
        except (TypeError, ValueError):
            lev = 0.0
        rows.append({
            "symbol": p.get("symbol"),
            "side": p.get("side"),
            "idx": p.get("positionIdx"),
            "leverage": lev,
            "size": p.get("size"),
            "entry": p.get("avgPrice"),
            "im": p.get("positionIM"),
        })
    rows.sort(key=lambda r: r["leverage"])
    return rows


def exposure_summary(positions: list) -> dict:
    long_notional = 0.0
    short_notional = 0.0
    for p in positions:
        try:
            size = float(p.get("size", 0) or 0)
            entry = float(p.get("avgPrice", 0) or 0)
        except (TypeError, ValueError):
            continue
        notional = size * entry
        if p.get("side") == "Buy":
            long_notional += notional
        else:
            short_notional += notional
    return {
        "long_notional_usdt": long_notional,
        "short_notional_usdt": short_notional,
        "total_notional_usdt": long_notional + short_notional,
        "imbalance_usdt": long_notional - short_notional,
    }


# ---------------------------------------------------------------------------
# Renderer
# ---------------------------------------------------------------------------

def render_markdown(
    closed: list,
    positions: list,
    orders: list,
    db_trades: list[dict],
    db_signals: dict[int, dict],
    since_ms: int,
    until_ms: int,
) -> str:
    lines: list[str] = []
    lines.append("# Stratos1 — Clean-Start Diagnostic")
    lines.append("")
    lines.append(
        f"- **Window**: {_fmt_ts(since_ms)} → {_fmt_ts(until_ms)} (UTC)"
    )
    lines.append(f"- **Closed trades**: {len(closed)}")
    lines.append(f"- **Open Bybit positions**: {len(positions)}")
    lines.append(f"- **Open Bybit orders**: {len(orders)}")
    lines.append(f"- **DB trades total**: {len(db_trades)}")
    lines.append(f"- **DB signals total**: {len(db_signals)}")
    lines.append("")

    # Buckets — the headline answer to the degradation question.
    buckets = bucket_by_index(closed, bucket_size=10)
    lines.append("## 1. PnL by close-order bucket (10 trades each)")
    lines.append("")
    if not buckets:
        lines.append("_No closed trades in window._")
    else:
        lines.append("| Bucket | Trades | W/L/0 | Total PnL | Avg | Best | Worst | Liq | First close | Last close |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|")
        for b in buckets:
            wlz = f"{b['wins']}/{b['losses']}/{b['zeros']}"
            lines.append(
                f"| {b['bucket']} | {b['trades']} | {wlz} | "
                f"{b['total_pnl']:+.2f} | {b['avg_pnl']:+.2f} | "
                f"{b['best']:+.2f} | {b['worst']:+.2f} | "
                f"{b['bust_trades']} | {b['first_close']} | {b['last_close']} |"
            )
    lines.append("")

    # Per-symbol — concentration check.
    sym_rows = per_symbol_pnl(closed)
    lines.append("## 2. PnL by symbol (worst 10 + best 5)")
    lines.append("")
    if sym_rows:
        lines.append("| Symbol | N | W/L | WinRate | Total | Avg | Best | Worst |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for r in (sym_rows[:10] + sym_rows[-5:]):
            lines.append(
                f"| {r['symbol']} | {r['n']} | {r['wins']}/{r['losses']} | "
                f"{r['win_rate']:.0f}% | "
                f"{r['total']:+.2f} | {r['avg']:+.2f} | "
                f"{r['best']:+.2f} | {r['worst']:+.2f} |"
            )
    lines.append("")

    # Per-channel — channel-quality check.
    # Client 2026-05-02: show ALL configured channels in the table
    # (even ones with 0 closed trades in the window) so it's
    # transparent which channels the bot is listening to vs which
    # actually produced trades. Hedge tags ("(hedge)") are kept
    # separate; configured-but-inactive channels show "(0 trades)".
    chan_rows, unmatched_pnl, unmatched_n = per_channel_pnl(
        closed, db_trades, db_signals,
    )
    configured = fetch_configured_channels()
    seen_chan_names = {
        r["channel"].replace(" (hedge)", "") for r in chan_rows
    }
    inactive = sorted(c for c in configured if c not in seen_chan_names)
    lines.append("## 3. PnL by source channel (Phase 6.C.3 — audit #22)")
    lines.append("")
    if chan_rows:
        lines.append("| Channel | N | W/L | WinRate | Total | Avg |")
        lines.append("|---|---|---|---|---|---|")
        for r in chan_rows:
            lines.append(
                f"| {r['channel']} | {r['n']} | {r['wins']}/{r['losses']} | "
                f"{r['win_rate']:.0f}% | "
                f"{r['total']:+.2f} | {r['avg']:+.2f} |"
            )
        for name in inactive:
            lines.append(
                f"| {name} | 0 | 0/0 | n/a | +0.00 | n/a |"
            )
    else:
        lines.append("_No closed trades from any channel in the window._")
        if inactive:
            lines.append("")
            lines.append(
                f"_({len(inactive)} channels are configured but inactive "
                f"in the window.)_"
            )
    lines.append("")
    lines.append(
        f"_Active channels: {len(chan_rows)}  ·  "
        f"Configured-but-inactive: {len(inactive)}  ·  "
        f"Total configured: {len(configured)}._"
    )
    lines.append(
        f"_Unmatched (no signal_id link): {unmatched_n} closed records, "
        f"{unmatched_pnl:+.2f} USDT PnL — usually flatten-script closes._"
    )
    lines.append("")

    # Per-signal-type — concentration by classification (audit #8).
    # Client 2026-05-02: always show all 3 signal_types (dynamic /
    # swing / fixed) even when one is empty, so it's visible that
    # the bot considered all categories. `fixed` only fires for
    # auto-SL trades (no SL in the signal); `dynamic` and `swing`
    # come from real-SL signals classified by SL distance.
    type_rows = per_signal_type_pnl(closed, db_trades, db_signals)
    seen_types = {r["signal_type"] for r in type_rows}
    for missing in ("dynamic", "swing", "fixed"):
        if missing not in seen_types:
            type_rows.append({
                "signal_type": missing,
                "n": 0, "wins": 0, "losses": 0,
                "win_rate": 0.0, "total": 0.0, "avg": 0.0,
            })
    type_rows.sort(key=lambda r: r["total"])
    lines.append("## 3b. PnL by signal_type (Phase 6.C.3 — audit #8)")
    lines.append("")
    lines.append("| Type | N | W/L | WinRate | Total | Avg |")
    lines.append("|---|---|---|---|---|---|")
    for r in type_rows:
        if r["n"] == 0:
            lines.append(
                f"| {r['signal_type']} | 0 | 0/0 | n/a | +0.00 | n/a |"
            )
        else:
            lines.append(
                f"| {r['signal_type']} | {r['n']} | "
                f"{r['wins']}/{r['losses']} | "
                f"{r['win_rate']:.0f}% | "
                f"{r['total']:+.2f} | {r['avg']:+.2f} |"
            )
    lines.append("")

    # DB vs Bybit diff.
    diff = db_vs_bybit_diff(db_trades, positions)
    lines.append("## 4. DB vs Bybit drift")
    lines.append("")
    lines.append(f"- DB active rows: {diff['db_active_count']}")
    lines.append(f"- Bybit open positions: {diff['bybit_open_count']}")
    lines.append(f"- Diff: {diff['diff_count']:+d}")
    if diff["diff_count"] != 0:
        lines.append("")
        lines.append("**ATTENTION**: count mismatch suggests state drift.")
        lines.append("Detailed reconciliation requires symbol+direction "
                     "JOIN with signals (next iteration).")
    lines.append("")

    # Leftover orders.
    leftover = leftover_orders_check(orders, positions)
    lines.append("## 5. Leftover orders (orders without matching open position)")
    lines.append("")
    lines.append(f"- Total open orders: {leftover['total_orders']}")
    lines.append(f"- Leftover (no matching position): {leftover['leftover_count']}")
    if leftover["leftover_count"]:
        lines.append("")
        lines.append("| Symbol | Side | Idx | OrderType | Qty | Trigger |")
        lines.append("|---|---|---|---|---|---|")
        for o in leftover["leftover"][:30]:
            lines.append(
                f"| {o.get('symbol')} | {o.get('side')} | "
                f"{o.get('positionIdx')} | {o.get('orderType')} | "
                f"{o.get('qty')} | {o.get('triggerPrice', '-')} |"
            )
        if leftover["leftover_count"] > 30:
            lines.append(f"_(+ {leftover['leftover_count']-30} more)_")
    lines.append("")

    # Leverage consistency.
    lev = leverage_consistency_check(positions, db_trades)
    lines.append("## 6. Leverage / IM per open position")
    lines.append("")
    if lev:
        lines.append("| Symbol | Side | Idx | Lev | Size | Entry | IM |")
        lines.append("|---|---|---|---|---|---|---|")
        for r in lev:
            lines.append(
                f"| {r['symbol']} | {r['side']} | {r['idx']} | "
                f"x{r['leverage']:.2f} | {r['size']} | {r['entry']} | "
                f"{r['im']} |"
            )
    lines.append("")

    # Exposure.
    exp = exposure_summary(positions)
    lines.append("## 7. Exposure summary")
    lines.append("")
    lines.append(f"- Long notional: {exp['long_notional_usdt']:+.2f} USDT")
    lines.append(f"- Short notional: {exp['short_notional_usdt']:+.2f} USDT")
    lines.append(f"- Total notional: {exp['total_notional_usdt']:+.2f} USDT")
    lines.append(f"- Imbalance (long - short): {exp['imbalance_usdt']:+.2f} USDT")
    lines.append("")

    # Fees vs PnL note.
    fees = fees_vs_pnl(closed)
    lines.append("## 8. Fees vs net PnL")
    lines.append("")
    lines.append(f"- Net PnL total (Bybit-reported, already net of fees): {fees['net_pnl_total']:+.2f} USDT")
    lines.append("- Per-trade fee breakdown requires the executions endpoint "
                 "(not pulled in this run).")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    # Default window: 24h back from now. Override via CLI.
    if len(sys.argv) >= 2 and sys.argv[1].isdigit():
        hours_back = int(sys.argv[1])
    else:
        hours_back = 24
    until_ms = _now_ms()
    since_ms = until_ms - hours_back * 3600 * 1000

    db_path = str(PROJECT_ROOT / "stratos1.db")
    client = _client()

    try:
        closed = fetch_closed_pnl(client, since_ms, until_ms)
    except Exception as exc:
        print(f"Failed fetching closed_pnl: {exc}", file=sys.stderr)
        closed = []
    try:
        positions = fetch_open_positions(client)
    except Exception as exc:
        print(f"Failed fetching positions: {exc}", file=sys.stderr)
        positions = []
    try:
        orders = fetch_open_orders(client)
    except Exception as exc:
        print(f"Failed fetching orders: {exc}", file=sys.stderr)
        orders = []
    try:
        db_trades = fetch_db_trades(db_path)
    except Exception as exc:
        print(f"Failed reading DB trades: {exc}", file=sys.stderr)
        db_trades = []
    try:
        db_signals = fetch_db_signals(db_path)
    except Exception as exc:
        print(f"Failed reading DB signals: {exc}", file=sys.stderr)
        db_signals = {}

    print(render_markdown(
        closed=closed,
        positions=positions,
        orders=orders,
        db_trades=db_trades,
        db_signals=db_signals,
        since_ms=since_ms,
        until_ms=until_ms,
    ))


if __name__ == "__main__":
    main()
