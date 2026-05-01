"""One-shot reconciliation: align DB active trades with Bybit positions.

Built for the client 2026-05-02 audit. The diagnostic found +3 / +4
ghost rows in the DB whose underlying Bybit position no longer exists.
Phase 5c added the WS-driven hedge-close handler to prevent FUTURE
drift; this script cleans up the BACKLOG that accumulated before
Phase 5c was deployed.

Two-pass run:

  Pass 1 (always):
    - Load DB trades whose state is non-terminal (POSITION_OPEN /
      HEDGE_ACTIVE / etc.).
    - For each one, JOIN to its signal (or to the parent's signal for
      hedge child rows where signal_id is NULL).
    - Look up the matching Bybit position (symbol + side + positionIdx).
    - If no matching position exists: candidate ghost.

  Pass 2 (when --apply is passed, otherwise dry-run):
    - For each ghost: mark the DB row as CLOSED with reason
      "drift_cleanup_2026_05_02" and a closed_at timestamp.
    - Cancel every Bybit order matching the ghost's symbol +
      positionIdx (orphan force-close conditionals, leftover
      hedge pre-arms).

Usage:

    # Dry run — print what WOULD be cleaned up.
    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/reconcile_drift.py"

    # Apply — actually clean up.
    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/reconcile_drift.py --apply"
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


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Data pulls
# ---------------------------------------------------------------------------

def fetch_open_positions(client: HTTP) -> list:
    resp = client.get_positions(category="linear", settleCoin="USDT")
    raw = (resp.get("result", {}) or {}).get("list", []) or []
    return [p for p in raw if float(p.get("size", 0) or 0) > 0]


def fetch_open_orders(client: HTTP) -> list:
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


def fetch_db_state(db_path: str) -> tuple[list[dict], dict[int, dict]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    trades_cur = conn.execute("SELECT * FROM trades ORDER BY id ASC")
    trades = [dict(r) for r in trades_cur.fetchall()]
    sigs_cur = conn.execute("SELECT * FROM signals")
    signals = {int(r["id"]): dict(r) for r in sigs_cur.fetchall()}
    conn.close()
    return trades, signals


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

ACTIVE_STATES = {
    "PENDING", "ENTRY1_PLACED", "ENTRY1_FILLED",
    "ENTRY2_PLACED", "ENTRY2_FILLED", "POSITION_OPEN",
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
}


def resolve_trade_keys(
    trades: list[dict],
    signals: dict[int, dict],
) -> list[dict]:
    """For each active trade row, resolve symbol + (side, positionIdx)
    so we can look it up against Bybit positions.

    The trades table stores ``signal_id`` and ``hedge_trade_id``. A
    hedge child has signal_id=NULL — its symbol/direction are
    inferred from the parent (the trade row whose hedge_trade_id ==
    this child's id).
    """
    by_id = {int(t["id"]): t for t in trades}
    out = []
    for t in trades:
        state = (t.get("state") or "").upper()
        if state not in ACTIVE_STATES:
            continue
        sig_id = t.get("signal_id")
        symbol: str | None = None
        side: str | None = None
        position_idx: int | None = None
        derivation = "?"
        if sig_id is not None:
            sig = signals.get(int(sig_id))
            if sig:
                symbol = sig.get("symbol")
                direction = (sig.get("direction") or "").upper()
                side = "Buy" if direction == "LONG" else "Sell"
                position_idx = 1 if direction == "LONG" else 2
                derivation = f"signal#{sig_id}"
        if symbol is None:
            # hedge child: find a parent whose hedge_trade_id matches.
            for cand in trades:
                cand_id = cand.get("hedge_trade_id")
                if cand_id is None:
                    continue
                try:
                    if int(cand_id) == int(t["id"]):
                        # parent found.
                        cand_sig_id = cand.get("signal_id")
                        if cand_sig_id is None:
                            continue
                        sig = signals.get(int(cand_sig_id))
                        if sig:
                            symbol = sig.get("symbol")
                            parent_dir = (sig.get("direction") or "").upper()
                            # hedge is OPPOSITE of parent.
                            side = "Buy" if parent_dir == "SHORT" else "Sell"
                            position_idx = 2 if parent_dir == "LONG" else 1
                            derivation = (
                                f"hedge_of_parent#{cand['id']}_signal#{cand_sig_id}"
                            )
                        break
                except (TypeError, ValueError):
                    continue
        out.append({
            "trade_id": int(t["id"]),
            "state": state,
            "symbol": symbol,
            "side": side,
            "position_idx": position_idx,
            "derivation": derivation,
            "raw": t,
        })
    return out


def find_ghosts(
    resolved: list[dict],
    positions: list,
) -> list[dict]:
    """A 'ghost' is a DB-active trade with no matching Bybit position."""
    pos_keys = {
        (
            p.get("symbol"),
            p.get("side"),
            int(p.get("positionIdx") or 0),
        )
        for p in positions
    }
    ghosts = []
    for t in resolved:
        if not t["symbol"] or not t["side"] or t["position_idx"] is None:
            ghosts.append({**t, "reason": "unresolvable_keys"})
            continue
        key = (t["symbol"], t["side"], t["position_idx"])
        if key not in pos_keys:
            ghosts.append({**t, "reason": "no_bybit_position"})
    return ghosts


def find_untracked_positions(
    positions: list,
    resolved: list[dict],
) -> list[dict]:
    """Bybit positions with no matching DB-active trade.

    Opposite drift direction from ``find_ghosts``. These are usually
    hedges whose parent trade was closed (cleaned up via reconcile)
    leaving the hedge orphaned on Bybit, or positions opened before
    the bot started tracking. They commonly have NO SL / NO trailing
    because the WS handler that should have set them up missed the
    fill event (pre-Phase-5c bug).
    """
    db_keys = {
        (t["symbol"], t["side"], t["position_idx"])
        for t in resolved
        if t["symbol"] and t["side"] and t["position_idx"] is not None
    }
    out = []
    for p in positions:
        sym = p.get("symbol")
        side = p.get("side")
        try:
            idx = int(p.get("positionIdx") or 0)
        except (TypeError, ValueError):
            idx = 0
        if (sym, side, idx) in db_keys:
            continue
        try:
            sl = float(p.get("stopLoss") or 0) or None
        except (TypeError, ValueError):
            sl = None
        try:
            trailing = float(p.get("trailingStop") or 0) or None
        except (TypeError, ValueError):
            trailing = None
        try:
            size = float(p.get("size") or 0)
            avg_price = float(p.get("avgPrice") or 0)
        except (TypeError, ValueError):
            size = 0
            avg_price = 0
        try:
            unreal = float(p.get("unrealisedPnl") or 0)
        except (TypeError, ValueError):
            unreal = 0
        out.append({
            "symbol": sym,
            "side": side,
            "position_idx": idx,
            "size": size,
            "avg_price": avg_price,
            "stop_loss": sl,
            "trailing_stop": trailing,
            "unrealised_pnl": unreal,
            "raw": p,
        })
    return out


def find_orphan_orders(
    orders: list, positions: list,
) -> list[dict]:
    """Bybit orders that are TRULY orphan and safe to cancel.

    Two correct categories of orphan:
      A) The symbol has NO position in any positionIdx -> any order
         on it (force-close, hedge pre-arm, leftover TP) is dead.
      B) The order is reduce-only AND its positionIdx has no position
         -> it's a leftover close-side conditional whose target is
         already gone.

    NOT orphan (must NEVER cancel):
      - Hedge pre-arm conditional: NOT reduce-only, lives on the
        OPPOSITE positionIdx from the active original position.
        E.g. AAVE LONG at idx=1 + a Sell idx=2 conditional armed at
        -1.5% — the Sell is the hedge pre-arm, NOT an orphan.
      - Original-trade -2% force-close conditional: reduce-only on
        the SAME positionIdx as the active position. (positionIdx
        match -> already excluded by category A/B logic.)
    """
    # Symbols with at least one open position (any idx).
    symbols_with_position = {p.get("symbol") for p in positions}
    # (symbol, positionIdx) keys that have an open position.
    pos_keys = {
        (p.get("symbol"), int(p.get("positionIdx") or 0))
        for p in positions
    }
    out = []
    for o in orders:
        sym = o.get("symbol")
        try:
            idx = int(o.get("positionIdx") or 0)
        except (TypeError, ValueError):
            idx = 0
        reduce_only = bool(o.get("reduceOnly", False))
        is_orphan = False
        why = ""
        if sym not in symbols_with_position:
            # Category A: symbol fully closed.
            is_orphan = True
            why = "symbol_fully_closed"
        elif reduce_only and (sym, idx) not in pos_keys:
            # Category B: reduce-only on empty positionIdx.
            is_orphan = True
            why = "reduce_only_on_empty_idx"
        if not is_orphan:
            continue
        out.append({
            "symbol": sym,
            "side": o.get("side"),
            "position_idx": idx,
            "order_id": o.get("orderId"),
            "order_type": o.get("orderType"),
            "qty": o.get("qty"),
            "trigger_price": o.get("triggerPrice"),
            "reduce_only": reduce_only,
            "why": why,
        })
    return out


# ---------------------------------------------------------------------------
# Apply (mutating actions)
# ---------------------------------------------------------------------------

def close_db_ghost(db_path: str, ghost: dict) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE trades "
            "SET state = ?, close_reason = ?, closed_at = ?, updated_at = ? "
            "WHERE id = ?",
            (
                "CLOSED",
                "drift_cleanup_2026_05_02",
                _now_iso(),
                _now_iso(),
                ghost["trade_id"],
            ),
        )
        conn.commit()
    finally:
        conn.close()


def protect_untracked_position(
    client: HTTP, pos: dict, hedge_hard_sl_pct: float = 2.0,
    hedge_trailing_pct: float = 1.2,
) -> bool:
    """Set hedge-spec SL + trailing on an untracked Bybit position.

    Uses the hedge model's hard SL (% from current entry) + trailing
    (% native). active_price is OMITTED so Bybit defaults to immediate
    activation. Same path as the WS-driven _maybe_activate_hedge_from_fill
    in the live bot.

    For SHORT (Sell): SL above entry.
    For LONG  (Buy):  SL below entry.

    Fallback: if Bybit rejects the SL because price has already
    moved past it ("StopLoss ... should greater/less than base_price"),
    retry with trailing-only — at least the position gets a profit-
    lock mechanism. Without trailing the position would ride to
    liquidation unsupervised.
    """
    sym = pos["symbol"]
    side = pos["side"]
    idx = pos["position_idx"]
    avg = pos["avg_price"]
    if avg <= 0:
        print(f"  skip {sym} idx={idx}: avg_price unknown", file=sys.stderr)
        return False
    sl_pct = hedge_hard_sl_pct / 100.0
    tr_pct = hedge_trailing_pct / 100.0
    if side == "Sell":
        sl_price = round(avg * (1 + sl_pct), 8)
    else:
        sl_price = round(avg * (1 - sl_pct), 8)
    trailing_distance = round(avg * tr_pct, 8)
    # First attempt: SL + trailing.
    try:
        client.set_trading_stop(
            category="linear",
            symbol=sym,
            positionIdx=idx,
            stopLoss=str(sl_price),
            trailingStop=str(trailing_distance),
            slTriggerBy="LastPrice",
        )
        return True
    except Exception as exc:
        msg = str(exc)
        # Detect "SL would trigger immediately" — the spec error
        # signature is "should greater/less than base_price". Other
        # errors (auth, network) propagate as failure.
        if "base_price" not in msg and "session_average_price" not in msg:
            print(
                f"  set_trading_stop failed for {sym} idx={idx}: {exc}",
                file=sys.stderr,
            )
            return False
        print(
            f"  SL rejected for {sym} idx={idx} (price moved past it). "
            f"Retrying with trailing-only ...",
            file=sys.stderr,
        )

    # Fallback: trailing-only.
    try:
        client.set_trading_stop(
            category="linear",
            symbol=sym,
            positionIdx=idx,
            trailingStop=str(trailing_distance),
            slTriggerBy="LastPrice",
        )
        return True
    except Exception as exc2:
        print(
            f"  trailing-only fallback ALSO failed for {sym} idx={idx}: {exc2}",
            file=sys.stderr,
        )
        return False


def close_untracked_position(client: HTTP, pos: dict) -> bool:
    """Market reduce-only close on an untracked Bybit position."""
    sym = pos["symbol"]
    side = pos["side"]
    idx = pos["position_idx"]
    size = pos["size"]
    close_side = "Sell" if side == "Buy" else "Buy"
    try:
        client.place_order(
            category="linear",
            symbol=sym,
            side=close_side,
            orderType="Market",
            qty=str(size),
            positionIdx=idx,
            reduceOnly=True,
        )
        return True
    except Exception as exc:
        print(
            f"  close failed for {sym} idx={idx}: {exc}",
            file=sys.stderr,
        )
        return False


def cancel_orphan_order(client: HTTP, order: dict) -> bool:
    try:
        client.cancel_order(
            category="linear",
            symbol=order["symbol"],
            orderId=order["order_id"],
        )
        return True
    except Exception as exc:
        print(
            f"  cancel failed for {order['symbol']} "
            f"{order['order_id']}: {exc}",
            file=sys.stderr,
        )
        return False


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------

def main() -> None:
    apply = "--apply" in sys.argv
    protect_untracked = "--protect-untracked" in sys.argv
    close_untracked = "--close-untracked" in sys.argv
    db_path = str(PROJECT_ROOT / "stratos1.db")
    client = _client()

    positions = fetch_open_positions(client)
    orders = fetch_open_orders(client)
    trades, signals = fetch_db_state(db_path)
    resolved = resolve_trade_keys(trades, signals)
    ghosts = find_ghosts(resolved, positions)
    orphan_orders = find_orphan_orders(orders, positions)
    untracked = find_untracked_positions(positions, resolved)

    print(f"DB active rows:        {len(resolved)}")
    print(f"Bybit open positions:  {len(positions)}")
    print(f"Bybit open orders:     {len(orders)}")
    print()
    print(f"Ghost DB rows (DB active, no Bybit position): {len(ghosts)}")
    for g in ghosts:
        print(
            f"  trade_id={g['trade_id']:>3}  state={g['state']:<22}  "
            f"sym={g['symbol'] or '?':<12}  side={g['side'] or '?':<5}  "
            f"idx={g['position_idx']}  derivation={g['derivation']}  "
            f"({g['reason']})"
        )
    print()
    print(f"Truly orphan Bybit orders (safe to cancel): {len(orphan_orders)}")
    for o in orphan_orders:
        print(
            f"  {o['symbol']:<12}  {o['side']:<5}  idx={o['position_idx']}  "
            f"trigger={o['trigger_price']}  qty={o['qty']}  "
            f"reduce_only={o['reduce_only']}  why={o['why']}  "
            f"order_id={o['order_id']}"
        )
    print()
    print(f"Untracked Bybit positions (open on Bybit, no DB row): {len(untracked)}")
    for u in untracked:
        prot = (
            f"SL={u['stop_loss']}  trail={u['trailing_stop']}"
        )
        print(
            f"  {u['symbol']:<12}  {u['side']:<5}  idx={u['position_idx']}  "
            f"size={u['size']}  entry={u['avg_price']}  "
            f"pnl={u['unrealised_pnl']:+.2f}  {prot}"
        )
    if untracked:
        print()
        print(
            "  Untracked positions usually mean a hedge whose parent "
            "trade was closed (often via reconcile). They will NOT "
            "auto-self-manage — operator must choose:"
        )
        print(
            "    --protect-untracked : set hedge-spec SL+trailing "
            "(2.0% / 1.2%, LastPrice)"
        )
        print(
            "    --close-untracked   : Market reduce-only close right now"
        )

    if not (apply or protect_untracked or close_untracked):
        print()
        print(
            "DRY RUN. Re-run with --apply to clean up DB ghosts + "
            "orphan orders, --protect-untracked to set SL+trailing on "
            "untracked positions, --close-untracked to flatten them."
        )
        return

    if apply:
        print()
        print("APPLYING DB + ORDER CLEANUP ...")
        closed_count = 0
        for g in ghosts:
            try:
                close_db_ghost(db_path, g)
                closed_count += 1
                print(f"  marked CLOSED in DB: trade_id={g['trade_id']}")
            except Exception as exc:
                print(
                    f"  FAILED to close trade_id={g['trade_id']}: {exc}",
                    file=sys.stderr,
                )
        cancelled_count = 0
        for o in orphan_orders:
            if cancel_orphan_order(client, o):
                cancelled_count += 1
                print(f"  cancelled order: {o['symbol']} {o['order_id']}")
        print()
        print(
            f"DONE. Closed {closed_count} ghost rows, "
            f"cancelled {cancelled_count} orphan orders."
        )

    if protect_untracked and untracked:
        print()
        print("APPLYING --protect-untracked ...")
        protected_count = 0
        for u in untracked:
            if protect_untracked_position(client, u):
                protected_count += 1
                print(
                    f"  protected: {u['symbol']} {u['side']} "
                    f"idx={u['position_idx']} (SL+trailing set)"
                )
        print(f"DONE. Protected {protected_count}/{len(untracked)} positions.")

    if close_untracked and untracked:
        print()
        print("APPLYING --close-untracked ...")
        closed_count = 0
        for u in untracked:
            if close_untracked_position(client, u):
                closed_count += 1
                print(
                    f"  closed: {u['symbol']} {u['side']} "
                    f"idx={u['position_idx']} (Market reduce-only)"
                )
        print(f"DONE. Closed {closed_count}/{len(untracked)} positions.")


if __name__ == "__main__":
    main()
