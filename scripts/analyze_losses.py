"""Loss & "gave-back" analyzer (client 2026-05-02 verification ask:
'manual line-by-line review of why the bot is losing money').

Reads the DB and produces a Markdown report with:

  1. Top 10 LARGEST LOSING TRADES by realized PnL (USDT).
     For each: the full chain — channel, signal, entry, SL, TPs,
     state transitions, leverage, close reason, PnL.

  2. Top 10 "GAVE BACK PROFIT" TRADES — trades that reached one of
     the favorable progression states (BREAKEVEN_ACTIVE,
     PROFIT_LOCK_1_ACTIVE, PROFIT_LOCK_2_ACTIVE, TRAILING_ACTIVE,
     SL_MOVED_TO_TP1..TP4) but closed at a worse PnL than expected
     for that state. Ranked by 'expected vs actual' gap.

  3. ARCHITECTURE-CONFLICT SCAN: counts of state transitions per
     trade. A trade that visited an unexpected combination of
     states (e.g. HEDGE_ACTIVE + TRAILING_ACTIVE + multiple
     PROTECTION_FAILED) hints at a logic conflict.

  4. PROTECTION_FAILED + ERROR roster — every trade that hit a
     failure state, with reason.

  5. HEDGE EFFECTIVENESS — paired parent + hedge child rows so the
     operator can see whether hedges were profit-positive or just
     added confusion.

Read-only. Renders Markdown to stdout.

Usage:
    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/analyze_losses.py"
    ssh Tomas "cd /opt/stratos1 && ./venv/bin/python scripts/analyze_losses.py 48"  # last 48h only
"""

from __future__ import annotations

import json
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "stratos1.db"


# --- expected PnL per favorable state (ROUGH baseline; used for
#     "gave back" detection). Pct refers to PRICE movement from
#     entry — for a LONG, positive favorable_pct means price moved
#     up by that %. The "expected" is the LOWEST PnL the position
#     should still have given the state was reached.
EXPECTED_FAVORABLE_PCT = {
    "BREAKEVEN_ACTIVE":     0.0,    # SL moved to entry
    "SL_MOVED_TO_TP1":      0.0,    # SL moved to TP1 (>0% always)
    "SL_MOVED_TO_TP2":      0.0,
    "SL_MOVED_TO_TP3":      0.0,
    "SL_MOVED_TO_TP4":      0.0,
    "PROFIT_LOCK_1_ACTIVE": 1.5,    # SL moved to entry +1.5%
    "PROFIT_LOCK_2_ACTIVE": 2.5,    # SL moved to entry +2.5%
    "TRAILING_ACTIVE":      3.6,    # 6.1% activation - 2.5% distance
    "TRAILING_UPDATED":     3.6,
}


def fetch_trades(since_hours: int | None) -> list[dict]:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        if since_hours is not None:
            cutoff = (
                datetime.now(timezone.utc) - timedelta(hours=since_hours)
            ).isoformat()
            cur = conn.execute(
                "SELECT t.*, s.symbol AS signal_symbol, "
                "       s.direction AS signal_direction, "
                "       s.entry AS signal_entry, "
                "       s.sl AS signal_sl, "
                "       s.tps AS signal_tps, "
                "       s.signal_type AS signal_type, "
                "       s.source_channel_name AS signal_channel "
                "FROM trades t LEFT JOIN signals s ON t.signal_id = s.id "
                "WHERE COALESCE(t.closed_at, t.updated_at) >= ? "
                "ORDER BY t.id ASC",
                (cutoff,),
            )
        else:
            cur = conn.execute(
                "SELECT t.*, s.symbol AS signal_symbol, "
                "       s.direction AS signal_direction, "
                "       s.entry AS signal_entry, "
                "       s.sl AS signal_sl, "
                "       s.tps AS signal_tps, "
                "       s.signal_type AS signal_type, "
                "       s.source_channel_name AS signal_channel "
                "FROM trades t LEFT JOIN signals s ON t.signal_id = s.id "
                "ORDER BY t.id ASC"
            )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def fetch_events_for(trade_ids: list[int]) -> dict[int, list[dict]]:
    if not trade_ids:
        return {}
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" * len(trade_ids))
        cur = conn.execute(
            f"SELECT trade_id, event_type, details, created_at "
            f"FROM events WHERE trade_id IN ({placeholders}) "
            f"ORDER BY id ASC",
            trade_ids,
        )
        out: dict[int, list[dict]] = defaultdict(list)
        for r in cur.fetchall():
            out[int(r["trade_id"])].append(dict(r))
        return out
    finally:
        conn.close()


def derive_states_visited(events: list[dict]) -> list[str]:
    """Pull every state-transition target from the events table."""
    states: list[str] = []
    for e in events:
        if (e.get("event_type") or "") != "trade.state_transition":
            continue
        try:
            details = json.loads(e.get("details") or "{}")
        except Exception:
            continue
        to_state = details.get("to_state")
        if to_state:
            states.append(str(to_state))
    return states


def estimate_gave_back(trade: dict, states_visited: list[str]) -> dict | None:
    """If the trade reached a favorable state but closed at a worse
    PnL than what that state implies, return the gap details.
    Otherwise None."""
    if (trade.get("state") or "").upper() != "CLOSED":
        return None
    pnl_pct = trade.get("pnl_pct")
    if pnl_pct is None:
        return None
    try:
        actual = float(pnl_pct)
    except (TypeError, ValueError):
        return None

    # Find the BEST favorable state the trade reached (highest
    # expected pct).
    best_state = None
    best_expected = None
    for st in states_visited:
        exp = EXPECTED_FAVORABLE_PCT.get(st)
        if exp is None:
            continue
        if best_expected is None or exp > best_expected:
            best_state = st
            best_expected = exp
    if best_state is None or best_expected is None:
        return None
    if actual >= best_expected:
        # Closed at or above the expected level — not a "gave back".
        return None
    return {
        "best_state": best_state,
        "best_expected_pct": best_expected,
        "actual_pct": actual,
        "gave_back_pct": best_expected - actual,
    }


def render_chain(
    trade: dict, events: list[dict], indent: str = "  ",
) -> list[str]:
    """Compact one-trade narrative for the loss/gave-back tables."""
    L: list[str] = []
    sym = trade.get("signal_symbol") or "?"
    direction = trade.get("signal_direction") or "?"
    chan = trade.get("signal_channel") or trade.get("source_channel_name") or "?"
    sig_entry = trade.get("signal_entry")
    sig_sl = trade.get("signal_sl")
    sig_tps = trade.get("signal_tps")
    avg_entry = trade.get("avg_entry")
    sl_db = trade.get("sl_price")
    leverage = trade.get("leverage")
    margin = trade.get("margin")
    state = trade.get("state")
    close_reason = trade.get("close_reason")
    pnl_pct = trade.get("pnl_pct")
    pnl_usdt = trade.get("pnl_usdt")
    sig_type = trade.get("signal_type")

    L.append(
        f"{indent}**Trade #{trade['id']} | {sym} {direction} | "
        f"channel: {chan} | type: {sig_type or '?'} | x{leverage}**"
    )
    L.append(
        f"{indent}signal: entry={sig_entry}  sl={sig_sl}  tps={sig_tps}"
    )
    L.append(
        f"{indent}filled: entry={avg_entry}  bot_sl={sl_db}  margin={margin}"
    )
    L.append(
        f"{indent}closed: state={state}  reason={close_reason}  "
        f"pnl_pct={pnl_pct}  pnl_usdt={pnl_usdt}"
    )
    # Render every state transition in chronological order.
    transitions = []
    for e in events:
        if (e.get("event_type") or "") != "trade.state_transition":
            continue
        try:
            d = json.loads(e.get("details") or "{}")
            transitions.append(
                f"{e.get('created_at')[:19]} {d.get('from_state','?')}->{d.get('to_state','?')}"
            )
        except Exception:
            pass
    if transitions:
        L.append(f"{indent}transitions:")
        for t in transitions:
            L.append(f"{indent}  · {t}")
    # Render any non-transition lifecycle events (sl_moved, tp_hit,
    # protection_failed, hedge.fill.activated, etc.).
    other_events = [
        e for e in events
        if (e.get("event_type") or "") != "trade.state_transition"
    ]
    if other_events:
        L.append(f"{indent}events:")
        for e in other_events:
            details_raw = e.get("details") or "{}"
            try:
                details = json.loads(details_raw)
                details_str = json.dumps(details, sort_keys=True)
            except Exception:
                details_str = details_raw
            L.append(
                f"{indent}  · {e.get('created_at')[:19]} "
                f"{e.get('event_type'):<22} {details_str}"
            )
    return L


def main() -> None:
    since_hours: int | None = None
    if len(sys.argv) >= 2 and sys.argv[1].isdigit():
        since_hours = int(sys.argv[1])

    trades = fetch_trades(since_hours)
    closed_trades = [
        t for t in trades
        if (t.get("state") or "").upper() == "CLOSED"
        and t.get("pnl_usdt") is not None
    ]
    if not closed_trades:
        print("# Loss analysis")
        print("")
        print(
            f"_No CLOSED trades in window "
            f"({'last ' + str(since_hours) + 'h' if since_hours else 'all-time'})._"
        )
        return

    events_by_id = fetch_events_for([int(t["id"]) for t in trades])

    # --- 1. Top 10 worst losers (most negative pnl_usdt). ---
    losers = sorted(
        closed_trades, key=lambda t: float(t.get("pnl_usdt") or 0)
    )[:10]

    # --- 2. Top 10 "gave back". ---
    gave_back: list[tuple[dict, dict]] = []
    for t in closed_trades:
        evs = events_by_id.get(int(t["id"]), [])
        states = derive_states_visited(evs)
        gb = estimate_gave_back(t, states)
        if gb:
            gave_back.append((t, gb))
    gave_back.sort(key=lambda pair: -pair[1]["gave_back_pct"])
    gave_back = gave_back[:10]

    # --- 3. State-transition stats (architecture-conflict scan). ---
    state_counter: Counter[str] = Counter()
    transitions_counter: Counter[tuple[str, str]] = Counter()
    suspicious: list[dict] = []
    for t in closed_trades:
        evs = events_by_id.get(int(t["id"]), [])
        states = derive_states_visited(evs)
        for s in states:
            state_counter[s] += 1
        # transitions tuples
        for e in evs:
            if (e.get("event_type") or "") != "trade.state_transition":
                continue
            try:
                d = json.loads(e.get("details") or "{}")
                transitions_counter[(
                    d.get("from_state") or "?",
                    d.get("to_state") or "?",
                )] += 1
            except Exception:
                pass
        # Suspicious: trade visits PROTECTION_FAILED AND a deeper
        # active state — should be impossible per the matrix.
        if "PROTECTION_FAILED" in states and any(
            s in states for s in (
                "POSITION_OPEN", "HEDGE_ARMED", "HEDGE_ACTIVE",
                "BREAKEVEN_ACTIVE", "PROFIT_LOCK_1_ACTIVE",
                "PROFIT_LOCK_2_ACTIVE", "TRAILING_ACTIVE",
            )
        ):
            ordering_ok = True
            try:
                last_active = max(
                    i for i, s in enumerate(states)
                    if s in (
                        "POSITION_OPEN", "HEDGE_ARMED", "HEDGE_ACTIVE",
                        "BREAKEVEN_ACTIVE", "PROFIT_LOCK_1_ACTIVE",
                        "PROFIT_LOCK_2_ACTIVE", "TRAILING_ACTIVE",
                    )
                )
                first_protfail = min(
                    i for i, s in enumerate(states)
                    if s == "PROTECTION_FAILED"
                )
                if first_protfail < last_active:
                    ordering_ok = False
            except ValueError:
                pass
            if not ordering_ok:
                suspicious.append({
                    "trade_id": t["id"],
                    "states": states,
                })

    # --- 4. PROTECTION_FAILED + ERROR roster. ---
    failures = [
        t for t in trades
        if (t.get("state") or "").upper() in ("PROTECTION_FAILED", "ERROR")
    ]

    # --- 5. Hedge effectiveness (parent + child pairs). ---
    by_id = {int(t["id"]): t for t in trades}
    pairs: list[tuple[dict, dict]] = []
    for parent in trades:
        hid = parent.get("hedge_trade_id")
        if hid is None:
            continue
        try:
            child = by_id.get(int(hid))
        except (TypeError, ValueError):
            continue
        if child is None:
            continue
        pairs.append((parent, child))

    # --- Render ---
    print("# Loss analysis")
    print("")
    print(
        f"Window: {('last ' + str(since_hours) + 'h') if since_hours else 'all-time'}  ·  "
        f"closed trades: {len(closed_trades)}  ·  "
        f"failures: {len(failures)}"
    )
    print("")

    print("## 1. Top 10 worst losers (by realized USDT)")
    print("")
    if not losers:
        print("_None._")
    for t in losers:
        for line in render_chain(t, events_by_id.get(int(t["id"]), [])):
            print(line)
        print("")

    print("## 2. Top 10 \"gave back\" trades")
    print("")
    print(
        "_A trade reached a favorable state implying a profit floor "
        "(BREAKEVEN, PROFIT_LOCK_1/2, TRAILING) but closed below "
        "that floor. Ranked by gap (expected_pct − actual_pct)._"
    )
    print("")
    if not gave_back:
        print("_No gave-back trades in window._")
    for t, gb in gave_back:
        print(
            f"### Trade #{t['id']} — gave back {gb['gave_back_pct']:.2f}% "
            f"(reached {gb['best_state']}, expected ≥{gb['best_expected_pct']:.2f}%, "
            f"closed at {gb['actual_pct']:.2f}%)"
        )
        for line in render_chain(t, events_by_id.get(int(t["id"]), [])):
            print(line)
        print("")

    print("## 3. State-transition statistics")
    print("")
    print("**States visited (count across all CLOSED trades):**")
    print("")
    print("| State | Count |")
    print("|---|---|")
    for st, n in state_counter.most_common():
        print(f"| {st} | {n} |")
    print("")
    print("**Top transitions:**")
    print("")
    print("| From | To | Count |")
    print("|---|---|---|")
    for (frm, to), n in transitions_counter.most_common(20):
        print(f"| {frm} | {to} | {n} |")
    print("")
    print(
        f"_Suspicious orderings (PROTECTION_FAILED followed by a deeper "
        f"active state): {len(suspicious)}._"
    )
    if suspicious:
        for s in suspicious:
            print(f"  - trade #{s['trade_id']}: {s['states']}")
    print("")

    print("## 4. Failures roster (PROTECTION_FAILED + ERROR)")
    print("")
    if not failures:
        print("_No failures in window._")
    else:
        print("| Trade | State | Symbol | Direction | Channel | Close reason |")
        print("|---|---|---|---|---|---|")
        for t in failures:
            print(
                f"| #{t['id']} | {t.get('state')} | "
                f"{t.get('signal_symbol') or '?'} | "
                f"{t.get('signal_direction') or '?'} | "
                f"{t.get('signal_channel') or '?'} | "
                f"{t.get('close_reason') or '?'} |"
            )
    print("")

    print("## 5. Hedge effectiveness (parent + child pairs)")
    print("")
    if not pairs:
        print("_No hedges fired in window._")
    else:
        print("| Parent | Symbol | Parent PnL | Hedge | Hedge PnL | Combined |")
        print("|---|---|---|---|---|---|")
        for parent, child in pairs:
            ppnl = parent.get("pnl_usdt")
            hpnl = child.get("pnl_usdt")
            try:
                combined = (
                    (float(ppnl) if ppnl is not None else 0.0)
                    + (float(hpnl) if hpnl is not None else 0.0)
                )
                combined_str = f"{combined:+.2f}"
            except (TypeError, ValueError):
                combined_str = "?"
            print(
                f"| #{parent['id']} | {parent.get('signal_symbol') or '?'} | "
                f"{ppnl if ppnl is not None else 'open'} | "
                f"#{child['id']} | "
                f"{hpnl if hpnl is not None else 'open'} | "
                f"{combined_str} |"
            )


if __name__ == "__main__":
    main()
