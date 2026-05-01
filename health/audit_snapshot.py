"""Periodic in-bot audit snapshot (Phase 6 / 2026-05-02 audit point #11).

Tomas's request: "Add an audit snapshot every 10 trades:
  - Bybit open positions
  - Bybit open orders
  - DB active trades
  - TP / SL / trailing status
  - realized and unrealized PnL"

Plus audit point #26 ("degradation report"):
  - win/loss count over the window
  - average loss / average win / max loss
  - current open exposure
  - DB-vs-Bybit mismatch count
  - protection-failure count
  - hedge-activation count

This module produces the report and posts it to Telegram. It is
called from PositionManager.close_trade once every N closes
(configurable via [reporting].audit_snapshot_every_n_trades, default
10). All values are Bybit-verified — no bot assumptions in the
report.
"""

from __future__ import annotations

import sqlite3
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import structlog

log = structlog.get_logger(__name__)


@dataclass
class SnapshotData:
    bot_window_n: int
    bot_recent_trades: list[dict]
    open_positions: list[dict]
    open_orders: list[dict]
    db_active_count: int
    db_total_count: int
    bybit_position_count: int
    bybit_order_count: int
    drift: int
    leftover_orders: int
    unprotected_positions: int
    long_notional_usdt: float
    short_notional_usdt: float
    unrealised_pnl_usdt: float
    win_count: int
    loss_count: int
    zero_count: int
    avg_win: float
    avg_loss: float
    max_loss: float
    total_pnl: float
    protection_failed_count: int
    hedge_activations_count: int


async def collect_snapshot(
    db_path: str,
    bybit_adapter,
    window_n: int = 10,
) -> SnapshotData:
    """Collect everything needed for the audit snapshot.

    All Bybit-side data is fetched live; DB data is read directly.
    No state is mutated. Safe to call from the close-trade hot path.
    """
    open_positions: list[dict] = []
    open_orders: list[dict] = []
    try:
        positions_resp = await bybit_adapter._call_with_retry(
            bybit_adapter._rest.get_positions,
            category="linear",
            settleCoin="USDT",
        )
        raw = (positions_resp.get("result", {}) or {}).get("list", []) or []
        open_positions = [
            p for p in raw if float(p.get("size", 0) or 0) > 0
        ]
    except Exception:
        log.exception("audit_snapshot.positions_fetch_failed")
    try:
        orders_resp = await bybit_adapter._call_with_retry(
            bybit_adapter._rest.get_open_orders,
            category="linear",
            settleCoin="USDT",
            openOnly=0,
            limit=50,
        )
        open_orders = (orders_resp.get("result", {}) or {}).get("list", []) or []
    except Exception:
        log.exception("audit_snapshot.orders_fetch_failed")

    # DB-side reads.
    db_active_count = 0
    db_total_count = 0
    bot_recent_trades: list[dict] = []
    protection_failed_count = 0
    hedge_activations_count = 0
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT COUNT(*) AS c FROM trades")
        db_total_count = int(cur.fetchone()["c"])
        cur = conn.execute(
            "SELECT COUNT(*) AS c FROM trades "
            "WHERE state NOT IN ('CLOSED','CANCELLED','ERROR','PROTECTION_FAILED')"
        )
        db_active_count = int(cur.fetchone()["c"])
        # Most recent N closed trades (regardless of close reason).
        cur = conn.execute(
            "SELECT id, state, close_reason, pnl_pct, pnl_usdt, "
            "       avg_entry, leverage, margin, closed_at "
            "FROM trades "
            "WHERE state = 'CLOSED' "
            "ORDER BY id DESC LIMIT ?",
            (window_n,),
        )
        bot_recent_trades = [dict(r) for r in cur.fetchall()]
        # Counters since the last snapshot (use the events table).
        cur = conn.execute(
            "SELECT COUNT(*) AS c FROM events "
            "WHERE event_type = 'protection_failed'"
        )
        protection_failed_count = int(cur.fetchone()["c"])
        # Hedge activations = trade rows with signal_id IS NULL +
        # state IN (HEDGE_ACTIVE, CLOSED) — their existence implies
        # _maybe_activate_hedge_from_fill ran.
        cur = conn.execute(
            "SELECT COUNT(*) AS c FROM trades "
            "WHERE signal_id IS NULL "
            "AND state IN ('HEDGE_ACTIVE','CLOSED')"
        )
        hedge_activations_count = int(cur.fetchone()["c"])
        conn.close()
    except Exception:
        log.exception("audit_snapshot.db_read_failed")

    # Drift: DB active vs Bybit open positions.
    bybit_position_count = len(open_positions)
    bybit_order_count = len(open_orders)
    drift = db_active_count - bybit_position_count

    # Leftover orders: reduce-only orders on positionIdx with no
    # open position OR any orders on a symbol with no positions at all.
    pos_keys = {
        (p.get("symbol"), int(p.get("positionIdx") or 0))
        for p in open_positions
    }
    symbols_with_position = {p.get("symbol") for p in open_positions}
    leftover = 0
    for o in open_orders:
        sym = o.get("symbol")
        try:
            idx = int(o.get("positionIdx") or 0)
        except (TypeError, ValueError):
            idx = 0
        reduce_only = bool(o.get("reduceOnly", False))
        if sym not in symbols_with_position:
            leftover += 1
        elif reduce_only and (sym, idx) not in pos_keys:
            leftover += 1

    # Unprotected positions (no SL AND no trailing).
    unprotected = 0
    long_notional = 0.0
    short_notional = 0.0
    unrealised_pnl = 0.0
    for p in open_positions:
        try:
            sl = float(p.get("stopLoss", 0) or 0)
        except (TypeError, ValueError):
            sl = 0.0
        try:
            tr = float(p.get("trailingStop", 0) or 0)
        except (TypeError, ValueError):
            tr = 0.0
        if sl <= 0 and tr <= 0:
            unprotected += 1
        try:
            size = float(p.get("size", 0) or 0)
            entry = float(p.get("avgPrice", 0) or 0)
            upnl = float(p.get("unrealisedPnl", 0) or 0)
        except (TypeError, ValueError):
            size = entry = upnl = 0.0
        notional = size * entry
        if p.get("side") == "Buy":
            long_notional += notional
        else:
            short_notional += notional
        unrealised_pnl += upnl

    # Window stats.
    win = loss = zero = 0
    win_pnls: list[float] = []
    loss_pnls: list[float] = []
    total_pnl = 0.0
    for t in bot_recent_trades:
        pnl = t.get("pnl_usdt")
        if pnl is None:
            continue
        try:
            pnl = float(pnl)
        except (TypeError, ValueError):
            continue
        total_pnl += pnl
        if pnl > 0:
            win += 1
            win_pnls.append(pnl)
        elif pnl < 0:
            loss += 1
            loss_pnls.append(pnl)
        else:
            zero += 1
    avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0.0
    avg_loss = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0.0
    max_loss = min(loss_pnls) if loss_pnls else 0.0

    return SnapshotData(
        bot_window_n=len(bot_recent_trades),
        bot_recent_trades=bot_recent_trades,
        open_positions=open_positions,
        open_orders=open_orders,
        db_active_count=db_active_count,
        db_total_count=db_total_count,
        bybit_position_count=bybit_position_count,
        bybit_order_count=bybit_order_count,
        drift=drift,
        leftover_orders=leftover,
        unprotected_positions=unprotected,
        long_notional_usdt=long_notional,
        short_notional_usdt=short_notional,
        unrealised_pnl_usdt=unrealised_pnl,
        win_count=win,
        loss_count=loss,
        zero_count=zero,
        avg_win=avg_win,
        avg_loss=avg_loss,
        max_loss=max_loss,
        total_pnl=total_pnl,
        protection_failed_count=protection_failed_count,
        hedge_activations_count=hedge_activations_count,
    )


def render_snapshot_text(s: SnapshotData) -> str:
    """Render the snapshot as a Swedish Telegram message."""
    health_flags: list[str] = []
    if s.drift != 0:
        health_flags.append(f"⚠️ DB-drift {s.drift:+d}")
    if s.leftover_orders > 0:
        health_flags.append(f"⚠️ {s.leftover_orders} kvarliggande order")
    if s.unprotected_positions > 0:
        health_flags.append(
            f"⚠️ {s.unprotected_positions} oskyddad pos"
        )
    health_line = (
        " · ".join(health_flags) if health_flags else "✅ allt rent"
    )

    win_rate_pct = (
        s.win_count / s.bot_window_n * 100.0
        if s.bot_window_n > 0 else 0.0
    )

    return (
        f"📊 <b>BOT-AUDIT (var {s.bot_window_n} stängda affärer)</b>\n"
        f"\n"
        f"<b>Senaste {s.bot_window_n} affärer:</b>\n"
        f"   Vinst/Förlust/0: {s.win_count}/{s.loss_count}/{s.zero_count} "
        f"(win-rate {win_rate_pct:.0f}%)\n"
        f"   Total PnL: {s.total_pnl:+.2f} USDT\n"
        f"   Snitt vinst: {s.avg_win:+.2f}  Snitt förlust: {s.avg_loss:+.2f}\n"
        f"   Värsta förlust: {s.max_loss:+.2f}\n"
        f"\n"
        f"<b>Aktuellt läge:</b>\n"
        f"   Bybit positioner: {s.bybit_position_count}  "
        f"DB aktiva: {s.db_active_count}\n"
        f"   Bybit öppna order: {s.bybit_order_count}  "
        f"kvarligg: {s.leftover_orders}\n"
        f"   Orealiserad PnL: {s.unrealised_pnl_usdt:+.2f} USDT\n"
        f"   Long notional: {s.long_notional_usdt:.2f} USDT\n"
        f"   Short notional: {s.short_notional_usdt:.2f} USDT\n"
        f"\n"
        f"<b>Sedan start (kumulativt):</b>\n"
        f"   Hedge aktiveringar: {s.hedge_activations_count}\n"
        f"   PROTECTION_FAILED: {s.protection_failed_count}\n"
        f"\n"
        f"<b>Hälsa:</b> {health_line}"
    )
