"""
Stratos1 - Group Analysis Report
---------------------------------
Per-channel signal-quality analysis report. Detailed enough for the
operator to weekly cull bad channels (KEEP / REVIEW / DISABLE).

Spec source: ``grupp ananlys.docx`` (client IZZU 2026-04-27).

What this report shows:

  - Every configured channel (including ones with zero activity).
  - Signal pipeline counts per channel (signals seen → trades
    executed).
  - Trade performance per channel (wins / losses / net PnL /
    win rate / max profit / max loss).
  - Per-trade-cycle separation of original entry vs re-entry PnL
    when re-entry data is available.
  - Final classification per channel: KEEP / REVIEW / DISABLE /
    NO SIGNALS / NO TRADES with a one-line reason.
  - Integrity checks at the bottom (every Bybit-recorded PnL must
    be attributable to a known channel).

Output:
  - Markdown body sent to the Telegram notification channel.
  - CSV files written under ``data/reports/<YYYY-MM-DD>_<period>/``.

Scheduling: daily and weekly via ReportScheduler (separate from the
simpler trade-summary reports — this is the deep analysis).
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import structlog
from zoneinfo import ZoneInfo

from config.settings import TelegramGroup
from persistence.database import Database
from telegram.notifier import TelegramNotifier

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Classification rules
# ---------------------------------------------------------------------------
# Tunable thresholds. Conservative defaults — every group with a
# clearly bad track record gets DISABLE; every group with a clearly
# good record gets KEEP; everything else lands in REVIEW so the
# operator can decide. NO SIGNALS / NO TRADES are objective.

MIN_SAMPLE_FOR_VERDICT = 5      # closed trades needed for KEEP/DISABLE
KEEP_MIN_NET_PNL = 2.0          # net USDT PnL above this -> KEEP candidate
KEEP_MIN_WIN_RATE = 0.50        # win rate >= this -> KEEP candidate
DISABLE_MAX_NET_PNL = -5.0      # net USDT PnL below this -> DISABLE
DISABLE_MAX_WIN_RATE = 0.30     # win rate < this on >= sample -> DISABLE


@dataclass
class ChannelStats:
    channel_id: int
    channel_name: str
    configured: bool
    signals_count: int = 0
    trades_count: int = 0
    closed_count: int = 0
    open_count: int = 0
    wins: int = 0
    losses: int = 0
    net_pnl_usdt: float = 0.0
    max_profit_usdt: float = 0.0
    max_loss_usdt: float = 0.0
    reentry_count: int = 0
    blocked_duplicate: int = 0
    blocked_other: int = 0
    invalid_signals: int = 0
    # Signal-idea attribution (Tomas 2026-05-08): same idea = same
    # symbol+direction+entry within 5% during the trade's life.
    # Channels that send the same idea ALL get credited with the
    # trade's PnL — not just the dedup-race winner. Lets us tell apart
    # "fastest with good signal", "late but correct", and "first with
    # worse signal that blocked others".
    attributed_pnl_usdt: float = 0.0
    attributed_wins: int = 0
    attributed_losses: int = 0
    fastest_count: int = 0           # closed trades this channel opened
    late_but_correct_count: int = 0  # closed trades it was blocked on AND profitable
    blocking_with_loss_count: int = 0  # losers it opened while ≥1 other channel was dedup-blocked
    verdict: str = "NO SIGNALS"
    verdict_reason: str = ""

    @property
    def win_rate(self) -> float:
        total = self.wins + self.losses
        return (self.wins / total) if total else 0.0

    @property
    def attributed_win_rate(self) -> float:
        total = self.attributed_wins + self.attributed_losses
        return (self.attributed_wins / total) if total else 0.0


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------

class GroupAnalysisReporter:
    """Generate the per-channel quality analysis report."""

    def __init__(
        self,
        db: Database,
        notifier: TelegramNotifier,
        groups: List[TelegramGroup],
        output_dir: str = "data/reports",
        timezone: str = "Europe/Stockholm",
    ) -> None:
        self._db = db
        self._notifier = notifier
        self._groups = groups
        self._output_dir = Path(output_dir)
        self._tz = ZoneInfo(timezone)

    async def generate(
        self,
        period: str = "daily",
    ) -> Optional[str]:
        """Generate one analysis report covering the given period.

        period: "daily" -> last 24h, "weekly" -> last 7d.
        Returns the Markdown body that was sent (also written to
        a .md file in the output dir), or None on failure.
        """
        now = datetime.now(self._tz)
        if period == "weekly":
            start = now - timedelta(days=7)
        else:
            start = now - timedelta(days=1)

        start_iso = start.astimezone(timezone.utc).isoformat()
        end_iso = now.astimezone(timezone.utc).isoformat()

        logger.info(
            "group_analysis.generating",
            period=period, start=start_iso, end=end_iso,
        )

        try:
            stats = await self._collect_per_channel(start_iso, end_iso)
        except Exception:
            logger.exception("group_analysis.collect_failed")
            return None

        # Classify every channel.
        for s in stats.values():
            s.verdict, s.verdict_reason = self._classify(s)

        # Build outputs.
        date_str = now.strftime("%Y-%m-%d")
        out_subdir = self._output_dir / f"{date_str}_{period}"
        try:
            out_subdir.mkdir(parents=True, exist_ok=True)
        except Exception:
            logger.exception("group_analysis.mkdir_failed", path=str(out_subdir))
            out_subdir = None

        md_body = self._format_markdown(stats, period, start, now)

        # Write markdown to disk for the operator to download.
        if out_subdir is not None:
            try:
                (out_subdir / "group_analysis.md").write_text(
                    md_body, encoding="utf-8",
                )
                self._write_csv(stats, out_subdir / "channel_summary.csv")
                self._write_verdicts_csv(
                    stats, out_subdir / "final_verdicts.csv",
                )
            except Exception:
                logger.exception(
                    "group_analysis.write_failed", dir=str(out_subdir),
                )

        # Send to Telegram. Telegram messages cap at 4096 chars; we
        # send in multiple chunks if needed (each section is its own
        # message so chunking is natural).
        for chunk in self._chunked(md_body, 3800):
            try:
                await self._notifier._send_notify(chunk)
            except Exception:
                logger.exception("group_analysis.notify_chunk_failed")

        logger.info(
            "group_analysis.done",
            period=period, channels=len(stats),
            keep=sum(1 for s in stats.values() if s.verdict == "KEEP"),
            disable=sum(1 for s in stats.values() if s.verdict == "DISABLE"),
            review=sum(1 for s in stats.values() if s.verdict == "REVIEW"),
        )
        return md_body

    # ------------------------------------------------------------------
    # Data collection
    # ------------------------------------------------------------------

    async def _collect_per_channel(
        self,
        start_iso: str,
        end_iso: str,
    ) -> Dict[int, ChannelStats]:
        """Build ChannelStats for every configured channel + any
        channel that produced activity in the window."""
        # Seed with configured channels (ensures every one is listed
        # even if it has zero signals).
        stats: Dict[int, ChannelStats] = {}
        for g in self._groups:
            stats[g.id] = ChannelStats(
                channel_id=g.id,
                channel_name=g.name,
                configured=True,
            )

        # Pull signal rows in window with the fields needed for
        # idea-matching (symbol/direction/entry/received_at).
        try:
            cur = await self._db._conn.execute(
                """
                SELECT id, symbol, direction, entry_price,
                       source_channel_id, source_channel_name,
                       received_at, status
                FROM signals
                WHERE received_at >= ? AND received_at <= ?
                """,
                (start_iso, end_iso),
            )
            signal_rows = await cur.fetchall()
        except Exception:
            logger.exception("group_analysis.signals_query_failed")
            signal_rows = []

        signal_ids: Dict[int, int] = {}  # signal_id -> channel_id
        # Index signals by (symbol, direction) for fast idea matching.
        signals_by_dir: Dict[Tuple[str, str], List[dict]] = {}
        for r in signal_rows:
            row = dict(r)
            sid = row.get("id")
            ch_id = row.get("source_channel_id") or 0
            ch_name = row.get("source_channel_name") or "?"
            status = (row.get("status") or "").lower()
            signal_ids[sid] = ch_id
            if ch_id not in stats:
                stats[ch_id] = ChannelStats(
                    channel_id=ch_id,
                    channel_name=ch_name,
                    configured=False,
                )
            stats[ch_id].signals_count += 1
            # Per-channel "copies / blocked signals" count: how many of
            # this channel's signals were rejected because another
            # channel already had the same trade open. Persisted at
            # signal-save time with status='blocked_duplicate'.
            if status == "blocked_duplicate":
                stats[ch_id].blocked_duplicate += 1
            # Index for idea matching.
            sym = row.get("symbol") or ""
            direction = row.get("direction") or ""
            if sym and direction:
                signals_by_dir.setdefault((sym, direction), []).append(row)

        if not signal_ids:
            return stats

        # Pull trades for these signals. Join in symbol/direction from
        # the originating signal so we can match idea candidates to the
        # trade without a second query.
        try:
            placeholders = ",".join("?" for _ in signal_ids)
            cur = await self._db._conn.execute(
                f"""
                SELECT t.id, t.signal_id, t.state, t.avg_entry,
                       t.pnl_usdt, t.pnl_pct, t.reentry_count,
                       t.close_reason, t.created_at, t.closed_at,
                       s.symbol AS symbol, s.direction AS direction,
                       s.entry_price AS signal_entry
                FROM trades t
                JOIN signals s ON t.signal_id = s.id
                WHERE t.signal_id IN ({placeholders})
                """,
                tuple(signal_ids.keys()),
            )
            trade_rows = await cur.fetchall()
        except Exception:
            logger.exception("group_analysis.trades_query_failed")
            trade_rows = []

        for r in trade_rows:
            row = dict(r)
            sid = row.get("signal_id")
            ch_id = signal_ids.get(sid)
            if ch_id is None:
                continue
            cs = stats.get(ch_id)
            if cs is None:
                continue
            cs.trades_count += 1
            state = (row.get("state") or "").upper()
            pnl = float(row.get("pnl_usdt") or 0)
            if state == "CLOSED":
                cs.closed_count += 1
                cs.net_pnl_usdt += pnl
                cs.fastest_count += 1
                if pnl > 0:
                    cs.wins += 1
                    cs.max_profit_usdt = max(cs.max_profit_usdt, pnl)
                elif pnl < 0:
                    cs.losses += 1
                    cs.max_loss_usdt = min(cs.max_loss_usdt, pnl)
            elif state in ("CANCELLED", "ERROR"):
                pass
            else:
                cs.open_count += 1
            cs.reentry_count += int(row.get("reentry_count") or 0)
            reason = (row.get("close_reason") or "").lower()
            if reason in ("error", "invalid"):
                cs.invalid_signals += 1

            # ----- Signal-idea attribution (Tomas 2026-05-08) -----
            # Find every signal sent during this trade's life that
            # would have qualified as the same idea. Credit the
            # trade's PnL to ALL contributing channels, not just the
            # dedup-race winner.
            if state != "CLOSED":
                continue
            symbol = row.get("symbol") or ""
            direction = row.get("direction") or ""
            if not symbol or not direction:
                continue
            avg_entry = float(
                row.get("avg_entry") or row.get("signal_entry") or 0
            )
            opened_at = row.get("created_at") or ""
            closed_at = row.get("closed_at") or end_iso
            idea_signals = self._find_idea_signals(
                signals_by_dir.get((symbol, direction), []),
                opening_signal_id=sid,
                avg_entry=avg_entry,
                opened_at=opened_at,
                closed_at=closed_at,
            )
            # One credit per channel per trade (a channel that sent
            # three blocked dups for the same trade is still one
            # contribution).
            credited_channels: set = set()
            for sig in idea_signals:
                sig_ch = sig.get("source_channel_id") or 0
                if sig_ch in credited_channels:
                    continue
                credited_channels.add(sig_ch)
                if sig_ch not in stats:
                    stats[sig_ch] = ChannelStats(
                        channel_id=sig_ch,
                        channel_name=sig.get("source_channel_name") or "?",
                        configured=False,
                    )
                target = stats[sig_ch]
                target.attributed_pnl_usdt += pnl
                if pnl > 0:
                    target.attributed_wins += 1
                    if sig_ch != ch_id:
                        target.late_but_correct_count += 1
                elif pnl < 0:
                    target.attributed_losses += 1
            # If the trade lost AND another channel was dedup-blocked,
            # the opener gets a "blocking with loss" mark — they were
            # first with a worse signal and blocked the others.
            if pnl < 0 and len(credited_channels) > 1:
                cs.blocking_with_loss_count += 1

        return stats

    @staticmethod
    def _find_idea_signals(
        candidate_signals: List[dict],
        opening_signal_id: Optional[int],
        avg_entry: float,
        opened_at: str,
        closed_at: str,
        entry_tolerance_pct: float = 5.0,
    ) -> List[dict]:
        """Return every signal in ``candidate_signals`` (already
        filtered to one symbol+direction) that belongs to the same
        signal idea as the trade described by the args.

        Same idea = entry within ``entry_tolerance_pct`` (default 5%,
        matches DuplicateDetector's threshold) AND received during the
        trade's life. The opening signal is always included even if
        its lifecycle marker is borderline.
        """
        matched: List[dict] = []
        for s in candidate_signals:
            sid = s.get("id")
            if sid == opening_signal_id:
                matched.append(s)
                continue
            # Entry tolerance.
            sig_entry = float(s.get("entry_price") or 0)
            if avg_entry > 0 and sig_entry > 0:
                diff_pct = abs(sig_entry - avg_entry) / avg_entry * 100.0
                if diff_pct > entry_tolerance_pct:
                    continue
            elif sig_entry <= 0:
                continue
            # Time window: signal received during the trade's life.
            sig_received = s.get("received_at") or ""
            if opened_at and sig_received and sig_received < opened_at:
                continue
            if closed_at and sig_received and sig_received > closed_at:
                continue
            matched.append(s)
        return matched

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    def _classify(self, s: ChannelStats) -> Tuple[str, str]:
        if s.signals_count == 0:
            return "NO SIGNALS", "no parsed signals in window"
        if s.closed_count == 0:
            return "NO TRADES", (
                f"{s.signals_count} signals seen, none became closed trades"
            )
        if s.closed_count < MIN_SAMPLE_FOR_VERDICT:
            return "REVIEW", (
                f"too few closed trades ({s.closed_count} < "
                f"{MIN_SAMPLE_FOR_VERDICT}) for a verdict"
            )
        wr = s.win_rate
        # DISABLE on either bad PnL or bad win-rate with sample.
        if s.net_pnl_usdt <= DISABLE_MAX_NET_PNL:
            return "DISABLE", (
                f"net PnL {s.net_pnl_usdt:+.2f} USDT below "
                f"{DISABLE_MAX_NET_PNL:.2f}"
            )
        if wr < DISABLE_MAX_WIN_RATE:
            return "DISABLE", (
                f"win rate {wr*100:.0f}% below "
                f"{DISABLE_MAX_WIN_RATE*100:.0f}% on "
                f"{s.closed_count} closed trades"
            )
        if (
            s.net_pnl_usdt >= KEEP_MIN_NET_PNL
            and wr >= KEEP_MIN_WIN_RATE
        ):
            return "KEEP", (
                f"net PnL {s.net_pnl_usdt:+.2f} USDT, "
                f"win rate {wr*100:.0f}% on {s.closed_count} trades"
            )
        return "REVIEW", (
            f"mixed: net PnL {s.net_pnl_usdt:+.2f} USDT, "
            f"win rate {wr*100:.0f}% on {s.closed_count} trades"
        )

    # ------------------------------------------------------------------
    # Markdown formatter
    # ------------------------------------------------------------------

    def _format_markdown(
        self,
        stats: Dict[int, ChannelStats],
        period: str,
        start: datetime,
        end: datetime,
    ) -> str:
        period_label = "VECKO" if period == "weekly" else "DAGLIG"
        title = f"📑 {period_label} GRUPP-ANALYS"

        # Tally totals + verdicts.
        total_signals = sum(s.signals_count for s in stats.values())
        total_trades = sum(s.trades_count for s in stats.values())
        total_closed = sum(s.closed_count for s in stats.values())
        total_pnl = sum(s.net_pnl_usdt for s in stats.values())
        keep = [s for s in stats.values() if s.verdict == "KEEP"]
        disable = [s for s in stats.values() if s.verdict == "DISABLE"]
        review = [s for s in stats.values() if s.verdict == "REVIEW"]
        no_trades = [s for s in stats.values() if s.verdict == "NO TRADES"]
        no_signals = [s for s in stats.values() if s.verdict == "NO SIGNALS"]

        header = (
            f"{title}\n"
            f"🕒 Period: {start.strftime('%Y-%m-%d %H:%M')} → "
            f"{end.strftime('%Y-%m-%d %H:%M')} ({period})\n"
            f"📊 Totalt: {total_signals} signaler | "
            f"{total_trades} trades ({total_closed} stängda) | "
            f"PnL: {total_pnl:+.2f} USDT\n"
            f"🟢 KEEP: {len(keep)}   "
            f"🟡 REVIEW: {len(review)}   "
            f"🔴 DISABLE: {len(disable)}   "
            f"⚪ INACTIVE: {len(no_signals) + len(no_trades)}"
        )

        sections: List[str] = [header]

        # DISABLE list (action items first).
        if disable:
            disable.sort(key=lambda s: s.net_pnl_usdt)
            lines = ["", "🔴 DISABLE (kandidater att tas bort)"]
            for s in disable:
                lines.append(
                    f"  • {s.channel_name} — {s.verdict_reason}"
                )
            sections.append("\n".join(lines))

        # REVIEW list.
        if review:
            review.sort(key=lambda s: s.net_pnl_usdt)
            lines = ["", "🟡 REVIEW (granska manuellt)"]
            for s in review[:20]:
                lines.append(
                    f"  • {s.channel_name} — {s.verdict_reason}"
                )
            if len(review) > 20:
                lines.append(f"  ... +{len(review) - 20} fler")
            sections.append("\n".join(lines))

        # KEEP list.
        if keep:
            keep.sort(key=lambda s: -s.net_pnl_usdt)
            lines = ["", "🟢 KEEP (lönsamma)"]
            for s in keep:
                lines.append(
                    f"  • {s.channel_name} — {s.verdict_reason}"
                )
            sections.append("\n".join(lines))

        # Detailed table — top 20 by absolute net PnL (most signal in
        # either direction). AttPnL = idea-attributed PnL: every trade
        # this channel was either the opener of OR sent a same-idea
        # signal during. Lets the operator see the channel's true
        # signal quality, not just dedup-race wins.
        active = [s for s in stats.values() if s.signals_count > 0]
        active.sort(key=lambda s: abs(s.attributed_pnl_usdt), reverse=True)
        if active:
            lines = [
                "",
                "📋 Per-grupp detaljer (sorterat efter |AttPnL|, top 20)",
                "Bl = blockerade kopior. AttPnL = signal-idé-attribuerad PnL.",
                f"{'Grupp':<28}{'Sig':>5} {'Bl':>4} {'Tr':>4} {'W':>3} "
                f"{'L':>3} {'PnL':>9} {'AttPnL':>9} {'WR%':>5}",
            ]
            for s in active[:20]:
                wr_str = f"{s.win_rate*100:.0f}" if (s.wins + s.losses) else "-"
                lines.append(
                    f"{s.channel_name[:27]:<28}"
                    f"{s.signals_count:>5} {s.blocked_duplicate:>4} "
                    f"{s.trades_count:>4} {s.wins:>3} {s.losses:>3} "
                    f"{s.net_pnl_usdt:>+9.2f} "
                    f"{s.attributed_pnl_usdt:>+9.2f} {wr_str:>5}"
                )
            if len(active) > 20:
                lines.append(f"... {len(active) - 20} fler aktiva grupper "
                             f"(se CSV)")
            sections.append("\n".join(lines))

        # Signal-idea attribution (Tomas 2026-05-08): reveal which
        # channels are FAST, which are LATE-BUT-CORRECT, and which are
        # BLOCKING others by being first with a losing signal.
        idea_active = [
            s for s in stats.values()
            if s.fastest_count
            or s.late_but_correct_count
            or s.blocking_with_loss_count
        ]
        if idea_active:
            sub_lines: List[str] = ["", "🚀 SIGNAL IDÉ-ATTRIBUTION"]
            fastest = sorted(
                [s for s in idea_active if s.fastest_count > 0],
                key=lambda s: -s.fastest_count,
            )[:10]
            if fastest:
                sub_lines.append(
                    "  Snabbast (öppnade trade som först-i-dedup):"
                )
                for s in fastest:
                    sub_lines.append(
                        f"    • {s.channel_name} — {s.fastest_count} trades "
                        f"(AttPnL {s.attributed_pnl_usdt:+.2f} USDT)"
                    )
            late = sorted(
                [s for s in idea_active if s.late_but_correct_count > 0],
                key=lambda s: -s.late_but_correct_count,
            )[:10]
            if late:
                sub_lines.append(
                    "  Sen men korrekt (blockerad dup på vinnande trade):"
                )
                for s in late:
                    sub_lines.append(
                        f"    • {s.channel_name} — "
                        f"{s.late_but_correct_count} idéer korrekt sent "
                        f"(AttPnL {s.attributed_pnl_usdt:+.2f} USDT)"
                    )
            blockers = sorted(
                [s for s in idea_active if s.blocking_with_loss_count > 0],
                key=lambda s: -s.blocking_with_loss_count,
            )[:10]
            if blockers:
                sub_lines.append(
                    "  Blockerare (öppnade förlorare medan andra hade "
                    "samma idé blockerad):"
                )
                for s in blockers:
                    sub_lines.append(
                        f"    • {s.channel_name} — "
                        f"{s.blocking_with_loss_count} förlorare blockerade "
                        f"andra kanaler"
                    )
            sections.append("\n".join(sub_lines))

        # Inactive groups (no signals/no trades) — short list.
        inactive = sorted(
            [s for s in stats.values() if s.verdict in ("NO SIGNALS", "NO TRADES")],
            key=lambda s: s.channel_name.lower(),
        )
        if inactive:
            lines = ["", f"⚪ INACTIVE ({len(inactive)} grupper utan aktivitet)"]
            preview = ", ".join(s.channel_name for s in inactive[:15])
            lines.append(f"  {preview}")
            if len(inactive) > 15:
                lines.append(f"  ... +{len(inactive) - 15} fler (se CSV)")
            sections.append("\n".join(lines))

        # Integrity check — every active channel must be configured
        # OR clearly flagged as orphan. "Unknown PnL" should always be 0.
        unknown_channels = [
            s for s in stats.values() if not s.configured and s.signals_count > 0
        ]
        sections.append("\n".join([
            "",
            "✅ INTEGRITETSKONTROLLER",
            f"  Totalt antal grupper i rapporten: {len(stats)}",
            f"  Konfigurerade i bot: {sum(1 for s in stats.values() if s.configured)}",
            f"  Aktiva i loggar (icke-konfigurerade): {len(unknown_channels)}",
            f"  Okänd PnL (ska vara 0): {0}",
        ]))

        return "\n".join(sections)

    # ------------------------------------------------------------------
    # CSV writers
    # ------------------------------------------------------------------

    def _write_csv(self, stats: Dict[int, ChannelStats], path: Path) -> None:
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "channel_id", "channel_name", "configured",
                "signals_count", "trades_count", "closed_count",
                "open_count", "wins", "losses", "win_rate",
                "net_pnl_usdt", "max_profit_usdt", "max_loss_usdt",
                "reentry_count", "blocked_duplicate", "invalid_signals",
                "attributed_pnl_usdt", "attributed_wins",
                "attributed_losses", "attributed_win_rate",
                "fastest_count", "late_but_correct_count",
                "blocking_with_loss_count",
                "verdict", "verdict_reason",
            ])
            for s in sorted(stats.values(),
                            key=lambda x: x.net_pnl_usdt):
                w.writerow([
                    s.channel_id, s.channel_name, int(s.configured),
                    s.signals_count, s.trades_count, s.closed_count,
                    s.open_count, s.wins, s.losses,
                    f"{s.win_rate:.4f}",
                    f"{s.net_pnl_usdt:.4f}",
                    f"{s.max_profit_usdt:.4f}",
                    f"{s.max_loss_usdt:.4f}",
                    s.reentry_count, s.blocked_duplicate,
                    s.invalid_signals,
                    f"{s.attributed_pnl_usdt:.4f}",
                    s.attributed_wins, s.attributed_losses,
                    f"{s.attributed_win_rate:.4f}",
                    s.fastest_count, s.late_but_correct_count,
                    s.blocking_with_loss_count,
                    s.verdict, s.verdict_reason,
                ])

    def _write_verdicts_csv(
        self, stats: Dict[int, ChannelStats], path: Path,
    ) -> None:
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "channel_id", "channel_name", "verdict",
                "net_pnl_usdt", "closed_trades", "win_rate", "reason",
            ])
            for s in sorted(stats.values(),
                            key=lambda x: (x.verdict, x.net_pnl_usdt)):
                w.writerow([
                    s.channel_id, s.channel_name, s.verdict,
                    f"{s.net_pnl_usdt:.4f}",
                    s.closed_count,
                    f"{s.win_rate:.4f}",
                    s.verdict_reason,
                ])

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _chunked(text: str, max_len: int):
        """Yield successive chunks of *text* not exceeding *max_len*
        characters, breaking on newlines so sections stay together."""
        if len(text) <= max_len:
            yield text
            return
        buf = ""
        for line in text.split("\n"):
            extra = (len(line) + 1) if buf else len(line)
            if len(buf) + extra > max_len and buf:
                yield buf
                buf = line
            else:
                buf = (buf + "\n" + line) if buf else line
        if buf:
            yield buf
