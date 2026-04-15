"""
reporting/scheduler.py - Report generation and scheduling for Stratos1.

Uses APScheduler (AsyncIOScheduler) to send daily and weekly reports at
configured times.  All report templates follow the Swedish-language formats
specified in the project requirements.

Reports are generated per Telegram source group, formatted as plain text,
and delivered through the TelegramNotifier.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from zoneinfo import ZoneInfo

from config.settings import ReportingSettings, TelegramGroup
from persistence.database import Database
from telegram.notifier import TelegramNotifier

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_int(value: Any, default: int = 0) -> int:
    """Coerce a value to int, returning *default* on None / error."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Coerce a value to float, returning *default* on None / error."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_pyramid_counts(raw: Any) -> Dict[str, int]:
    """Parse the ``pyramid_counts`` JSON field into a dict.

    Accepts a JSON string, a dict, or None.  Always returns a dict
    with string keys and int values.
    """
    if raw is None:
        return {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return {}
    if isinstance(raw, dict):
        return {str(k): _safe_int(v) for k, v in raw.items()}
    return {}


def _merge_pyramid_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """Sum pyramid_counts across multiple report_stats rows."""
    merged: Dict[str, int] = {}
    for row in rows:
        pc = _parse_pyramid_counts(row.get("pyramid_counts"))
        for key, val in pc.items():
            merged[key] = merged.get(key, 0) + val
    return merged


def _aggregate_report_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate a list of report_stats rows into a single summary dict."""
    if not rows:
        return {
            "signals_count": 0,
            "active_count": 0,
            "blocked_count": 0,
            "wins": 0,
            "losses": 0,
            "tp_count": 0,
            "sl_count": 0,
            "trailing_stop_count": 0,
            "hedge_count": 0,
            "reentry_count": 0,
            "timeout_deleted": 0,
            "invalid_signals": 0,
            "api_failures": 0,
            "max_profit": 0.0,
            "min_profit": 0.0,
            "total_profit": 0.0,
            "pyramid_counts": {},
        }

    int_fields = [
        "signals_count", "active_count", "blocked_count", "wins", "losses",
        "tp_count", "sl_count", "trailing_stop_count", "hedge_count",
        "reentry_count", "timeout_deleted", "invalid_signals", "api_failures",
    ]
    result: Dict[str, Any] = {}
    for field in int_fields:
        result[field] = sum(_safe_int(r.get(field)) for r in rows)

    result["total_profit"] = sum(_safe_float(r.get("total_profit")) for r in rows)
    result["max_profit"] = max((_safe_float(r.get("max_profit")) for r in rows), default=0.0)
    result["min_profit"] = min((_safe_float(r.get("min_profit")) for r in rows), default=0.0)
    result["pyramid_counts"] = _merge_pyramid_counts(rows)

    return result


def _aggregate_error_stats(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """Aggregate a list of error_stats rows into a single summary dict."""
    fields = [
        "order_failed", "order_rejected", "position_not_opened",
        "position_not_closed", "insufficient_im", "insufficient_balance",
        "api_errors", "system_errors", "invalid_signals",
        "sl_tp_not_executed", "no_money", "timeout_deleted",
    ]
    result: Dict[str, int] = {}
    for field in fields:
        result[field] = sum(_safe_int(r.get(field)) for r in rows)
    return result


def _winrate(wins: int, losses: int) -> float:
    """Calculate win-rate percentage, returning 0 if no trades."""
    total = wins + losses
    if total == 0:
        return 0.0
    return (wins / total) * 100.0


# ---------------------------------------------------------------------------
# Per-symbol trade results query
# ---------------------------------------------------------------------------

async def _get_symbol_results(
    db: Database,
    group_id: int,
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    """Return per-symbol PnL for closed trades belonging to *group_id*
    within the date range.

    Joins trades -> signals to resolve the source group and symbol,
    then aggregates pnl_pct and pnl_usdt per symbol.
    """
    cursor = await db._conn.execute(
        """
        SELECT
            s.symbol,
            SUM(t.pnl_pct)  AS pct,
            SUM(t.pnl_usdt) AS usdt
        FROM trades t
        JOIN signals s ON t.signal_id = s.id
        WHERE s.source_channel_id = ?
          AND t.state = 'closed'
          AND t.closed_at >= ?
          AND t.closed_at <= ?
        GROUP BY s.symbol
        ORDER BY SUM(t.pnl_usdt) DESC
        """,
        (group_id, date_from, date_to + "T23:59:59"),
    )
    rows = await cursor.fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Report Scheduler
# ---------------------------------------------------------------------------

class ReportScheduler:
    """Generate and send scheduled trading reports via Telegram.

    Parameters
    ----------
    settings:
        ``ReportingSettings`` with ``daily_report_hour``,
        ``weekly_report_day``, and ``weekly_report_hour``.
    db:
        Initialised ``Database`` instance.
    notifier:
        Started ``TelegramNotifier`` instance.
    groups:
        List of ``TelegramGroup`` to report on.
    timezone:
        IANA timezone string for scheduling (default Europe/Stockholm).
    """

    def __init__(
        self,
        settings: ReportingSettings,
        db: Database,
        notifier: TelegramNotifier,
        groups: List[TelegramGroup],
        timezone: str = "Europe/Stockholm",
    ) -> None:
        self._settings = settings
        self._db = db
        self._notifier = notifier
        self._groups = groups
        self._tz_name = timezone
        self._tz = ZoneInfo(timezone)
        self._scheduler: Optional[AsyncIOScheduler] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Create APScheduler jobs for daily and weekly reports and start
        the scheduler."""
        self._scheduler = AsyncIOScheduler(timezone=self._tz_name)

        # Daily reports - every day at the configured hour
        daily_hour = self._settings.daily_report_hour
        self._scheduler.add_job(
            self.generate_daily_report,
            CronTrigger(hour=daily_hour, minute=0, timezone=self._tz_name),
            id="daily_report",
            name="Daily trading report",
            replace_existing=True,
        )
        self._scheduler.add_job(
            self.generate_daily_error_report,
            CronTrigger(hour=daily_hour, minute=5, timezone=self._tz_name),
            id="daily_error_report",
            name="Daily error report",
            replace_existing=True,
        )
        self._scheduler.add_job(
            lambda: self.generate_group_statistics(period="daily"),
            CronTrigger(hour=daily_hour, minute=10, timezone=self._tz_name),
            id="daily_group_statistics",
            name="Daily group statistics",
            replace_existing=True,
        )

        # Weekly reports - on the configured weekday at the configured hour
        weekly_day = self._settings.weekly_report_day  # 0 = Monday
        weekly_hour = self._settings.weekly_report_hour

        # APScheduler CronTrigger uses 'mon'-'sun' or 0(mon)-6(sun)
        self._scheduler.add_job(
            self.generate_weekly_report,
            CronTrigger(
                day_of_week=weekly_day,
                hour=weekly_hour,
                minute=15,
                timezone=self._tz_name,
            ),
            id="weekly_report",
            name="Weekly trading report",
            replace_existing=True,
        )
        self._scheduler.add_job(
            self.generate_weekly_error_report,
            CronTrigger(
                day_of_week=weekly_day,
                hour=weekly_hour,
                minute=20,
                timezone=self._tz_name,
            ),
            id="weekly_error_report",
            name="Weekly error report",
            replace_existing=True,
        )

        self._scheduler.start()
        logger.info(
            "report_scheduler.started",
            daily_hour=daily_hour,
            weekly_day=weekly_day,
            weekly_hour=weekly_hour,
            timezone=self._tz_name,
            group_count=len(self._groups),
        )

    async def stop(self) -> None:
        """Shut down the scheduler gracefully."""
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("report_scheduler.stopped")
            self._scheduler = None

    # ------------------------------------------------------------------
    # Date-range helpers
    # ------------------------------------------------------------------

    def _today_range(self) -> tuple[str, str]:
        """Return (date_from, date_to) for today in the configured timezone."""
        now = datetime.now(self._tz)
        date_str = now.strftime("%Y-%m-%d")
        return date_str, date_str

    def _week_range(self) -> tuple[str, str]:
        """Return (date_from, date_to) for the current Monday-to-Sunday week."""
        now = datetime.now(self._tz)
        monday = now - timedelta(days=now.weekday())
        sunday = monday + timedelta(days=6)
        return monday.strftime("%Y-%m-%d"), sunday.strftime("%Y-%m-%d")

    # ------------------------------------------------------------------
    # Daily report
    # ------------------------------------------------------------------

    async def generate_daily_report(self) -> None:
        """Generate and send a daily trading report for every source group."""
        date_from, date_to = self._today_range()
        logger.info("report.daily.generating", date=date_from, groups=len(self._groups))

        for group in self._groups:
            try:
                rows = await self._db.get_report_stats(group.id, date_from, date_to)
                stats = _aggregate_report_stats(rows)
                symbol_results = await _get_symbol_results(
                    self._db, group.id, date_from, date_to,
                )
                text = self._format_group_report(group.name, stats, "daily", symbol_results)
                await self._notifier._send_notify(text)
                logger.info("report.daily.sent", group=group.name)
            except Exception:
                logger.exception("report.daily.error", group=group.name)

    # ------------------------------------------------------------------
    # Weekly report
    # ------------------------------------------------------------------

    async def generate_weekly_report(self) -> None:
        """Generate and send a weekly trading report for every source group."""
        date_from, date_to = self._week_range()
        logger.info("report.weekly.generating", date_from=date_from, date_to=date_to)

        for group in self._groups:
            try:
                rows = await self._db.get_report_stats(group.id, date_from, date_to)
                stats = _aggregate_report_stats(rows)
                symbol_results = await _get_symbol_results(
                    self._db, group.id, date_from, date_to,
                )
                text = self._format_group_report(group.name, stats, "weekly", symbol_results)
                await self._notifier._send_notify(text)
                logger.info("report.weekly.sent", group=group.name)
            except Exception:
                logger.exception("report.weekly.error", group=group.name)

    # ------------------------------------------------------------------
    # Daily error report
    # ------------------------------------------------------------------

    async def generate_daily_error_report(self) -> None:
        """Generate and send a daily error report for every source group."""
        date_from, date_to = self._today_range()
        logger.info("report.daily_error.generating", date=date_from)

        for group in self._groups:
            try:
                rows = await self._db.get_error_stats(group.id, date_from, date_to)
                errors = _aggregate_error_stats(rows)
                # Also fetch total signals for error-rate calculation
                report_rows = await self._db.get_report_stats(group.id, date_from, date_to)
                total_signals = sum(_safe_int(r.get("signals_count")) for r in report_rows)

                text = self._format_error_report(group.name, errors, "daily", total_signals)
                await self._notifier._send_notify(text)
                logger.info("report.daily_error.sent", group=group.name)
            except Exception:
                logger.exception("report.daily_error.error", group=group.name)

    # ------------------------------------------------------------------
    # Weekly error report
    # ------------------------------------------------------------------

    async def generate_weekly_error_report(self) -> None:
        """Generate and send a weekly error report for every source group."""
        date_from, date_to = self._week_range()
        logger.info("report.weekly_error.generating", date_from=date_from, date_to=date_to)

        for group in self._groups:
            try:
                rows = await self._db.get_error_stats(group.id, date_from, date_to)
                errors = _aggregate_error_stats(rows)
                report_rows = await self._db.get_report_stats(group.id, date_from, date_to)
                total_signals = sum(_safe_int(r.get("signals_count")) for r in report_rows)

                text = self._format_error_report(group.name, errors, "weekly", total_signals)
                await self._notifier._send_notify(text)
                logger.info("report.weekly_error.sent", group=group.name)
            except Exception:
                logger.exception("report.weekly_error.error", group=group.name)

    # ------------------------------------------------------------------
    # Group statistics report
    # ------------------------------------------------------------------

    async def generate_group_statistics(self, period: str = "daily") -> None:
        """Generate and send a group-comparison statistics report.

        Parameters
        ----------
        period:
            ``"daily"`` uses today's date range;
            ``"weekly"`` uses the current week.
        """
        if period == "weekly":
            date_from, date_to = self._week_range()
        else:
            date_from, date_to = self._today_range()

        logger.info(
            "report.group_statistics.generating",
            period=period,
            date_from=date_from,
            date_to=date_to,
        )

        try:
            all_stats = await self._db.get_all_group_stats(date_from, date_to)
            text = self._format_group_statistics(all_stats)
            await self._notifier._send_notify(text)
            logger.info("report.group_statistics.sent", period=period, groups=len(all_stats))
        except Exception:
            logger.exception("report.group_statistics.error", period=period)

    # ===================================================================
    # Formatting methods
    # ===================================================================

    def _format_group_report(
        self,
        group_name: str,
        stats: Dict[str, Any],
        period: str,
        symbol_results: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        """Format a daily or weekly group trading report.

        Parameters
        ----------
        group_name:
            Human-readable Telegram group name.
        stats:
            Aggregated report_stats dict.
        period:
            ``"daily"`` or ``"weekly"`` - controls the header text.
        symbol_results:
            Per-symbol PnL list from ``_get_symbol_results``.

        Returns
        -------
        str
            Fully formatted report text ready to send.
        """
        if period == "weekly":
            header = f"\U0001f4d1 VECKORAPPORT FR\u00c5N GRUPP: {group_name}"
        else:
            header = f"\U0001f4d1 DAGLIG RAPPORT FR\u00c5N GRUPP: {group_name}"

        # --- Symbol results table ---
        symbol_lines: List[str] = []
        if symbol_results:
            # Header row
            symbol_lines.append(f"{'Symbol':<14}{'%':<13}{'USDT'}")
            for sr in symbol_results:
                sym = sr.get("symbol", "???")
                pct = _safe_float(sr.get("pct"))
                usdt = _safe_float(sr.get("usdt"))
                symbol_lines.append(f"{sym:<14}{pct:<+13.2f}{usdt:+.2f}")
        else:
            symbol_lines.append("Inga avslutade trades under perioden.")

        # --- Pyramid counts ---
        pc = stats.get("pyramid_counts", {})
        pyramid_lines: List[str] = []
        for i in range(1, 7):
            count = _safe_int(pc.get(str(i)))
            pyramid_lines.append(f"\U0001f4cd Pyramid {i}: {count}")

        # --- Totals ---
        signals_count = _safe_int(stats.get("signals_count"))
        total_profit = _safe_float(stats.get("total_profit"))
        wins = _safe_int(stats.get("wins"))
        losses = _safe_int(stats.get("losses"))
        wr = _winrate(wins, losses)

        timeout_deleted = _safe_int(stats.get("timeout_deleted"))
        invalid_signals = _safe_int(stats.get("invalid_signals"))
        api_failures = _safe_int(stats.get("api_failures"))
        tp_count = _safe_int(stats.get("tp_count"))
        sl_count = _safe_int(stats.get("sl_count"))
        trailing_stop_count = _safe_int(stats.get("trailing_stop_count"))
        hedge_count = _safe_int(stats.get("hedge_count"))
        reentry_count = _safe_int(stats.get("reentry_count"))

        lines = [
            header,
            "\U0001f4ca RESULTAT",
            *symbol_lines,
            "------------------------------------",
            f"\U0001f4cd Fel Order ej \u00f6ppnad inom till\u00e5ten tid (raderad enligt reglerna 24 tim): {timeout_deleted}",
            f"\U0001f4cd Fel: Order ej \u00f6ppnad, dubletter blokerade: {invalid_signals}",
            f"\U0001f4cd Fel: Order ej Fel vid placering: {api_failures}",
            f"\U0001f4cd TP: {tp_count}",
            f"\U0001f4cd SL: {sl_count}",
            *pyramid_lines,
            f"\U0001f4cd Trailing stop: {trailing_stop_count}",
            f"\U0001f4cd Hedge: {hedge_count}",
            f"\U0001f4cd Re Entry: {reentry_count}",
            "",
            f"\U0001f4c8 Antal signaler: {signals_count}",
            f"\U0001f4b9 Totalt resultat: {total_profit:.2f} USDT",
            f"\U0001f4ca Vinst/F\u00f6rlust: {wr:.1f}%",
        ]
        return "\n".join(lines)

    def _format_error_report(
        self,
        group_name: str,
        errors: Dict[str, int],
        period: str,
        total_signals: int = 0,
    ) -> str:
        """Format a daily or weekly error report.

        Parameters
        ----------
        group_name:
            Human-readable Telegram group name.
        errors:
            Aggregated error_stats dict.
        period:
            ``"daily"`` or ``"weekly"``.
        total_signals:
            Total signal count for the period (for error-rate calculation).

        Returns
        -------
        str
            Fully formatted error report text.
        """
        if period == "weekly":
            header = f"\U0001f4d1 VECKORAPPORT \u2013 FELMEDDELANDEN: {group_name}"
        else:
            header = f"\U0001f4d1 DAGSRAPPORT \u2013 FELMEDDELANDEN: {group_name}"

        order_failed = _safe_int(errors.get("order_failed"))
        order_rejected = _safe_int(errors.get("order_rejected"))
        position_not_opened = _safe_int(errors.get("position_not_opened"))
        position_not_closed = _safe_int(errors.get("position_not_closed"))
        insufficient_im = _safe_int(errors.get("insufficient_im"))
        insufficient_balance = _safe_int(errors.get("insufficient_balance"))
        api_errors = _safe_int(errors.get("api_errors"))
        system_errors = _safe_int(errors.get("system_errors"))
        invalid_signals = _safe_int(errors.get("invalid_signals"))
        sl_tp_not_executed = _safe_int(errors.get("sl_tp_not_executed"))
        no_money = _safe_int(errors.get("no_money"))
        timeout_deleted = _safe_int(errors.get("timeout_deleted"))

        total_errors = (
            order_failed + order_rejected + position_not_opened
            + position_not_closed + insufficient_im + insufficient_balance
            + api_errors + system_errors + invalid_signals
            + sl_tp_not_executed + no_money + timeout_deleted
        )

        if total_signals > 0:
            error_rate = (total_errors / total_signals) * 100.0
        else:
            error_rate = 0.0

        # Right-align counts using fixed-width columns
        w = 30  # label width

        lines = [
            header,
            "\U0001f4ca FELMEDDELANDEN",
            f"{'Typ':<{w}}Antal",
            f"{'Order misslyckades:':<{w}}{order_failed}",
            f"{'Order avvisad:':<{w}}{order_rejected}",
            f"{'Position ej \u00f6ppnad:':<{w}}{position_not_opened}",
            f"{'Position ej st\u00e4ngd:':<{w}}{position_not_closed}",
            f"{'Otillr\u00e4cklig IM:':<{w}}{insufficient_im}",
            f"{'Otillr\u00e4cklig balans:':<{w}}{insufficient_balance}",
            f"{'API-fel:':<{w}}{api_errors}",
            f"{'Systemfel:':<{w}}{system_errors}",
            f"{'Signal ogiltig:':<{w}}{invalid_signals}",
            f"{'SL/TP ej utf\u00f6rd:':<{w}}{sl_tp_not_executed}",
            f"{'Slut p\u00e5 pengar:':<{w}}{no_money}",
            f"{'Order raderad (V):':<{w}}{timeout_deleted}",
            "------------------------------------",
            f"\U0001f4c8 Totalt antal fel: {total_errors}",
            f"\U0001f4ca Felfrekvens: {error_rate:.1f}% av totala signaler",
        ]
        return "\n".join(lines)

    def _format_group_statistics(
        self,
        all_stats: List[Dict[str, Any]],
    ) -> str:
        """Format the group-comparison statistics report.

        Parameters
        ----------
        all_stats:
            List of aggregated per-group dicts from
            ``Database.get_all_group_stats``.

        Returns
        -------
        str
            Fully formatted group statistics text.
        """
        header = "\U0001f4d1 DAGSRAPPORT \u2013 statistik p\u00e5 telegramgrupper"

        if not all_stats:
            return f"{header}\n\nInga data tillg\u00e4ngliga f\u00f6r perioden."

        # --- Per-group table ---
        table_header = (
            f"{'Signaler':<10}{'Aktiva':<8}{'Block':<7}"
            f"{'Vinst':<7}{'F\u00f6rlust':<9}{'Winrate':<9}"
            f"{'Max profit':<12}{'Min profit':<12}{'Total profit'}"
        )

        group_lines: List[str] = []
        for gs in all_stats:
            name = gs.get("group_name", "???")
            signals = _safe_int(gs.get("signals_count"))
            active = _safe_int(gs.get("active_count"))
            blocked = _safe_int(gs.get("blocked_count"))
            wins = _safe_int(gs.get("wins"))
            losses = _safe_int(gs.get("losses"))
            wr = _winrate(wins, losses)
            max_p = _safe_float(gs.get("max_profit"))
            min_p = _safe_float(gs.get("min_profit"))
            total_p = _safe_float(gs.get("total_profit"))

            group_lines.append(f"\U0001f539 Grupp: {name}")
            group_lines.append(
                f"{signals:<10}{active:<8}{blocked:<7}"
                f"{wins:<7}{losses:<9}{wr:<9.1f}%"
                f"{max_p:<12.2f} usdt{min_p:<12.2f} usdt{total_p:.2f} usdt"
            )

        # --- Top 10 best ---
        sorted_best = sorted(
            all_stats,
            key=lambda g: _safe_float(g.get("total_profit")),
            reverse=True,
        )[:10]
        top_lines: List[str] = []
        for i, gs in enumerate(sorted_best, start=1):
            name = gs.get("group_name", "???")
            signals = _safe_int(gs.get("signals_count"))
            active = _safe_int(gs.get("active_count"))
            wr = _winrate(_safe_int(gs.get("wins")), _safe_int(gs.get("losses")))
            total_p = _safe_float(gs.get("total_profit"))
            top_lines.append(
                f"\u2705 {i}) {name} | {signals} signaler \u2013 {active} aktiva"
                f" | Winrate {wr:.1f}% | Total profit {total_p:.2f} usdt"
            )

        # --- Bottom 10 worst ---
        sorted_worst = sorted(
            all_stats,
            key=lambda g: _safe_float(g.get("total_profit")),
        )[:10]
        bottom_lines: List[str] = []
        for i, gs in enumerate(sorted_worst, start=1):
            name = gs.get("group_name", "???")
            signals = _safe_int(gs.get("signals_count"))
            active = _safe_int(gs.get("active_count"))
            wr = _winrate(_safe_int(gs.get("wins")), _safe_int(gs.get("losses")))
            total_p = _safe_float(gs.get("total_profit"))
            bottom_lines.append(
                f"\u274c {i}) {name} | {signals} signaler \u2013 {active} aktiva"
                f" | Winrate {wr:.1f}% | Total profit {total_p:.2f} usdt"
            )

        lines = [
            header,
            "",
            table_header,
            *group_lines,
            "",
            "\U0001f3c6 Topp 10 grupper \u2013 b\u00e4st resultat",
            *top_lines,
            "",
            "\u26a0\ufe0f 10 grupper med s\u00e4mst resultat",
            *bottom_lines,
        ]
        return "\n".join(lines)
