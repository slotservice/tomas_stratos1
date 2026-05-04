"""Periodic missed-signal audit.

Tomas (client) 2026-05-04: "we have to monitor all channels and all
signals — we don't have to miss any signals."

Every N minutes the bot scans its own log file, groups events by
``signal_id``, and reports any signal that arrived from a monitored
channel but never reached the operator's Telegram channel — a
"silent drop" in the pipeline.

A signal is one of four buckets:

  * ``visible`` — at least one ``notification_sent`` or
    ``trade.opened`` event with that signal_id (operator saw it)
  * ``intentional_silent`` — explicitly dropped by an operator-
    approved filter (status_update_skipped, no_symbol/no_direction
    chatter, skipped_by_operator_override)
  * ``silent_drop`` — passed every silent-OK filter but never
    reached a ``notification_sent``. THIS is the bug class
    Tomas is asking us to surface.
  * ``in_flight`` — newest events still inside a small recency
    grace window (5 s). Excluded from the report so we don't flag
    a signal that's mid-pipeline at the audit moment.

Output is a Swedish Telegram summary listing the silent-drop count,
worst offenders (channel + last logged event + truncated signal_id),
and the total throughput in the window. The summary is suppressed
entirely when ``silent_drops == 0`` so a healthy bot does not spam
the channel with empty audit reports.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import structlog

log = structlog.get_logger(__name__)


# Events that prove the signal reached the operator's channel.
_VISIBLE_EVENTS = frozenset({
    "notification_sent",
    "trade.opened",
})

# Events that mean "intentionally silent" — operator-approved drops.
_INTENTIONAL_DROP_EVENTS = frozenset({
    "status_update_skipped",
    "signal_parse_no_symbol",
    "signal_parse_no_direction",
    "signal_parse.skipped_by_operator_override",
    "message_unmonitored_chat",
})

# Recency grace: skip signals whose newest event is within this many
# seconds of "now" — they may still be mid-pipeline.
_IN_FLIGHT_GRACE_SECONDS = 5


def audit_log_window(
    log_path: Path,
    window_minutes: int = 30,
) -> dict:
    """Scan *log_path* for the last *window_minutes* and return a
    summary dict suitable for both logging and Telegram rendering.
    """
    if not log_path.exists():
        return {
            "error": f"log file not found: {log_path}",
            "received": 0, "visible": 0,
            "intentional": 0, "silent_drops": 0,
            "in_flight": 0, "silent_drop_details": [],
            "window_minutes": window_minutes,
        }

    now = datetime.now(timezone.utc)
    cutoff_dt = now - timedelta(minutes=window_minutes)
    in_flight_cutoff_dt = now - timedelta(seconds=_IN_FLIGHT_GRACE_SECONDS)

    cutoff_iso = cutoff_dt.isoformat().replace("+00:00", "Z")
    in_flight_iso = in_flight_cutoff_dt.isoformat().replace("+00:00", "Z")

    chains: dict[str, list] = defaultdict(list)

    try:
        with log_path.open(encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                ts = e.get("timestamp", "")
                if not ts or ts < cutoff_iso:
                    continue
                sid = e.get("signal_id")
                if not sid:
                    continue
                chains[str(sid)].append({
                    "ts": ts,
                    "event": e.get("event", ""),
                    "channel": e.get("channel_name", "") or "",
                    "symbol": e.get("symbol", "") or "",
                })
    except Exception as exc:
        return {
            "error": f"audit read failed: {exc}",
            "received": 0, "visible": 0,
            "intentional": 0, "silent_drops": 0,
            "in_flight": 0, "silent_drop_details": [],
            "window_minutes": window_minutes,
        }

    received = 0
    visible = 0
    intentional = 0
    in_flight = 0
    silent_drop_details: list[dict] = []

    for sid, events in chains.items():
        evs = [ev["event"] for ev in events]
        if "message_received" not in evs:
            # The signal_id was bound but the listener filtered the
            # message before message_received (unmonitored chat, bot
            # message, service message). Not a real signal — skip.
            continue
        received += 1

        newest_ts = max(ev["ts"] for ev in events)
        if newest_ts > in_flight_iso:
            in_flight += 1
            continue

        if any(ev in _VISIBLE_EVENTS for ev in evs):
            visible += 1
            continue
        if any(ev in _INTENTIONAL_DROP_EVENTS for ev in evs):
            intentional += 1
            continue

        # Silent drop. Capture the first message_received (for channel
        # context) and the LAST event in the chain (for diagnosis).
        mr = next(
            (ev for ev in events if ev["event"] == "message_received"), None,
        )
        last = max(events, key=lambda ev: ev["ts"])
        silent_drop_details.append({
            "signal_id": sid,
            "channel": (mr or {}).get("channel", "?"),
            "symbol": last.get("symbol") or (mr or {}).get("symbol") or "?",
            "last_event": last.get("event", "?"),
            "ts": (mr or {}).get("ts", last["ts"]),
        })

    return {
        "window_minutes": window_minutes,
        "received": received,
        "visible": visible,
        "intentional": intentional,
        "in_flight": in_flight,
        "silent_drops": len(silent_drop_details),
        # Ordered newest-first so the operator sees the most recent
        # silent drops at the top of the report.
        "silent_drop_details": sorted(
            silent_drop_details, key=lambda d: d["ts"], reverse=True,
        ),
    }


def render_summary(audit: dict) -> Optional[str]:
    """Render a Swedish Telegram summary. Returns None when the audit
    is healthy (no silent drops) so the channel does not get spammed
    with empty reports."""
    if audit.get("error"):
        return f"⚠️ SIGNAL-AUDIT FEL: {audit['error']}"
    silent = audit.get("silent_drops", 0)
    received = audit.get("received", 0)
    if received == 0:
        return None
    if silent == 0:
        return None
    visible = audit.get("visible", 0)
    intentional = audit.get("intentional", 0)
    in_flight = audit.get("in_flight", 0)
    window = audit.get("window_minutes", 0)
    pct_silent = (silent / received) * 100 if received else 0

    lines = [
        f"📊 SIGNAL-AUDIT (senaste {window} min)",
        f"📥 Mottagna från övervakade kanaler: {received}",
        f"✅ Visade i kanal: {visible}",
        f"🔇 Avsiktligt tysta (chatter/status/skip): {intentional}",
        f"⏳ Pågående: {in_flight}",
        f"⚠️ TYST DROPPADE: {silent} ({pct_silent:.0f}%)",
        "",
        "🔍 Senaste tyst-droppade (kanal → senaste händelse):",
    ]
    for d in audit.get("silent_drop_details", [])[:6]:
        ch = (d.get("channel") or "?")[:28]
        sym = (d.get("symbol") or "?")[:18]
        last = (d.get("last_event") or "?")[:30]
        sid = (d.get("signal_id") or "?")[:8]
        lines.append(f"  • {ch} | {sym} | {last} | id={sid}")
    lines.append("")
    lines.append(
        "💡 Använd id i loggen för att se hela kedjan: "
        "`grep 'signal_id=<id>' stratos1.log`",
    )
    return "\n".join(lines)


async def run_audit_and_notify(
    notifier,
    log_path: Path,
    window_minutes: int = 30,
) -> dict:
    """Top-level entry called from the periodic task in main.py.
    Always returns the audit dict for logging; only sends a Telegram
    summary when there is at least one silent drop."""
    audit = audit_log_window(log_path, window_minutes)
    log.info(
        "missed_signal_audit.complete",
        window_minutes=audit.get("window_minutes"),
        received=audit.get("received"),
        visible=audit.get("visible"),
        intentional=audit.get("intentional"),
        in_flight=audit.get("in_flight"),
        silent_drops=audit.get("silent_drops"),
        error=audit.get("error"),
    )
    summary = render_summary(audit)
    if summary and notifier is not None:
        try:
            await notifier._send_notify(summary)
        except Exception:
            log.exception("missed_signal_audit.notify_failed")
    return audit
