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
                # text_preview is captured ONLY on message_received
                # (the listener stores the first 2000 chars). Other
                # events don't carry it. Persisting silent drops
                # needs the full source text to be useful as a
                # replay fixture, so we lift it from the
                # message_received row when present.
                chains[str(sid)].append({
                    "ts": ts,
                    "event": e.get("event", ""),
                    "channel": e.get("channel_name", "") or "",
                    "symbol": e.get("symbol", "") or "",
                    "text_preview": e.get("text_preview", "") or "",
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
        # context + raw text) and the LAST event in the chain (for
        # diagnosis).
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
            "text_preview": (mr or {}).get("text_preview", ""),
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


def _load_existing_signal_ids(persist_path: Path) -> set[str]:
    """Read the persistence file and return the set of signal_ids
    already recorded. Used for dedup on append. Returns empty set if
    the file is missing or unreadable — the audit must never crash
    on a persistence problem."""
    if not persist_path.exists():
        return set()
    out: set[str] = set()
    try:
        with persist_path.open(encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                sid = e.get("signal_id")
                if sid:
                    out.add(str(sid))
    except Exception:
        log.exception(
            "missed_signal_audit.persist_read_failed",
            path=str(persist_path),
        )
    return out


def _persist_silent_drops(
    persist_path: Path,
    new_drops: list[dict],
    max_entries: int = 5000,
) -> int:
    """Append *new_drops* (already deduped by caller) to the
    persistence file. Trims oldest entries if the file would exceed
    *max_entries*. Returns the number of newly-persisted rows.
    Errors are logged and swallowed — persistence must never block
    the audit from completing."""
    if not new_drops:
        return 0
    try:
        persist_path.parent.mkdir(parents=True, exist_ok=True)
        with persist_path.open("a", encoding="utf-8") as f:
            for d in new_drops:
                f.write(json.dumps(d, ensure_ascii=False) + "\n")
    except Exception:
        log.exception(
            "missed_signal_audit.persist_append_failed",
            path=str(persist_path),
        )
        return 0

    # Trim oldest if the file overflows the cap. Read all, keep the
    # last max_entries, rewrite. Done in-process rather than via a
    # rotation library to keep the dependency surface tiny.
    if max_entries > 0:
        try:
            lines: list[str] = []
            with persist_path.open(encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
            if len(lines) > max_entries:
                kept = lines[-max_entries:]
                with persist_path.open("w", encoding="utf-8") as f:
                    f.writelines(kept)
                log.info(
                    "missed_signal_audit.persist_trimmed",
                    path=str(persist_path),
                    kept=len(kept),
                    dropped=len(lines) - len(kept),
                )
        except Exception:
            log.exception(
                "missed_signal_audit.persist_trim_failed",
                path=str(persist_path),
            )
    return len(new_drops)


async def run_audit_and_notify(
    notifier,
    log_path: Path,
    window_minutes: int = 30,
    persist_path: Optional[Path] = None,
    persist_max_entries: int = 5000,
) -> dict:
    """Top-level entry called from the periodic task in main.py.
    Always returns the audit dict for logging; sends a Telegram
    summary only when there is at least one silent drop. When
    *persist_path* is provided, NEW silent drops (deduped against
    existing entries by signal_id) are appended to that file."""
    audit = audit_log_window(log_path, window_minutes)
    persisted_count = 0
    if persist_path is not None and audit.get("silent_drops", 0) > 0:
        try:
            already = _load_existing_signal_ids(persist_path)
            new_drops = [
                d for d in audit.get("silent_drop_details", [])
                if d.get("signal_id") not in already
            ]
            persisted_count = _persist_silent_drops(
                persist_path, new_drops, max_entries=persist_max_entries,
            )
        except Exception:
            log.exception("missed_signal_audit.persist_dispatch_failed")
    log.info(
        "missed_signal_audit.complete",
        window_minutes=audit.get("window_minutes"),
        received=audit.get("received"),
        visible=audit.get("visible"),
        intentional=audit.get("intentional"),
        in_flight=audit.get("in_flight"),
        silent_drops=audit.get("silent_drops"),
        persisted=persisted_count,
        error=audit.get("error"),
    )
    summary = render_summary(audit)
    if summary and notifier is not None:
        try:
            await notifier._send_notify(summary)
        except Exception:
            log.exception("missed_signal_audit.notify_failed")
    return audit
