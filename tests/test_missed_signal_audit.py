"""Tests for the missed-signal audit.

Tomas (client) 2026-05-04: "we have to monitor all channels and all
signals — we don't have to miss any signals." This suite locks in
the categorization rules so a future change can't accidentally hide
a real silent drop, or start spamming the channel with empty
audit reports.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from health.missed_signal_audit import (
    audit_log_window,
    render_summary,
)


def _line(ts_offset_seconds: int, sid: str, event: str, **extra) -> str:
    """Build one JSON log line with a timestamp `ts_offset_seconds`
    BEFORE now (negative offset => in the past)."""
    ts = (
        datetime.now(timezone.utc) + timedelta(seconds=ts_offset_seconds)
    ).isoformat().replace("+00:00", "Z")
    e = {"timestamp": ts, "signal_id": sid, "event": event}
    e.update(extra)
    return json.dumps(e) + "\n"


def _write_log(tmp_path: Path, lines: list[str]) -> Path:
    p = tmp_path / "stratos1.log"
    p.write_text("".join(lines), encoding="utf-8")
    return p


def test_visible_chain_counted_as_visible(tmp_path: Path):
    """A signal whose chain reached notification_sent is visible —
    not flagged as missed."""
    log = _write_log(tmp_path, [
        _line(-60, "abc12345", "message_received", channel_name="CoinAura"),
        _line(-59, "abc12345", "signal_parsed", symbol="BTCUSDT"),
        _line(-58, "abc12345", "notification_sent"),
    ])
    audit = audit_log_window(log, window_minutes=10)
    assert audit["received"] == 1
    assert audit["visible"] == 1
    assert audit["silent_drops"] == 0


def test_trade_opened_counts_as_visible(tmp_path: Path):
    """trade.opened also proves the operator saw the signal."""
    log = _write_log(tmp_path, [
        _line(-60, "ggg11111", "message_received", channel_name="CoinAura"),
        _line(-59, "ggg11111", "signal_parsed", symbol="ETHUSDT"),
        _line(-58, "ggg11111", "trade.opened"),
    ])
    audit = audit_log_window(log, window_minutes=10)
    assert audit["visible"] == 1
    assert audit["silent_drops"] == 0


def test_status_skipped_counts_as_intentional(tmp_path: Path):
    """is_status_update dropped the message before parser ran. The
    operator chose this filter — it is intentional, not a silent drop."""
    log = _write_log(tmp_path, [
        _line(-60, "sss11111", "message_received", channel_name="CoinAura"),
        _line(-59, "sss11111", "status_update_skipped"),
    ])
    audit = audit_log_window(log, window_minutes=10)
    assert audit["intentional"] == 1
    assert audit["silent_drops"] == 0


def test_no_symbol_counts_as_intentional(tmp_path: Path):
    """Pure chatter (no symbol extracted) is operator-approved silent."""
    log = _write_log(tmp_path, [
        _line(-60, "cha11111", "message_received", channel_name="Random"),
        _line(-59, "cha11111", "signal_parse_no_symbol"),
    ])
    audit = audit_log_window(log, window_minutes=10)
    assert audit["intentional"] == 1
    assert audit["silent_drops"] == 0


def test_no_entry_without_notification_is_silent_drop(tmp_path: Path):
    """signal_parse_no_entry with NO downstream notification_sent
    is the bug class Tomas wants surfaced. Symbol+direction were
    extracted (signal-shaped) but operator never saw it."""
    log = _write_log(tmp_path, [
        _line(-60, "drop1234", "message_received",
              channel_name="CoinAura"),
        _line(-59, "drop1234", "signal_parse_no_entry",
              symbol="BABYUSDT", direction="LONG"),
    ])
    audit = audit_log_window(log, window_minutes=10)
    assert audit["silent_drops"] == 1
    detail = audit["silent_drop_details"][0]
    assert detail["channel"] == "CoinAura"
    assert detail["symbol"] == "BABYUSDT"
    assert detail["last_event"] == "signal_parse_no_entry"
    assert detail["signal_id"] == "drop1234"


def test_no_entry_with_notification_counts_as_visible(tmp_path: Path):
    """Same no_entry but rejection-notify gate fired — operator saw
    the 'Blokerad Entre saknas' message. Visible, not silent."""
    log = _write_log(tmp_path, [
        _line(-60, "vis11111", "message_received", channel_name="CoinAura"),
        _line(-59, "vis11111", "signal_parse_no_entry",
              symbol="BABYUSDT", direction="LONG"),
        _line(-58, "vis11111", "notification_sent"),
    ])
    audit = audit_log_window(log, window_minutes=10)
    assert audit["visible"] == 1
    assert audit["silent_drops"] == 0


def test_in_flight_signal_excluded(tmp_path: Path):
    """A signal whose newest event is within the 5-second grace
    window is likely still mid-pipeline. Don't flag it yet."""
    log = _write_log(tmp_path, [
        _line(-2, "fly11111", "message_received", channel_name="CoinAura"),
        _line(-1, "fly11111", "signal_parsed", symbol="ETHUSDT"),
    ])
    audit = audit_log_window(log, window_minutes=10)
    assert audit["in_flight"] == 1
    assert audit["silent_drops"] == 0


def test_outside_window_ignored(tmp_path: Path):
    """Events older than window_minutes are not counted."""
    log = _write_log(tmp_path, [
        _line(-3600, "old11111", "message_received", channel_name="CoinAura"),
        _line(-3599, "old11111", "signal_parse_no_entry"),
    ])
    audit = audit_log_window(log, window_minutes=10)
    assert audit["received"] == 0
    assert audit["silent_drops"] == 0


def test_chain_without_message_received_ignored(tmp_path: Path):
    """A signal_id seen on a downstream event but never on a
    message_received (e.g. unmonitored chat dropped before the
    listener emitted message_received) is not a real signal — skip."""
    log = _write_log(tmp_path, [
        _line(-60, "orphan11", "message_unmonitored_chat",
              channel_name="other"),
    ])
    audit = audit_log_window(log, window_minutes=10)
    assert audit["received"] == 0


def test_render_summary_silent_when_healthy(tmp_path: Path):
    """No silent drops -> render returns None so the channel does
    not get an empty audit report."""
    log = _write_log(tmp_path, [
        _line(-60, "ok111111", "message_received", channel_name="CoinAura"),
        _line(-59, "ok111111", "notification_sent"),
    ])
    audit = audit_log_window(log, window_minutes=10)
    assert render_summary(audit) is None


def test_render_summary_emits_when_silent_drop_present(tmp_path: Path):
    """When at least one silent drop, the summary is non-empty and
    contains the channel + last event for diagnosis."""
    log = _write_log(tmp_path, [
        _line(-60, "drop1234", "message_received", channel_name="OBriansSpam"),
        _line(-59, "drop1234", "signal_parse_no_entry",
              symbol="USEUSDT", direction="LONG"),
    ])
    audit = audit_log_window(log, window_minutes=10)
    summary = render_summary(audit)
    assert summary is not None
    assert "TYST DROPPADE: 1" in summary
    assert "OBriansSpam" in summary
    assert "drop1234" in summary
    assert "signal_parse_no_entry" in summary


def test_missing_log_returns_error(tmp_path: Path):
    """Audit on a missing file returns an error dict, not a crash."""
    audit = audit_log_window(tmp_path / "no_such_file.log", window_minutes=10)
    assert audit.get("error")
    assert audit["received"] == 0


def test_malformed_lines_skipped(tmp_path: Path):
    """Non-JSON lines in the log are silently skipped."""
    p = tmp_path / "stratos1.log"
    p.write_text(
        "garbage line not json\n"
        + _line(-60, "good1234", "message_received", channel_name="CoinAura")
        + "more garbage\n"
        + _line(-59, "good1234", "notification_sent"),
        encoding="utf-8",
    )
    audit = audit_log_window(p, window_minutes=10)
    assert audit["received"] == 1
    assert audit["visible"] == 1
