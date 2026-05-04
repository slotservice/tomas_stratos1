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


def test_text_preview_captured_on_silent_drop(tmp_path: Path):
    """A silent-drop detail must carry the raw text from
    message_received so it can be persisted + replayed."""
    log = _write_log(tmp_path, [
        _line(-60, "txt11111", "message_received",
              channel_name="CoinAura",
              text_preview="#FOO/USDT LONG\nEntry: 1.0"),
        _line(-59, "txt11111", "signal_parse_no_tps",
              symbol="FOOUSDT", direction="LONG"),
    ])
    audit = audit_log_window(log, window_minutes=10)
    assert audit["silent_drops"] == 1
    detail = audit["silent_drop_details"][0]
    assert "#FOO/USDT LONG" in detail["text_preview"]


# ---------- Persistence ----------

from health.missed_signal_audit import (
    _load_existing_signal_ids,
    _persist_silent_drops,
    run_audit_and_notify,
)


def test_persist_appends_new_drops(tmp_path: Path):
    persist = tmp_path / "silent_drops.jsonl"
    drops = [
        {"signal_id": "d1", "channel": "A", "symbol": "BTCUSDT",
         "last_event": "x", "ts": "2026-05-04T00:00:00Z",
         "text_preview": "raw text 1"},
        {"signal_id": "d2", "channel": "B", "symbol": "ETHUSDT",
         "last_event": "y", "ts": "2026-05-04T00:01:00Z",
         "text_preview": "raw text 2"},
    ]
    n = _persist_silent_drops(persist, drops)
    assert n == 2
    assert persist.exists()
    lines = persist.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2


def test_persist_dedup_via_load(tmp_path: Path):
    persist = tmp_path / "silent_drops.jsonl"
    initial = [
        {"signal_id": "exists1", "channel": "A", "symbol": "X",
         "last_event": "x", "ts": "2026-05-04T00:00:00Z",
         "text_preview": "t1"},
    ]
    _persist_silent_drops(persist, initial)
    existing = _load_existing_signal_ids(persist)
    assert existing == {"exists1"}


def test_persist_max_entries_trim(tmp_path: Path):
    persist = tmp_path / "silent_drops.jsonl"
    # Write 10 entries with cap = 4 — only the LAST 4 should survive.
    drops = [
        {"signal_id": f"sid{i}", "channel": "A", "symbol": "X",
         "last_event": "x", "ts": f"2026-05-04T00:00:0{i}Z",
         "text_preview": f"t{i}"}
        for i in range(10)
    ]
    _persist_silent_drops(persist, drops, max_entries=4)
    lines = persist.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4
    # Should be the LAST 4 (sid6..sid9).
    sids = [json.loads(l)["signal_id"] for l in lines]
    assert sids == ["sid6", "sid7", "sid8", "sid9"]


def test_persist_missing_file_returns_empty_set(tmp_path: Path):
    assert _load_existing_signal_ids(tmp_path / "nope.jsonl") == set()


def test_persist_corrupt_lines_ignored(tmp_path: Path):
    persist = tmp_path / "silent_drops.jsonl"
    persist.write_text(
        "garbage\n"
        + json.dumps({"signal_id": "ok"}) + "\n"
        + "more garbage\n",
        encoding="utf-8",
    )
    assert _load_existing_signal_ids(persist) == {"ok"}


import asyncio


class _DummyNotifier:
    def __init__(self):
        self.sent = []

    async def _send_notify(self, text):
        self.sent.append(text)


def test_run_audit_persists_only_new_signal_ids(tmp_path: Path):
    """End-to-end: audit detects a silent drop, persists it, then on
    a second audit run the same signal_id is NOT duplicated."""
    log = _write_log(tmp_path, [
        _line(-60, "newdrop1", "message_received",
              channel_name="CoinAura",
              text_preview="#X/USDT LONG"),
        _line(-59, "newdrop1", "signal_parse_no_entry",
              symbol="XUSDT", direction="LONG"),
    ])
    persist = tmp_path / "silent_drops.jsonl"
    notifier = _DummyNotifier()

    # First run — should persist the drop.
    asyncio.run(run_audit_and_notify(
        notifier=notifier, log_path=log, window_minutes=10,
        persist_path=persist,
    ))
    first_lines = persist.read_text(encoding="utf-8").splitlines()
    assert len(first_lines) == 1
    assert json.loads(first_lines[0])["signal_id"] == "newdrop1"

    # Second run on the SAME log — should NOT duplicate.
    asyncio.run(run_audit_and_notify(
        notifier=notifier, log_path=log, window_minutes=10,
        persist_path=persist,
    ))
    second_lines = persist.read_text(encoding="utf-8").splitlines()
    assert len(second_lines) == 1


def test_run_audit_no_persistence_when_path_none(tmp_path: Path):
    """When persist_path is None, no file is created."""
    log = _write_log(tmp_path, [
        _line(-60, "noper001", "message_received", channel_name="X",
              text_preview="raw"),
        _line(-59, "noper001", "signal_parse_no_entry",
              symbol="YUSDT", direction="LONG"),
    ])
    asyncio.run(run_audit_and_notify(
        notifier=_DummyNotifier(), log_path=log, window_minutes=10,
        persist_path=None,
    ))
    assert not (tmp_path / "silent_drops.jsonl").exists()
