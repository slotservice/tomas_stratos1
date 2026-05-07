"""
Unit tests for the signal-idea attribution logic in
GroupAnalysisReporter (Tomas 2026-05-08).

The spec: same idea = same symbol+direction + entry within 5% +
received during the trade's life. Every channel that contributed a
matching signal gets credited with the trade's PnL — not just the
channel that won the dedup race.
"""

from __future__ import annotations

from reporting.group_analysis import GroupAnalysisReporter


def _sig(sid, ch_id, ch_name, entry, received_at):
    return {
        "id": sid,
        "source_channel_id": ch_id,
        "source_channel_name": ch_name,
        "entry_price": entry,
        "received_at": received_at,
        "symbol": "BTCUSDT",
        "direction": "LONG",
    }


def test_find_idea_signals_includes_opener_always():
    """Opener is always in the result, even on edge cases."""
    candidates = [
        _sig(1, 100, "A", 50000.0, "2026-05-07T10:00:00Z"),
    ]
    result = GroupAnalysisReporter._find_idea_signals(
        candidate_signals=candidates,
        opening_signal_id=1,
        avg_entry=50000.0,
        opened_at="2026-05-07T10:00:00Z",
        closed_at="2026-05-07T11:00:00Z",
    )
    assert len(result) == 1
    assert result[0]["id"] == 1


def test_find_idea_signals_within_entry_tolerance():
    """Signals with entry within 5% of avg_entry are matched."""
    candidates = [
        _sig(1, 100, "A", 50000.0, "2026-05-07T10:00:00Z"),
        _sig(2, 200, "B", 51000.0, "2026-05-07T10:02:00Z"),  # +2% — match
        _sig(3, 300, "C", 49000.0, "2026-05-07T10:03:00Z"),  # -2% — match
        _sig(4, 400, "D", 53000.0, "2026-05-07T10:05:00Z"),  # +6% — REJECT
    ]
    result = GroupAnalysisReporter._find_idea_signals(
        candidate_signals=candidates,
        opening_signal_id=1,
        avg_entry=50000.0,
        opened_at="2026-05-07T10:00:00Z",
        closed_at="2026-05-07T11:00:00Z",
    )
    sids = sorted(s["id"] for s in result)
    assert sids == [1, 2, 3]


def test_find_idea_signals_excludes_outside_trade_life():
    """Signals received before opened_at or after closed_at are excluded."""
    candidates = [
        _sig(1, 100, "A", 50000.0, "2026-05-07T10:00:00Z"),
        _sig(2, 200, "B", 50100.0, "2026-05-07T09:30:00Z"),  # before — REJECT
        _sig(3, 300, "C", 50100.0, "2026-05-07T10:30:00Z"),  # in — match
        _sig(4, 400, "D", 50100.0, "2026-05-07T11:30:00Z"),  # after — REJECT
    ]
    result = GroupAnalysisReporter._find_idea_signals(
        candidate_signals=candidates,
        opening_signal_id=1,
        avg_entry=50000.0,
        opened_at="2026-05-07T10:00:00Z",
        closed_at="2026-05-07T11:00:00Z",
    )
    sids = sorted(s["id"] for s in result)
    assert sids == [1, 3]


def test_find_idea_signals_zero_entry_signal_skipped():
    """Defensive: candidate with entry_price=0 is skipped (except opener)."""
    candidates = [
        _sig(1, 100, "A", 50000.0, "2026-05-07T10:00:00Z"),
        _sig(2, 200, "B", 0.0, "2026-05-07T10:02:00Z"),
    ]
    result = GroupAnalysisReporter._find_idea_signals(
        candidate_signals=candidates,
        opening_signal_id=1,
        avg_entry=50000.0,
        opened_at="2026-05-07T10:00:00Z",
        closed_at="2026-05-07T11:00:00Z",
    )
    assert [s["id"] for s in result] == [1]


def test_find_idea_signals_custom_tolerance():
    """Tolerance is configurable; 1% drops the +2% candidate."""
    candidates = [
        _sig(1, 100, "A", 50000.0, "2026-05-07T10:00:00Z"),
        _sig(2, 200, "B", 51000.0, "2026-05-07T10:02:00Z"),  # +2%
    ]
    strict = GroupAnalysisReporter._find_idea_signals(
        candidate_signals=candidates,
        opening_signal_id=1,
        avg_entry=50000.0,
        opened_at="2026-05-07T10:00:00Z",
        closed_at="2026-05-07T11:00:00Z",
        entry_tolerance_pct=1.0,
    )
    assert [s["id"] for s in strict] == [1]


def test_find_idea_signals_opener_borderline_entry_still_included():
    """If the opener's own entry is far from avg_entry (e.g. avg from
    fills drifted), it is STILL included by id match."""
    candidates = [
        _sig(1, 100, "A", 60000.0, "2026-05-07T10:00:00Z"),
    ]
    result = GroupAnalysisReporter._find_idea_signals(
        candidate_signals=candidates,
        opening_signal_id=1,
        avg_entry=50000.0,    # 20% off the signal's stated entry
        opened_at="2026-05-07T10:00:00Z",
        closed_at="2026-05-07T11:00:00Z",
    )
    assert [s["id"] for s in result] == [1]


def test_find_idea_signals_empty_candidates():
    assert GroupAnalysisReporter._find_idea_signals(
        candidate_signals=[],
        opening_signal_id=1,
        avg_entry=50000.0,
        opened_at="2026-05-07T10:00:00Z",
        closed_at="2026-05-07T11:00:00Z",
    ) == []
