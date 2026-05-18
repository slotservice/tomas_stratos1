"""Tests for the sub-2% TP marker in operator-template TP lines.

Tomas 2026-05-12 explicit rule: any TP whose distance from entry is
below 2% must be visible in the template with the "⛔ <2%" marker so
the operator sees every TP the source channel sent (proof chain)
while also seeing which ones the bot will not act on (mirrors the 2%
min distance filter in position_manager._place_partial_tp_orders).

Marker text shortened from "— Blocked <2%" to "⛔ <2%" on 2026-05-19
per Tomas's compact-line spec.
"""

from __future__ import annotations

from telegram.notifier import _tp_lines_pct


def test_sub_2pct_tp_is_marked_blocked_long():
    """LONG entry 100, TPs at 101 (+1%) and 103 (+3%)."""
    out = _tp_lines_pct([101.0, 103.0], entry=100.0, direction="LONG")
    assert "TP1: 101.0 (+1.00%) ⛔ <2%" in out
    assert "TP2: 103.0 (+3.00%)" in out
    assert "⛔" not in out.split("\n")[1]


def test_sub_2pct_tp_is_marked_blocked_short():
    """SHORT entry 100, TPs at 99 (+1% favorable) and 97 (+3%)."""
    out = _tp_lines_pct([99.0, 97.0], entry=100.0, direction="SHORT")
    assert "TP1: 99.0 (+1.00%) ⛔ <2%" in out
    assert "TP2: 97.0 (+3.00%)" in out


def test_all_tps_above_2pct_have_no_marker():
    out = _tp_lines_pct([103.0, 105.0, 110.0], entry=100.0, direction="LONG")
    assert "⛔" not in out
    assert "TP1: 103.0 (+3.00%)" in out
    assert "TP2: 105.0 (+5.00%)" in out
    assert "TP3: 110.0 (+10.00%)" in out


def test_mixed_sub_and_above_2pct():
    """Real-world MAHEE pattern: scalp signal with all 3 TPs sub-2%
    plus a stretch target at 5%."""
    out = _tp_lines_pct(
        [100.5, 101.0, 101.5, 105.0], entry=100.0, direction="LONG",
    )
    lines = out.split("\n")
    assert "⛔ <2%" in lines[0]
    assert "⛔ <2%" in lines[1]
    assert "⛔ <2%" in lines[2]
    assert "⛔" not in lines[3]


def test_exactly_2pct_is_NOT_blocked():
    """Boundary: 2.00% itself should pass the filter (mirrors the
    position_manager `dist_pct < min_tp_distance_pct` check)."""
    out = _tp_lines_pct([102.0], entry=100.0, direction="LONG")
    assert "⛔" not in out
    assert "TP1: 102.0 (+2.00%)" in out


def test_zero_and_missing_tps_dropped():
    out = _tp_lines_pct([0.0, None, 105.0], entry=100.0, direction="LONG")  # type: ignore[list-item]
    assert "TP1" not in out
    assert "TP2" not in out
    assert "TP3: 105.0 (+5.00%)" in out
