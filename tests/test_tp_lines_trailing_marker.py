"""Tests for the "— Trailing" marker on trailing-merged TPs (Fix A).

Tomas 2026-05-13 IOTAUSDT incident: TP3 at +5.97% (calculated from
signal.entry) had no Bybit-verified marker because it was actually
above 6.1% trailing-activation when calculated from avg_entry, so the
placement logic merged it into the trailing stop and never created an
individual conditional order to verify. Operator couldn't tell why
the marker was missing.

Fix: _tp_lines_pct now takes trailing_activation_pct. TPs at or above
that threshold get a "— Trailing" marker that makes the merged status
explicit. Companion to Tomas's later separate request to use
avg_entry instead of signal.entry for the pct calc, so the displayed
% matches what the placement logic actually decided.
"""

from __future__ import annotations

from telegram.notifier import _tp_lines_pct


def test_tp_above_trailing_activation_marked_trailing():
    """TP whose distance from entry is >= activation pct is merged
    into trailing — render "— Trailing"."""
    out = _tp_lines_pct(
        [103.0, 105.0, 110.0], entry=100.0, direction="LONG",
        trailing_activation_pct=6.1,
    )
    assert "TP1: 103.0 (+3.00%)" in out
    assert "Trailing" not in out.split("\n")[0]
    assert "TP2: 105.0 (+5.00%)" in out
    assert "Trailing" not in out.split("\n")[1]
    assert "TP3: 110.0 (+10.00%) — Trailing" in out


def test_tp_just_above_activation_is_trailing():
    """Just above threshold counts as merged (mirrors the
    position_manager `dist_pct >= activation_pct` check). Using 6.2%
    rather than exact 6.1% to dodge the float-precision boundary that
    `(106.1 - 100) / 100 * 100` produces 6.099999... in IEEE-754."""
    out = _tp_lines_pct(
        [106.2], entry=100.0, direction="LONG",
        trailing_activation_pct=6.1,
    )
    assert "TP1: 106.2 (+6.20%) — Trailing" in out


def test_tp_just_below_activation_is_not_trailing():
    """Just below threshold should be an individual order, no
    Trailing marker."""
    out = _tp_lines_pct(
        [106.0], entry=100.0, direction="LONG",
        trailing_activation_pct=6.1,
        verified={"106.0": True},
    )
    assert "TP1: 106.0 (+6.00%) (Bybit verifierad)" in out
    assert "Trailing" not in out


def test_short_direction_with_trailing_marker():
    """Mirror the IOTAUSDT case but cleaner: SHORT entry 100, TP3 at
    93.5 is +6.5% favorable -> trailing-merged."""
    out = _tp_lines_pct(
        [97.0, 95.0, 93.5], entry=100.0, direction="SHORT",
        trailing_activation_pct=6.1,
    )
    assert "TP1: 97.0 (+3.00%)" in out
    assert "TP2: 95.0 (+5.00%)" in out
    assert "TP3: 93.5 (+6.50%) — Trailing" in out


def test_no_activation_arg_falls_back_to_pre_fix_behaviour():
    """If trailing_activation_pct is None, no Trailing marker fires —
    existing tests that don't pass the arg keep working."""
    out = _tp_lines_pct(
        [110.0], entry=100.0, direction="LONG",
    )
    assert "TP1: 110.0 (+10.00%)" in out
    assert "Trailing" not in out


def test_blocked_under_2pct_still_wins_over_trailing():
    """A TP somehow both <2% AND >= activation can't happen
    geometrically, but the priority order (Blocked > Trailing >
    Verified) is intentional. Verify Blocked still wins when it
    applies."""
    out = _tp_lines_pct(
        [101.0], entry=100.0, direction="LONG",
        trailing_activation_pct=0.5,  # absurdly low threshold for the test
    )
    # +1% is below 2 — Blocked marker wins over Trailing.
    assert "— Blocked <2%" in out
    assert "Trailing" not in out
