"""Tests for the Bybit TP verification (step 9b).

After all partial-TP conditional close orders are placed, the bot
fetches get_open_orders(symbol) once and confirms each placed order's
orderId appears in the live list. Result is stored on
trade.tp_bybit_verified as {str(tp_price): bool}. Position öppnad
surfaces it as per-TP markers in the template.

TPs that didn't get an individual order (merged into trailing stop or
filtered as <2%) don't appear in the verification dict — those carry
their existing markers (or no marker) without "EJ på Bybit" noise.
"""

from __future__ import annotations

import pytest

from telegram.notifier import _tp_lines_pct


def test_tp_lines_verified_marks_bybit_verifierad():
    out = _tp_lines_pct(
        [103.0, 105.0], entry=100.0, direction="LONG",
        verified={"103.0": True, "105.0": True},
    )
    assert "TP1: 103.0 (+3.00%) ◉ Verifierad" in out
    assert "TP2: 105.0 (+5.00%) ◉ Verifierad" in out


def test_tp_lines_unverified_marks_ej_pa_bybit():
    out = _tp_lines_pct(
        [103.0], entry=100.0, direction="LONG",
        verified={"103.0": False},
    )
    assert "TP1: 103.0 (+3.00%) ⊘ EJ Bybit" in out


def test_tp_lines_no_verified_dict_falls_back_to_no_marker():
    out = _tp_lines_pct([103.0], entry=100.0, direction="LONG")
    assert "TP1: 103.0 (+3.00%)" in out
    assert "Verifierad" not in out
    assert "EJ" not in out


def test_tp_lines_blocked_marker_wins_over_verification():
    """A TP below 2% should keep the '⛔ <2%' marker even if it
    somehow ended up in the verified dict — the Blocked check runs
    first because such TPs were never placed on Bybit."""
    out = _tp_lines_pct(
        [101.0, 105.0], entry=100.0, direction="LONG",
        verified={"101.0": False, "105.0": True},
    )
    assert "TP1: 101.0 (+1.00%) ⛔ <2%" in out
    # Should NOT also carry "⊘ EJ Bybit" — that line's already
    # blocked-marked and won't have a Bybit order at all.
    assert "EJ Bybit" not in out
    assert "TP2: 105.0 (+5.00%) ◉ Verifierad" in out


def test_tp_lines_mixed_verified_and_no_entry_in_dict():
    """Trailing-merged TPs are absent from the verified dict — render
    as plain lines without any extra marker."""
    out = _tp_lines_pct(
        [103.0, 110.0], entry=100.0, direction="LONG",
        verified={"103.0": True},  # 110.0 was merged into trailing
    )
    lines = out.split("\n")
    assert "◉ Verifierad" in lines[0]
    # TP2 should have no Bybit marker (merged-into-trailing).
    assert "Verifierad" not in lines[1]
    assert "EJ" not in lines[1]


def test_tp_lines_short_direction_works_with_verification():
    out = _tp_lines_pct(
        [97.0, 95.0], entry=100.0, direction="SHORT",
        verified={"97.0": True, "95.0": False},
    )
    assert "TP1: 97.0 (+3.00%) ◉ Verifierad" in out
    assert "TP2: 95.0 (+5.00%) ⊘ EJ Bybit" in out
