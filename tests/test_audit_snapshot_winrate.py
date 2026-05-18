"""Tests for the BOT-AUDIT win-rate / header-label fix (Tomas
2026-05-18 msg 55688 P3, msg 55704).

The audit report shows the last N closed trades. Before the fix:
  - win_rate was win_count / bot_window_n × 100, so the denominator
    was always the WINDOW SIZE (= 10) regardless of how many trades
    actually had PnL data. 1 win / 2 losses / 0 zero → 1/10 = 10%,
    not 1/3 = 33%.
  - The header always said "Senaste 10 affärer" even when fewer
    than 10 trades existed, making the "Vinst/Förlust/0: 1/2/0"
    line look like 7 trades were missing.

Live examples Tomas saw across the weekend:
  msg 55064: 0/8/0 (win-rate 0%) — actually 0/8 = 0% (looked correct
             only because both numerators happen to give 0).
  msg 55649: 1/2/0 (win-rate 10%) — should be 1/3 = 33%.
  msg 55651: 0/1/0 (win-rate 0%) — actually 0/1 = 0% (matches).
"""

from __future__ import annotations

from health.audit_snapshot import SnapshotData, render_snapshot_text


def _snap(*, win=0, loss=0, zero=0, bot_window_n=10):
    return SnapshotData(
        bot_window_n=bot_window_n,
        bot_recent_trades=[],
        open_positions=[],
        open_orders=[],
        db_active_count=0,
        db_total_count=0,
        bybit_position_count=0,
        bybit_order_count=0,
        drift=0,
        leftover_orders=0,
        unprotected_positions=0,
        long_notional_usdt=0.0,
        short_notional_usdt=0.0,
        unrealised_pnl_usdt=0.0,
        win_count=win,
        loss_count=loss,
        zero_count=zero,
        avg_win=0.0,
        avg_loss=0.0,
        max_loss=0.0,
        total_pnl=0.0,
        protection_failed_count=0,
        hedge_activations_count=0,
    )


def test_win_rate_uses_actual_count_not_window_size():
    """Tomas msg 55649: 1 win + 2 losses + 0 zero = 3 trades. Bug:
    rendered "win-rate 10%" (1/10). Fix: 1/3 = 33%."""
    text = render_snapshot_text(_snap(win=1, loss=2, zero=0))
    assert "(win-rate 33%)" in text
    assert "(win-rate 10%)" not in text


def test_header_shows_actual_count_when_below_window():
    """Tomas msg 55704: the "Senaste 10 affärer" header was a lie
    when fewer than 10 trades had data. Now it shows the real count
    so "1/2/0" makes sense next to "Senaste 3 affärer"."""
    text = render_snapshot_text(_snap(win=1, loss=2, zero=0))
    assert "Senaste 3 affärer:" in text
    assert "Senaste 10 affärer:" not in text


def test_header_shows_window_size_when_full():
    """When the window IS full (10 trades counted), the header
    matches the cadence."""
    text = render_snapshot_text(_snap(win=4, loss=5, zero=1))
    assert "Senaste 10 affärer:" in text
    # 4/10 = 40%
    assert "(win-rate 40%)" in text


def test_win_rate_zero_when_no_trades():
    """No trades yet → 0% (don't divide by zero)."""
    text = render_snapshot_text(_snap(win=0, loss=0, zero=0))
    assert "(win-rate 0%)" in text
    assert "Senaste 0 affärer:" in text


def test_win_rate_handles_all_losses():
    """Tomas msg 55064: 0 wins, 8 losses, 0 zero → 0% (matches both
    old and new logic, but the actual count goes from 10 → 8)."""
    text = render_snapshot_text(_snap(win=0, loss=8, zero=0))
    assert "(win-rate 0%)" in text
    assert "Senaste 8 affärer:" in text


def test_win_rate_handles_single_loss():
    """Tomas msg 55651: 0/1/0 → 0% (1 loss, no wins). Both old and
    new logic produce 0%, but the header now shows "Senaste 1
    affärer" instead of "Senaste 10 affärer"."""
    text = render_snapshot_text(_snap(win=0, loss=1, zero=0))
    assert "(win-rate 0%)" in text
    assert "Senaste 1 affärer:" in text
