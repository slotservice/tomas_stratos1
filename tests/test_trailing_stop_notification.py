"""Tests for the trailing-stop notification suppression / unified
locked-profit math (Tomas 2026-05-18 msg 55688 priorities 1 & 2).

P1 (negative locked profit): When Bybit's stopLoss for a position is
at or beyond the trade's entry on the LOSING side (e.g. for a SHORT
with entry 0.3708, stopLoss 0.4036 is 8.85% above entry — i.e. a
loss), neither the "Trailing Stop Aktiverad" nor the
"Trailing Stop uppdaterad" template may report a "Låst vinst: -X%"
line. The notifier silently suppresses (returns "") and logs a
warning instead. Live incidents the suppression fixes: TAC (msg
55595, -80%) and XTZ (msg 55634, -53%).

P2 (locked profit dropping between messages): The activation
template used to compute "Låst vinst" from the theoretical
`activation_pct - distance_pct`, while the uppdaterad template
computes from the actual `(entry - trailing_stop_price) / entry`.
For BCH at 07:21 (msg 55675 + 55676), the two messages disagreed:
activation showed +90%, uppdaterad seconds later showed +63%. The
fix unifies both templates onto the same actual-trigger formula,
falling back to trade.sl_price and finally the theoretical floor.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from telegram.notifier import TelegramNotifier


class _Sig:
    def __init__(self, symbol="BCHUSDT", direction="SHORT",
                 entry=404.6, signal_type="dynamic", channel_name="ShirinAhmadi"):
        self.symbol = symbol
        self.direction = direction
        self.entry = entry
        self.signal_type = signal_type
        self.channel_name = channel_name


class _Trade:
    def __init__(self, *, signal, avg_entry=None, leverage=25.0,
                 quantity=1.2, sl_price=None, id="2831",
                 bybit_order_ids=()):
        self.signal = signal
        self.avg_entry = avg_entry
        self.leverage = leverage
        self.quantity = quantity
        self.sl_price = sl_price
        self.id = id
        self.bybit_order_ids = list(bybit_order_ids)


def _notifier() -> TelegramNotifier:
    """Bare notifier with the I/O surface mocked so we can call the
    template methods directly without a Telegram client."""
    n = TelegramNotifier.__new__(TelegramNotifier)
    n._send_notify = AsyncMock(return_value="sent")
    return n


@pytest.mark.asyncio
async def test_trailing_updated_suppressed_when_locked_is_negative():
    """XTZ msg 55634: SHORT entry 0.3708, Bybit stopLoss reported as
    0.4036 (= 8.85% ABOVE entry). The notifier must NOT fire a
    "Trailing Stop uppdaterad" with Låst vinst: -53% — locked profit
    can never be negative."""
    n = _notifier()
    trade = _Trade(signal=_Sig(symbol="XTZUSDT", direction="SHORT",
                                entry=0.3708),
                   avg_entry=0.3708, leverage=6.0)
    result = await n.trailing_stop_updated(
        trade=trade,
        trailing_stop_price=0.4036,
        mark_price=0.3479,
        quantity=108.0,
        unrealised_pnl=0.0,
    )
    assert result == ""
    n._send_notify.assert_not_called()


@pytest.mark.asyncio
async def test_trailing_updated_fires_when_locked_is_positive():
    """Sanity guard: a real trailing-stop move into profit territory
    still fires the notification."""
    n = _notifier()
    trade = _Trade(signal=_Sig(symbol="BCHUSDT", direction="SHORT",
                                entry=404.6),
                   avg_entry=404.6, leverage=25.0)
    result = await n.trailing_stop_updated(
        trade=trade,
        trailing_stop_price=394.4,
        mark_price=377.0,
        quantity=1.2,
        unrealised_pnl=33.12,
    )
    assert result != ""
    n._send_notify.assert_awaited_once()
    text = n._send_notify.await_args.args[0]
    # (404.6 - 394.4) / 404.6 = 2.52% × 25 = 63.03%
    assert "+63.03% med hävstång" in text


@pytest.mark.asyncio
async def test_trailing_activated_suppressed_when_locked_is_negative():
    """Same suppression on the activation side — a SHORT whose
    effective trigger is still ABOVE entry (no real profit lock)
    must not produce a "Trailing Stop Aktiverad" notification."""
    n = _notifier()
    trade = _Trade(signal=_Sig(symbol="TACUSDT", direction="SHORT",
                                entry=0.021767),
                   avg_entry=0.021767, leverage=6.0,
                   sl_price=0.025258)
    result = await n.trailing_stop_activated(
        trade=trade,
        activation_price=0.020452,
        trailing_distance=0.000544,
        activation_pct=6.04,
        distance_pct=2.5,
        trailing_stop_price=0.025258,
        mark_price=0.0204,
        quantity=1530.0,
        unrealised_pnl=0.0,
    )
    assert result == ""
    n._send_notify.assert_not_called()


@pytest.mark.asyncio
async def test_trailing_activated_uses_actual_trigger_not_theoretical():
    """BCH msg 55675 + 55676 P2: when the SL has been moved up by the
    TP cascade (e.g. to 394.4) BEFORE trailing activates, the
    activation template must show the ACTUAL locked profit derived
    from 394.4, not the theoretical activation_pct - distance_pct.
    Otherwise the very-next "uppdaterad" message disagrees and the
    operator sees locked profit "drop" (90% → 63%)."""
    n = _notifier()
    trade = _Trade(signal=_Sig(symbol="BCHUSDT", direction="SHORT",
                                entry=404.6),
                   avg_entry=404.6, leverage=25.0, sl_price=394.4)
    result = await n.trailing_stop_activated(
        trade=trade,
        activation_price=379.9194,
        trailing_distance=10.115,
        activation_pct=6.10,
        distance_pct=2.50,
        trailing_stop_price=394.4,   # Bybit-confirmed
        mark_price=377.0,
        quantity=1.2,
        unrealised_pnl=0.0,
    )
    assert result != ""
    text = n._send_notify.await_args.args[0]
    # Actual locked: (404.6 - 394.4) / 404.6 × 25 = 63.03%
    # NOT the theoretical (6.10 - 2.50) × 25 = 90%
    assert "+63.03% med hävstång" in text
    assert "+90.00% med hävstång" not in text


@pytest.mark.asyncio
async def test_trailing_activated_falls_back_to_sl_price():
    """If the WS event's stopLoss is 0/None at activation, the
    notifier falls back to trade.sl_price for the locked-profit
    computation — still actual, not theoretical."""
    n = _notifier()
    trade = _Trade(signal=_Sig(symbol="BCHUSDT", direction="SHORT",
                                entry=404.6),
                   avg_entry=404.6, leverage=25.0, sl_price=394.4)
    result = await n.trailing_stop_activated(
        trade=trade,
        activation_price=379.9194,
        trailing_distance=10.115,
        activation_pct=6.10,
        distance_pct=2.50,
        trailing_stop_price=None,   # NOT available from WS
        mark_price=377.0,
        quantity=1.2,
        unrealised_pnl=0.0,
    )
    assert result != ""
    text = n._send_notify.await_args.args[0]
    assert "+63.03% med hävstång" in text
