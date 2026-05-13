"""Tests for the silent-path notifications added in Tomas 2026-05-12
"nothing silent" spec.

Two new notifier templates surface previously-invisible bot decisions:

* signal_skipped_by_override — when symbol_overrides.toml says skip
* api_error — when a Bybit API call fails non-fatally
  (slippage_check_error, set_leverage_failed, etc.)
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from telegram.notifier import TelegramNotifier


def _notifier() -> TelegramNotifier:
    n = TelegramNotifier.__new__(TelegramNotifier)
    n._send_notify = AsyncMock(side_effect=lambda text: text)
    return n


@pytest.mark.asyncio
async def test_signal_skipped_by_override_includes_symbol_and_note():
    n = _notifier()
    text = await n.signal_skipped_by_override(
        symbol="GUAUSDT",
        direction="LONG",
        channel_name="Panda Traders",
        note="Not listed on Bybit perp (any prefix).",
    )
    assert "Skippad av operator-override" in text
    assert "#GUAUSDT" in text
    assert "LONG" in text
    assert "Panda" in text
    assert "Not listed on Bybit" in text


@pytest.mark.asyncio
async def test_signal_skipped_by_override_default_note():
    n = _notifier()
    text = await n.signal_skipped_by_override(
        symbol="FOOUSDT",
        direction="SHORT",
        channel_name="Test Channel",
    )
    assert "symbol_overrides.toml: skip = true" in text


@pytest.mark.asyncio
async def test_api_error_includes_kind_and_reason():
    n = _notifier()
    text = await n.api_error(
        symbol="TOSHIUSDT",
        direction="LONG",
        channel_name="Crypto Master Vip",
        kind="set_leverage",
        reason="params error: symbol invalid",
    )
    assert "API-fel" in text
    assert "set_leverage" in text
    assert "#TOSHIUSDT" in text
    assert "LONG" in text
    assert "params error: symbol invalid" in text


@pytest.mark.asyncio
async def test_api_error_fallback_reason():
    n = _notifier()
    text = await n.api_error(
        symbol="ABCUSDT",
        direction="SHORT",
        channel_name="X",
        kind="slippage_check",
    )
    assert "API-fel" in text
    assert "slippage_check" in text
    assert "Kontrollera manuellt" in text
