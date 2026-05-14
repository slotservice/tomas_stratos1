"""Tests for the silent-path notifications added in Tomas 2026-05-12
"nothing silent" spec.

* api_error — when a Bybit API call fails non-fatally
  (slippage_check_error, set_leverage_failed, etc.)

Note: signal_skipped_by_override was removed 2026-05-15 — Tomas asked
for the per-skip operator-channel message to be dropped (it duplicates
the "not on Bybit" concept); the skip is still recorded in the file log.
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
