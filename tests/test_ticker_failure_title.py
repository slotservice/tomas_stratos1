"""Tests for _ticker_failure_title (Tomas 2026-05-15).

AIN, COS, ATA and SKYAI are listed on Bybit but their ticker payload was
empty during 2026-05-14/15. The adapter raises "No ticker data for X"
when that happens; the bot used to surface this as "⚠️ Finns inte på
bybit ⚠️", which is wrong — the coin IS on Bybit, only the price is
missing. Tomas (msg 54701) asked for a distinct "Finns på bybit, pris
saknas" title for that case while keeping the original title for genuine
"not on Bybit" failures (Bybit 110074 / "not live" / "not exists").
"""

from __future__ import annotations

from managers.position_manager import _ticker_failure_title


def test_no_ticker_data_uses_price_missing_title():
    assert _ticker_failure_title("no ticker data for ainusdt") == (
        "Finns på bybit, pris saknas"
    )


def test_not_live_uses_not_on_bybit_title():
    assert _ticker_failure_title(
        "bybit 110074: contract is not live"
    ) == "Finns inte på bybit"


def test_not_exists_uses_not_on_bybit_title():
    assert _ticker_failure_title("symbol is not exists") == "Finns inte på bybit"


def test_unknown_error_defaults_to_not_on_bybit_title():
    assert _ticker_failure_title("some unrelated error") == "Finns inte på bybit"


def test_empty_err_defaults_to_not_on_bybit_title():
    assert _ticker_failure_title("") == "Finns inte på bybit"
