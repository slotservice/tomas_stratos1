"""
Tests for the signal parser (core/signal_parser.py).

Covers:
- Basic long/short signals
- Hashtag and slash-pair symbol formats
- Entry ranges
- Missing SL (allowed) and missing direction (rejected)
- Multiple take-profit targets
- Fake TP filtering (TP5=0)
- Symbol normalization
- Signal type classification
- Validation (direction vs TP/SL consistency)
"""

from __future__ import annotations

import pytest

from core.signal_parser import (
    ParsedSignal,
    _classify_signal_type,
    extract_direction,
    extract_prices,
    normalize_symbol,
    parse_signal,
    validate_signal,
)


# ===================================================================
# Basic parsing
# ===================================================================

class TestParseBasicSignals:
    """Test basic long and short signal parsing."""

    def test_parse_basic_long_signal(self):
        text = (
            "BTCUSDT LONG\n"
            "Entry: 65000\n"
            "TP1: 66000\n"
            "TP2: 67000\n"
            "SL: 64000"
        )
        signal = parse_signal(text, channel_id=123, channel_name="TestGroup")

        assert signal is not None
        assert signal.symbol == "BTCUSDT"
        assert signal.direction == "LONG"
        assert signal.entry == 65000.0
        assert signal.tps == [66000.0, 67000.0]
        assert signal.sl == 64000.0

    def test_parse_basic_short_signal(self):
        text = (
            "ETHUSDT SHORT\n"
            "Entry: 3200\n"
            "TP1: 3100\n"
            "TP2: 3000\n"
            "SL: 3300"
        )
        signal = parse_signal(text, channel_id=456, channel_name="ShortGroup")

        assert signal is not None
        assert signal.symbol == "ETHUSDT"
        assert signal.direction == "SHORT"
        assert signal.entry == 3200.0
        assert signal.tps == [3100.0, 3000.0]
        assert signal.sl == 3300.0

    def test_parse_signal_with_hashtag_symbol(self):
        text = (
            "#SOLUSDT LONG\n"
            "Entry: 150\n"
            "TP1: 155\n"
            "SL: 145"
        )
        signal = parse_signal(text)

        assert signal is not None
        assert signal.symbol == "SOLUSDT"
        assert signal.direction == "LONG"

    def test_parse_signal_with_slash_pair(self):
        text = (
            "SOL/USDT LONG\n"
            "Entry: 150\n"
            "TP1: 155\n"
            "SL: 145"
        )
        signal = parse_signal(text)

        assert signal is not None
        assert signal.symbol == "SOLUSDT"


# ===================================================================
# Entry range
# ===================================================================

class TestEntryRange:
    """Test parsing of entry price ranges."""

    def test_parse_signal_with_entry_range(self):
        text = (
            "BTCUSDT LONG\n"
            "Entry: 64000 - 66000\n"
            "TP1: 68000\n"
            "SL: 63000"
        )
        signal = parse_signal(text)

        assert signal is not None
        # Entry should be the midpoint.
        assert signal.entry == 65000.0
        assert signal.entry_low == 64000.0
        assert signal.entry_high == 66000.0


# ===================================================================
# Missing fields
# ===================================================================

class TestMissingFields:
    """Test handling of missing signal components."""

    def test_parse_signal_missing_sl(self):
        """A signal without SL should succeed (SL=None triggers auto-SL)."""
        text = (
            "BTCUSDT LONG\n"
            "Entry: 65000\n"
            "TP1: 66000\n"
            "TP2: 67000"
        )
        signal = parse_signal(text)

        assert signal is not None
        assert signal.sl is None

    def test_parse_signal_missing_direction(self):
        """A signal without direction should fail (return None)."""
        text = (
            "BTCUSDT\n"
            "Entry: 65000\n"
            "TP1: 66000\n"
            "SL: 64000"
        )
        signal = parse_signal(text)
        assert signal is None

    def test_parse_signal_missing_symbol(self):
        """A signal without a recognisable symbol should fail."""
        text = (
            "LONG\n"
            "Entry: 65000\n"
            "TP1: 66000\n"
            "SL: 64000"
        )
        signal = parse_signal(text)
        assert signal is None

    def test_percentage_sl_rejected(self):
        """CRYPTO BANANA BOT EDU regression 2026-04-28: messages with a
        percentage-based SL like "Stop Loss: 5-10%" must NOT capture the
        leading number as an absolute price. The parser must return
        sl=None (or reject the signal entirely if the sanity check fires).
        """
        text = (
            "#EDU/USDT short 50X\n\n"
            "Entry Targets: 0.0460\n"
            "Take-Profit Targets:\n\n"
            "1) 0.04531\n"
            "2) 0.04485\n"
            "3) 0.04439\n"
            "4) 0.0437\n\n"
            "Stop Loss: 5-10%"
        )
        signal = parse_signal(text, channel_name="CRYPTO BANANA BOT")
        # Must NOT come back with sl=5.0 (the bug). Either sl=None
        # (parser skipped the %-suffixed match) or signal=None
        # (validator rejected the SL>50%-from-entry sanity check).
        if signal is not None:
            assert signal.sl is None, (
                f"percentage SL captured as absolute price: sl={signal.sl}"
            )

    def test_percentage_sl_short_form_rejected(self):
        """Short form ``SL: 5%`` must also be rejected as a price."""
        text = (
            "BTCUSDT LONG\n"
            "Entry: 65000\n"
            "TP1: 66000\n"
            "SL: 5%"
        )
        signal = parse_signal(text)
        # Either sl=None or signal rejected entirely.
        if signal is not None:
            assert signal.sl is None

    def test_implausible_sl_rejected(self):
        """Defence-in-depth: any SL more than 50% from entry is rejected
        as a parser error (would leave a position effectively unprotected
        on Bybit if accepted)."""
        # Manually-built signal with a bogus SL ~99% from entry.
        signal = ParsedSignal(
            symbol="BTCUSDT",
            direction="LONG",
            entry=100.0,
            tps=[110.0],
            sl=1.0,  # 99% away — implausible
        )
        ok, reason = validate_signal(signal)
        assert not ok
        assert "implausibly far" in reason


# ===================================================================
# Take-profit targets
# ===================================================================

class TestTakeProfitTargets:
    """Test multiple TP parsing and fake-TP filtering."""

    def test_parse_multiple_tps(self):
        text = (
            "BTCUSDT LONG\n"
            "Entry: 65000\n"
            "TP1: 66000\n"
            "TP2: 67000\n"
            "TP3: 68000\n"
            "TP4: 70000\n"
            "SL: 64000"
        )
        signal = parse_signal(text)

        assert signal is not None
        assert len(signal.tps) == 4
        assert signal.tps == [66000.0, 67000.0, 68000.0, 70000.0]

    def test_parse_no_fake_tp(self):
        """TP5=0 should be excluded from the list."""
        text = (
            "BTCUSDT LONG\n"
            "Entry: 65000\n"
            "TP1: 66000\n"
            "TP2: 67000\n"
            "TP5: 0\n"
            "SL: 64000"
        )
        signal = parse_signal(text)

        assert signal is not None
        # TP5=0 is filtered out (zero prices are dropped by the parser).
        assert 0.0 not in signal.tps
        assert len(signal.tps) == 2


# ===================================================================
# Symbol normalization
# ===================================================================

class TestNormalizeSymbol:
    """Test the normalize_symbol utility."""

    def test_normalize_symbol_variants(self):
        assert normalize_symbol("BTC") == "BTCUSDT"
        assert normalize_symbol("#ETHUSDT") == "ETHUSDT"
        assert normalize_symbol("SOL/USDT") == "SOLUSDT"
        assert normalize_symbol("sol") == "SOLUSDT"
        assert normalize_symbol("$DOGE") == "DOGEUSDT"
        assert normalize_symbol("BTCUSDT") == "BTCUSDT"
        assert normalize_symbol("1000PEPE") == "1000PEPEUSDT"
        assert normalize_symbol("BTC-USDT") == "BTCUSDT"


# ===================================================================
# Signal type classification
# ===================================================================

class TestSignalTypeClassification:
    """Test _classify_signal_type based on SL distance."""

    def test_signal_type_classification(self):
        # Client taxonomy (2026-04-24):
        #   fixed   -> SL missing (auto-SL + x10)
        #   swing   -> SL > 4% from entry
        #   dynamic -> SL present and <= 4% from entry
        assert _classify_signal_type(100.0, 95.0) == "swing"    # 5% dist
        assert _classify_signal_type(100.0, 97.0) == "dynamic"  # 3% dist
        assert _classify_signal_type(100.0, 99.0) == "dynamic"  # 1% dist (was "fixed" pre-2026-04-24)
        assert _classify_signal_type(100.0, None) == "fixed"    # no SL
        assert _classify_signal_type(100.0, 0.0) == "fixed"     # invalid SL


# ===================================================================
# Validation
# ===================================================================

class TestValidation:
    """Test signal validation rules."""

    def test_validate_long_signal_tp_below_entry(self):
        """LONG with all TPs below entry should fail validation."""
        signal = ParsedSignal(
            symbol="BTCUSDT",
            direction="LONG",
            entry=65000.0,
            tps=[64000.0, 63000.0],
            sl=64500.0,
        )
        valid, reason = validate_signal(signal)
        assert not valid
        assert "no TP above entry" in reason

    def test_validate_short_signal_sl_below_entry(self):
        """SHORT with SL below entry should fail validation."""
        signal = ParsedSignal(
            symbol="ETHUSDT",
            direction="SHORT",
            entry=3200.0,
            tps=[3100.0, 3000.0],
            sl=3100.0,
        )
        valid, reason = validate_signal(signal)
        assert not valid
        assert "SL" in reason

    def test_validate_valid_long(self):
        """A well-formed LONG signal should pass."""
        signal = ParsedSignal(
            symbol="BTCUSDT",
            direction="LONG",
            entry=65000.0,
            tps=[66000.0, 67000.0],
            sl=64000.0,
        )
        valid, reason = validate_signal(signal)
        assert valid
        assert reason == ""

    def test_validate_valid_short(self):
        """A well-formed SHORT signal should pass."""
        signal = ParsedSignal(
            symbol="ETHUSDT",
            direction="SHORT",
            entry=3200.0,
            tps=[3100.0, 3000.0],
            sl=3300.0,
        )
        valid, reason = validate_signal(signal)
        assert valid
        assert reason == ""


# ===================================================================
# Direction extraction
# ===================================================================

class TestDirectionExtraction:
    """Test extract_direction edge cases."""

    def test_explicit_long(self):
        assert extract_direction("BTCUSDT LONG entry 65000") == "LONG"

    def test_explicit_short(self):
        assert extract_direction("ETHUSDT SHORT entry 3200") == "SHORT"

    def test_buy_keyword(self):
        assert extract_direction("Buy SOLUSDT at 150") == "LONG"

    def test_sell_keyword(self):
        assert extract_direction("Sell DOGEUSDT at 0.15") == "SHORT"

    def test_no_direction(self):
        assert extract_direction("BTCUSDT entry 65000") is None
