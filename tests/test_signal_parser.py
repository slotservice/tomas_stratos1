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

    def test_parse_maheek_kripto_bracket_entry(self):
        """Maheek Kripto format wraps the entry value in square
        brackets — "Entry = [ 1.040 TO 1.037 ]". Before 2026-05-14 the
        entry pattern only consumed "(" / ")", so this fell through to
        no_entry and fired a false "Blokerad, Entre saknas" while the
        SL and TPs parsed fine. The whole signal must now resolve.
        """
        text = (
            "Pairs:  FIL/USDT\n\n"
            " Trade Type = SHORT\n\n"
            " Leverage :- 20x\n\n"
            "Entry = [ 1.040 TO 1.037 ]\n\n"
            "StopLoss :- 1.078\n\n"
            "Take profit = [ 1.025, 1.011, 0.998, 0.980, 0.969, 0.947 ]"
        )
        signal = parse_signal(text)

        assert signal is not None
        assert signal.symbol == "FILUSDT"
        assert signal.direction == "SHORT"
        assert signal.entry == 1.0385
        assert signal.entry_low == 1.037
        assert signal.entry_high == 1.040
        assert signal.sl == 1.078
        assert signal.tps

    def test_parse_coinaura_enter_keyword_emoji_tps(self):
        """CoinAura / American Crypto format: the entry verb is "ENTER"
        (not "ENTRY") with an arrow emoji glued to the price, and the
        TPs are bare emoji-prefixed prices under a "TARGETS" header.
        Before 2026-05-14 this fell through to no_entry / no_tps and
        fired a false "Blokerad, Entre saknas".
        """
        text = (
            "$FIL/USDT { 25X }\n\n"
            "DIRECTION : { LONG }\n\n"
            "ENTER ➡️1.046 - 1.043\n\n"
            "TARGETS\n\n"
            "➡️1.056\n"
            "➡️1.066\n"
            "➡️1.078\n"
            "➡️1.086\n"
            "➡️1.098\n"
            "➡️1.118\n\n"
            "STOPLOSS : 1.005"
        )
        signal = parse_signal(text)

        assert signal is not None
        assert signal.symbol == "FILUSDT"
        assert signal.direction == "LONG"
        assert signal.entry == 1.0445
        assert signal.sl == 1.005
        assert signal.tps == [1.056, 1.066, 1.078, 1.086, 1.098, 1.118]

    def test_parse_spot_future_long_zone_numbered_emoji_tps(self):
        """Spot Future Signals format: the entry line is "LONG ZONE:
        <range>" and the TPs are a numbered list with an emoji glued
        between the index separator and the price ("1.\U0001f3af 0.1160").
        Before 2026-05-14 this fell through to no_entry / no_tps.
        """
        text = (
            "Trade: #DOGE/USDT\n\n"
            "LONG ZONE: 0.1145 - 0.1120\n\n"
            "LEVERAGE: 20x\n\n"
            "1.\U0001f3af 0.1160\n"
            "2.\U0001f3af 0.1175\n"
            "3.\U0001f3af 0.1190\n"
            "4.\U0001f3af 0.1205\n"
            "5.\U0001f3af 0.1220\n"
            "6.\U0001f3af 0.1240\n\n"
            "STOP-LOSS: 0.1100"
        )
        signal = parse_signal(text)

        assert signal is not None
        assert signal.symbol == "DOGEUSDT"
        assert signal.direction == "LONG"
        assert signal.entry == 0.11325
        assert signal.sl == 0.11
        assert signal.tps == [0.116, 0.1175, 0.119, 0.1205, 0.122, 0.124]

    def test_parse_coinaura_take_entry_now_numbered_arrow_lists(self):
        """CoinAura / American Crypto "TAKE ENTRY NOW" format. Before
        2026-05-14 every field of this signal failed in turn:
          - entry: the word "NOW" sat between "ENTRY" and the price;
          - TPs: each line is "1) TP ➡️ 0.04120" — the "TP " word plus
            the 2-codepoint arrow is 6 chars, over the {0,5} marker gap;
          - SL: "STOPLOSS\n1) ➡️ 0.04520" — the price regex grabbed the
            "1" of the "1)" marker as SL=1.0.
        The whole signal must now resolve.
        """
        text = (
            "\U0001f4c8 SHORT #AIGENSYN/USDT\n\n"
            "LEVERAGE : 50x\n\n"
            "✔️ TAKE ENTRY NOW 0.04270\n\n"
            "\U0001fa99 TAKE PROFIT TARGETS\n\n"
            "1) TP ➡️ 0.04120\n"
            "2) TP ➡️ 0.04000\n"
            "3) TP ➡️ 0.03880\n\n"
            "⚠️ STOPLOSS\n\n"
            "1) ➡️ 0.04520"
        )
        signal = parse_signal(text)

        assert signal is not None
        assert signal.symbol == "AIGENSYNUSDT"
        assert signal.direction == "SHORT"
        assert signal.entry == 0.0427
        assert signal.tps == [0.0412, 0.04, 0.0388]
        assert signal.sl == 0.0452

    def test_parse_entry_market_dash_price(self):
        """Sweden Crypto format: the entry line is "ENTRY MARKET -
        0.6100" — "MARKET" sits between the entry keyword and the
        price. Same root cause as the "NOW" gap above."""
        text = (
            "LONG\n\n#SIREN/USDT\n\nLEVERAGE 50x\n\n"
            "ENTRY MARKET -  0.6100\n\n"
            "SET TP1 0.65217\nSET TP2 0.70000\n\nSL 0.55"
        )
        signal = parse_signal(text)

        assert signal is not None
        assert signal.symbol == "SIRENUSDT"
        assert signal.entry == 0.61

    def test_parse_long_below_parenthesised_zone(self):
        """CoinAura format: the entry line is "LONG BELOW
        (0.03100)(0.03080)" — the verb+"below" replaces "entry", and
        the price is parenthesised. Before 2026-05-14 only "long zone"
        was accepted, so this fell through to no_entry."""
        text = (
            "➡️#IDOL /USDT (LONG)\U0001f53c\n\n"
            "LEVERAGE : 20X TO 75X\n\n"
            "\U0001f4ca LONG BELOW (0.03100)(0.03080)\n\n"
            "\U0001fa99 TARGETS\n\n"
            "\U0001f947 0.03150\n\U0001f948 0.03180\n\U0001f949 0.03280\n\n"
            "⚠️ STOPLOSS : 0.02980"
        )
        signal = parse_signal(text)

        assert signal is not None
        assert signal.symbol == "IDOLUSDT"
        assert signal.direction == "LONG"
        assert signal.entry == 0.031
        assert signal.sl == 0.0298
        assert signal.tps == [0.0315, 0.0318, 0.0328]

    def test_parse_coinaura_pair_label_space_separator_entrys(self):
        """CoinAura "PAIR:- UB USDT" format. Before 2026-05-14:
          - symbol: the ticker-to-USDT separator was a literal "/"/"-",
            but here it is just a space, so "UB" was missed and the
            line-start fallback grabbed "PROJECT" from "Project Type";
          - entry: the keyword is "Entrys" (plural-with-s), matched by
            neither "entry" nor "entries";
          - TPs: the positional fallback could not find the entry line
            because it, too, did not recognise "Entrys".
        The whole signal must now resolve to UBUSDT end-to-end.
        """
        text = (
            "Project Type :- SCALP\n\n"
            "PAIR:- UB USDT\n\n"
            "Direction : LONG\n\n"
            "Leverage: Cross 50\n\n"
            "Entrys : 0.2145 - 0.2045\n\n"
            "Targets:\n 0.2250\n 0.2400\n 0.2700\n\n"
            "Stop-Loss: 0.1980"
        )
        signal = parse_signal(text)

        assert signal is not None
        assert signal.symbol == "UBUSDT"
        assert signal.direction == "LONG"
        assert signal.entry == 0.2095
        assert signal.tps == [0.225, 0.24, 0.27]
        assert signal.sl == 0.198

    def test_parse_scientific_notation_stop_loss(self):
        """Coin Master Trading writes tiny-value stop losses in
        scientific notation — "Stop loss :7.16044E-5". Before
        2026-05-14 the price regex stopped at the "E" and parsed the
        SL as 7.16, so the whole signal was rejected as "SL
        implausibly far from entry". The exponent is now captured."""
        text = (
            "🔥 MTC Indicator 🔥\n 15 Minutes Timeframe\n🔴 SHORT\n"
            "#DOGS/USDT\n"
            "Entry zone : 0.00006893 - 0.00006692\n"
            "Take Profits : \n0.00006658\n0.00006456\n0.00006255\n"
            "Stop loss :7.16044E-5\n"
            "Leverage: 10x"
        )
        signal = parse_signal(text)

        assert signal is not None
        assert signal.symbol == "DOGSUSDT"
        assert signal.direction == "SHORT"
        assert signal.sl == 7.16044e-05
        assert signal.tps == [6.658e-05, 6.456e-05, 6.255e-05]


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


# ===================================================================
# Blocklisted-word tickers (CROSS / HIGH / MAX)
# ===================================================================

class TestBlocklistRealTickers:
    """CROSS, HIGH, MAX are real Bybit tickers that collide with English
    signal words. They must parse when hash/$ or /USDT anchored, but the
    bare English word must still be rejected for the fallback patterns."""

    def test_cross_hash_pair_parses(self):
        text = (
            "\U0001f518 Coin : #CROSS/USDT \U0001f518\n\nSHORT\n\n"
            "Entry: 0.11911\n\nTarget: 0.11811 - 0.11778\n\n"
            "StopLoss : 0.12313\n\nLeverage: 25x"
        )
        signal = parse_signal(text)
        assert signal is not None
        assert signal.symbol == "CROSSUSDT"
        assert signal.direction == "SHORT"

    def test_cross_coin_hash_parses(self):
        text = (
            "New signal\n\nCoin:#CROSS\n\nSignal type:Long\n\n"
            "Entry: 0.10137\n\nTake profits Targets\n1) 0.10400\n"
            "2) 0.10800\n\nLeverage:50x\n\nStop loss: 0.09800"
        )
        signal = parse_signal(text)
        assert signal is not None
        assert signal.symbol == "CROSSUSDT"

    def test_high_pair_parses(self):
        signal = parse_signal("#HIGH/USDT LONG\nEntry: 1.5\nTP: 1.6\nSL: 1.4")
        assert signal is not None
        assert signal.symbol == "HIGHUSDT"

    def test_cross_leverage_word_not_a_symbol(self):
        """'Cross' in 'Leverage: Cross 20x' must NOT become CROSSUSDT."""
        from core.signal_parser import _extract_symbol
        assert _extract_symbol("Leverage: Cross 20x\nUse 2% margin") is None

    def test_short_hashtag_still_rejected(self):
        """#SHORT is a direction word, never a ticker — stays blocked."""
        from core.signal_parser import _extract_symbol
        assert _extract_symbol("#SHORT term view on the market") is None
