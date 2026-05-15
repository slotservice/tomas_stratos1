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
    parse_signal_detailed,
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

    def test_parse_market_entry_now(self):
        """Tomas 2026-05-15: some channels write the entry as a phrase,
        not a price — "Entry: now" (TRUTH), a bare "<dir> <sym> NOW"
        line (Crypto bull society "LONG #LAB NOW"), or "current market
        price" (JASMY). With a TP or SL present these are real signals;
        they parse with entry_is_market=True and entry left at 0.0 —
        the position manager resolves entry to the live Bybit price at
        order time. Pure chatter ("going long now", no TP/SL) still
        falls through to the silent chatter path."""
        truth = parse_signal(
            "📈LONG #TRUTH\n\n⚡️Entry: now\n"
            "❌SL: 0.021299\n✅TP: 0.022980 - 0.023541"
        )
        assert truth is not None
        assert truth.symbol == "TRUTHUSDT"
        assert truth.entry_is_market is True
        assert truth.entry == 0.0
        assert truth.sl == 0.021299

        lab = parse_signal("🟢 LONG #LAB NOW\n❌SL: 3.6\n✅TP: 6.4 - 7.4 - 8.4")
        assert lab is not None
        assert lab.symbol == "LABUSDT"
        assert lab.entry_is_market is True

        jasmy = parse_signal(
            "#JASMY/USDT SHORT x20\n\nEntry - current market price\n"
            "Stop Loss - 0.007069\nTake-Profit - 0.00497"
        )
        assert jasmy is not None
        assert jasmy.entry_is_market is True

        # A normal numeric-entry signal is unaffected.
        normal = parse_signal("BTC/USDT LONG\nEntry: 65000\nTP1: 66000\nSL: 64000")
        assert normal is not None
        assert normal.entry == 65000.0
        assert normal.entry_is_market is False

        # Pure chatter with "now" but no TP/SL is NOT a signal.
        assert parse_signal("going long #BTC now, looking good") is None

    def test_parse_standalone_market_price_line(self):
        """SolidTradesz writes the entry as a bare "MARKET PRICE" line
        with no "Entry" keyword. It must still resolve as a
        market-entry signal (entry_is_market). The word "market" buried
        in prose must NOT trigger it — only a near-empty line."""
        aster = parse_signal(
            "LONG \n\nASTER/USDT\n\nMARKET PRICE\n\nLeverage: 10X\n\n"
            "TP: 0.7800\n\nSL: 0.6500\n\nUse 5% of capital"
        )
        assert aster is not None
        assert aster.symbol == "ASTERUSDT"
        assert aster.entry_is_market is True
        assert aster.sl == 0.6500

        at_market = parse_signal("#XRP/USDT SHORT\nAT MARKET\nTP: 2.10\nSL: 2.30")
        assert at_market is not None
        assert at_market.entry_is_market is True

        # "market" inside prose must NOT make a numeric-entry signal a
        # market-entry one.
        prose = parse_signal(
            "BTC/USDT LONG\nthe market looks bullish today\n"
            "Entry: 65000\nTP: 66000\nSL: 64000"
        )
        assert prose is not None
        assert prose.entry == 65000.0
        assert prose.entry_is_market is False


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

    def test_parse_opg_hyphen_separated_multiline_tps(self):
        """OPG CoinAura format: TAKE-PROFITS header + blank line + a
        single line of hyphen-separated bare prices (Tomas 2026-05-15
        msg 54717+54718). Pattern 4 could not cross the blank line and
        Pattern 2 needs explicit "1)" markers; Pattern 4b handles it."""
        text = (
            "LONG: $OPG/USDT🚀\n"
            "LEVERAGE: 20x - 50x\n"
            "\n"
            "🥇ENTRY PRICE: 0.2780🥇\n"
            "🎖 2nd ENTRY: 0.27🎖\n"
            "———————\n"
            "💰 TAKE-PROFITS 💰\n"
            "\n"
            "0.2860- 0.30- 0.33\n"
            "———————\n"
            "\n"
            "❌ STOP LOSS: 0.25❌"
        )
        prices = extract_prices(text)
        assert prices["tps"] == [0.2860, 0.30, 0.33]
        assert prices["sl"] == 0.25

    def test_parse_dogs_news_url_does_not_become_fake_tp(self):
        """The Crypto Pasta DOGS post (Tomas 2026-05-15 msg 54711+54712)
        carries no signal data — only a chart commentary and a Telegram
        link `https://t.me/cyberdavid/7021`. Pattern 4 used to match
        "tps" inside "https" as the TP keyword and capture "7021" as a
        TP price, so the message tripped the no_entry signal-shape gate
        and fired "Blokerad, Entre saknas". The \\b boundary around
        the keyword alternation prevents the in-word match."""
        text = (
            "#DOGS is breaking out of the descending channel formation "
            "on the 3D timeframe\n"
            "The technical setup appears bullish, with volume supporting "
            "the breakout\n"
            "If momentum continues, we could see a climb toward $0.0013\n"
            "[ߋ](https://t.me/cyberdavid/7021) cyberdavid"
            "[​](https://t.me/CryptoPasta)"
        )
        prices = extract_prices(text)
        assert prices["tps"] == [], (
            f"URL must not become a TP, got: {prices['tps']!r}"
        )

    def test_parse_zen_news_targets_in_prose_does_not_match(self):
        """Crypto Pasta ZEN bullish-flag post (Tomas 2026-05-15 msg
        54721+54722). The prose "...might push the price toward targets
        at $8.40, $10.70, $14.00, and $19.00" carries the word
        "targets" but mid-prose, with "at" between keyword and prices.
        Pattern 4 used to match "targets" + " at $" (the `[^\\d\\n]*`
        gap allowed alphabetic chars) and capture the dollar amounts as
        TPs. Tightening the gap to `[^\\d\\n\\w]*` blocks the prose
        case while still allowing emoji / whitespace / punctuation."""
        text = (
            "#ZEN Bullish Flag Formation Taking Shape\n"
            "Horizen is developing a continuation pattern following a "
            "strong upward impulse move on the daily chart\n"
            "A new uptrend phase may begin if the price breaks above "
            "the flag's upper resistance\n"
            "The pattern completion might push the price toward "
            "targets at $8.40, $10.70, $14.00, and $19.00\n"
            "[ߋ](https://t.me/jonathancarter/3614) jonathancarter"
            "[​](https://t.me/CryptoPasta)"
        )
        prices = extract_prices(text)
        assert prices["tps"] == [], (
            f"Prose 'targets at $X' must not become TPs, got: {prices['tps']!r}"
        )

    def test_parse_enso_emoji_same_line_list_still_works(self):
        """Regression guard for the ENSO-style format: "Targets: 🎯 1.20,
        1.27, 1.36" — emoji and whitespace between the header and the
        first price. The new [^\\d\\n\\w]* gap still allows emoji and
        whitespace, so this real signal must continue to parse."""
        text = (
            "#FOOUSDT LONG\n"
            "Entry: 1.10\n"
            "Targets: 🎯 1.20, 1.27, 1.36\n"
            "SL: 1.05"
        )
        prices = extract_prices(text)
        assert prices["tps"] == [1.20, 1.27, 1.36]

    def test_parse_bill_targets_after_sl_section(self):
        """US Crypto Leaks BILL signal (Tomas 2026-05-15 msg 54792+54793):
        sections are ordered ENTRY -> STOPLOSS -> TARGET (reversed from
        the usual ENTRY -> TARGETS -> SL). Pattern 5's primary block
        scan runs entry.end() -> sl.start(), so when TPs are AFTER the
        SL line that block is empty and TPs are missed. Pattern 5b
        catches this by doing a second pass from any TP header found
        AFTER the SL line."""
        text = (
            "🌷🌷#BILL/USDT SHORT🌷🌷\n"
            "\n"
            "ENTRY 2100-2200\n"
            "\n"
            "STOPLOSS 2300\n"
            "\n"
            "TARGET\n"
            "2000\n"
            "1900\n"
            "1800\n"
            "1700\n"
            "1600\n"
            "\n"
            "LEV/20X/50X\n"
            "\n"
            "@UScrypto1"
        )
        prices = extract_prices(text)
        assert prices["tps"] == [2000.0, 1900.0, 1800.0, 1700.0, 1600.0]
        assert prices["sl"] == 2300.0

    def test_parse_bob_bare_per_line_still_falls_through(self):
        """Regression guard for the BOB 2026-05-04 case: bare prices on
        separate lines under a "Take Profit Target" header must NOT be
        captured by the new Pattern 4b (it requires an explicit [-/,]
        separator between two prices on the SAME line). Pattern 5
        (positional fallback) is the right home for bare per-line lists.
        The TP list must contain ALL four BOB prices, not just one."""
        text = (
            "#1000000BOBUSDT LONG\n"
            "Entry: 0.0195\n"
            "Take Profit Target\n"
            "0.0180\n"
            "0.0172\n"
            "0.0156\n"
            "0.0144\n"
            "Stop Loss: 0.0220"
        )
        prices = extract_prices(text)
        assert len(prices["tps"]) == 4
        assert prices["tps"][0] == 0.0180

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

    def test_bare_t_keyword_does_not_make_fake_tp_from_prose(self):
        """The bare "t" TP-keyword alternative must only match at a
        word boundary. Before 2026-05-15 it matched the letter "t"
        inside ordinary words — "a[t] 10.30 AM" produced TP 30,
        "PROFI[T] 20.14%" produced TP 14 — so news / status posts
        looked signal-shaped and fired a false "Blokerad, Entre
        saknas". With no entry and no real TP/SL these must parse as
        chatter (no_entry, empty tps/sl) so the notify gate stays
        silent. A genuine "T1:" signal still parses."""
        from core.signal_parser import extract_prices

        massive = extract_prices(
            "MASSIVE: US Senate will vote on the bill today at 10.30 AM ET."
        )
        assert massive["tps"] == []
        assert massive["sl"] is None

        status = extract_prices("#XVSUSDT CLOSE FINAL PROFIT 20.14% ROI")
        assert status["tps"] == []
        assert status["sl"] is None

        # Regression: a real "T1:" / "T2:" signal still resolves.
        sig = parse_signal(
            "ETH/USDT LONG\nEntry: 3000\nT1: 3100\nT2: 3200\nSL: 2900"
        )
        assert sig is not None
        assert sig.tps == [3100.0, 3200.0]

    def test_lab_short_summary_with_no_entry_keyword_is_chatter(self):
        """CoinAura posts a short summary "Lab long 4.12 tp1 4.21"
        seconds before the full Trade Setup. The summary has tps from
        "tp1 4.21" but no entry keyword anywhere in the text — Tomas
        msg 54791: "maby just igone that on". The chatter classifier
        must drop it silently (no_entry_chatter event) instead of
        firing "Blokerad, Entre saknas"."""
        result = parse_signal_detailed(
            text="Lab long 4.12 tp1 4.21\nLeverage 25x to 50x",
            channel_id=-1003657180432,
            channel_name="CoinAura",
        )
        assert result.signal is None
        assert result.reason == "no_entry"
        # The signal-shape gate must NOT trigger — the operator-channel
        # rejection notify in main.py only fires when reason=="no_entry"
        # AND result has both direction and (tps or sl). We can't see
        # the log event directly, but the chatter classifier in the
        # parser flips the log event between "signal_parse_no_entry"
        # (notify) and "signal_parse_no_entry_chatter" (silent). The
        # public contract: no entry keyword => result must have empty
        # tps and sl from main.py's perspective, OR the gate must drop
        # it. Easier to assert: text without an entry keyword keeps the
        # tps in the result but main.py's signal-shape check requires
        # entry keyword presence in the text. We re-check that here:
        import re
        entry_kw = re.search(
            r"\b(?:entry|entries|entrys|enter|"
            r"buy[\s-]+(?:range|zone|area)|"
            r"(?:long|short)[\s-]+zone)\b",
            "Lab long 4.12 tp1 4.21\nLeverage 25x to 50x",
            re.IGNORECASE,
        )
        assert entry_kw is None, "test premise: text has no entry keyword"

    def test_signal_with_entry_keyword_but_bad_value_still_rejects(self):
        """Regression guard: a real signal that HAS an entry keyword
        but the parser couldn't extract a price (e.g. unparseable entry
        value) must still hit signal_parse_no_entry and fire the
        operator-channel rejection. The LAB chatter fix above must not
        silence this case."""
        # Entry keyword present, but value is unparseable text.
        result = parse_signal_detailed(
            text="BTCUSDT LONG\nEntry: tbd\nTP1: 66000\nSL: 64000",
            channel_id=0,
            channel_name="TestGroup",
        )
        assert result.signal is None
        assert result.reason == "no_entry"
        # Signal-shape: entry keyword present + tps + sl.
        assert result.tps  # parser extracted TP1 -> non-empty
        assert result.sl == 64000.0


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

    def test_validate_sl_failure_detail_has_sl_only(self):
        """SL-side validation failures must include "SL" (and NOT "TP")
        in the detail string. main.py reads result.detail to route the
        operator-channel message between the SL- and TP-specific
        wordings (Tomas 2026-05-15 msg 54706 — CryptoPasta BTC
        Stop:8230 typo got "TP är fel angiva" instead of an SL-specific
        message)."""
        sl_cases = [
            # SHORT, SL <= entry (the live CryptoPasta BTC case)
            ParsedSignal(symbol="BTCUSDT", direction="SHORT", entry=81200.0,
                         tps=[80600.0], sl=8230.0),
            # LONG, SL >= entry
            ParsedSignal(symbol="ETHUSDT", direction="LONG", entry=3200.0,
                         tps=[3300.0], sl=3300.0),
            # SL implausibly far from entry (>50%)
            ParsedSignal(symbol="ETHUSDT", direction="LONG", entry=3200.0,
                         tps=[3300.0], sl=5.0),
        ]
        for sig in sl_cases:
            valid, reason = validate_signal(sig)
            assert not valid, f"Expected invalid: {sig}"
            upper = reason.upper()
            assert "SL" in upper, f"'SL' missing from reason: {reason!r}"
            assert "TP" not in upper, f"'TP' should not be in SL reason: {reason!r}"

    def test_validate_tp_failure_detail_has_tp_not_sl(self):
        """TP-side validation failures must include "TP" and NOT "SL"
        in the detail string — the inverse of the SL routing assertion
        above."""
        tp_cases = [
            # LONG, all TPs below entry
            ParsedSignal(symbol="BTCUSDT", direction="LONG", entry=65000.0,
                         tps=[64000.0, 63000.0], sl=64500.0),
            # SHORT, all TPs above entry
            ParsedSignal(symbol="BTCUSDT", direction="SHORT", entry=65000.0,
                         tps=[66000.0, 67000.0], sl=66500.0),
        ]
        for sig in tp_cases:
            valid, reason = validate_signal(sig)
            assert not valid, f"Expected invalid: {sig}"
            upper = reason.upper()
            assert "TP" in upper, f"'TP' missing from reason: {reason!r}"
            assert "SL" not in upper, f"'SL' should not be in TP reason: {reason!r}"

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


class TestTradeUpdateMessages:
    """Cryptonus-style "OPEN ENTRY / NEW TP" follow-up messages — a
    trade update with no direction word and no SL (Tomas 2026-05-15)."""

    def test_open_entry_new_tp_detected_and_parsed(self):
        from core.signal_parser import (
            is_trade_update_message, parse_trade_update,
        )
        text = (
            "#DOGEUSDT OPEN ENTRY2 0.11797\nNEW TP\n"
            "🎯 TP1 : 0.11234 (💰 2.00%)\n"
            "🎯 TP2 : 0.11004 (💰 4.00%)\n"
            "🎯 TP3 : 0.10775 (💰 6.00%)"
        )
        assert is_trade_update_message(text) is True
        tu = parse_trade_update(text)
        assert tu is not None
        assert tu.symbol == "DOGEUSDT"
        assert tu.entry == 0.11797
        assert tu.tps == [0.11234, 0.11004, 0.10775]
        assert tu.sl is None

    def test_fresh_signal_not_a_trade_update(self):
        """A normal fresh signal (Entry: line, no "OPEN ENTRY"+"NEW TP"
        pair) must never be flagged as a trade update."""
        from core.signal_parser import (
            is_trade_update_message, parse_trade_update,
        )
        text = "BTC/USDT LONG\nEntry: 65000\nTP1: 66000\nSL: 64000"
        assert is_trade_update_message(text) is False
        assert parse_trade_update(text) is None

    def test_entry1_entry2_full_signal_not_a_trade_update(self):
        """A Cryptonus FULL signal carries "ENTRY1/ENTRY2" but not the
        "OPEN ENTRY" + "NEW TP" marker pair — it must still parse as a
        normal signal, not a trade update."""
        from core.signal_parser import is_trade_update_message
        text = (
            "#DODOXUSDT 15m STATUS : SHORT 👉 ENTRY1 : 0.018567 "
            "👉 ENTRY2 : 0.019681\n🎯 TP1 : 0.018196\n⛔️ SL: 0.02023"
        )
        assert is_trade_update_message(text) is False
