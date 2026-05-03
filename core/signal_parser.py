"""
Stratos1 - Signal Parser & Normalizer
--------------------------------------
Parses messy real-world Telegram trading signals from 100+ crypto groups
into a clean, validated ParsedSignal dataclass.

Handles wildly different formatting: emojis, slashes, hashtags, ranges,
multiple TP formats, missing SL, and many more edge cases.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Optional

import structlog

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class ParsedSignal:
    """Normalized trading signal ready for execution."""
    symbol: str                          # e.g. "BTCUSDT"
    direction: str                       # "LONG" or "SHORT"
    entry: float                         # Single entry price (midpoint if range)
    entry_low: Optional[float] = None    # Lower bound of entry zone (if range)
    entry_high: Optional[float] = None   # Upper bound of entry zone (if range)
    tps: list[float] = field(default_factory=list)   # TP1, TP2, ... in order
    sl: Optional[float] = None           # Stop-loss price (None = auto-SL)
    signal_type: str = "dynamic"         # "fixed", "dynamic", or "swing"
    channel_id: int = 0
    channel_name: str = ""
    raw_text: str = ""
    parsed_at: float = 0.0              # Unix timestamp


@dataclass
class ParseResult:
    """Detailed parse result with rejection reason and partial state.

    The ``signal`` field is set only when parsing succeeded. On rejection,
    ``reason`` indicates which stage failed and ``symbol``/``direction``
    expose whatever was successfully extracted before the failure — so
    callers can build a "BLOCKED: <reason>" Telegram notification that
    still names the symbol/direction the user can recognise.

    Reason codes:
        "ok"           - parsed successfully, signal is set
        "empty"        - empty/whitespace input, ignored silently
        "no_symbol"    - no recognisable symbol found (silent — too noisy)
        "no_direction" - symbol found but no LONG/SHORT keyword
        "no_entry"     - symbol+direction found but no entry price
        "no_tps"       - symbol+direction+entry found but no TPs
        "invalid"      - all fields parsed but validate_signal rejected
                         (e.g. TP on wrong side of entry, SL too far)
    """
    signal: Optional[ParsedSignal] = None
    reason: str = "ok"
    detail: str = ""
    symbol: Optional[str] = None
    direction: Optional[str] = None
    entry: float = 0.0
    tps: list[float] = field(default_factory=list)
    sl: Optional[float] = None
    channel_id: int = 0
    channel_name: str = ""
    # Operator-attached free-form note from symbol_overrides.toml.
    # Carried so the notifier can append it to relevant warnings
    # (e.g. instrument-info-fetch-failed for a "skip" override) so
    # the operator's own context is visible alongside the bot's
    # automatic message.
    override_note: Optional[str] = None

# ---------------------------------------------------------------------------
# Known USDT-paired symbols on Bybit (common ones used for bare-symbol
# detection like "BTC" -> "BTCUSDT"). In production this would come from
# the exchange module; here we keep a generous static set for the parser.
# ---------------------------------------------------------------------------

# Symbols that might appear without the "USDT" suffix in signals.
# The parser appends "USDT" when the raw token does not already end with it.
_KNOWN_BASES = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "DOT",
    "MATIC", "LINK", "UNI", "ATOM", "LTC", "FIL", "APT", "ARB", "OP",
    "IMX", "INJ", "SUI", "SEI", "TIA", "JUP", "WLD", "NEAR", "FTM",
    "ALGO", "SAND", "MANA", "GALA", "AXS", "ENJ", "CHZ", "SHIB", "PEPE",
    "FLOKI", "BONK", "WIF", "RENDER", "FET", "AGIX", "OCEAN", "TAO",
    "RNDR", "CFX", "CKB", "STX", "ORDI", "RUNE", "AAVE", "MKR", "SNX",
    "CRV", "COMP", "SUSHI", "1INCH", "LDO", "RPL", "GMX", "DYDX",
    "BLUR", "PENDLE", "ENA", "ETHFI", "W", "STRK", "ZK", "LISTA",
    "NOT", "TON", "TRX", "EOS", "XLM", "VET", "HBAR", "ICP", "EGLD",
    "THETA", "GRT", "ENS", "AR", "JASMY", "MASK", "SSV", "PYTH",
    "JTO", "MEME", "ACE", "AI", "ALT", "PIXEL", "PORTAL", "AEVO",
    "BOME", "SAGA", "TNSR", "REZ", "BB", "IO", "ZRO",
}

# ---------------------------------------------------------------------------
# Status-update detector (must run BEFORE the parser)
# ---------------------------------------------------------------------------
#
# Tomas (client) 2026-05-03: "Trade updates from groups, for example
# 'TP1 taken' or result messages, are being interpreted by the bot as
# new signals." Confirmed in production: a message
# "#PLAY ALL TARGET REACHED 🔥 So Far Profit (334%) ENTRY: 0.1336"
# parsed as a PLAY signal with entry 0.1336.
#
# This regex catches the unambiguous status-update vocabulary used by
# every monitored channel. Conservative — favour false negatives (let
# parser handle it) over false positives (block a real signal).
#
# Anchors REQUIRE a completion verb (reached/done/hit/taken/triggered/
# achieved/closed-at) within ~25 chars of a target/stop/tp/profit
# noun, OR an explicit closure phrase (stopped out, position closed).
# Standalone "TP1: 0.05" in a real signal does NOT match.
_STATUS_UPDATE_PATTERNS = [
    # "ALL TARGETS REACHED / DONE / ACHIEVED / HIT"
    re.compile(
        r"\ball\s+target[s]?\b[\s\W]{0,15}\b(?:reached|achieved|done|hit|taken|completed)\b",
        re.IGNORECASE,
    ),
    # "Target 1 hit", "Target 3 reached", "TP2 taken", "TP1 hit"
    re.compile(
        r"\b(?:target|tp)\s*\d+\b[\s\W]{0,20}\b(?:reached|achieved|done|hit|taken|secured)\b",
        re.IGNORECASE,
    ),
    # "Take Profit target N ✅" — Telegram bots often signal hits this way
    re.compile(
        r"\btake[\s_-]*profit\s+target\s+\d+\b[^\n]{0,5}[✅✓☑]",
        re.IGNORECASE,
    ),
    # "Stop Target Hit", "Stop Loss Hit", "SL Hit", "Stop Hit"
    re.compile(
        r"\b(?:stop(?:[\s_-]*loss|[\s_-]*target)?|sl)\s+(?:hit|triggered|reached)\b",
        re.IGNORECASE,
    ),
    # "Stopped out"
    re.compile(r"\bstopped\s+out\b", re.IGNORECASE),
    # "Closed at stoploss / take profit / TP / SL / profit / loss"
    # Note: \bstop\b doesn't match inside "stoploss" — explicit alternates.
    re.compile(
        r"\bclosed\s+at\s+(?:stop[\s_-]*loss|stoploss|take[\s_-]*profit|takeprofit|stop|sl|tp|profit|loss)\b",
        re.IGNORECASE,
    ),
    # "Position closed", "Trade closed"
    re.compile(r"\b(?:position|trade)\s+closed\b", re.IGNORECASE),
    # "+646% GAIN", "+50% Profit" — completion brag with explicit %
    re.compile(
        r"[+]\d+(?:\.\d+)?\s*%\s*(?:gain|profit|in\s+profit)\b",
        re.IGNORECASE,
    ),
    # "So far profit (X%)" — running-PnL update style
    re.compile(r"\bso\s+far\s+profit\b", re.IGNORECASE),
]


def is_status_update(text: str) -> bool:
    """Return True if *text* is a trade-status update (TP hit, SL hit,
    closed, etc.) rather than a new signal.

    Used by the message handler to drop status messages BEFORE the
    parser can misinterpret them. See _STATUS_UPDATE_PATTERNS for the
    exact vocabulary.
    """
    if not text:
        return False
    for pat in _STATUS_UPDATE_PATTERNS:
        if pat.search(text):
            return True
    return False


# ---------------------------------------------------------------------------
# Regex building blocks
# ---------------------------------------------------------------------------

# Matches a decimal or integer price, possibly with comma thousands separators.
_PRICE_RE = r"[\$]?\s*(\d[\d,]*\.?\d*)"

# Range separator: dash/tilde/to but NOT across newlines.
# Uses [^\S\n] (whitespace except newline) instead of \s.
_RANGE_SEP = r"(?:[^\S\n]*[-–~]+[^\S\n]*|[^\S\n]+to[^\S\n]+)"

# Entry patterns
_ENTRY_PATTERNS = [
    # "Entry Market Price (0.2050)" format (match FIRST - most specific)
    re.compile(
        r"entry\s+market\s+price\s*\(\s*" + _PRICE_RE + r"\s*\)",
        re.IGNORECASE,
    ),
    # "ENTRY PRICE: 0.2050" - prioritise before numbered list to avoid
    # the 0. trap. STRICTLY single-line: uses an inline price match
    # without _PRICE_RE's leading \s* so the regex can't bleed across
    # newlines and grab the "1" of a following "1) 0.907" ordinal
    # (Pair: #QTUM regression 2026-04-25).
    re.compile(
        r"entry\s+price[^\S\n]*[:=@]?[^\S\n]*(\d[\d,]*\.?\d*)"
        + r"(?:[^\S\n]*[-–~][^\S\n]*(\d[\d,]*\.?\d*))?",
        re.IGNORECASE,
    ),
    # "ENTRY1: 0.4241" / "ENTRY 1: ..."
    re.compile(
        r"entry\s*\d+\s*[:=@]\s*" + _PRICE_RE
        + r"(?:" + _RANGE_SEP + _PRICE_RE + r")?",
        re.IGNORECASE,
    ),
    # Numbered list under "Entry:" / "Entry Price:" / "Entries:" /
    # "Entry Zone:" / "BUY:" / "Buy Zone:" header. e.g.
    # "Entry :\n1) 87.07\n2) 84.45" or
    # "Pair: #QTUM Entry Price:\n1) 0.907\n2) 0.934" or the CRYPTO
    # WORLD UPTADES format "ENTRY ZONE:\n\n1) 0.02850" or the
    # American Crypto BUY format "BUY :\n\n1) 75610 - 76xxx".
    # Tried BEFORE the emoji-prefix pattern below so a numbered list
    # with a blank line between header and price doesn't end up
    # grabbing just "1" as the entry (CRYPTO WORLD UPTADES ZKJ +
    # American Crypto BTC regressions 2026-04-28).
    re.compile(
        r"(?:entry|entries|buy)\s*(?:zone|price|orders?|area|targets?|range)?\s*[:=]?"
        r"\s*\n\s*\d+[\)\.]\s+" + _PRICE_RE
        + r"(?:\s*\n\s*\d+[\)\.]\s+" + _PRICE_RE + r")?",
        re.IGNORECASE,
    ),
    # "Entry Targets:\n0.4101" / "Entry Zone:\n💠 9.19\n💠 9.11" —
    # header on one line, price(s) on following line(s) with optional
    # emoji/bullet prefix. Accepted keywords: target(s), zone, area,
    # order(s). An OPTIONAL second price is captured on the IMMEDIATELY
    # next line only. The second capture uses an inline digit regex
    # (NO leading \s*) because _PRICE_RE's leading \s* would cross the
    # blank line and grab the "1" of a following TP ordinal like
    # "1) 2.65" — the CoinAura MOVR regression on 2026-04-24.
    re.compile(
        r"entry\s+(?:targets?|zone|orders?|area)\s*[:=]?[^\S\n]*\n[^\d\n]*"
        + _PRICE_RE
        + r"(?:[^\S\n]*\n[^\d\n]+(\d[\d,]*\.?\d*))?",
        re.IGNORECASE,
    ),
    # "Entry: 65000" / "Entry Zone: 3200-3250" / "Buy: 0.152" /
    # "Entry Target: (0.07300)" / "Entries: - 1.1480 - 1.10" /
    # "ENTRY :- 0.0715 - 0.069" / "Buy Range: 0.009880 – 0.0096".
    # IMPORTANT: uses [^\S\n] (whitespace excluding newline) instead of
    # \s* before/after the separator so this pattern matches only on
    # a SINGLE LINE. Without that, the leading \s* would bleed past
    # the colon/blank line and capture the "1" of a following
    # numbered-list ordinal (Pair: #QTUM regression 2026-04-24).
    re.compile(
        r"(?:(?:entry|entries)\s*(?:zone|price|area|orders?|targets?)?|"
        r"buy(?:\s*(?:zone|price|area|range|at|around|@))?|"
        r"open|limit|market\s*(?:buy|entry))"
        r"[^\S\n]*[:=@]?[^\S\n]*-?[^\S\n]*\(?[^\S\n]*"
        + _PRICE_RE + r"(?:" + _RANGE_SEP + _PRICE_RE + r")?[^\S\n]*\)?",
        re.IGNORECASE,
    ),
    # Standalone "Entry 65000" without colon
    re.compile(
        r"entry\s+" + _PRICE_RE + r"(?:" + _RANGE_SEP + _PRICE_RE + r")?",
        re.IGNORECASE,
    ),
    # Benjamin Cowen format: "$BIRB add long @ 0.14509" — "@" acts
    # as the separator, "add long" / "add short" is the direction
    # phrase, the price follows. Covers both `@ 0.145` and `@0.145`.
    re.compile(
        r"add\s+(?:long|short)\s*(?:at|around|@)\s*" + _PRICE_RE,
        re.IGNORECASE,
    ),
]

# TP patterns - match TP1/T1/Target 1/Take Profit 1 etc.
_TP_PATTERNS = [
    # "TP1: 66000" / "T1: 66000" / "Target 1: 66000" / "TP1 ➝ 2.640"
    # / "TP1.➝2.500" — allow any non-digit, non-newline separators
    # (colons, periods, arrows like ➝ → ⇒ = etc.) between the TP
    # number and the price.
    re.compile(
        r"(?:tp|t|target|take[\s_-]*profit)\s*(\d+)[^\d\n]{0,6}" + _PRICE_RE,
        re.IGNORECASE,
    ),
    # Numbered list under a "Targets :" / "Take Profits :" / "Take
    # Profit Targets :" / "TARGET :-" header. e.g. "Targets :\n1) 87.57"
    # or "TARGET :-\n1 : 0.074\n2 : 0.078" (WCT format). The list-marker
    # accepts ")" "." ":" "-" with optional whitespace AROUND them so
    # "1)0.5", "1) 0.5", "1 : 0.5", "1 - 0.5" all match. Trailing
    # "(?:\s+targets?)?" absorbs the extra word in "Take Profit Targets".
    #
    # The bare ``targets?`` alternative is anchored to start-of-line
    # (or after a newline) so headers like "Entry Targets:" don't match
    # — that header followed by a single price was making the parser
    # capture the ENTRY price as TP1. "Take-Profit Targets" still
    # matches anywhere because the "take..." prefix is unambiguous.
    re.compile(
        r"(?:take[\s_-]*profits?(?:\s+targets?)?|(?:^|\n)\s*(?:tps?|targets?))"
        r"\s*[:=]?\s*-?"
        r"\s*\n\s*((?:\d+[^\d\n\w]{1,4}[\d.]+\s*\n?\s*){1,8})",
        re.IGNORECASE | re.MULTILINE,
    ),
    # "Targets: 3300/3400/3500" / "Targets: 3300, 3400, 3500" / "Targets:
    # 🎯 1.20, 1.27, 1.36" — same-line list with optional emojis between
    # the header and the first price (ENSO format). The "[^\d\n]*" gap
    # absorbs emoji + spaces + bullet markers between the header and
    # the digits. The captured class still excludes \n so the match
    # can't bleed into a separate "Targets:\n1 : 0.074" numbered list.
    re.compile(
        r"(?:targets?|tps?|take[\s_-]*profits?(?:\s+targets?)?)[^\S\n]*[:=]?[^\d\n]*"
        r"([\d.,/|\-•• ]+)",
        re.IGNORECASE,
    ),
]

# SL patterns
_SL_PATTERNS = [
    # Accepts "SL", "Stop Loss" (any hyphen/space/none between the
    # words), "Stoploss", "Invalidation". Bare "stop" is intentionally
    # NOT accepted — it appears in commentary text ("don't stop", "1H
    # stop", "stop trailing 1%") and the bounded gap below was grabbing
    # whatever digit followed (Benjamin Cowen WET regression
    # 2026-04-28: SL captured as 1.0 from a "1H Timeframe" line).
    # ``[^\d\n]{0,10}`` between the colon and the price absorbs an arrow
    # / bullet / emoji prefix on the next line ("STOP LOSS:\n→ 0.027",
    # CRYPTO WORLD UPTADES format) without crossing into a different
    # line of the message.
    #
    # The trailing ``(?!\s*[-\d]*\s*%)`` negative lookahead rejects
    # percentage-based SL specs such as "Stop Loss: 5-10%" or "SL: 5%"
    # (CRYPTO BANANA BOT EDU regression 2026-04-28: live message
    # "Stop Loss: 5-10%" produced sl=5.0 on a 0.046 entry — 100x above
    # entry, leaving the position effectively unprotected on Bybit).
    # We accept only ABSOLUTE-price SLs; percentage specs lack a price
    # the bot can verify, so the signal is rejected upstream.
    re.compile(
        r"(?:sl|stop[-\s]*loss|stoploss|invalidation)\s*[:=]?\s*"
        r"[^\d\n]{0,10}" + _PRICE_RE
        + r"(?!\s*[-\d]*\s*%)",
        re.IGNORECASE,
    ),
]

# Direction keywords
_LONG_KEYWORDS = re.compile(
    r"\b(long|buy|bull(?:ish)?|green|up(?:side)?|calls?)\b"
    r"|[\U0001F7E2\U0001F4C8\u2705\U0001F525]",  # green circle, chart up, check, fire
    re.IGNORECASE,
)
_SHORT_KEYWORDS = re.compile(
    r"\b(short|sell|bear(?:ish)?|red|down(?:side)?|puts?)\b"
    r"|[\U0001F534\U0001F4C9\u274C]",  # red circle, chart down, cross
    re.IGNORECASE,
)

# Symbol patterns (ordered by specificity)
# Ordered from most-specific to least — first match wins. USDT-suffix
# patterns are preferred because they're self-anchoring. Bare "#TICKER"
# fallbacks come last and only fire when nothing else matched.
_SYMBOL_PATTERNS = [
    # "#BTCUSDT" / "BTCUSDT" / "$BTCUSDT"
    re.compile(r"[#$]?\b([A-Z0-9]{2,20}USDT)\b", re.IGNORECASE),
    # "BTC/USDT" / "SOL/USDT" / "BTC-USDT" / "Q/USDT" (1-char ticker
    # allowed — the /USDT anchor makes it unambiguous).
    re.compile(r"\b([A-Z0-9]{1,15})\s*[/\-]\s*USDT\b", re.IGNORECASE),
    # "Coin: SOL/USDT" / "Pair: BTC/USDT"
    re.compile(r"(?:coin|pair|symbol|ticker)\s*[:=]\s*[#$]?([A-Z0-9]{1,15})\s*[/\-]?\s*USDT\b", re.IGNORECASE),
    # "Coin: SOL" (no USDT suffix)
    re.compile(r"(?:coin|pair|symbol|ticker)\s*[:=]\s*[#$]?([A-Z0-9]{2,15})\b", re.IGNORECASE),
    # Standalone symbol at start of line or after emoji: "BTCUSDT" / "#SOL"
    # Prefix emoji class extended with more currency/signal emojis.
    re.compile(r"(?:^|[\n\r])\s*[#$🔥🟢🔴📈📉⚡️💰💵💴💶💷💹💲✅❌]*\s*([A-Z0-9]{2,15}USDT)\b", re.IGNORECASE),
    # "PAIR ✈️MOVR/USDT" — "PAIR" / "pair" label followed by an emoji
    # or whitespace then the ticker (CoinAura header format).
    re.compile(r"\bpair\b[^A-Za-z0-9\n]*([A-Z0-9]{1,15})\s*[/\-]\s*USDT\b", re.IGNORECASE),
    # FALLBACK: "#MIVR" / "#DAM" / "#Q" — hash-prefixed ticker with no
    # USDT suffix. We append USDT via normalize_symbol. Must start with
    # a LETTER (so "#123" hashtags / arbitrary numeric tags don't match)
    # and be followed by a non-word char (emoji, newline, end of string)
    # so we don't capture half of a longer word. Letter-then-0-to-9
    # chars allows 1-char tickers like Q on CoinAura's STRONG SHORT
    # format ("💵#Q🔥").
    #
    # 2026-05-03: this hashtag pattern moved BEFORE the line-start bare
    # fallback below. The `#`-prefix anchors the token to a deliberate
    # ticker tag from the operator, while the line-start pattern
    # incorrectly fired on lines like "TP1: 0.026" and produced the
    # TP1USDT ghost-symbol class (130+ events/day in production).
    # Hashtag is HIGHER confidence than bare-line-start, so it must win
    # the first-match-wins race.
    re.compile(r"#([A-Z][A-Z0-9]{0,9})(?=[^A-Za-z0-9]|$)", re.IGNORECASE),
    # LAST-RESORT FALLBACK: bare ticker at line start, after optional
    # emojis. Subject to the short-name + blocklist filters at
    # _extract_symbol so "TP", "SL", "ENTRY", "LEVERAGE", etc. are
    # rejected. Still the source of edge-case false positives — only
    # fires when every more-specific pattern above failed.
    re.compile(r"(?:^|[\n\r])\s*[#$🔥🟢🔴📈📉⚡️💰💵💴💶💷💹💲✅❌]*\s*([A-Z0-9]{2,10})\b", re.IGNORECASE),
]


# ---------------------------------------------------------------------------
# Helper: parse a price string like "65,000.50" -> 65000.50
# ---------------------------------------------------------------------------

def _parse_price(raw: str) -> float:
    """Convert a raw price string to float, stripping commas and dollar signs."""
    cleaned = raw.replace(",", "").replace("$", "").strip()
    try:
        val = float(cleaned)
        return val if val > 0 else 0.0
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def normalize_symbol(raw: str) -> str:
    """
    Normalize a raw symbol string to XXXUSDT format.

    Examples:
        "BTC"       -> "BTCUSDT"
        "#ETHUSDT"  -> "ETHUSDT"
        "SOL/USDT"  -> "SOLUSDT"
        "sol"       -> "SOLUSDT"
        "1000PEPE"  -> "1000PEPEUSDT"
    """
    # Strip common prefixes and whitespace
    s = raw.strip().upper()
    s = s.lstrip("#$")
    # Remove slash/dash separators before USDT
    s = re.sub(r"[/\-\s]+", "", s)

    # Already ends with USDT? Done.
    if s.endswith("USDT"):
        return s

    # Known base currency -> append USDT
    if s in _KNOWN_BASES:
        return s + "USDT"

    # If it's 2-10 alphanumeric chars, assume it's a base and append USDT.
    # This handles new coins not in our static list.
    if re.fullmatch(r"[A-Z0-9]{2,15}", s):
        return s + "USDT"

    return s + "USDT"


def extract_direction(text: str) -> Optional[str]:
    """
    Determine LONG or SHORT from signal text.

    Checks SHORT first because some messages say "short-term buy" which
    would otherwise false-match on "short". We look for unambiguous short
    keywords, then fall back to long keywords.

    Returns "LONG", "SHORT", or None if direction cannot be determined.
    """
    # Normalize for matching
    t = text

    # Look for explicit "LONG" or "SHORT" as standalone words first
    # (highest confidence).
    if re.search(r"\blong\b", t, re.IGNORECASE):
        # Make sure it's not "long-term" without an actual long signal
        if not re.search(r"\blong[\s-]*term\b", t, re.IGNORECASE):
            return "LONG"
        # If "long-term" is present, still check if "LONG" appears separately
        # e.g. "LONG signal (long-term hold)"
        stripped = re.sub(r"long[\s-]*term", "", t, flags=re.IGNORECASE)
        if re.search(r"\blong\b", stripped, re.IGNORECASE):
            return "LONG"

    if re.search(r"\bshort\b", t, re.IGNORECASE):
        # Exclude "short-term" false positives
        if not re.search(r"\bshort[\s-]*term\b", t, re.IGNORECASE):
            return "SHORT"
        stripped = re.sub(r"short[\s-]*term", "", t, flags=re.IGNORECASE)
        if re.search(r"\bshort\b", stripped, re.IGNORECASE):
            return "SHORT"

    # Fall back to broader keyword/emoji matching
    short_match = _SHORT_KEYWORDS.search(t)
    long_match = _LONG_KEYWORDS.search(t)

    if short_match and not long_match:
        return "SHORT"
    if long_match and not short_match:
        return "LONG"
    if long_match and short_match:
        # Both matched -- use position: whichever appears first is the signal.
        if long_match.start() < short_match.start():
            return "LONG"
        return "SHORT"

    return None


def extract_prices(text: str) -> dict:
    """
    Extract entry, TP, and SL prices from signal text.

    Returns:
        {
            "entry": float or 0.0,
            "entry_low": float or None,
            "entry_high": float or None,
            "tps": [float, ...],
            "sl": float or None,
        }
    """
    result: dict = {
        "entry": 0.0,
        "entry_low": None,
        "entry_high": None,
        "tps": [],
        "sl": None,
    }

    # Normalize emoji digits (1️⃣, 2️⃣, etc.) to regular digits with ")"
    # so they match the numbered list patterns.
    _EMOJI_DIGITS = {
        "\u0031\ufe0f\u20e3": "1) ",  # 1️⃣
        "\u0032\ufe0f\u20e3": "2) ",  # 2️⃣
        "\u0033\ufe0f\u20e3": "3) ",  # 3️⃣
        "\u0034\ufe0f\u20e3": "4) ",  # 4️⃣
        "\u0035\ufe0f\u20e3": "5) ",  # 5️⃣
        "\u0036\ufe0f\u20e3": "6) ",  # 6️⃣
        "\u0037\ufe0f\u20e3": "7) ",  # 7️⃣
        "\u0038\ufe0f\u20e3": "8) ",  # 8️⃣
        "\u0039\ufe0f\u20e3": "9) ",  # 9️⃣
        "🥇": "1) ",  # 🥇
        "🥈": "2) ",  # 🥈
        "🥉": "3) ",  # 🥉
    }
    for emoji, repl in _EMOJI_DIGITS.items():
        text = text.replace(emoji, repl)

    # --- Entry ---
    for pat in _ENTRY_PATTERNS:
        m = pat.search(text)
        if m:
            groups = m.groups()
            price1 = _parse_price(groups[0]) if groups[0] else 0.0
            price2 = _parse_price(groups[1]) if len(groups) > 1 and groups[1] else 0.0

            if price1 > 0 and price2 > 0:
                # Range: take midpoint as entry
                low, high = min(price1, price2), max(price1, price2)
                result["entry"] = round((low + high) / 2, 8)
                result["entry_low"] = low
                result["entry_high"] = high
            elif price1 > 0:
                result["entry"] = price1
            break

    # --- Take Profits ---
    collected_tps: dict[int, float] = {}  # index -> price

    # Pattern 1: individual TP lines (TP1, T1, Target 1, etc.)
    for m in _TP_PATTERNS[0].finditer(text):
        idx = int(m.group(1))
        price = _parse_price(m.group(2))
        if price > 0:
            collected_tps[idx] = price

    # Pattern 2: numbered list under "Targets:" header
    # e.g. "Targets :\n1) 87.57\n2) 89.38\n3) 91.20"
    if not collected_tps:
        m = _TP_PATTERNS[1].search(text)
        if m:
            block = m.group(1)
            # Extract numbered TP entries from the block. The first
            # character between the index and the price must be one
            # of ``)``, ``:``, ``-`` or whitespace — never a period.
            # Allowing ``.`` was reading bare-price lines like
            # ``0.39200`` as ``idx=0, price=39200`` and collapsing
            # every entry into ``collected_tps[0]`` (CoinAura PRL
            # incident 2026-04-28). Bare-price-per-line lists fall
            # through to Pattern 5 below, which is the right home
            # for them.
            for item in re.finditer(
                r"(\d+)[)\:\-\s][^\d\n\w]{0,3}([\d.]+)", block
            ):
                idx = int(item.group(1))
                price = _parse_price(item.group(2))
                if price > 0:
                    collected_tps[idx] = price

    # Pattern 3: parenthesised list - "TP (0.1950-0.1900-0.1850-0.1750)"
    # or "Take Profits: (0.07100, 0.06860, 0.06400)" — allow an
    # optional ":" / "=" between the header and the parenthesis.
    # Bare "targets?" is DELIBERATELY omitted from the alternative
    # list because "Entry Target: (<price>)" would otherwise match
    # here and steal the TP slot (UB/CHIP/Q regression 2026-04-24).
    if not collected_tps:
        pat = re.compile(
            r"(?:tps?|take[\s_-]*profits?(?:\s+targets?)?)\s*[:=]?\s*\(([^)]+)\)",
            re.IGNORECASE,
        )
        m = pat.search(text)
        if m:
            raw_list = m.group(1)
            parts = re.split(r"[/|,\-\s]+", raw_list.strip())
            for i, part in enumerate(parts, start=1):
                price = _parse_price(part.rstrip(".,"))
                if price > 0:
                    collected_tps[i] = price

    # Pattern 4: comma/slash/bullet-separated list on the header line
    # ("Targets: 3300/3400/3500" / "Targets: 0.0101• 0.0107• 0.0115").
    if not collected_tps:
        m = _TP_PATTERNS[2].search(text)
        if m:
            raw_list = m.group(1)
            # Split on / , | - • • or whitespace
            parts = re.split(r"[/|,\-\s•• ]+", raw_list.strip())
            for i, part in enumerate(parts, start=1):
                price = _parse_price(part.rstrip(".,"))
                if price > 0:
                    collected_tps[i] = price

    # Pattern 5: unlabeled TP list between Entry and Stop-Loss
    # (CoinAura format — price-only lines follow the Entry line,
    # no "TP:" / "Target:" labels). A TP line is a line whose only
    # content is a price, optionally preceded by a bullet/emoji or
    # "N)" / "N." index marker, and optionally followed by a
    # parenthetical comment such as "(Close 50%)" (CRYPTO WORLD
    # UPTADES format).
    if not collected_tps:
        # Anchor on the first line that contains "entry", "entries",
        # "buy range", or "buy zone" — so signals using "Buy Range:"
        # as their entry-zone header (BANANAS31 format) also benefit
        # from the positional fallback.
        entry_line_re = re.compile(
            r"(?im)^.*\b(?:entry|entries|buy[\s-]+(?:range|zone|area))\b.*$"
        )
        sl_line_re = re.compile(
            r"(?im)^.*\b(?:sl|stop[-\s]*loss|stoploss|invalidation)\b.*$"
        )
        # If a TARGETS / TP header sits between the Entry line and
        # the SL line, the actual TPs follow that header — the lines
        # before it are entry-zone prices and must NOT be captured
        # (CRYPTO WORLD UPTADES ZKJ regression: entry value 0.02850
        # was being captured as TP[1]).
        tp_header_re = re.compile(
            r"(?im)^.*\b(?:targets?|take[\s_-]*profits?|tps?)\b.*$"
        )
        em = entry_line_re.search(text)
        if em:
            sm = sl_line_re.search(text, em.end())
            end_pos = sm.start() if sm else len(text)
            block_start = em.end()
            tpm = tp_header_re.search(text, em.end(), end_pos)
            if tpm:
                block_start = tpm.end()
            block = text[block_start:end_pos]
            # Normalise bullet separators "•" "·" inside the block to
            # newlines so a single line like "0.0101• 0.0107• 0.0115"
            # (BANANAS31 format) becomes three separate price-only
            # lines that the per-line regex below can match.
            block = re.sub(r"[•·]+", "\n", block)
            # Ordinal prefix is optional. Accept "1)" / "1." / "1 :" / "1 -".
            # Require whitespace AFTER the separator so a decimal point
            # (e.g. "0.02580") is never mistaken for an ordinal. Limit
            # ordinal digits to 1-2 so 4-digit integer prices aren't
            # swallowed either. Allow an optional parenthetical
            # comment after the price ("(Close 50%)").
            tp_line_re = re.compile(
                r"^\s*\W*(?:\d{1,2}\s*[)\.\:\-]\s+)?"
                + _PRICE_RE
                + r"(?:\s*\([^)]*\))?\s*\W*$",
                re.MULTILINE,
            )
            idx = 0
            for m in tp_line_re.finditer(block):
                price = _parse_price(m.group(1))
                if price > 0:
                    idx += 1
                    collected_tps[idx] = price
                    if idx >= 10:
                        break

    # Sort by index and store
    if collected_tps:
        result["tps"] = [collected_tps[k] for k in sorted(collected_tps)]

    # --- Stop Loss ---
    for pat in _SL_PATTERNS:
        m = pat.search(text)
        if m:
            price = _parse_price(m.group(1))
            if price > 0:
                result["sl"] = price
            break

    return result


def _classify_signal_type(entry: float, sl: Optional[float]) -> str:
    """
    Classify signal based on SL presence + distance from entry.

    Client taxonomy (2026-04-24):
        "fixed"   - SL is missing -> bot uses auto-SL (-3%) and locks
                    leverage to x10. Applies ONLY to SL-missing signals.
        "swing"   - SL is present and > 4% from entry (long-timeframe
                    signal; leverage comes out on the lower end).
        "dynamic" - SL is present and <= 4% from entry (normal signal).
    """
    if sl is None or sl <= 0 or entry <= 0:
        return "fixed"

    distance_pct = abs(entry - sl) / entry * 100
    if distance_pct > 4.0:
        return "swing"
    return "dynamic"


def validate_signal(signal: ParsedSignal) -> tuple[bool, str]:
    """
    Validate a parsed signal for logical consistency.

    Returns:
        (is_valid, reason) - reason is empty string if valid,
        otherwise a human-readable explanation of what's wrong.
    """
    if not signal.symbol:
        return False, "Missing symbol"

    if signal.direction not in ("LONG", "SHORT"):
        return False, f"Invalid direction: {signal.direction!r}"

    if signal.entry <= 0:
        return False, f"Entry price must be > 0, got {signal.entry}"

    if not signal.tps:
        return False, "No take-profit targets found"

    # Direction consistency checks
    if signal.direction == "LONG":
        # At least one TP must be above entry
        if not any(tp > signal.entry for tp in signal.tps):
            return False, (
                f"LONG signal but no TP above entry ({signal.entry}). "
                f"TPs: {signal.tps}"
            )
        if signal.sl is not None and signal.sl >= signal.entry:
            return False, (
                f"LONG signal but SL ({signal.sl}) >= entry ({signal.entry})"
            )

    elif signal.direction == "SHORT":
        # At least one TP must be below entry
        if not any(tp < signal.entry for tp in signal.tps):
            return False, (
                f"SHORT signal but no TP below entry ({signal.entry}). "
                f"TPs: {signal.tps}"
            )
        if signal.sl is not None and signal.sl <= signal.entry:
            return False, (
                f"SHORT signal but SL ({signal.sl}) <= entry ({signal.entry})"
            )

    # Sanity check: SL implausibly far from entry (>50%) — defence-in-depth
    # against parser errors that capture an unrelated number as the SL price.
    # CRYPTO BANANA BOT EDU 2026-04-28: "Stop Loss: 5-10%" produced sl=5.0
    # on a 0.046 entry (10752% away). The SL regex now rejects %-suffixed
    # captures, but this check ensures any future regex slip doesn't open
    # a position with effectively no protection on Bybit.
    if signal.sl is not None and signal.entry > 0:
        sl_distance_pct = abs(signal.entry - signal.sl) / signal.entry * 100
        if sl_distance_pct > 50.0:
            return False, (
                f"SL ({signal.sl}) implausibly far from entry "
                f"({signal.entry}): {sl_distance_pct:.1f}%"
            )

    return True, ""


def _extract_symbol(text: str) -> Optional[str]:
    """
    Try every symbol pattern in priority order and return the first
    plausible match, normalized to XXXUSDT.
    """
    for pat_idx, pat in enumerate(_SYMBOL_PATTERNS):
        m = pat.search(text)
        if m:
            raw = m.group(1)
            upper = raw.upper()
            # High-confidence patterns — the "USDT" suffix (0,1,2,4,5)
            # or explicit "#"-prefix hash-ticker fallback (6) makes
            # even 1-char tickers like "Q/USDT" or "#Q" unambiguous.
            # Skip the short-name filter for those. Index 7 (bare
            # line-start) is the low-confidence last-resort.
            high_confidence_indices = {0, 1, 2, 4, 5, 6}
            if pat_idx not in high_confidence_indices:
                # Skip very short matches that are likely false positives
                # (e.g. "TP", "SL", "T1") unless they are known bases.
                if len(upper) <= 2 and upper not in _KNOWN_BASES:
                    continue
            # Skip tokens that are obviously not symbols
            if upper in (
                "TP", "SL", "BE", "PNL", "ROI", "USD", "BUY", "SELL",
                "LONG", "SHORT", "ENTRY", "STOP", "TARGET", "TAKE",
                "PROFIT", "LOSS", "ZONE", "AREA", "PRICE", "COIN",
                "PAIR", "SIGNAL", "ALERT", "UPDATE", "NEW", "FREE",
                "VIP", "CALL", "CALLS",
                # Signal-metadata words that often appear at line start
                # and would otherwise false-match the generic fallback.
                "LEVERAGE", "ORDERS", "ORDER", "TYPE", "SETUP",
                "STRATEGY", "ACCURACY", "CROSS", "ISOLATED", "TREND",
                "TRENDLINE", "SCALPING", "SWING", "DYNAMIC", "FIXED",
                "HIGH", "LOW", "MID", "TERM", "OPPORTUNITY", "STRONG",
                "RISK", "MANAGE", "MANUAL", "MANUALLY",
            ):
                continue
            return normalize_symbol(raw)
    return None


def parse_signal_detailed(
    text: str,
    channel_id: int = 0,
    channel_name: str = "",
) -> ParseResult:
    """
    Parse a raw Telegram message and return a detailed result.

    Unlike :func:`parse_signal` (which collapses every failure to ``None``),
    this returns a :class:`ParseResult` whose ``reason`` field tells the
    caller WHY parsing failed and exposes whichever fields were already
    extracted. Callers can use that to format meaningful "BLOCKED" Telegram
    notifications (client request 2026-04-28: notify on missing entry and
    invalid TPs).
    """
    if not text or not text.strip():
        return ParseResult(reason="empty",
                           channel_id=channel_id,
                           channel_name=channel_name)

    # Strip zero-width characters and normalize whitespace slightly.
    # Also remove Markdown bold markers (**) — some channels (Coin
    # Bearu, Crypto Beast, CoinAura) post messages with literal '**'
    # characters that confuse the symbol regex. Same for inline-code
    # backticks (`) used by AI Felix Crypto and similar to wrap prices
    # like "Entry: `449.33`" — the backticks confuse the entry regex.
    # Stripping them BEFORE parsing lets the existing patterns handle
    # the content cleanly. Markdown italics '__' are left alone; they
    # are rare in signals and removing them risks dropping legitimate
    # characters.
    clean = text.replace("**", "").replace("`", "").strip()

    # --- Symbol ---
    symbol = _extract_symbol(clean)
    # Post-process: some channels send messages where SHORT/LONG is
    # concatenated to the ticker with no separator — e.g. Coin Bearu's
    # "SHORTBAS**/USDT" becomes "SHORTBAS/USDT" after the strip above,
    # and the regex captures "SHORTBAS" as the ticker. Detect that
    # and strip the SHORT/LONG prefix so BAS/BSB/etc. resolve.
    if symbol:
        upper = symbol.upper()
        for prefix in ("SHORT", "LONG"):
            if upper.startswith(prefix) and len(upper) > len(prefix) + 4:
                # +4 because USDT is appended; rest must be a real ticker
                stripped = upper[len(prefix):]
                # Re-strip USDT, validate the remainder, re-append.
                if stripped.endswith("USDT"):
                    base = stripped[:-4]
                else:
                    base = stripped
                if 2 <= len(base) <= 15 and base.isalnum():
                    symbol = base + "USDT"
                    log.info(
                        "signal_parse.symbol_prefix_stripped",
                        original=upper,
                        stripped_prefix=prefix,
                        result=symbol,
                        channel_name=channel_name,
                    )
                break
    if not symbol:
        log.debug(
            "signal_parse_no_symbol",
            channel_id=channel_id,
            channel_name=channel_name,
            text_snippet=clean[:120],
        )
        return ParseResult(reason="no_symbol",
                           channel_id=channel_id,
                           channel_name=channel_name)

    # --- Operator overrides (Tomas 2026-05-03) ---
    # symbol_overrides.toml lets the operator skip non-tradable
    # symbols, rename them to Bybit's canonical form, or attach a
    # note that the notifier will surface on warnings. Lookup is
    # case-insensitive and module-cached.
    from core.symbol_overrides import get_override
    override = get_override(symbol)
    override_note = (override or {}).get("note")
    if override and override.get("skip"):
        log.info(
            "signal_parse.skipped_by_operator_override",
            symbol=symbol,
            note=override_note,
            channel_name=channel_name,
        )
        return ParseResult(
            reason="skipped_by_override",
            symbol=symbol,
            channel_id=channel_id,
            channel_name=channel_name,
            override_note=override_note,
        )
    if override and override.get("rename_to"):
        renamed = override["rename_to"]
        log.info(
            "signal_parse.symbol_renamed_by_operator",
            original=symbol,
            renamed=renamed,
            note=override_note,
            channel_name=channel_name,
        )
        symbol = renamed

    # --- Direction ---
    direction = extract_direction(clean)
    if not direction:
        log.debug(
            "signal_parse_no_direction",
            symbol=symbol,
            channel_id=channel_id,
            channel_name=channel_name,
            text_snippet=clean[:120],
        )
        return ParseResult(reason="no_direction",
                           symbol=symbol,
                           channel_id=channel_id,
                           channel_name=channel_name)

    # --- Prices ---
    prices = extract_prices(clean)
    entry = prices["entry"]
    tps = prices["tps"]
    sl = prices["sl"]

    if entry <= 0:
        log.debug(
            "signal_parse_no_entry",
            symbol=symbol,
            direction=direction,
            channel_id=channel_id,
            channel_name=channel_name,
            text_snippet=clean[:120],
        )
        # Populate sl/tps so the caller can distinguish "real signal
        # missing entry" from "news/article that happens to contain a
        # ticker + direction word". A real signal always has at least
        # one of entry/SL/TP — when none of them are present, the
        # message is non-signal chatter and the rejection should stay
        # silent (avoids spamming "Entre saknas" for news headlines
        # like "The Market Is Correcting" or "Wasabi Protocol Hacked",
        # 2026-04-30 Tomas report).
        return ParseResult(reason="no_entry",
                           symbol=symbol,
                           direction=direction,
                           tps=tps,
                           sl=sl,
                           channel_id=channel_id,
                           channel_name=channel_name,
                           override_note=override_note)

    if not tps:
        log.debug(
            "signal_parse_no_tps",
            symbol=symbol,
            direction=direction,
            entry=entry,
            channel_id=channel_id,
            channel_name=channel_name,
            text_snippet=clean[:120],
        )
        return ParseResult(reason="no_tps",
                           symbol=symbol,
                           direction=direction,
                           entry=entry,
                           sl=sl,
                           channel_id=channel_id,
                           channel_name=channel_name,
                           override_note=override_note)

    # --- Build signal ---
    signal = ParsedSignal(
        symbol=symbol,
        direction=direction,
        entry=entry,
        entry_low=prices.get("entry_low"),
        entry_high=prices.get("entry_high"),
        tps=tps,
        sl=sl,
        signal_type=_classify_signal_type(entry, sl),
        channel_id=channel_id,
        channel_name=channel_name,
        raw_text=clean,
        parsed_at=time.time(),
    )

    # --- Validate ---
    valid, reason = validate_signal(signal)
    if not valid:
        log.warning(
            "signal_parse_invalid",
            symbol=symbol,
            direction=direction,
            entry=entry,
            tps=tps,
            sl=sl,
            reason=reason,
            channel_id=channel_id,
            channel_name=channel_name,
        )
        return ParseResult(reason="invalid",
                           detail=reason,
                           symbol=symbol,
                           direction=direction,
                           entry=entry,
                           tps=tps,
                           sl=sl,
                           channel_id=channel_id,
                           channel_name=channel_name,
                           override_note=override_note)

    log.info(
        "signal_parsed",
        symbol=signal.symbol,
        direction=signal.direction,
        entry=signal.entry,
        tps=signal.tps,
        sl=signal.sl,
        signal_type=signal.signal_type,
        channel_id=channel_id,
        channel_name=channel_name,
    )

    return ParseResult(signal=signal,
                       reason="ok",
                       symbol=symbol,
                       direction=direction,
                       entry=entry,
                       tps=tps,
                       sl=sl,
                       channel_id=channel_id,
                       channel_name=channel_name,
                       override_note=override_note)


def parse_signal(
    text: str,
    channel_id: int = 0,
    channel_name: str = "",
) -> Optional[ParsedSignal]:
    """
    Parse a raw Telegram message into a ParsedSignal.

    Returns None if the message cannot be parsed into a valid signal.
    This is a backwards-compatible thin wrapper around
    :func:`parse_signal_detailed` for callers that don't need the
    rejection-reason information.
    """
    return parse_signal_detailed(text, channel_id, channel_name).signal
