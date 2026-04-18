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
    # "ENTRY PRICE: 0.2050" - prioritise before numbered list to avoid 0. trap
    re.compile(
        r"entry\s+price\s*[:=@]?\s*" + _PRICE_RE
        + r"(?:" + _RANGE_SEP + _PRICE_RE + r")?",
        re.IGNORECASE,
    ),
    # "ENTRY1: 0.4241" / "ENTRY 1: ..."
    re.compile(
        r"entry\s*\d+\s*[:=@]\s*" + _PRICE_RE
        + r"(?:" + _RANGE_SEP + _PRICE_RE + r")?",
        re.IGNORECASE,
    ),
    # "Entry Targets:\n0.4101" - Hassan Crypto format (price on next line)
    re.compile(
        r"entry\s+targets?\s*[:=]?\s*\n\s*" + _PRICE_RE
        + r"(?:\s*\n\s*" + _PRICE_RE + r")?",
        re.IGNORECASE,
    ),
    # Numbered list under "Entry:" header e.g. "Entry :\n1) 87.0700\n2) 84.4579"
    re.compile(
        r"entry\s*(?:zone|orders?)?\s*[:=]?"
        r"\s*\n\s*\d+[\)\.]\s+" + _PRICE_RE
        + r"(?:\s*\n\s*\d+[\)\.]\s+" + _PRICE_RE + r")?",
        re.IGNORECASE,
    ),
    # "Entry: 65000" / "Entry Zone: 3200-3250" / "Buy: 0.152"
    re.compile(
        r"(?:entry\s*(?:zone|price|area|orders?)?|buy(?:\s*(?:zone|price|area|at|around|@))?|"
        r"open|limit|market\s*(?:buy|entry))\s*[:=@]\s*"
        + _PRICE_RE + r"(?:" + _RANGE_SEP + _PRICE_RE + r")?",
        re.IGNORECASE,
    ),
    # Standalone "Entry 65000" without colon
    re.compile(
        r"entry\s+" + _PRICE_RE + r"(?:" + _RANGE_SEP + _PRICE_RE + r")?",
        re.IGNORECASE,
    ),
]

# TP patterns - match TP1/T1/Target 1/Take Profit 1 etc.
_TP_PATTERNS = [
    # "TP1: 66000" / "T1: 66000" / "Target 1: 66000"
    re.compile(
        r"(?:tp|t|target|take[\s_-]*profit)\s*(\d+)\s*[:=]?\s*" + _PRICE_RE,
        re.IGNORECASE,
    ),
    # Numbered list under "Targets :" header e.g. "Targets :\n1) 87.57\n2) 89.38"
    # List markers must be "N)" or "N." followed by SPACE - not matching "0." in prices.
    re.compile(
        r"(?:targets?|take[\s_-]*profits?)\s*[:=]?"
        r"\s*\n\s*((?:\d+[\)\.]\s+[\d.]+\s*\n?\s*){1,8})",
        re.IGNORECASE,
    ),
    # "Targets: 3300/3400/3500" or "Targets: 3300, 3400, 3500" (comma/slash separated)
    re.compile(
        r"(?:targets?|tps?|take[\s_-]*profits?)\s*[:=]?\s*"
        r"([\d.,\s/|\-]+)",
        re.IGNORECASE,
    ),
]

# SL patterns
_SL_PATTERNS = [
    re.compile(
        r"(?:sl|stop\s*loss|stop|stoploss|invalidation)\s*[:=]?\s*" + _PRICE_RE,
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
_SYMBOL_PATTERNS = [
    # "#BTCUSDT" / "BTCUSDT" / "$BTCUSDT"
    re.compile(r"[#$]?\b([A-Z0-9]{2,20}USDT)\b", re.IGNORECASE),
    # "BTC/USDT" / "SOL/USDT" / "BTC-USDT"
    re.compile(r"\b([A-Z0-9]{2,15})\s*[/\-]\s*USDT\b", re.IGNORECASE),
    # "Coin: SOL/USDT" / "Pair: BTC/USDT"
    re.compile(r"(?:coin|pair|symbol|ticker)\s*[:=]\s*[#$]?([A-Z0-9]{2,15})\s*[/\-]?\s*USDT\b", re.IGNORECASE),
    # "Coin: SOL" (no USDT suffix)
    re.compile(r"(?:coin|pair|symbol|ticker)\s*[:=]\s*[#$]?([A-Z0-9]{2,15})\b", re.IGNORECASE),
    # Standalone symbol at start of line or after emoji: "BTCUSDT" / "#SOL"
    re.compile(r"(?:^|[\n\r])\s*[#$🔥🟢🔴📈📉⚡️💰✅❌]*\s*([A-Z0-9]{2,15}USDT)\b", re.IGNORECASE),
    re.compile(r"(?:^|[\n\r])\s*[#$🔥🟢🔴📈📉⚡️💰✅❌]*\s*([A-Z0-9]{2,10})\b", re.IGNORECASE),
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
        "\u0031\ufe0f\u20e3": "1)",  # 1️⃣
        "\u0032\ufe0f\u20e3": "2)",  # 2️⃣
        "\u0033\ufe0f\u20e3": "3)",  # 3️⃣
        "\u0034\ufe0f\u20e3": "4)",  # 4️⃣
        "\u0035\ufe0f\u20e3": "5)",  # 5️⃣
        "\u0036\ufe0f\u20e3": "6)",  # 6️⃣
        "\u0037\ufe0f\u20e3": "7)",  # 7️⃣
        "\u0038\ufe0f\u20e3": "8)",  # 8️⃣
        "\u0039\ufe0f\u20e3": "9)",  # 9️⃣
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
            # Extract "N) price" or "N. price" pairs from the block
            for item in re.finditer(
                r"(\d+)[\)\.]\s*([\d.]+)", block
            ):
                idx = int(item.group(1))
                price = _parse_price(item.group(2))
                if price > 0:
                    collected_tps[idx] = price

    # Pattern 3: parenthesised list - "TP (0.1950-0.1900-0.1850-0.1750)"
    if not collected_tps:
        pat = re.compile(
            r"(?:tps?|targets?|take[\s_-]*profits?)\s*\(([^)]+)\)",
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

    # Pattern 4: comma/slash-separated list ("Targets: 3300/3400/3500")
    if not collected_tps:
        m = _TP_PATTERNS[2].search(text)
        if m:
            raw_list = m.group(1)
            # Split on / , | - or whitespace
            parts = re.split(r"[/|,\-\s]+", raw_list.strip())
            for i, part in enumerate(parts, start=1):
                price = _parse_price(part.rstrip(".,"))
                if price > 0:
                    collected_tps[i] = price

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
    Classify signal based on SL distance from entry.

    Returns:
        "swing"   - SL > 4% from entry
        "dynamic" - SL 2-4% from entry (or no SL)
        "fixed"   - SL < 2% from entry
    """
    if sl is None or sl <= 0 or entry <= 0:
        return "dynamic"

    distance_pct = abs(entry - sl) / entry * 100

    if distance_pct > 4.0:
        return "swing"
    elif distance_pct >= 2.0:
        return "dynamic"
    else:
        return "fixed"


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

    return True, ""


def _extract_symbol(text: str) -> Optional[str]:
    """
    Try every symbol pattern in priority order and return the first
    plausible match, normalized to XXXUSDT.
    """
    for pat in _SYMBOL_PATTERNS:
        m = pat.search(text)
        if m:
            raw = m.group(1)
            # Skip very short matches that are likely false positives
            # (e.g. "TP", "SL", "T1") unless they are known bases.
            upper = raw.upper()
            if len(upper) <= 2 and upper not in _KNOWN_BASES:
                continue
            # Skip tokens that are obviously not symbols
            if upper in (
                "TP", "SL", "BE", "PNL", "ROI", "USD", "BUY", "SELL",
                "LONG", "SHORT", "ENTRY", "STOP", "TARGET", "TAKE",
                "PROFIT", "LOSS", "ZONE", "AREA", "PRICE", "COIN",
                "PAIR", "SIGNAL", "ALERT", "UPDATE", "NEW", "FREE",
                "VIP", "CALL", "CALLS",
            ):
                continue
            return normalize_symbol(raw)
    return None


def parse_signal(
    text: str,
    channel_id: int = 0,
    channel_name: str = "",
) -> Optional[ParsedSignal]:
    """
    Parse a raw Telegram message into a ParsedSignal.

    Returns None if the message cannot be parsed into a valid signal.
    Logs structured warnings/debug info on failure.

    Args:
        text:         Raw message text (may include emojis, markdown, etc.)
        channel_id:   Telegram chat ID the message came from
        channel_name: Human-readable channel name for logging/reporting
    """
    if not text or not text.strip():
        return None

    # Strip zero-width characters and normalize whitespace slightly
    clean = text.strip()

    # --- Symbol ---
    symbol = _extract_symbol(clean)
    if not symbol:
        log.debug(
            "signal_parse_no_symbol",
            channel_id=channel_id,
            channel_name=channel_name,
            text_snippet=clean[:120],
        )
        return None

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
        return None

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
        return None

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
        return None

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
        return None

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

    return signal
