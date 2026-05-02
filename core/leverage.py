"""
Stratos1 - Dynamic leverage calculator.

The leverage for each trade is computed from the stop-loss distance
relative to the theoretical wallet risk budget:

    stop_distance_pct = |entry - SL| / entry      (always positive)
    leverage = (wallet * risk_pct) / (initial_margin * stop_distance_pct)

The raw value is then bucketed:

    < 6.0         -> FIXED at min (6.0)
    6.0  .. 6.75  -> FIXED at min (6.0)
    6.75 .. 7.5   -> DYNAMIC, but floored to neutral_zone_upper (7.5)
    7.5  .. 25.0  -> DYNAMIC, use the computed value
    > 25.0        -> DYNAMIC, capped at max_entry_leverage (25.0)

All thresholds are configurable via ``LeverageSettings``.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from config.settings import LeverageSettings, WalletSettings


def _round_half_up(value: float, ndigits: int = 2) -> float:
    """Half-up rounding (e.g. 7.345 -> 7.35).

    Python's built-in :func:`round` uses banker's rounding (half-to-even)
    which can produce results that diverge from the client's spec at the
    .x5 boundary. Client 2026-04-30 explicitly required half-up rounding
    for the dynamic leverage zone.
    """
    quant = Decimal(10) ** -ndigits
    return float(
        Decimal(str(value)).quantize(quant, rounding=ROUND_HALF_UP)
    )

# Default values matching config.toml (used when no settings object given).
_DEFAULT_WALLET = 402.1
_DEFAULT_RISK_PCT = 0.02
_DEFAULT_INITIAL_MARGIN = 20.0
_DEFAULT_MIN_LEV = 6.0
_DEFAULT_SPLIT = 6.75
_DEFAULT_UPPER = 7.5
_DEFAULT_MAX_ENTRY = 25.0


def compute_raw_leverage(
    entry: float,
    sl: float,
    wallet: float = _DEFAULT_WALLET,
    risk_pct: float = _DEFAULT_RISK_PCT,
    initial_margin: float = _DEFAULT_INITIAL_MARGIN,
    settings: Optional[tuple] = None,
) -> float:
    """Return the unbucketed raw leverage from the risk formula.

    Useful for classifying signal_type (swing vs dynamic) per the
    client 2026-05-02 consolidated rule:
      raw < 6.75   -> SWING
      raw >= 6.75  -> DYNAMIC

    The actual leverage applied to the trade still goes through
    :func:`calculate_leverage`, which buckets / floors / caps the
    raw value to the configured limits.
    """
    if settings is not None:
        wallet_cfg, _ = settings
        wallet = wallet_cfg.bot_wallet
        risk_pct = wallet_cfg.risk_pct
        initial_margin = wallet_cfg.initial_margin

    if entry == 0:
        raise ValueError("Entry price must not be zero.")
    stop_distance_pct = abs(entry - sl) / entry
    if stop_distance_pct == 0:
        raise ValueError("Stop distance is zero (entry == SL).")
    return (wallet * risk_pct) / (initial_margin * stop_distance_pct)


def calculate_leverage(
    entry: float,
    sl: float,
    wallet: float = _DEFAULT_WALLET,
    risk_pct: float = _DEFAULT_RISK_PCT,
    initial_margin: float = _DEFAULT_INITIAL_MARGIN,
    settings: Optional[tuple] = None,   # (WalletSettings, LeverageSettings) or None
) -> float:
    """
    Calculate the effective leverage for a trade.

    Parameters
    ----------
    entry:
        Entry price of the position.
    sl:
        Stop-loss price.
    wallet:
        Theoretical wallet value (USDT).  Ignored when *settings* is provided.
    risk_pct:
        Risk fraction per trade.  Ignored when *settings* is provided.
    initial_margin:
        Margin allocated to the position (USDT).  Ignored when *settings*
        is provided.
    settings:
        Optional ``(WalletSettings, LeverageSettings)`` tuple.  When given,
        all wallet/leverage parameters are read from the settings objects
        instead of the individual keyword arguments.

    Returns
    -------
    float
        The leverage to apply, rounded to two decimal places.

    Raises
    ------
    ValueError
        If *entry* is zero or *entry* equals *sl* (zero stop distance).
    """
    # Unpack settings if provided.
    if settings is not None:
        wallet_cfg, lev_cfg = settings
        wallet = wallet_cfg.bot_wallet
        risk_pct = wallet_cfg.risk_pct
        initial_margin = wallet_cfg.initial_margin
        min_lev = lev_cfg.min_leverage
        split = lev_cfg.neutral_zone_split
        upper = lev_cfg.neutral_zone_upper
        max_entry = lev_cfg.max_entry_leverage
    else:
        min_lev = _DEFAULT_MIN_LEV
        split = _DEFAULT_SPLIT
        upper = _DEFAULT_UPPER
        max_entry = _DEFAULT_MAX_ENTRY

    if entry == 0:
        raise ValueError("Entry price must not be zero.")

    stop_distance_pct = abs(entry - sl) / entry

    if stop_distance_pct == 0:
        raise ValueError("Stop distance is zero (entry == SL).")

    raw = (wallet * risk_pct) / (initial_margin * stop_distance_pct)

    # --- Bucketing rules (client 2026-04-30 explicit spec) ---
    # All boundaries use half-up rounding via _round_half_up.
    if raw < split:
        # raw < 6.75  (covers raw < min_lev too) -> use min_lev (x6).
        return _round_half_up(min_lev, 2)

    if raw < upper:
        # 6.75 <= raw < 7.5 -> use neutral_zone_upper (x7.5).
        return _round_half_up(upper, 2)

    if raw > max_entry:
        # raw > 25.0 -> cap at max_entry_leverage (x25).
        return _round_half_up(max_entry, 2)

    # 7.5 <= raw <= 25.0 -> use computed value directly, half-up to 2 dp.
    return _round_half_up(raw, 2)


def classify_leverage(leverage: float) -> str:
    """
    Classify a leverage value as ``"FIXED"`` or ``"DYNAMIC"``.

    Fixed leverage is any value that was clamped to the minimum (6.0 by
    default).  Everything else is dynamic.

    Parameters
    ----------
    leverage:
        The effective leverage returned by :func:`calculate_leverage`.

    Returns
    -------
    str
        ``"FIXED"`` if the leverage equals the minimum (6.0), otherwise
        ``"DYNAMIC"``.
    """
    if leverage <= _DEFAULT_MIN_LEV:
        return "FIXED"
    return "DYNAMIC"


def classify_leverage_with_settings(
    leverage: float,
    min_leverage: float,
) -> str:
    """
    Settings-aware variant of :func:`classify_leverage`.

    Parameters
    ----------
    leverage:
        The effective leverage.
    min_leverage:
        The configured minimum leverage (from ``LeverageSettings``).

    Returns
    -------
    str
        ``"FIXED"`` or ``"DYNAMIC"``.
    """
    if leverage <= min_leverage:
        return "FIXED"
    return "DYNAMIC"
