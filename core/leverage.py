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

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from config.settings import LeverageSettings, WalletSettings

# Default values matching config.toml (used when no settings object given).
_DEFAULT_WALLET = 402.1
_DEFAULT_RISK_PCT = 0.02
_DEFAULT_INITIAL_MARGIN = 20.0
_DEFAULT_MIN_LEV = 6.0
_DEFAULT_SPLIT = 6.75
_DEFAULT_UPPER = 7.5
_DEFAULT_MAX_ENTRY = 25.0


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

    # --- Bucketing rules ---
    if raw < min_lev:
        # Below absolute minimum -> clamp to min.
        return round(min_lev, 2)

    if raw < split:
        # 6.0 .. 6.75 -> fixed at min.
        return round(min_lev, 2)

    if raw < upper:
        # 6.75 .. 7.5 (neutral zone) -> dynamic, but floor to upper bound.
        return round(upper, 2)

    if raw > max_entry:
        # Above max entry leverage -> cap.
        return round(max_entry, 2)

    # 7.5 .. 25.0 -> use computed value directly.
    return round(raw, 2)


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
