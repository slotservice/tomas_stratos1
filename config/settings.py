"""
Stratos1 Configuration - Pydantic v2 models.

Loads secrets from .env (via pydantic-settings) and all other settings
from config.toml.  Telegram source groups come from a separate
telegram_groups.toml whose path is declared inside config.toml.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Use tomllib on 3.11+, otherwise the backport.
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomllib
    except ModuleNotFoundError:          # pragma: no cover
        import tomli as tomllib          # type: ignore[no-redef]

# ---------------------------------------------------------------------------
# Project root (directory that contains config.toml)
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ===================================================================
# Secrets loaded from .env  (env-prefix mapping)
# ===================================================================

class TelegramSettings(BaseSettings):
    """Telegram credentials - loaded from environment / .env file."""
    model_config = SettingsConfigDict(
        env_prefix="TG_",
        env_file=str(_PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_id: str = Field(..., description="Telegram API ID (from my.telegram.org)")
    api_hash: str = Field(..., description="Telegram API hash")
    phone: str = Field(..., description="Phone number linked to the Telegram account")
    bot_token: str = Field(..., description="Bot token for sending notifications")
    notify_channel_id: int = Field(
        ..., description="Telegram channel/chat ID for bot reports"
    )


class BybitSettings(BaseSettings):
    """Bybit API credentials - loaded from environment / .env file."""
    model_config = SettingsConfigDict(
        env_prefix="BYBIT_",
        env_file=str(_PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_key: str = Field(..., description="Bybit API key")
    api_secret: str = Field(..., description="Bybit API secret")
    demo: bool = Field(True, description="Use Bybit demo/testnet if True")


# ===================================================================
# Settings loaded from config.toml
# ===================================================================

class WalletSettings(BaseModel):
    """Internal theoretical wallet used for position-sizing calculations."""
    bot_wallet: float = 402.1
    risk_pct: float = 0.02
    initial_margin: float = 20.0


class LeverageSettings(BaseModel):
    """Dynamic leverage thresholds and limits."""
    min_leverage: float = Field(6.0, alias="min_leverage")
    neutral_zone_split: float = 6.75
    neutral_zone_upper: float = 7.5
    max_entry_leverage: float = 25.0
    scaling_leverage: float = 50.0


class EntrySettings(BaseModel):
    """Entry order configuration."""
    order_type: str = "Market"
    num_entry_orders: int = 2
    slippage_tolerance_pct: float = 0.5
    entry_timeout_seconds: int = 30


class BreakevenSettings(BaseModel):
    """Breakeven stop-loss activation rules."""
    trigger_pct: float = 2.3
    buffer_pct: float = 0.15


class ScalingStep(BaseModel):
    """A single position-scaling step."""
    trigger_pct: float
    add_margin: Optional[float] = None
    set_leverage: Optional[float] = None


class ScalingSettings(BaseModel):
    """Position scaling / DCA configuration."""
    enabled: bool = True
    steps: List[ScalingStep] = Field(default_factory=lambda: [
        ScalingStep(trigger_pct=2.4, add_margin=20.0, set_leverage=50.0),
        ScalingStep(trigger_pct=4.0, add_margin=20.0),
        ScalingStep(trigger_pct=6.0, add_margin=20.0),
        ScalingStep(trigger_pct=8.0, add_margin=20.0),
    ])


class TrailingStopSettings(BaseModel):
    """Trailing stop configuration."""
    activation_pct: float = 6.1
    trailing_distance_pct: float = 2.5
    trigger_type: str = "MarkPrice"


class HedgeSettings(BaseModel):
    """Hedge trade configuration."""
    enabled: bool = True
    trigger_pct: float = -2.0
    max_hedge_count: int = 1
    max_combined_loss_usdt: float = 4.0


class ReentrySettings(BaseModel):
    """Re-entry configuration after a stopped-out trade."""
    enabled: bool = True
    max_reentries: int = 2


class DuplicateSettings(BaseModel):
    """Duplicate signal detection parameters."""
    threshold_pct: float = 5.0
    lookback_hours: int = 24


class StaleSignalSettings(BaseModel):
    """Reject signals that are too old."""
    max_age_seconds: int = 120


class TimeoutSettings(BaseModel):
    """Order timeout / cleanup rules."""
    unfilled_order_hours: int = 24


class CapacitySettings(BaseModel):
    """Bot capacity and order-loop safety limits."""
    max_active_trades: int = 500
    order_loop_window_seconds: int = 60
    order_loop_max_count: int = 10


class AutoSLSettings(BaseModel):
    """Auto stop-loss fallback when signal has no SL."""
    fallback_pct: float = 3.0
    fallback_leverage: float = 10.0


class ReportingSettings(BaseModel):
    """Scheduled reporting times."""
    daily_report_hour: int = 23
    weekly_report_day: int = 0   # 0 = Monday
    weekly_report_hour: int = 23


class TpSlSettings(BaseModel):
    """TP/SL trigger type shared across the bot."""
    trigger_type: str = "MarkPrice"


class GeneralSettings(BaseModel):
    """General / global settings."""
    timezone: str = "Europe/Stockholm"
    log_level: str = "INFO"
    log_file: str = "stratos1.log"
    db_path: str = "stratos1.db"


# ===================================================================
# Telegram group model (from telegram_groups.toml)
# ===================================================================

class TelegramGroup(BaseModel):
    """A single monitored Telegram source group."""
    id: int
    name: str


# ===================================================================
# Top-level application settings container
# ===================================================================

class AppSettings(BaseModel):
    """Root container that aggregates every settings section."""
    general: GeneralSettings = Field(default_factory=GeneralSettings)
    telegram: TelegramSettings
    bybit: BybitSettings
    wallet: WalletSettings = Field(default_factory=WalletSettings)
    leverage: LeverageSettings = Field(default_factory=LeverageSettings)
    entry: EntrySettings = Field(default_factory=EntrySettings)
    breakeven: BreakevenSettings = Field(default_factory=BreakevenSettings)
    scaling: ScalingSettings = Field(default_factory=ScalingSettings)
    trailing_stop: TrailingStopSettings = Field(default_factory=TrailingStopSettings)
    hedge: HedgeSettings = Field(default_factory=HedgeSettings)
    reentry: ReentrySettings = Field(default_factory=ReentrySettings)
    duplicate: DuplicateSettings = Field(default_factory=DuplicateSettings)
    stale_signal: StaleSignalSettings = Field(default_factory=StaleSignalSettings)
    timeout: TimeoutSettings = Field(default_factory=TimeoutSettings)
    capacity: CapacitySettings = Field(default_factory=CapacitySettings)
    auto_sl: AutoSLSettings = Field(default_factory=AutoSLSettings)
    reporting: ReportingSettings = Field(default_factory=ReportingSettings)
    tp_sl: TpSlSettings = Field(default_factory=TpSlSettings)
    telegram_groups: List[TelegramGroup] = Field(default_factory=list)


# ===================================================================
# Loader helpers
# ===================================================================

def _read_toml(path: Path) -> dict:
    """Read and parse a TOML file, returning an empty dict if missing."""
    if not path.is_file():
        return {}
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def _load_telegram_groups(
    config_data: dict,
    project_root: Path,
) -> List[TelegramGroup]:
    """
    Load monitored Telegram groups from the file referenced in config.toml
    (``[telegram].source_groups_file``).  Falls back to ``telegram_groups.toml``
    in the project root.
    """
    tg_section = config_data.get("telegram", {})
    groups_file = tg_section.get("source_groups_file", "telegram_groups.toml")
    groups_path = project_root / groups_file

    groups_data = _read_toml(groups_path)
    raw_groups = groups_data.get("groups", [])
    return [TelegramGroup(**g) for g in raw_groups]


def load_settings(
    project_root: Path | str | None = None,
) -> AppSettings:
    """
    Build the complete ``AppSettings`` object.

    1. Reads secrets from ``.env`` via pydantic-settings (TelegramSettings,
       BybitSettings).
    2. Reads everything else from ``config.toml``.
    3. Reads Telegram source groups from ``telegram_groups.toml``.

    Parameters
    ----------
    project_root:
        Override the auto-detected project root.  Useful for testing.

    Returns
    -------
    AppSettings
        Fully validated application configuration.
    """
    root = Path(project_root) if project_root else _PROJECT_ROOT
    config_path = root / "config.toml"
    config_data = _read_toml(config_path)

    # --- Secrets (env / .env) ---
    telegram = TelegramSettings()
    bybit = BybitSettings()

    # --- TOML sections ---
    general = GeneralSettings(**config_data.get("general", {}))
    wallet = WalletSettings(**config_data.get("wallet", {}))
    leverage = LeverageSettings(**config_data.get("leverage", {}))
    entry = EntrySettings(**config_data.get("entry", {}))
    breakeven = BreakevenSettings(**config_data.get("breakeven", {}))
    scaling = ScalingSettings(**config_data.get("scaling", {}))
    trailing_stop = TrailingStopSettings(**config_data.get("trailing_stop", {}))
    hedge = HedgeSettings(**config_data.get("hedge", {}))
    reentry = ReentrySettings(**config_data.get("reentry", {}))
    duplicate = DuplicateSettings(**config_data.get("duplicate", {}))
    stale_signal = StaleSignalSettings(**config_data.get("stale_signal", {}))
    timeout = TimeoutSettings(**config_data.get("timeout", {}))
    capacity = CapacitySettings(**config_data.get("capacity", {}))
    auto_sl = AutoSLSettings(**config_data.get("auto_sl", {}))
    reporting = ReportingSettings(**config_data.get("reporting", {}))
    tp_sl = TpSlSettings(**config_data.get("tp_sl", {}))

    # --- Telegram groups ---
    telegram_groups = _load_telegram_groups(config_data, root)

    return AppSettings(
        general=general,
        telegram=telegram,
        bybit=bybit,
        wallet=wallet,
        leverage=leverage,
        entry=entry,
        breakeven=breakeven,
        scaling=scaling,
        trailing_stop=trailing_stop,
        hedge=hedge,
        reentry=reentry,
        duplicate=duplicate,
        stale_signal=stale_signal,
        timeout=timeout,
        capacity=capacity,
        auto_sl=auto_sl,
        reporting=reporting,
        tp_sl=tp_sl,
        telegram_groups=telegram_groups,
    )
