"""Operator-managed symbol overrides.

Tomas (client) 2026-05-03 explicit request: he wants to be able to
write per-symbol notes himself ("checked manually 2026-05-03 — not on
Bybit perp", "use 1000SHIB instead of SHIB") and have the bot honour
them, instead of the bot deciding alone whether a symbol is tradable.

The file lives at ``stratos1/symbol_overrides.toml`` and is reloaded
ONLY on bot restart (kept simple — clean-start-on-restart already
flushes everything else, so a config change becomes effective after
the operator restarts the service).

Each entry supports three optional fields:

    [TAGUSDT]
    skip      = true
    note      = "Checked 2026-05-03 — not on Bybit perpetuals"

    [SHIBUSDT]
    rename_to = "1000SHIBUSDT"
    note      = "Bybit lists 1000SHIB instead of SHIB"

    [BTCUSDT]
    note      = "Operator: high-conviction symbol, no caps."

- ``skip``      -> the parser drops the signal silently (no trade,
                   no warning to the operator) and logs once per
                   signal.
- ``rename_to`` -> the parser replaces the extracted symbol with the
                   override value before any downstream lookup. Bybit
                   then sees the renamed symbol.
- ``note``      -> free-form text. Carried on ParseResult.override_note
                   so the notifier can append it to warnings about
                   that symbol (e.g. "delisted" / "instrument info
                   fetch failed"). Without an override note the bot
                   still warns, just without the operator's annotation.

Symbol keys are CASE-INSENSITIVE for ergonomics (``[shib]`` /
``[ShibUsdt]`` / ``[SHIBUSDT]`` all match the same symbol). They
must be USDT-suffixed at lookup time — the parser already normalises
to ``XXXUSDT`` before the lookup, so operator entries should follow
the same convention.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import structlog

if sys.version_info >= (3, 11):
    import tomllib  # type: ignore[import]
else:  # pragma: no cover — bot runs on 3.12
    import tomli as tomllib  # type: ignore[import]

log = structlog.get_logger(__name__)


_OVERRIDES: dict[str, dict] = {}
_LOADED_FROM: Optional[Path] = None


def load_overrides(path: Path) -> dict[str, dict]:
    """Load symbol overrides from *path*. Idempotent — safe to call
    multiple times. Returns the loaded mapping (also cached in this
    module).

    Missing file -> empty mapping (no overrides, normal operation).
    Malformed file -> empty mapping + warning log; the bot starts
    normally rather than blocking on a config typo.
    """
    global _OVERRIDES, _LOADED_FROM
    _OVERRIDES = {}
    _LOADED_FROM = path
    if not path.exists():
        log.info("symbol_overrides.no_file", path=str(path))
        return _OVERRIDES
    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except Exception:
        log.exception("symbol_overrides.parse_failed", path=str(path))
        return _OVERRIDES

    for raw_key, entry in data.items():
        if not isinstance(entry, dict):
            continue
        key = raw_key.upper()
        if not key.endswith("USDT"):
            key = key + "USDT"
        _OVERRIDES[key] = {
            "skip": bool(entry.get("skip", False)),
            "rename_to": (entry.get("rename_to") or "").upper() or None,
            "note": entry.get("note") or None,
        }
    log.info(
        "symbol_overrides.loaded",
        path=str(path),
        entries=len(_OVERRIDES),
        symbols=sorted(_OVERRIDES.keys()),
    )
    return _OVERRIDES


def get_override(symbol: str) -> Optional[dict]:
    """Return the override entry for *symbol* (USDT-suffixed,
    case-insensitive), or None if no override is registered."""
    if not symbol:
        return None
    return _OVERRIDES.get(symbol.upper())


def overrides_loaded_from() -> Optional[Path]:
    """Path the overrides were loaded from (None if never loaded).
    Useful for diagnostics / startup logs."""
    return _LOADED_FROM
