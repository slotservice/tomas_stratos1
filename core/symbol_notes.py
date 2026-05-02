"""
Stratos1 — per-symbol operator-note loader.

Operator-editable file at ``symbol_notes.toml`` lets Tomas mark
symbols with a custom note that overrides the default
"Kontrolera manuellt" line in the "Finns inte på bybit"
rejection notification. Client request 2026-05-02:

    "I want to be able to manually write:
       FETUSDT checked 2026-05-01
     Then that text should appear in every future error
     message for FETUSDT."

The file is reloaded on every call (with a small mtime cache
to avoid re-parsing on each signal in a burst), so edits take
effect with no restart.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import structlog

log = structlog.get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
NOTES_PATH = PROJECT_ROOT / "symbol_notes.toml"

# Cache: (mtime, dict). Reload only when the file mtime changes.
_cache: tuple[float, dict[str, str]] = (0.0, {})


def _load() -> dict[str, str]:
    """Load + cache the symbol notes from symbol_notes.toml."""
    global _cache
    try:
        st = os.stat(NOTES_PATH)
    except FileNotFoundError:
        _cache = (0.0, {})
        return {}
    mtime = st.st_mtime
    if mtime == _cache[0]:
        return _cache[1]

    try:
        try:
            import tomllib  # py311+
            with open(NOTES_PATH, "rb") as f:
                data = tomllib.load(f)
        except ImportError:
            import tomli as tomllib  # type: ignore
            with open(NOTES_PATH, "rb") as f:
                data = tomllib.load(f)
    except Exception as exc:
        log.warning(
            "symbol_notes.parse_failed",
            path=str(NOTES_PATH),
            error=str(exc)[:120],
        )
        _cache = (mtime, {})
        return {}

    raw = data.get("symbol_notes", {}) or {}
    # Normalise keys to uppercase so the lookup is case-insensitive.
    notes: dict[str, str] = {}
    for k, v in raw.items():
        if isinstance(k, str) and isinstance(v, str) and k.strip() and v.strip():
            notes[k.strip().upper()] = v.strip()
    _cache = (mtime, notes)
    return notes


def get_note(symbol: str, default: str = "Kontrolera manuellt") -> str:
    """Return the operator's custom note for ``symbol`` if set,
    otherwise ``default``. Symbol matching is case-insensitive."""
    if not symbol:
        return default
    notes = _load()
    return notes.get(symbol.strip().upper(), default)


def all_notes() -> dict[str, str]:
    """Return a copy of every loaded note. Useful for tests / a
    'list current notes' operator command."""
    return dict(_load())
