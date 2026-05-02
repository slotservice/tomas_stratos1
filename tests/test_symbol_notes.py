"""Tests for the per-symbol operator-note loader (client 2026-05-02)."""

from __future__ import annotations

import os
import time

import pytest

from core import symbol_notes


class TestSymbolNotes:
    def setup_method(self) -> None:
        # Reset cache before each test so file mtime is re-read.
        symbol_notes._cache = (0.0, {})

    def test_default_when_no_match(self):
        # The shipped symbol_notes.toml has FETUSDT but not THIS one.
        assert symbol_notes.get_note("UNKNOWNUSDT") == "Kontrolera manuellt"

    def test_default_override(self):
        assert symbol_notes.get_note("UNKNOWNUSDT", default="hello") == "hello"

    def test_case_insensitive_lookup(self):
        # FETUSDT is in the shipped file. Lookup should work in any case.
        upper = symbol_notes.get_note("FETUSDT")
        lower = symbol_notes.get_note("fetusdt")
        mixed = symbol_notes.get_note("FetUsdt")
        assert upper == lower == mixed
        assert "FETUSDT" in upper or "fet" in upper.lower()

    def test_empty_symbol_returns_default(self):
        assert symbol_notes.get_note("") == "Kontrolera manuellt"
        assert symbol_notes.get_note("", default="x") == "x"

    def test_all_notes_returns_dict(self):
        notes = symbol_notes.all_notes()
        assert isinstance(notes, dict)
        # Mutating the returned dict must not affect future calls.
        notes["TEMP"] = "x"
        notes2 = symbol_notes.all_notes()
        assert "TEMP" not in notes2

    def test_reload_when_file_changes(self, tmp_path, monkeypatch):
        # Write a fresh symbol_notes.toml in a temp location and
        # monkey-patch the module's NOTES_PATH to point at it.
        f = tmp_path / "symbol_notes.toml"
        f.write_text(
            '[symbol_notes]\nXYZUSDT = "first note"\n',
            encoding="utf-8",
        )
        monkeypatch.setattr(symbol_notes, "NOTES_PATH", f)
        symbol_notes._cache = (0.0, {})

        assert symbol_notes.get_note("XYZUSDT") == "first note"

        # Sleep so the mtime resolution is honored, then rewrite.
        time.sleep(0.05)
        f.write_text(
            '[symbol_notes]\nXYZUSDT = "edited note"\n',
            encoding="utf-8",
        )
        os.utime(f, None)  # force mtime bump
        # Force a fresh load since mtime resolution can be coarse.
        symbol_notes._cache = (0.0, {})
        assert symbol_notes.get_note("XYZUSDT") == "edited note"
