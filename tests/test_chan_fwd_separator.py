"""
_chan formatter — covers Tomas 2026-05-08 fix:

When the listener annotates a forward source (e.g. "DeFi Milion
(fwd: Mastermind premium)"), the operator notification used to render
"#DeFiMilionfwdMastermindpremium" — one un-clickable hashtag with
"fwd" concatenated into it. The fix splits the receiving channel
into a clickable hashtag and renders the forward annotation as
readable text after it.
"""

from __future__ import annotations

from telegram.notifier import _chan


def test_chan_simple_name_unchanged():
    assert _chan("CoinAura") == "#CoinAura"


def test_chan_strips_whitespace_and_specials():
    assert _chan("AiphaMint Signals") == "#AiphaMintSignals"


def test_chan_handles_missing_name():
    assert _chan("") == "#Unknown"
    assert _chan(None) == "#Unknown"  # type: ignore[arg-type]


def test_chan_strips_existing_hash_prefix():
    assert _chan("#CoinAura") == "#CoinAura"


def test_chan_keeps_fwd_source_as_readable_text():
    assert (
        _chan("DeFi Milion (fwd: Mastermind premium)")
        == "#DeFiMilion (fwd: Mastermind premium)"
    )


def test_chan_fwd_with_spaces_preserved():
    assert (
        _chan("Sweden Crypto (fwd: Sweden Crypto premium)")
        == "#SwedenCrypto (fwd: Sweden Crypto premium)"
    )


def test_chan_fwd_with_emoji_in_source_kept():
    """Forward source can contain emoji and special chars — keep
    them in the readable suffix, only sanitize the hashtag part."""
    assert (
        _chan("Crypto Possibility (fwd: VIP PREMIUM 99.9% 💯)")
        == "#CryptoPossibility (fwd: VIP PREMIUM 99.9% 💯)"
    )


def test_chan_empty_fwd_source_falls_back_cleanly():
    """Defensive: an empty forward source shouldn't produce a dangling
    suffix — drop it and just hashtag the receiving channel."""
    assert _chan("Channel (fwd: )") == "#Channel"


def test_chan_no_paren_close_treated_as_part_of_name():
    """If the input has '(fwd:' but no closing ')', treat as a normal
    name to avoid accidentally truncating the hashtag."""
    out = _chan("Weird Name (fwd: missing close")
    # The function should NOT split — the whole thing becomes the
    # hashtag (with all non-alphanumerics stripped).
    assert "(fwd:" not in out
    assert out.startswith("#")
