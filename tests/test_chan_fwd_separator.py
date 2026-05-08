"""
_chan formatter tests.

Tomas 2026-05-08 (revised in same day): when the listener annotates a
forward source as "Receiving Channel (fwd: Source Channel)", the
operator only wants the receiving-channel hashtag in the
notification — the "(fwd: ...)" annotation should be DROPPED. (An
earlier same-day fix kept it as readable text, but Tomas reviewed
the output and asked for it gone — bracketed/emoji-laden source
names like "(fwd: COIN RISE [VIP] 💎)" cluttered messages.)
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


def test_chan_drops_fwd_suffix_entirely():
    assert (
        _chan("DeFi Milion (fwd: Mastermind premium)")
        == "#DeFiMilion"
    )


def test_chan_drops_fwd_with_brackets_and_emoji():
    """Tomas's literal example: source has brackets and emoji — drop
    the whole annotation, keep only the receiving channel hashtag."""
    assert (
        _chan("CoinRise (fwd: COIN RISE [VIP] 💎)")
        == "#CoinRise"
    )


def test_chan_drops_fwd_with_spaces_in_source():
    assert (
        _chan("Sweden Crypto (fwd: Sweden Crypto premium)")
        == "#SwedenCrypto"
    )


def test_chan_drops_fwd_with_emoji_in_source():
    assert (
        _chan("Crypto Possibility (fwd: VIP PREMIUM 99.9% 💯)")
        == "#CryptoPossibility"
    )


def test_chan_drops_fwd_when_source_empty():
    """Defensive: an empty forward source still drops cleanly."""
    assert _chan("Channel (fwd: )") == "#Channel"


def test_chan_no_paren_close_treated_as_part_of_name():
    """If the input has '(fwd:' but no closing ')', treat as a normal
    name to avoid accidentally truncating the hashtag."""
    out = _chan("Weird Name (fwd: missing close")
    # The function should NOT split — the whole thing becomes the
    # hashtag (with all non-alphanumerics stripped).
    assert "(fwd:" not in out
    assert out.startswith("#")
