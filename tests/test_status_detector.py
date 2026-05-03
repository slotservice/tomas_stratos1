"""Tests for is_status_update — must catch trade-result messages
without false-positive on real signal text.

Tomas (client) 2026-05-03 reported result-style messages like
"#PLAY ALL TARGET REACHED ENTRY: 0.1336" being misparsed as new
signals. The detector lives in core/signal_parser.py and is gated
in main._on_signal_message before parser runs.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from core.signal_parser import is_status_update


# Real status messages observed in production / chat.md.
STATUS_MESSAGES = [
    "#PLAY ALL TARGET REACHED 🔥",
    "#MASK/USDT All targets achieved 😎\nProfit: 34.6535%",
    "#STRK/USDT Stop Target Hit ⛔\nLoss: 222.2832%",
    "#UAI/USDT Closed at stoploss after reaching take profit",
    "#CLO/USDT Stop Target Hit ⛔",
    "#DASH/USDT Take-Profit target 1 ✅\nProfit: 20.8981%",
    "TAG/USDT UPDATE\nALL TARGETS DONE\n+646% GAIN",
    "Trade closed at TP3",
    # 2026-05-04 ONT/THETA "Entry N ✅" entry-fill confirmations.
    "#Free Signal ✅ Long #ONT/USDT Entry Palm: 0.0668 0.0689\n"
    "#ONT/USDT Entry 1 ✅\nAverage Entry Price: 0.0689 ✅",
    "🔴 Short Pair: #THETA/USDT Entry Price: 1) 0.2155 2) 0.2219\n"
    "#THETA/USDT Entry 1 ✅\nAverage Entry Price: 0.2155 ✅",
    "#BTC/USDT Trade update — Entry 1 filled",
    "ETH Position Update — TP1 hit",
    "Position closed in profit",
    "TP1 hit, TP2 taken",
    "Stopped out at SL",
    "So far profit (334%)",
    "ETH stop hit, +12% gain",
]


# Real signal messages from chat.md — must NOT be flagged as status.
REAL_SIGNALS = [
    """#BTC/USDT LONG
Entry: 65000
TP1: 66000
TP2: 67500
SL: 64000""",
    """🖼 PAIR $BABY/USDT
📊SHORT
Cross (50-x)
✔️ Entry Target:
💡 0.02820
☑️ Take Profits:
1️⃣0.02670
2️⃣0.02555
❌ STOP LOSS: 0.03200""",
    """📩 ETHUSDT 30M | Mid-Term
📉 Trade Type: Long
🎯Entry Orders: 2311.64
❌Stop-loss: 2308.41
Target 1: 2312.93
Target 2: 2314.55
Target 3: 2327.79""",
    """💥 #AKT LONG ALERT 💥
© Entry: 0.6293
💬 Leverage 50X
🎯 Targets:
TP1: 0.6420
TP2: 0.6590
TP3: 0.6945""",
    """⚡️⚡️ #XPL/USDT ⚡️⚡️
Signal Type: Regular (Short)
Leverage: Cross (75х)
Entry Targets: 0.0888
Take-Profit Targets:
1) 0.08747
2) 0.08658
Stop Targets: 5-10%""",
    """#LAB/USDT LONG
Leverage: 20X
Entries: 1.89 - 1.84
Targets:
1) 1.94
2) 1.99
3) 2.04
Stop Loss: 1.80""",
]


@pytest.mark.parametrize("msg", STATUS_MESSAGES)
def test_status_messages_detected(msg: str) -> None:
    assert is_status_update(msg), f"Status message not detected: {msg[:60]}"


@pytest.mark.parametrize("msg", REAL_SIGNALS)
def test_real_signals_not_flagged(msg: str) -> None:
    assert not is_status_update(msg), (
        f"Real signal falsely flagged as status: {msg[:60]}"
    )


def test_empty_input() -> None:
    assert not is_status_update("")
    assert not is_status_update(None)  # type: ignore[arg-type]


def test_status_detector_on_chatmd_signals() -> None:
    """No real signal in chat.md should be flagged as a status update.

    This guards against the detector becoming over-eager and dropping
    actual trade signals.
    """
    chat_md = Path(__file__).resolve().parent.parent.parent / "chat.md"
    if not chat_md.exists():
        pytest.skip("chat.md not present")
    text = chat_md.read_text(encoding="utf-8")
    parts = re.split(r"\n> Tomas Test:\n", text)
    false_positives: list[str] = []
    for p in parts:
        p = p.strip()
        if not p or len(p) < 20:
            continue
        # Only check messages that look like signals (have entry/TP/SL).
        if not re.search(r"(entry|target|tp\d|sl\b|stoploss)", p, re.IGNORECASE):
            continue
        # Skip the chat.md entries that ARE explicit status messages
        # (Tomas pasted some result examples too).
        if "TARGETS DONE" in p or "ALL TARGET" in p:
            continue
        if is_status_update(p):
            false_positives.append(p[:80].replace("\n", " | "))
    assert not false_positives, (
        "Status detector flagged real signals as status:\n"
        + "\n".join(false_positives)
    )
