"""Regression suite — every real signal Tomas pasted in chat.md.

Tomas (client) 2026-05-03 explicit requirement: "this type of issue
cannot come back again." This suite locks in the parser's behaviour
on every real signal he has sent so a future change can't silently
regress them. Snapshot lives in regression_chatmd_baseline.json.

To regenerate the baseline (only when intentionally changing parser
behaviour): delete the JSON file and re-run the test — it will fail
with a "baseline not found" message, then a one-shot
`tests/build_regression_baseline.py` (or the snippet in this docstring)
rebuilds it. Then carefully review the diff before committing.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import pytest

from core.signal_parser import parse_signal_detailed


_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_CHAT_MD = _PROJECT_ROOT / "chat.md"
_BASELINE = Path(__file__).parent / "regression_chatmd_baseline.json"


def _extract_signals_from_chat() -> list[str]:
    if not _CHAT_MD.exists():
        return []
    text = _CHAT_MD.read_text(encoding="utf-8")
    parts = re.split(r"\n> Tomas Test:\n", text)
    out: list[str] = []
    for p in parts:
        p = p.strip()
        if not p or len(p) < 20:
            continue
        if not re.search(
            r"(entry|target|stop|leverage|tp\d|sl\b|long|short|/usdt)",
            p, re.IGNORECASE,
        ):
            continue
        if re.match(r"^(check this|🖼 ?[Pp]hoto|🖼 ?,|control)", p):
            continue
        # Skip status updates that should not be parsed as signals.
        if "TARGETS DONE" in p or "ALL TARGET" in p:
            continue
        out.append(p)
    return out


def _snapshot(text: str, idx: int) -> dict:
    logging.disable(logging.CRITICAL)
    try:
        r = parse_signal_detailed(
            text=text, channel_id=-1, channel_name="regression",
        )
    finally:
        logging.disable(logging.NOTSET)
    return {
        "idx": idx,
        "snippet": text[:60].replace("\n", " | "),
        "reason": r.reason,
        "symbol": r.symbol,
        "direction": r.direction,
        "tps": r.tps,
        "sl": r.sl,
        "has_signal": r.signal is not None,
    }


@pytest.mark.skipif(not _CHAT_MD.exists(), reason="chat.md not present")
@pytest.mark.skipif(not _BASELINE.exists(), reason="baseline not built")
def test_chatmd_signals_match_baseline():
    """Every signal in chat.md must parse identically to the baseline.

    A failure here means a parser change caused a regression on a real
    signal Tomas has sent. Inspect the diff and decide whether the
    change is intentional (rebuild baseline) or a regression (revert).
    """
    signals = _extract_signals_from_chat()
    baseline = json.loads(_BASELINE.read_text(encoding="utf-8"))
    assert len(signals) == len(baseline), (
        f"signal count drift: chat.md has {len(signals)} signals "
        f"but baseline locked-in {len(baseline)}. Rebuild baseline."
    )
    diffs: list[str] = []
    for sig, base in zip(signals, baseline):
        new = _snapshot(sig, base["idx"])
        for k in ("reason", "symbol", "direction", "tps", "sl", "has_signal"):
            if base.get(k) != new.get(k):
                diffs.append(
                    f"idx={base['idx']} {k}: {base.get(k)} -> {new.get(k)} "
                    f"| {base['snippet']}"
                )
    assert not diffs, (
        "Parser regression detected on real chat.md signals:\n"
        + "\n".join(diffs)
    )
