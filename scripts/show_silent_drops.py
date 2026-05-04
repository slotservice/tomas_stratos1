"""Summarise silent_drops.jsonl — the backlog of monitored-channel
messages the bot received but never relayed to the operator's
Telegram channel.

Tomas (client) 2026-05-04: persist every silent drop so we can see
which formats keep failing and prioritise parser fixes.

Usage:
    cd /opt/stratos1
    venv/bin/python scripts/show_silent_drops.py             # full summary
    venv/bin/python scripts/show_silent_drops.py --top 10    # top 10 channels
    venv/bin/python scripts/show_silent_drops.py --replay    # re-parse each
                                                              # entry with the
                                                              # CURRENT parser
                                                              # to see how many
                                                              # would now parse
                                                              # ok (i.e. how
                                                              # many can be
                                                              # pruned).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def load_entries(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out: list[dict] = []
    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def summarise(entries: list[dict], top_n: int) -> str:
    if not entries:
        return "silent_drops.jsonl is empty — nothing to summarise."

    total = len(entries)
    by_channel: Counter[str] = Counter()
    by_last_event: Counter[str] = Counter()
    by_channel_event: Counter[tuple[str, str]] = Counter()
    samples_per_pair: dict[tuple[str, str], str] = {}
    for e in entries:
        ch = (e.get("channel") or "?")
        ev = (e.get("last_event") or "?")
        by_channel[ch] += 1
        by_last_event[ev] += 1
        key = (ch, ev)
        by_channel_event[key] += 1
        # Keep the FIRST sample text we see for each (channel, event)
        # pair as a representative example.
        if key not in samples_per_pair:
            txt = (e.get("text_preview") or "").strip()
            samples_per_pair[key] = txt[:200]

    lines: list[str] = [
        f"=== silent_drops.jsonl summary ===",
        f"Total entries: {total}",
        "",
        f"By channel (top {top_n}):",
    ]
    for ch, cnt in by_channel.most_common(top_n):
        lines.append(f"  {cnt:5d}  {ch}")
    lines.append("")
    lines.append(f"By last event (top {top_n}):")
    for ev, cnt in by_last_event.most_common(top_n):
        lines.append(f"  {cnt:5d}  {ev}")
    lines.append("")
    lines.append(f"Top (channel, last_event) pairs (top {top_n}):")
    for (ch, ev), cnt in by_channel_event.most_common(top_n):
        sample = samples_per_pair.get((ch, ev), "").replace("\n", " | ")
        lines.append(f"  {cnt:5d}  {ch[:30]:30s}  {ev[:32]:32s}")
        if sample:
            lines.append(f"         sample: {sample[:140]}")
    return "\n".join(lines)


def replay_check(entries: list[dict]) -> str:
    """Re-parse each entry with the current parser and count how many
    would NOW parse OK. The operator can then prune those entries
    from silent_drops.jsonl."""
    try:
        from core.signal_parser import parse_signal_detailed
    except Exception as exc:
        return f"replay unavailable: {exc}"
    if not entries:
        return "no entries to replay"

    now_parses_ok = 0
    still_dropped: Counter[str] = Counter()
    for e in entries:
        text = e.get("text_preview") or ""
        if not text:
            continue
        try:
            r = parse_signal_detailed(
                text=text, channel_id=-1, channel_name="replay",
            )
        except Exception:
            still_dropped["parser_crash"] += 1
            continue
        if r.signal is not None:
            now_parses_ok += 1
        else:
            still_dropped[r.reason or "unknown"] += 1

    lines = [
        "",
        "=== replay against current parser ===",
        f"Now parse OK (can be pruned): {now_parses_ok} / {len(entries)}",
        "Still dropped, by reason:",
    ]
    for reason, cnt in still_dropped.most_common():
        lines.append(f"  {cnt:5d}  {reason}")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--path", default=str(ROOT / "silent_drops.jsonl"),
        help="path to silent_drops.jsonl",
    )
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument(
        "--replay", action="store_true",
        help="also re-parse each entry with the current parser",
    )
    args = ap.parse_args()

    entries = load_entries(Path(args.path))
    print(summarise(entries, args.top))
    if args.replay:
        print(replay_check(entries))


if __name__ == "__main__":
    main()
