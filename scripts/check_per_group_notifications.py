"""One-shot diagnostic: for each of Tomas's 17 reported groups, in the
last 24 hours, count incoming signal_ids vs how many of those signal_ids
ended up in a notification_sent event. Helps verify whether the bot is
actually sending channel notifications for messages from those groups.

Used 2026-05-12 to answer Tomas's "no telegram message from these groups"
complaint by proving (or disproving) per-group notification coverage.
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

LOG = Path("/opt/stratos1/stratos1.log")
DATE_PREFIX = "2026-05-12"

GROUPS = [
    (-1001971948809, "Master of Binance"),
    (-1003230942173, "KING CRYPTO"),
    (-1003888293429, "MAHEE CRYPTO"),
    (-1001804798661, "CRYPTO SIGNAL"),
    (-1002554800063, "COIN RISE"),
    (-1003615491164, "American Crypto Traders"),
    (-1003936733333, "Shirin Ahmadi"),
    (-1001463871021, "Crypto Master Vip"),
    (-1002787842089, "Crypto Futures & Spot"),
    (-1002283259889, "Crypto Insight Hub"),
    (-1002359767632, "The Crypto Possibility"),
    (-1001935772577, "Cryptonus Trade"),
    (-1002625944679, "(unknown id Tomas)"),
    (-1001731265095, "DeFi Million"),
    (-1001945142080, "Binance Features Signals"),
    (-1002133313711, "Crypto Musk VIP"),
    (-1002339729195, "Smart Crypto"),
]

# chat_id -> set of signal_ids that originated there today
sigs_by_chat: dict[int, set[str]] = defaultdict(set)
# signal_id -> events
events_by_sig: dict[str, list[tuple[str, str]]] = defaultdict(list)

with LOG.open() as f:
    for line in f:
        if DATE_PREFIX not in line:
            continue
        try:
            d = json.loads(line)
        except Exception:
            continue
        ev = d.get("event", "")
        sid = d.get("signal_id", "")
        ts = d.get("timestamp", "")[:19]
        if not sid:
            continue
        events_by_sig[sid].append((ts, ev))
        cid = d.get("chat_id") or d.get("channel_id")
        if isinstance(cid, int):
            sigs_by_chat[cid].add(sid)

target_ids = {gid for gid, _ in GROUPS}

print(f"=== Per-group notification coverage on {DATE_PREFIX} ===\n")
print(f"{'Group':<28} {'Sigs':>5} {'Notif':>5} {'Parsed':>7} {'Dup':>4} {'Skip':>4} {'NoSig':>5}")
for gid, name in GROUPS:
    sids = sigs_by_chat.get(gid, set())
    n_total = len(sids)
    n_notified = 0
    n_parsed = 0
    n_dup = 0
    n_skip = 0
    n_nosig = 0
    for s in sids:
        evs = [e for _, e in events_by_sig.get(s, [])]
        if "notification_sent" in evs:
            n_notified += 1
        if "signal_parsed" in evs:
            n_parsed += 1
        if any("duplicate_blocked" in e or "duplicate_check.blocked" in e for e in evs):
            n_dup += 1
        if "status_update_skipped" in evs:
            n_skip += 1
        if all(
            e not in ("signal_parsed",)
            and "duplicate" not in e
            and "status_update" not in e
            for e in evs
        ):
            # signal_id existed but neither parsed nor classified as known drop type
            n_nosig += 1
    print(f"{name:<28} {n_total:>5} {n_notified:>5} {n_parsed:>7} {n_dup:>4} {n_skip:>4} {n_nosig:>5}")

print()
print("Columns: Sigs=distinct signal_ids today, Notif=of those that produced ≥1 channel notification,")
print("Parsed=fully parsed entry, Dup=duplicate-blocked, Skip=status update, NoSig=neither parsed nor classified.")
