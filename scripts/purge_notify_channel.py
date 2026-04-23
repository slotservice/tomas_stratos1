"""
One-shot utility: purge every message in the bot's notification channel.

Usage (on VPS):
    cd /opt/stratos1
    venv/bin/python scripts/purge_notify_channel.py --yes

Safety:
- Requires --yes flag to actually delete. Without it, runs in dry-run
  mode (counts messages, deletes nothing).
- Uses the USER Telethon session (TG_SESSION_STRING), not the bot
  client. The user account must be an admin of notify_channel_id.
- Deletes in chunks of 100 (Telegram's per-request limit). Prints
  progress so you can Ctrl+C between batches if you change your mind.

This does NOT touch the bot database, trade history, or Bybit state.
It only removes presentation-layer messages from the notification
channel.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

# Make the project root importable when run as a script.
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from telethon import TelegramClient
from telethon.sessions import StringSession

from config.settings import load_settings


async def main(dry_run: bool) -> None:
    settings = load_settings()
    tg = settings.telegram
    session_string = os.environ.get("TG_SESSION_STRING", "")
    if not session_string:
        print("ERROR: TG_SESSION_STRING env var is empty.")
        sys.exit(1)

    channel_id = tg.notify_channel_id
    print(f"Target channel: {channel_id}")
    print(f"Dry run: {dry_run}")

    client = TelegramClient(
        StringSession(session_string),
        int(tg.api_id),
        tg.api_hash,
    )
    await client.connect()
    if not await client.is_user_authorized():
        print("ERROR: user session not authorized. Re-login required.")
        sys.exit(1)

    me = await client.get_me()
    print(f"Acting as user: {me.username or me.first_name} (id {me.id})")

    try:
        entity = await client.get_entity(channel_id)
    except Exception as e:
        print(f"ERROR: cannot resolve channel {channel_id}: {e}")
        sys.exit(1)
    title = getattr(entity, "title", None) or getattr(entity, "username", "?")
    print(f"Resolved to channel: {title!r}")

    total_seen = 0
    total_deleted = 0
    batch: list[int] = []

    async for message in client.iter_messages(entity, limit=None):
        total_seen += 1
        batch.append(message.id)
        if len(batch) >= 100:
            if not dry_run:
                try:
                    await client.delete_messages(entity, batch, revoke=True)
                    total_deleted += len(batch)
                except Exception as e:
                    print(f"  batch delete error: {e}")
            print(f"  scanned {total_seen:>6}   deleted {total_deleted:>6}")
            batch.clear()

    # Flush the tail.
    if batch:
        if not dry_run:
            try:
                await client.delete_messages(entity, batch, revoke=True)
                total_deleted += len(batch)
            except Exception as e:
                print(f"  final batch delete error: {e}")
        batch.clear()

    print("--- done ---")
    print(f"Total messages seen   : {total_seen}")
    print(f"Total messages deleted: {total_deleted}")
    if dry_run:
        print("Dry run: no messages were actually deleted. Re-run with --yes to purge.")

    await client.disconnect()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--yes",
        action="store_true",
        help="Actually delete messages (otherwise dry-run only).",
    )
    args = p.parse_args()
    asyncio.run(main(dry_run=not args.yes))
