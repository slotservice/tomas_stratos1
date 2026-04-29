"""One-shot Telegram channel purge.

Connects to the bot's notification channel using the user-session
credentials in .env and deletes every message in it. Exits when done.

Used to scrub history before a fresh bot restart so the operator
isn't staring at stale notifications from previous runs.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession


PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env", override=True)


async def purge() -> None:
    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]
    session_string = os.environ.get("TG_SESSION_STRING", "")
    channel_id = int(os.environ["TG_NOTIFY_CHANNEL_ID"])

    if not session_string:
        print("ERROR: TG_SESSION_STRING is empty — cannot purge.")
        sys.exit(1)

    client = TelegramClient(
        StringSession(session_string),
        api_id,
        api_hash,
    )
    await client.connect()

    if not await client.is_user_authorized():
        print("ERROR: TG_SESSION_STRING is not authorised. Re-run "
              "generate_session.py to refresh.")
        await client.disconnect()
        sys.exit(1)

    entity = await client.get_entity(channel_id)
    print(f"Purging messages from channel {channel_id} ...")

    batch: list[int] = []
    total = 0
    async for msg in client.iter_messages(entity, limit=None):
        batch.append(msg.id)
        if len(batch) >= 100:
            try:
                await client.delete_messages(entity, batch, revoke=True)
                total += len(batch)
                print(f"  ... {total} messages deleted")
            except Exception as exc:
                print(f"  batch error: {exc}")
            batch.clear()

    if batch:
        try:
            await client.delete_messages(entity, batch, revoke=True)
            total += len(batch)
        except Exception as exc:
            print(f"  final batch error: {exc}")

    print(f"Done. {total} messages deleted.")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(purge())
