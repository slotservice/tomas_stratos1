"""
Stratos1 - client-chat fetcher
------------------------------
Reads new messages (and images) from the client chat on Andrew's
personal Telegram session and prints them as a plain-text dump.

Uses ANDREW_TG_SESSION_STRING from .env — NOT the bot's
TG_SESSION_STRING. The signal-listener bot is never touched.

Usage (on the server):
    venv/bin/python fetch_andrew_chat.py            # new messages since last run
    venv/bin/python fetch_andrew_chat.py --recent 30   # last 30 messages, ignore state
    venv/bin/python fetch_andrew_chat.py --name "Tomas Test"   # override contact name

State (last-seen message id + resolved chat id) is kept in
andrew_chat_state.json. Images are downloaded to andrew_chat_media/.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    print("ERROR: python-dotenv is not installed.")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except ImportError:
    print("ERROR: telethon is not installed.")
    sys.exit(1)

STATE_PATH = PROJECT_ROOT / "andrew_chat_state.json"
MEDIA_DIR = PROJECT_ROOT / "andrew_chat_media"
DEFAULT_CLIENT_NAME = "Tomas Test"
FIRST_RUN_LIMIT = 30


def _load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


async def _resolve_chat(client: TelegramClient, name: str, state: dict):
    """Find the client dialog. Prefer the cached id; fall back to name search."""
    cached_id = state.get("chat_id")
    if cached_id is not None:
        try:
            return await client.get_entity(cached_id)
        except Exception:
            pass  # cache stale — fall through to name search

    matches = []
    async for dialog in client.iter_dialogs():
        if (dialog.name or "").strip() == name.strip():
            matches.append(dialog)

    if not matches:
        print(f"ERROR: no chat found with display name {name!r}.")
        print("Run with --list to see available chats.")
        sys.exit(1)
    if len(matches) > 1:
        print(f"ERROR: {len(matches)} chats match {name!r} — be more specific:")
        for d in matches:
            print(f"  id={d.id}  name={d.name!r}")
        sys.exit(1)
    return matches[0].entity


async def _list_dialogs(client: TelegramClient) -> None:
    print("Chats on this account:")
    async for dialog in client.iter_dialogs():
        kind = "user" if dialog.is_user else ("group" if dialog.is_group else "channel")
        print(f"  id={dialog.id:>16}  [{kind:>7}]  {dialog.name!r}")


async def _run(args: argparse.Namespace) -> None:
    api_id = os.environ.get("TG_API_ID", "")
    api_hash = os.environ.get("TG_API_HASH", "")
    session_string = os.environ.get("ANDREW_TG_SESSION_STRING", "")

    if not api_id or not api_hash:
        print("ERROR: TG_API_ID / TG_API_HASH missing from .env")
        sys.exit(1)
    if not session_string:
        print("ERROR: ANDREW_TG_SESSION_STRING is empty — run auth_andrew_session.py first.")
        sys.exit(1)

    client = TelegramClient(StringSession(session_string), int(api_id), api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        print("ERROR: ANDREW_TG_SESSION_STRING is not authorised — re-run auth_andrew_session.py.")
        await client.disconnect()
        sys.exit(1)

    if args.list:
        await _list_dialogs(client)
        await client.disconnect()
        return

    state = _load_state()
    entity = await _resolve_chat(client, args.name, state)
    me = await client.get_me()

    last_id = state.get("last_message_id", 0)
    if args.recent is not None:
        kwargs = {"limit": args.recent}
        print(f"=== Last {args.recent} messages with {args.name!r} ===")
    elif last_id:
        kwargs = {"min_id": last_id, "limit": None}
        print(f"=== New messages with {args.name!r} since id {last_id} ===")
    else:
        kwargs = {"limit": FIRST_RUN_LIMIT}
        print(f"=== First run — last {FIRST_RUN_LIMIT} messages with {args.name!r} ===")

    # iter_messages yields newest-first; collect then reverse to chronological.
    messages = [m async for m in client.iter_messages(entity, **kwargs)]
    messages.reverse()

    if not messages:
        print("(no new messages)")
        await client.disconnect()
        return

    MEDIA_DIR.mkdir(exist_ok=True)
    newest_id = last_id
    image_count = 0

    for msg in messages:
        newest_id = max(newest_id, msg.id)
        sender = "Andrew (you)" if msg.out else args.name
        ts = msg.date.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if msg.date else "?"
        header = f"[{ts}] {sender} (msg {msg.id}):"

        media_note = ""
        if msg.photo or (msg.document and (msg.document.mime_type or "").startswith("image/")):
            ext = "jpg"
            if msg.document and msg.document.mime_type:
                ext = msg.document.mime_type.split("/")[-1] or "jpg"
            dest = MEDIA_DIR / f"msg_{msg.id}.{ext}"
            try:
                await client.download_media(msg, file=str(dest))
                image_count += 1
                media_note = f"  [IMAGE saved: {dest.name}]"
            except Exception as exc:
                media_note = f"  [IMAGE download failed: {exc}]"
        elif msg.media:
            media_note = "  [non-image media — skipped]"

        text = (msg.message or "").strip()
        print(header + media_note)
        if text:
            print(text)
        print()

    state["chat_id"] = entity.id
    if args.recent is None:
        state["last_message_id"] = newest_id
    _save_state(state)

    print(f"=== {len(messages)} message(s), {image_count} image(s) downloaded to {MEDIA_DIR.name}/ ===")
    if args.recent is None:
        print(f"=== state advanced to id {newest_id} ===")
    await client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch client chat via Andrew's TG session.")
    parser.add_argument("--name", default=DEFAULT_CLIENT_NAME,
                        help=f"Client display name (default: {DEFAULT_CLIENT_NAME!r})")
    parser.add_argument("--recent", type=int, default=None,
                        help="Fetch the last N messages and do NOT advance the state pointer.")
    parser.add_argument("--list", action="store_true",
                        help="List all chats on the account and exit.")
    args = parser.parse_args()
    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("\nCancelled.")
        sys.exit(0)


if __name__ == "__main__":
    main()
