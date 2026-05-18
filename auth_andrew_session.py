"""
Stratos1 - Andrew operator-session generator
---------------------------------------------
One-time interactive script. Authenticates Andrew's personal Telegram
account and stores the resulting Telethon StringSession in .env as
ANDREW_TG_SESSION_STRING.

This session is used ONLY by fetch_andrew_chat.py to read the client
chat. It is completely separate from TG_SESSION_STRING (the bot's
signal-listener session) — the bot is never touched.

Usage (on the server):
    venv/bin/python auth_andrew_session.py

Reads TG_API_ID / TG_API_HASH from .env, prompts for phone number,
login code, and 2FA password if enabled. Phone/code/password are typed
in here and never leave this machine.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    print("ERROR: python-dotenv is not installed. Run: pip install python-dotenv")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(ENV_PATH)

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except ImportError:
    print("ERROR: telethon is not installed. Run: pip install telethon")
    sys.exit(1)

ENV_KEY = "ANDREW_TG_SESSION_STRING"


def _write_session_to_env(session_string: str) -> None:
    """Insert or replace ANDREW_TG_SESSION_STRING in .env."""
    line = f"{ENV_KEY}={session_string}\n"
    if ENV_PATH.exists():
        lines = ENV_PATH.read_text(encoding="utf-8").splitlines(keepends=True)
    else:
        lines = []

    replaced = False
    for i, existing in enumerate(lines):
        if existing.strip().startswith(f"{ENV_KEY}="):
            lines[i] = line
            replaced = True
            break
    if not replaced:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] += "\n"
        lines.append(line)

    ENV_PATH.write_text("".join(lines), encoding="utf-8")
    print(f"  {ENV_KEY} {'updated in' if replaced else 'appended to'} {ENV_PATH}")


async def _generate() -> None:
    api_id = os.environ.get("TG_API_ID", "")
    api_hash = os.environ.get("TG_API_HASH", "")
    if not api_id or not api_hash:
        print("ERROR: TG_API_ID and TG_API_HASH must be set in .env")
        sys.exit(1)

    print("=" * 60)
    print("Stratos1 - Andrew operator-session generator")
    print("=" * 60)
    print()

    client = TelegramClient(StringSession(), int(api_id), api_hash)
    await client.connect()

    phone = input("Enter your phone number (with country code, e.g. +1...): ").strip()
    if not phone:
        print("ERROR: Phone number is required.")
        await client.disconnect()
        sys.exit(1)

    print(f"Sending login code to {phone} ...")
    await client.send_code_request(phone)

    code = input("Enter the login code Telegram sent you: ").strip()
    if not code:
        print("ERROR: Login code is required.")
        await client.disconnect()
        sys.exit(1)

    try:
        await client.sign_in(phone, code)
    except Exception as exc:
        if "password" in str(exc).lower() or "two" in str(exc).lower():
            password = input("2FA is enabled. Enter your Telegram password: ").strip()
            await client.sign_in(password=password)
        else:
            print(f"ERROR: Sign-in failed: {exc}")
            await client.disconnect()
            sys.exit(1)

    me = await client.get_me()
    if me is None:
        print("ERROR: Authentication failed — could not retrieve user info.")
        await client.disconnect()
        sys.exit(1)

    session_string = client.session.save()
    await client.disconnect()

    print()
    print("=" * 60)
    print("SUCCESS — authenticated as:")
    print(f"  User ID:  {me.id}")
    print(f"  Username: @{me.username or 'N/A'}")
    print(f"  Name:     {(me.first_name or '')} {(me.last_name or '')}".rstrip())
    print("=" * 60)
    _write_session_to_env(session_string)
    print()
    print("Done. fetch_andrew_chat.py can now read the client chat.")


def main() -> None:
    try:
        asyncio.run(_generate())
    except KeyboardInterrupt:
        print("\nCancelled.")
        sys.exit(0)


if __name__ == "__main__":
    main()
