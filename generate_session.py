"""
Stratos1 - Telethon Session String Generator
----------------------------------------------
Utility script to create a Telethon StringSession for the Telegram
user account.  The resulting session string must be stored in the
.env file as TG_SESSION_STRING (or set as an environment variable)
so the bot can authenticate without interactive login on subsequent
starts.

Usage:
    python generate_session.py

The script reads TG_API_ID and TG_API_HASH from the .env file in the
project root, then prompts interactively for the phone number and
verification code.

After successful authentication, the session string is printed to the
console.  Copy it into your .env file:

    TG_SESSION_STRING=<the long string>
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

# Load .env before importing anything that might need the variables.
try:
    from dotenv import load_dotenv
except ImportError:
    print("ERROR: python-dotenv is not installed.")
    print("Run: pip install python-dotenv")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except ImportError:
    print("ERROR: telethon is not installed.")
    print("Run: pip install telethon")
    sys.exit(1)


async def _generate() -> None:
    """Run the interactive session generation flow."""
    api_id = os.environ.get("TG_API_ID", "")
    api_hash = os.environ.get("TG_API_HASH", "")

    if not api_id or not api_hash:
        print("ERROR: TG_API_ID and TG_API_HASH must be set in .env")
        print()
        print("Example .env file:")
        print("  TG_API_ID=12345678")
        print("  TG_API_HASH=abcdef0123456789abcdef0123456789")
        sys.exit(1)

    print("=" * 60)
    print("Stratos1 - Telegram Session Generator")
    print("=" * 60)
    print()
    print(f"  API ID:   {api_id}")
    print(f"  API Hash: {api_hash[:8]}...")
    print()

    # Create a client with a fresh StringSession.
    client = TelegramClient(
        StringSession(),
        int(api_id),
        api_hash,
    )

    await client.connect()

    # Prompt for phone number.
    phone = input("Enter your phone number (with country code, e.g. +46...): ").strip()
    if not phone:
        print("ERROR: Phone number is required.")
        await client.disconnect()
        sys.exit(1)

    # Send the verification code.
    print(f"Sending verification code to {phone}...")
    await client.send_code_request(phone)

    # Prompt for the code.
    code = input("Enter the verification code you received: ").strip()
    if not code:
        print("ERROR: Verification code is required.")
        await client.disconnect()
        sys.exit(1)

    # Try to sign in.
    try:
        await client.sign_in(phone, code)
    except Exception as exc:
        # If 2FA is enabled, prompt for password.
        if "password" in str(exc).lower() or "two" in str(exc).lower():
            password = input("Two-factor authentication is enabled. Enter your password: ").strip()
            await client.sign_in(password=password)
        else:
            print(f"ERROR: Sign-in failed: {exc}")
            await client.disconnect()
            sys.exit(1)

    # Verify we are authorised.
    me = await client.get_me()
    if me is None:
        print("ERROR: Authentication failed -- could not retrieve user info.")
        await client.disconnect()
        sys.exit(1)

    # Save the session string.
    session_string = client.session.save()

    print()
    print("=" * 60)
    print("SUCCESS! Authenticated as:")
    print(f"  User ID:  {me.id}")
    print(f"  Username: @{me.username or 'N/A'}")
    print(f"  Name:     {me.first_name or ''} {me.last_name or ''}")
    print("=" * 60)
    print()
    print("Your session string (add this to your .env file):")
    print()
    print(f"TG_SESSION_STRING={session_string}")
    print()
    print("IMPORTANT: Keep this string secret! Anyone with it can")
    print("access your Telegram account.")

    await client.disconnect()


def main() -> None:
    """Entry point."""
    try:
        asyncio.run(_generate())
    except KeyboardInterrupt:
        print("\nCancelled.")
        sys.exit(0)


if __name__ == "__main__":
    main()
