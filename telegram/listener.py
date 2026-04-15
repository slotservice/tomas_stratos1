"""
Stratos1 - Telegram Listener (Telethon user session).

Monitors 100+ Telegram groups/channels for trading signals using a
**user session** (not a bot), because many private/restricted groups
do not allow bots.

The listener feeds every incoming text message to a callback, which is
expected to be the signal-parser pipeline entry point.
"""

from __future__ import annotations

import asyncio
from typing import Callable, Coroutine, Any, Dict, List, Optional

import structlog
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import (
    Channel,
    Chat,
    PeerChannel,
    PeerChat,
    User,
    MessageService,
)
from telethon.errors import (
    ChannelPrivateError,
    ChatAdminRequiredError,
    FloodWaitError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    UserAlreadyParticipantError,
)

from config.settings import TelegramSettings, TelegramGroup

log = structlog.get_logger(__name__)

# Type alias for the callback the listener invokes on every message.
# Signature: async callback(raw_text: str, channel_id: int, channel_name: str)
SignalCallback = Callable[[str, int, str], Coroutine[Any, Any, None]]


class TelegramListener:
    """
    Listens to configured Telegram groups for trading signals.

    Uses Telethon ``TelegramClient`` with a ``StringSession`` so the
    session string can be stored in config / environment and reused
    across restarts without re-authenticating.

    Parameters
    ----------
    settings:
        ``TelegramSettings`` instance with API credentials.
    groups:
        List of ``TelegramGroup`` objects to monitor.
    on_signal_callback:
        Async callable invoked for every qualifying message::

            await callback(raw_text, channel_id, channel_name)

    session_string:
        Optional saved Telethon ``StringSession`` string.  When empty,
        a fresh session is started (requires interactive login on first
        run).
    """

    def __init__(
        self,
        settings: TelegramSettings,
        groups: List[TelegramGroup],
        on_signal_callback: SignalCallback,
        session_string: str = "",
    ) -> None:
        self._settings = settings
        self._groups = groups
        self._callback = on_signal_callback
        self._session_string = session_string

        # Telethon client (created in start())
        self._client: Optional[TelegramClient] = None

        # chat_id -> display name mapping built from config
        self._group_map: Dict[int, str] = self._build_group_map()

        # Track which groups we successfully resolved
        self._resolved_ids: set[int] = set()

    # ------------------------------------------------------------------
    # Group map
    # ------------------------------------------------------------------

    def _build_group_map(self) -> Dict[int, str]:
        """
        Build a mapping of ``chat_id -> display_name`` from the
        configured groups list.

        Telegram chat IDs for channels/supergroups are typically large
        negative numbers.  We store both the raw ID and a "bare" variant
        (without the -100 prefix that Telethon sometimes strips) so
        lookups work regardless of which form an event reports.
        """
        mapping: Dict[int, str] = {}
        for group in self._groups:
            mapping[group.id] = group.name

            # Telethon internally represents channel IDs without the
            # -100 prefix.  Store that variant too for reliable lookups.
            bare_id = group.id
            if bare_id < -1_000_000_000_000:
                # e.g. -1002290339976 -> strip the -100 prefix
                bare = int(str(abs(bare_id))[3:])
                mapping[bare] = group.name
                mapping[-bare] = group.name

        log.info(
            "group_map_built",
            total_groups=len(self._groups),
            map_entries=len(mapping),
        )
        return mapping

    def _resolve_channel_name(self, chat_id: int) -> Optional[str]:
        """Look up the display name for a chat ID, returning None if unknown."""
        return self._group_map.get(chat_id)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """
        Create the Telethon client, connect, and register the message
        handler for all monitored groups.
        """
        log.info("telegram_listener_starting", groups=len(self._groups))

        session = StringSession(self._session_string)
        self._client = TelegramClient(
            session,
            int(self._settings.api_id),
            self._settings.api_hash,
            auto_reconnect=True,
            retry_delay=5,
            connection_retries=10,
            request_retries=5,
        )

        # Connect and authenticate
        await self._client.connect()

        if not await self._client.is_user_authorized():
            log.info("telegram_listener_login_required", phone=self._settings.phone)
            await self._client.send_code_request(self._settings.phone)
            # In production, the code is supplied via an external mechanism
            # (e.g. environment variable, interactive prompt during first
            # setup).  For headless deployment the session string must be
            # pre-generated and stored.
            raise RuntimeError(
                "Telegram user session is not authorized.  Run the "
                "session-generator script first to create a session string, "
                "then store it in the environment / config."
            )

        me = await self._client.get_me()
        log.info(
            "telegram_listener_authenticated",
            user_id=me.id,
            username=me.username,
        )

        # Save the session string for persistence
        self._session_string = self._client.session.save()

        # Attempt to join configured groups (best-effort)
        await self._join_groups()

        # Collect resolved entity IDs for the event filter
        chats = await self._resolve_group_entities()

        # Register the new-message handler
        if chats:
            self._client.add_event_handler(
                self._on_new_message,
                events.NewMessage(chats=chats),
            )
            log.info(
                "telegram_listener_handler_registered",
                monitored_chats=len(chats),
            )
        else:
            # Fallback: listen to ALL incoming messages and filter in
            # the handler.  This works but is less efficient.
            self._client.add_event_handler(
                self._on_new_message,
                events.NewMessage(),
            )
            log.warning(
                "telegram_listener_no_resolved_chats",
                fallback="listening_to_all_messages",
            )

        log.info("telegram_listener_started")

    async def stop(self) -> None:
        """Disconnect the Telethon client gracefully."""
        if self._client and self._client.is_connected():
            log.info("telegram_listener_stopping")
            await self._client.disconnect()
            log.info("telegram_listener_stopped")

    @property
    def session_string(self) -> str:
        """Return the current session string for persistence."""
        if self._client:
            return self._client.session.save()
        return self._session_string

    # ------------------------------------------------------------------
    # Group joining
    # ------------------------------------------------------------------

    async def _join_groups(self) -> None:
        """
        Attempt to join every configured group.

        Groups may be specified by numeric ID or invite link.  Errors
        are logged and swallowed -- a single unreachable group must not
        prevent the bot from monitoring the rest.
        """
        if not self._client:
            return

        for group in self._groups:
            try:
                entity = await self._client.get_entity(group.id)
                self._resolved_ids.add(group.id)
                log.debug(
                    "group_resolved",
                    group_id=group.id,
                    group_name=group.name,
                    entity_type=type(entity).__name__,
                )
            except UserAlreadyParticipantError:
                self._resolved_ids.add(group.id)
                log.debug(
                    "group_already_joined",
                    group_id=group.id,
                    group_name=group.name,
                )
            except ChannelPrivateError:
                log.warning(
                    "group_private",
                    group_id=group.id,
                    group_name=group.name,
                    hint="channel_is_private_or_banned",
                )
            except ChatAdminRequiredError:
                log.warning(
                    "group_admin_required",
                    group_id=group.id,
                    group_name=group.name,
                )
            except (InviteHashExpiredError, InviteHashInvalidError):
                log.warning(
                    "group_invite_invalid",
                    group_id=group.id,
                    group_name=group.name,
                )
            except FloodWaitError as exc:
                log.warning(
                    "group_flood_wait",
                    group_id=group.id,
                    group_name=group.name,
                    wait_seconds=exc.seconds,
                )
                # Wait the required time, then continue with remaining groups
                await asyncio.sleep(min(exc.seconds, 30))
            except Exception:
                log.exception(
                    "group_join_error",
                    group_id=group.id,
                    group_name=group.name,
                )

        log.info(
            "group_join_complete",
            resolved=len(self._resolved_ids),
            total=len(self._groups),
        )

    async def _resolve_group_entities(self) -> List[int]:
        """
        Return a list of chat IDs that Telethon can use as an event
        filter.  Only includes groups we successfully resolved.
        """
        if not self._client:
            return []

        resolved: List[int] = []
        for group in self._groups:
            try:
                entity = await self._client.get_input_entity(group.id)
                resolved.append(group.id)
            except Exception:
                # Already logged during _join_groups; silently skip
                pass

        return resolved

    # ------------------------------------------------------------------
    # Message handler
    # ------------------------------------------------------------------

    async def _on_new_message(self, event: events.NewMessage.Event) -> None:
        """
        Handler invoked for every new message in monitored groups.

        Filters:
        - Skip service messages (joins, pins, etc.)
        - Skip messages without text content
        - Skip messages from bots
        - Skip messages from chats not in our group map (when using
          the fallback "all messages" handler)

        For qualifying messages, passes the raw text, chat ID, and
        channel name to the registered callback.
        """
        try:
            # Skip service messages (user joined, pinned message, etc.)
            if isinstance(event.message, MessageService):
                return

            # Skip messages without text
            message = event.message
            if not message.text:
                return

            # Skip messages from bots
            sender = await event.get_sender()
            if sender and isinstance(sender, User) and sender.bot:
                log.debug(
                    "message_from_bot_skipped",
                    chat_id=event.chat_id,
                    bot_id=sender.id,
                )
                return

            # Resolve channel name from our group map
            chat_id = event.chat_id
            channel_name = self._resolve_channel_name(chat_id)

            if channel_name is None:
                # Message from a chat we don't monitor -- skip
                # (only happens in fallback "all messages" mode)
                return

            raw_text = message.text.strip()
            if not raw_text:
                return

            log.debug(
                "message_received",
                chat_id=chat_id,
                channel_name=channel_name,
                text_length=len(raw_text),
                text_preview=raw_text[:100],
            )

            # Invoke the signal processing callback
            await self._callback(raw_text, chat_id, channel_name)

        except FloodWaitError as exc:
            log.warning(
                "handler_flood_wait",
                wait_seconds=exc.seconds,
            )
            await asyncio.sleep(min(exc.seconds, 30))
        except Exception:
            log.exception(
                "message_handler_error",
                chat_id=getattr(event, "chat_id", None),
            )
