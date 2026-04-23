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

        # Load all dialogs into the session entity cache.
        # This is CRITICAL: Telethon cannot resolve numeric channel IDs
        # unless the entity has been "seen" via get_dialogs() first.
        log.info("telegram_listener_loading_dialogs")
        dialogs = await self._client.get_dialogs()
        dialog_ids = set()
        for d in dialogs:
            if d.entity and hasattr(d.entity, "id"):
                dialog_ids.add(d.entity.id)
                # Also store with -100 prefix for matching
                dialog_ids.add(-int(f"100{d.entity.id}"))
        log.info(
            "telegram_listener_dialogs_loaded",
            total_dialogs=len(dialogs),
            dialog_ids_count=len(dialog_ids),
        )

        # Check which configured groups are in our dialogs
        matched = 0
        for group in self._groups:
            bare_id = group.id
            if bare_id < -1_000_000_000_000:
                bare = int(str(abs(bare_id))[3:])
            else:
                bare = abs(bare_id)
            if bare in dialog_ids or bare_id in dialog_ids or group.id in dialog_ids:
                self._resolved_ids.add(group.id)
                matched += 1

        log.info(
            "telegram_listener_groups_matched",
            matched=matched,
            total_configured=len(self._groups),
        )

        # Listen to ALL incoming AND outgoing messages and filter by chat
        # ID in the handler. outgoing=True is needed to catch messages
        # posted in channels where the user is an admin (the user's own
        # posts appear as "outgoing" in Telethon).
        # Single handler for ALL messages (both incoming and outgoing).
        # Using two handlers caused each message to be processed twice,
        # creating a race condition in duplicate detection.
        self._client.add_event_handler(
            self._on_new_message,
            events.NewMessage(),
        )
        log.info(
            "telegram_listener_handler_registered",
            mode="all_messages_single_handler",
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

            # Log every incoming message for debugging
            log.info(
                "message_incoming",
                chat_id=event.chat_id,
                text_preview=message.text[:80] if message.text else "",
            )

            # Skip messages from bots (but NOT from channels - channels
            # have no "sender" or sender is the channel itself)
            try:
                sender = await event.get_sender()
                if sender and isinstance(sender, User) and sender.bot:
                    log.debug(
                        "message_from_bot_skipped",
                        chat_id=event.chat_id,
                        bot_id=sender.id,
                    )
                    return
            except Exception:
                # Channel posts may not have a sender - that's fine
                pass

            # Resolve channel name from our group map.
            # Telethon may report chat_id in various forms:
            #   - positive (e.g. 2290339976)
            #   - negative (e.g. -2290339976)
            #   - with -100 prefix (e.g. -1002290339976)
            # We check all variants against our map.
            chat_id = event.chat_id
            channel_name = self._resolve_channel_name(chat_id)

            if channel_name is None and chat_id:
                # Try with -100 prefix
                prefixed = -int(f"100{abs(chat_id)}")
                channel_name = self._resolve_channel_name(prefixed)
                if channel_name:
                    chat_id = prefixed

            if channel_name is None and chat_id:
                # Try stripping -100 prefix
                abs_id = abs(chat_id)
                if abs_id > 1_000_000_000_000:
                    bare = int(str(abs_id)[3:])
                    channel_name = self._resolve_channel_name(bare)
                    if not channel_name:
                        channel_name = self._resolve_channel_name(-bare)

            if channel_name is None:
                # Message from a chat we don't monitor -- skip
                log.info(
                    "message_unmonitored_chat",
                    chat_id=event.chat_id,
                    text_preview=(message.text or "")[:50],
                )
                return

            raw_text = message.text.strip()
            if not raw_text:
                return

            # If this message was forwarded from another channel, append
            # the original source so the client can tell apart "native"
            # posts from forwards (client feedback 2026-04-24: SUI/XRP
            # signals arriving via OkxFreecryptosignal were actually
            # forwarded auto-generated signals, not that channel's own
            # posts). Telethon exposes forward metadata on message.forward.
            fwd = getattr(message, "forward", None)
            fwd_name: Optional[str] = None
            if fwd is not None:
                try:
                    fwd_chat = getattr(fwd, "chat", None)
                    if fwd_chat is not None:
                        fwd_name = (
                            getattr(fwd_chat, "title", None)
                            or getattr(fwd_chat, "username", None)
                            or getattr(fwd_chat, "first_name", None)
                        )
                    if not fwd_name:
                        fwd_sender = getattr(fwd, "sender", None)
                        if fwd_sender is not None:
                            fwd_name = (
                                getattr(fwd_sender, "title", None)
                                or getattr(fwd_sender, "username", None)
                                or getattr(fwd_sender, "first_name", None)
                            )
                    if not fwd_name:
                        fwd_name = getattr(fwd, "from_name", None)
                except Exception:
                    fwd_name = None
            if fwd_name:
                channel_name = f"{channel_name} (fwd: {fwd_name})"

            log.info(
                "message_received",
                chat_id=chat_id,
                channel_name=channel_name,
                forwarded=bool(fwd_name),
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
