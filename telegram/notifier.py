"""
Stratos1 - Telegram Notifier (Bot API via Telethon).

Sends formatted HTML messages to the client's notification channel
using a Telegram **bot** token.  All message templates follow the
Swedish-language formats specified in the project documentation.

Design rules enforced by every template:
- Channel NAME is shown, never the numeric ID.
- Symbol always has a ``#`` prefix for Telegram hashtag history.
- Only real TP values are shown (no fake TP5=0 padding).
- IM (Initial Margin) and leverage come from confirmed Bybit state.
- Signal type shown as ``swing / dynamic / fixed``.
- Timestamps use ``core.time_utils.format_time()`` in Europe/Stockholm.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

import structlog
from telethon import TelegramClient
from telethon.sessions import StringSession

from config.settings import TelegramSettings
from core.time_utils import format_time, now_utc

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts() -> str:
    """Current timestamp formatted for display."""
    return format_time(now_utc())


def _sym(symbol: str) -> str:
    """Ensure symbol has a # prefix for hashtag tracking."""
    s = symbol.strip()
    if not s.startswith("#"):
        return f"#{s}"
    return s


def _chan(name: str) -> str:
    """Format channel name with # prefix for hashtag tracking.

    Converts e.g. "AiphaMint Signals" to "#AiphaMintSignals" so Telegram
    treats it as a clickable hashtag for history filtering.
    """
    if not name:
        return "#Unknown"
    # Strip # if already present, remove whitespace/special chars for hashtag.
    clean = name.lstrip("#").strip()
    # Telegram hashtags only allow alphanumerics and underscore.
    import re as _re
    hashtag = _re.sub(r"[^A-Za-z0-9_]", "", clean)
    if not hashtag:
        return f"{name} #Unknown"
    return f"{name} #{hashtag}"


def _tp_lines(tp_list: list[float]) -> str:
    """Build TP lines, only including non-zero real values."""
    lines: list[str] = []
    for i, tp in enumerate(tp_list, start=1):
        if tp and tp > 0:
            lines.append(f"   TP{i}: {tp}")
    return "\n".join(lines)


def _tp_lines_pct(tp_list: list[float], entry: float, direction: str) -> str:
    """Build TP lines with percentages, only real TPs (no zeros)."""
    lines: list[str] = []
    for i, tp in enumerate(tp_list, start=1):
        if tp and tp > 0 and entry > 0:
            if direction == "LONG":
                pct = (tp - entry) / entry * 100
            else:
                pct = (entry - tp) / entry * 100
            lines.append(f"🎯 TP{i}: {tp} ({pct:+.2f}%)")
    return "\n".join(lines)


def _sl_line_pct(sl: float, entry: float, direction: str) -> str:
    """Build SL line with percentage."""
    if not sl or not entry:
        return f"🚩 SL: {sl or 'Auto (-3%)'}"
    if direction == "LONG":
        pct = (sl - entry) / entry * 100
    else:
        pct = (entry - sl) / entry * 100
    return f"🚩 SL: {sl} ({pct:+.2f}%)"


def _lev_class(signal_type: str) -> str:
    """Leverage label mirrors the signal classification.

    swing/dynamic/fixed is a single taxonomy (per client 2026-04-24):
        fixed   -> SL was missing (auto-SL + x10 leverage)
        swing   -> wide SL (>4% distance)
        dynamic -> normal SL
    The leverage display label must match signal_type so the two lines
    of the notification tell a consistent story.
    """
    return signal_type or "dynamic"


def _pnl_sign(value: float) -> str:
    """Format PnL with explicit sign."""
    if value >= 0:
        return f"+{value:.2f}"
    return f"{value:.2f}"


def _pct(value: float) -> str:
    """Format a percentage with sign and % suffix."""
    if value >= 0:
        return f"+{value:.2f} %"
    return f"{value:.2f} %"


# ---------------------------------------------------------------------------
# Notifier
# ---------------------------------------------------------------------------

class TelegramNotifier:
    """
    Sends formatted notification messages to a Telegram channel via
    the Bot API (Telethon bot client).

    Parameters
    ----------
    settings:
        ``TelegramSettings`` with ``bot_token`` and ``notify_channel_id``.
    """

    def __init__(self, settings: TelegramSettings) -> None:
        self._settings = settings
        self._client: Optional[TelegramClient] = None
        self._channel_id: int = settings.notify_channel_id

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Connect the bot client."""
        log.info("telegram_notifier_starting")
        self._client = TelegramClient(
            StringSession(),
            int(self._settings.api_id),
            self._settings.api_hash,
        )
        await self._client.start(bot_token=self._settings.bot_token)
        me = await self._client.get_me()
        log.info(
            "telegram_notifier_started",
            bot_id=me.id,
            bot_username=me.username,
        )

    async def stop(self) -> None:
        """Disconnect the bot client."""
        if self._client and self._client.is_connected():
            log.info("telegram_notifier_stopping")
            await self._client.disconnect()
            log.info("telegram_notifier_stopped")

    # ------------------------------------------------------------------
    # Low-level send
    # ------------------------------------------------------------------

    async def send(
        self,
        channel_id: int,
        text: str,
        parse_mode: str = "HTML",
    ) -> None:
        """
        Send a message to the specified channel.

        Parameters
        ----------
        channel_id:
            Telegram chat/channel ID to send to.
        text:
            Message body (may contain HTML formatting).
        parse_mode:
            Telegram parse mode (``"HTML"`` or ``"Markdown"``).
        """
        if not self._client:
            log.error("notifier_not_started", text_preview=text[:80])
            return

        try:
            await self._client.send_message(
                channel_id,
                text,
                parse_mode=parse_mode,
            )
            log.debug(
                "notification_sent",
                channel_id=channel_id,
                text_length=len(text),
            )
        except Exception:
            log.exception(
                "notification_send_error",
                channel_id=channel_id,
                text_preview=text[:120],
            )

    async def _send_notify(self, text: str) -> str:
        """Send to the default notification channel and return the text."""
        await self.send(self._channel_id, text)
        return text

    # ===================================================================
    # SIGNAL LIFECYCLE TEMPLATES
    # ===================================================================

    async def signal_received(
        self,
        signal,
        leverage: float,
        im: float,
        bot_order_id: str,
        bybit_order_id: str,
    ) -> str:
        """Signal received from external group and forwarded to channel."""
        entry = signal.entry
        direction = signal.direction
        tp_block = _tp_lines_pct(signal.tps, entry, direction)
        sl_line = _sl_line_pct(signal.sl, entry, direction)
        lev_type = signal.signal_type
        bot_id_str = bot_order_id or "pending"
        bybit_id_str = bybit_order_id or "pending"

        text = (
            f"✅ Signal mottagen & kopierad\n"
            f"🕒 Time: {_ts()}\n"
            f"📢 From channel: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Direction: {direction}\n"
            f"📍 Type: {lev_type}\n"
            f"\n"
            f"💥 Entry: {entry}\n"
            f"{tp_block}\n"
            f"{sl_line}\n"
            f"\n"
            f"⚙️ Leverage ({_lev_class(lev_type)}): x{leverage}\n"
            f"💰 IM: {im:.2f} USDT\n"
            f"🔑 Order-ID BOT: {bot_id_str}\n"
            f"🔑 Order-ID Bybit: {bybit_id_str}"
        )
        return await self._send_notify(text)

    async def signal_blocked_duplicate(
        self,
        signal,
        existing_entry: float,
        reason: str = "",
    ) -> str:
        """Signal blocked as duplicate (within 5% of active trade).

        Format per Meddelande telegram.docx — exactly the four lines
        plus the header. No "Duplicate within X% ..." footer (the
        operator can derive that from the timing of the previous
        SIGNAL MOTTAGEN message in the channel; the doc spec explicitly
        ends at Riktning).
        """
        text = (
            f"⚠️ SIGNAL BLOCKERAD (Dubblett ≤5%) ⚠️\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}"
        )
        return await self._send_notify(text)

    async def symbol_not_on_bybit(self, signal, suggestion: str = None) -> str:
        """Signal rejected because the symbol is not on Bybit.

        If a 1000x/10000x/1000000x-prefixed variant exists on Bybit
        futures (common for small-price meme tokens), ``suggestion``
        carries that Bybit symbol. We surface it so the operator can
        verify/trade manually — we don't auto-trade because the signal
        prices are per-1-token and would be off by the prefix factor.
        """
        extra = ""
        if suggestion:
            extra = (
                f"\n📍 Bybit har symbolen som {suggestion} "
                f"(priser per-{suggestion[:-len(signal.symbol)] or '1'}-token)."
            )
        text = (
            f"⚠️ Finns inte på bybit ⚠️\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Fel: Kontrolera manuellt"
            f"{extra}"
        )
        return await self._send_notify(text)

    async def tp_sl_update_failed(self, signal, reason: str = "") -> str:
        """TP/SL could not be updated on an existing trade (>5% signal)."""
        text = (
            f"❌ TP/SL UPPDATERING MISSLYCKADES ❌\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Fel: {reason or 'Kontrollera manuellt'}"
        )
        return await self._send_notify(text)

    async def order_place_failed(self, signal, order_label: str = "entry1",
                                 reason: str = "") -> str:
        """Entry order could not be placed on Bybit."""
        text = (
            f"❌ ORDER MISSLYCKADES ❌\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Fel: {order_label} kunde inte placeras. "
            f"{reason or 'Kontrollera manuellt.'}"
        )
        return await self._send_notify(text)

    async def signal_updated_tp_sl(
        self,
        signal,
        leverage: float,
        im: float,
        bot_order_id: str,
        bybit_order_id: str,
    ) -> str:
        """Signal with >5% entry difference - updates TP/SL on existing trade."""
        entry = signal.entry
        direction = signal.direction
        tp_block = _tp_lines_pct(signal.tps, entry, direction)
        sl_line = _sl_line_pct(signal.sl, entry, direction)
        lev_type = signal.signal_type
        bot_id_str = bot_order_id or "pending"
        bybit_id_str = bybit_order_id or "pending"

        text = (
            f"✅ Signal updated - difference above 5%\n"
            f"🕒 Time: {_ts()}\n"
            f"📢 From channel: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Direction: {direction}\n"
            f"📍 Type: {lev_type}\n"
            f"\n"
            f"💥 Entry: {entry}\n"
            f"{tp_block}\n"
            f"{sl_line}\n"
            f"\n"
            f"⚙️ Leverage ({_lev_class(lev_type)}): x{leverage}\n"
            f"💰 IM: {im:.2f} USDT\n"
            f"🔑 Order-ID BOT: {bot_id_str}\n"
            f"🔑 Order-ID Bybit: {bybit_id_str}"
        )
        return await self._send_notify(text)

    async def order_placed(
        self,
        signal,
        leverage: float,
        im: float,
        entry1: float,
        entry2: float,
        bot_id: str,
        bybit_id: str,
    ) -> str:
        """Order placed on Bybit (before fill confirmation)."""
        entry = signal.entry
        direction = signal.direction
        tp_block = _tp_lines_pct(signal.tps, entry, direction)
        sl_line = _sl_line_pct(signal.sl, entry, direction)
        lev_type = signal.signal_type

        # Signals carry a single entry price; only split into Entry1 +
        # Entry2 when the two values actually differ (future-proof for
        # signals that ever provide a two-leg entry).
        if entry1 == entry2:
            entry_lines = f"💥 Entry: {entry1}"
        else:
            entry_lines = f"💥 Entry1: {entry1}\n💥 Entry2: {entry2}"

        text = (
            f"✅ Order placerad ({lev_type})\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {direction}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"{entry_lines}\n"
            f"\n"
            f"{tp_block}\n"
            f"{sl_line}\n"
            f"\n"
            f"⚙️ Hävstång ({_lev_class(lev_type)}): x{leverage}\n"
            f"💰 IM: {im:.2f} USDT\n"
            f"🔑 Order-ID BOT: {bot_id}\n"
            f"🔑 Order-ID Bybit: {bybit_id}"
        )
        return await self._send_notify(text)

    async def position_opened(
        self,
        trade,
        signal,
    ) -> str:
        """Position confirmed open on Bybit."""
        entry = signal.entry
        direction = signal.direction
        tp_block = _tp_lines_pct(signal.tps, entry, direction)
        sl_line = _sl_line_pct(trade.sl_price or signal.sl, entry, direction)
        lev_type = signal.signal_type
        bybit_ids = ', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'

        # Collapse to single Entry line when the two fills are
        # identical (1-order mode always, and 2-order mode when both
        # legs happened to fill at the same price).
        e1 = trade.entry1_fill_price
        e2 = trade.entry2_fill_price
        if e1 == e2 or e2 in (None, 0):
            entry_lines = f"💥 Entry: {e1}"
        else:
            entry_lines = f"💥 Entry1: {e1}\n💥 Entry2: {e2}"

        text = (
            f"✅ Position öppnad ({lev_type})\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {direction}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"{entry_lines}\n"
            f"\n"
            f"{tp_block}\n"
            f"{sl_line}\n"
            f"\n"
            f"⚙️ Hävstång ({_lev_class(lev_type)}): x{trade.leverage}\n"
            f"💰 IM: {trade.margin:.2f} USDT (Bybit confirmed)\n"
            f"🔑 Order-ID BOT: {trade.id}\n"
            f"🔑 Order-ID Bybit: {bybit_ids}"
        )
        return await self._send_notify(text)

    async def entry1_filled(
        self,
        trade,
        qty: float,
        im: float,
        im_total: float,
        bot_id: str,
        bybit_id: str,
        single_order: bool = False,
    ) -> str:
        """Entry filled notification.

        In 1-order mode (single_order=True) this is the only entry message
        and renders as 'POSITION OPPNAD'. In 2-order mode it renders as
        'ENTRY 1 TAGEN' and entry2_filled + entries_merged follow.
        """
        signal = trade.signal
        lev_type = signal.signal_type
        if single_order:
            header = "POSITION ÖPPNAD"
            entry_label = "Entry"
        else:
            header = "ENTRY 1 TAGEN"
            entry_label = "Entry1"

        # Client 2026-04-24: position-opened message must show the
        # full contract (entry, all TPs, SL, leverage) alongside fill
        # info so the operator sees everything in one message.
        fill_price = trade.entry1_fill_price or signal.entry
        tp_block = _tp_lines_pct(signal.tps, fill_price, signal.direction)
        sl_line = _sl_line_pct(trade.sl_price or signal.sl, fill_price, signal.direction)
        leverage = trade.leverage if trade.leverage else 0.0

        text = (
            f"✅ {header}\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"💥 {entry_label}: {fill_price}\n"
            f"💵 Kvantitet: {qty}\n"
            f"\n"
            f"{tp_block}\n"
            f"{sl_line}\n"
            f"\n"
            f"⚙️ Hävstång ({_lev_class(lev_type)}): x{leverage}\n"
            f"💰 IM: {im:.2f} USDT (IM totalt: {im_total:.2f} USDT)\n"
            f"🔑 Order-ID BOT: {bot_id}\n"
            f"🔑 Order-ID Bybit: {bybit_id}"
        )
        return await self._send_notify(text)

    async def entry2_filled(
        self,
        trade,
        qty: float,
        im: float,
        im_total: float,
        bot_id: str,
        bybit_id: str,
    ) -> str:
        """Entry 2 filled notification."""
        signal = trade.signal
        lev_type = signal.signal_type
        text = (
            f"✅ ENTRY 2 TAGEN\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"💥 Entry2: {trade.entry2_fill_price}\n"
            f"💵 Kvantitet: {qty}\n"
            f"💰 IM: {im:.2f} USDT (IM totalt: {im_total:.2f} USDT)\n"
            f"🔑 Order-ID BOT: {bot_id}\n"
            f"🔑 Order-ID Bybit: {bybit_id}"
        )
        return await self._send_notify(text)

    async def entries_merged(
        self,
        trade,
        entry1: float,
        qty1: float,
        im1: float,
        entry2: float,
        qty2: float,
        im2: float,
        avg_entry: float,
        total_qty: float,
        im_total: float,
    ) -> str:
        """Both entries merged into a single position."""
        signal = trade.signal
        lev_type = signal.signal_type
        bybit_ids = ', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'
        text = (
            f"✅ Sammanslagning av ENTRY 1 + ENTRY 2\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"📌 ENTRY 1\n"
            f"💥 Entry: {entry1}\n"
            f"💵 Kvantitet: {qty1}\n"
            f"💰 IM: {im1:.2f} USDT (IM totalt: {im_total:.2f} USDT)\n"
            f"\n"
            f"📌 ENTRY 2\n"
            f"💥 Entry: {entry2}\n"
            f"💵 Kvantitet: {qty2}\n"
            f"💰 IM: {im2:.2f} USDT (IM totalt: {im_total:.2f} USDT)\n"
            f"\n"
            f"📌 SAMMANSATT POSITION\n"
            f"💥 Genomsnittligt Entry: {avg_entry}\n"
            f"💵 Total kvantitet: {total_qty}\n"
            f"💰 IM totalt: {im_total:.2f} USDT\n"
            f"🔑 Order-ID BOT: {trade.id}\n"
            f"🔑 Order-ID Bybit: {bybit_ids}"
        )
        return await self._send_notify(text)

    async def tp_hit(
        self,
        trade,
        tp_level: int,
        tp_price: float,
        tp_pct: float,
        closed_qty: float,
        closed_pct: float,
        result_pct: float,
        result_usdt: float,
    ) -> str:
        """Take-profit level hit."""
        signal = trade.signal
        text = (
            f"<b>✅ TAKE PROFIT {tp_level} TAGEN</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Riktning: {signal.direction}\n"
            f"   TP{tp_level}-pris: {tp_price}\n"
            f"   TP{tp_level} avstånd: {_pct(tp_pct)}\n"
            f"   Stängd qty: {closed_qty} ({closed_pct:.1f} %)\n"
            f"\n"
            f"   Resultat (med hävstång): {_pct(result_pct)}\n"
            f"   Resultat USDT: {_pnl_sign(result_usdt)} USDT\n"
            f"\n"
            f"   🔑 Order-ID BOT: {trade.id}\n"
            f"   🔑 Order-ID Bybit: {', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'}"
        )
        return await self._send_notify(text)

    # ===================================================================
    # PYRAMID / SCALING TEMPLATES (Steps 1-7)
    # ===================================================================

    async def pyramid_step(
        self,
        trade,
        step_num: int,
        trigger_pct: float,
        price: float,
        qty: float,
        im_added: float,
        im_total: float,
        leverage: Optional[float] = None,
    ) -> str:
        """
        Pyramid / scaling step executed.

        Step behaviour:
        - Step 1 (+1.5%): IM added
        - Step 2 (+2.3%): SL moved to BE, no IM added
        - Step 3 (+2.4%): Leverage changed, no IM added
        - Step 4 (+2.5%): IM added
        - Steps 5-7: IM added at increasing percentages
        """
        signal = trade.signal

        # Build step-specific details
        if step_num == 1:
            detail = (
                f"   IM tillagd: {im_added:.2f} USDT\n"
                f"   IM Total: {im_total:.2f} USDT"
            )
        elif step_num == 2:
            detail = (
                f"   SL flyttad till Break-Even\n"
                f"   Ingen IM tillagd"
            )
        elif step_num == 3:
            lev_str = f"x{leverage}" if leverage else "uppdaterad"
            detail = (
                f"   Hävstång ändrad: {lev_str}\n"
                f"   Ingen IM tillagd"
            )
        elif step_num >= 4:
            detail = (
                f"   IM tillagd: {im_added:.2f} USDT\n"
                f"   IM Total: {im_total:.2f} USDT"
            )
        else:
            detail = (
                f"   IM tillagd: {im_added:.2f} USDT\n"
                f"   IM Total: {im_total:.2f} USDT"
            )

        text = (
            f"<b>📈 PYRAMID STEG {step_num} ({_pct(trigger_pct)})</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Riktning: {signal.direction}\n"
            f"   Trigger: {_pct(trigger_pct)}\n"
            f"   Pris: {price}\n"
            f"   Qty: {qty}\n"
            f"\n"
            f"{detail}\n"
            f"\n"
            f"   🔑 Order-ID BOT: {trade.id}\n"
            f"   🔑 Order-ID Bybit: {', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'}"
        )
        return await self._send_notify(text)

    # ===================================================================
    # TRAILING STOP TEMPLATES
    # ===================================================================

    async def trailing_activated(
        self,
        trade,
        trigger_pct: float,
        distance_pct: float,
        new_sl: float,
    ) -> str:
        """Trailing stop activated."""
        signal = trade.signal
        text = (
            f"<b>🔄 TRAILING STOP AKTIVERAD</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Riktning: {signal.direction}\n"
            f"   Trigger: {_pct(trigger_pct)}\n"
            f"   Trailing-avstånd: {distance_pct:.2f} %\n"
            f"   Ny SL: {new_sl}\n"
            f"\n"
            f"   🔑 Order-ID BOT: {trade.id}\n"
            f"   🔑 Order-ID Bybit: {', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'}"
        )
        return await self._send_notify(text)

    async def trailing_updated(
        self,
        trade,
        new_extreme: float,
        new_sl: float,
        distance_pct: float,
    ) -> str:
        """Trailing stop updated to new extreme."""
        signal = trade.signal
        text = (
            f"<b>🔄 TRAILING STOP UPPDATERAD</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Riktning: {signal.direction}\n"
            f"   Nytt extremvärde: {new_extreme}\n"
            f"   Ny SL: {new_sl}\n"
            f"   Trailing-avstånd: {distance_pct:.2f} %\n"
            f"\n"
            f"   🔑 Order-ID BOT: {trade.id}\n"
            f"   🔑 Order-ID Bybit: {', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'}"
        )
        return await self._send_notify(text)

    # ===================================================================
    # BREAK-EVEN TEMPLATE
    # ===================================================================

    async def breakeven_adjusted(
        self,
        trade,
        sl_moved_to: float,
    ) -> str:
        """Stop-loss moved to break-even."""
        signal = trade.signal
        text = (
            f"<b>⚖️ BREAK-EVEN JUSTERAD</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Riktning: {signal.direction}\n"
            f"   SL flyttad till: {sl_moved_to}\n"
            f"\n"
            f"   🔑 Order-ID BOT: {trade.id}\n"
            f"   🔑 Order-ID Bybit: {', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'}"
        )
        return await self._send_notify(text)

    # ===================================================================
    # HEDGE TEMPLATES
    # ===================================================================

    async def hedge_activated(
        self,
        trade,
        hedge_entry: float,
        hedge_sl: float,
        hedge_tp: float,
        leverage: float,
        im: float,
    ) -> str:
        """🛡️ HEDGE / VÄNDNING AKTIVERAD — per Meddelande telegram.docx
        (client 2026-04-28). Format note: the spec describes a reversal
        ('SL flyttad till entry, TP till original SL, gammal position
        stängd') but the bot currently runs hedges in PARALLEL — both
        legs stay open and close on their own SL/TP. The template below
        shows what actually happened on Bybit so the operator can
        verify against the position list."""
        signal = trade.signal
        lev_type = signal.signal_type if signal else "dynamic"
        bybit_ids = ', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'
        old_side = signal.direction
        new_side = "SHORT" if old_side == "LONG" else "LONG"
        text = (
            f"🛡️ HEDGE / VÄNDNING AKTIVERAD\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {old_side}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"📍 SL (hedge): {hedge_sl}\n"
            f"📍 TP (hedge): {hedge_tp}\n"
            f"📈 Tidigare position: {old_side}\n"
            f"📉 Ny motriktad position: {new_side}\n"
            f"💥 Entry (hedge): {hedge_entry}\n"
            f"\n"
            f"⚙️ Hävstång ({_lev_class(lev_type)}): x{leverage}\n"
            f"💰 IM: {im:.2f} USDT (Bybit confirmed)\n"
            f"🔑 Order-ID BOT: {trade.id}\n"
            f"🔑 Order-ID Bybit: {bybit_ids}"
        )
        return await self._send_notify(text)

    async def hedge_completed(
        self,
        trade,
        exit_price: float,
        qty: float,
        pct_of_position: float,
        result_pct: float,
        result_usdt: float,
    ) -> str:
        """🛡️ HEDGE / VÄNDNING AVSLUTAD — hedge leg closed on its own
        SL/TP. Per Meddelande telegram.docx (client 2026-04-28)."""
        signal = trade.signal
        lev_type = signal.signal_type if signal else "dynamic"
        bybit_ids = ', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'
        old_side = "SHORT" if signal.direction == "LONG" else "LONG"
        text = (
            f"🛡️ HEDGE / VÄNDNING AVSLUTAD\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"⚙️ Hävstång ({_lev_class(lev_type)})\n"
            f"📈 Stängd position: {old_side}\n"
            f"💥 Stängningspris: {exit_price}\n"
            f"\n"
            f"💵 Stängd kvantitet: {qty} ({pct_of_position:.1f}% av positionen)\n"
            f"📊 Resultat: {_pct(result_pct)} with leverage\n"
            f"💰 Resultat: {_pnl_sign(result_usdt)} USDT\n"
            f"\n"
            f"🔑 Order-ID BOT: {trade.id}\n"
            f"🔑 Order-ID Bybit: {bybit_ids}"
        )
        return await self._send_notify(text)

    async def hedge_cancelled(
        self,
        trade,
        reason: str,
    ) -> str:
        """🛡️ HEDGE AVBRUTEN. Per Meddelande telegram.docx."""
        signal = trade.signal
        lev_type = signal.signal_type if signal else "dynamic"
        text = (
            f"🛡️ HEDGE AVBRUTEN\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"📍 Skäl: {reason}"
        )
        return await self._send_notify(text)

    async def hedge_denied(
        self,
        trade,
        reason: str,
    ) -> str:
        """🛡️ HEDGE NEKAD — hedge conditions not met. Re-added per
        Meddelande telegram.docx (client 2026-04-28)."""
        signal = trade.signal
        lev_type = signal.signal_type if signal else "dynamic"
        text = (
            f"🛡️ HEDGE NEKAD\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"📍 Skäl: {reason}"
        )
        return await self._send_notify(text)

    # ===================================================================
    # RE-ENTRY TEMPLATES
    # ===================================================================

    async def reentry_activated(
        self,
        trade,
        signal,
        leverage: float,
        im: float,
        max_reentries: int = 2,
    ) -> str:
        """♻️ RE-ENTRY / ÅTERINTRÄDE AKTIVERAD per Meddelande
        telegram.docx (client 2026-04-28). Lists the full TP / SL
        block for the new trade so the operator sees exactly what
        was placed on Bybit, plus the försök counter (e.g. 1/2)."""
        lev_type = signal.signal_type if signal else "dynamic"
        bybit_ids = ', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'
        entry = trade.avg_entry or signal.entry
        sl = trade.sl_price or getattr(signal, "sl", None)

        # Build TP block from signal (avoids depending on trade state).
        tp_list = (
            getattr(signal, "tps", None)
            or getattr(signal, "tp_list", None)
            or []
        )
        tp_lines: list[str] = []
        for i, tp in enumerate(tp_list, start=1):
            if not tp:
                continue
            if entry and entry > 0:
                if signal.direction == "LONG":
                    pct = (tp - entry) / entry * 100.0
                else:
                    pct = (entry - tp) / entry * 100.0
            else:
                pct = 0.0
            tp_lines.append(f"🎯 TP{i}: {tp} ({_pct(pct)})")
        tps_block = "\n".join(tp_lines)

        if sl and entry and entry > 0:
            if signal.direction == "LONG":
                sl_pct = (sl - entry) / entry * 100.0
            else:
                sl_pct = (entry - sl) / entry * 100.0
            sl_line = f"🚩 SL: {sl} ({_pct(sl_pct)})"
        else:
            sl_line = f"🚩 SL: {sl if sl else 'N/A'}"

        text = (
            f"♻️ RE-ENTRY / ÅTERINTRÄDE AKTIVERAD\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"💥 Entry: {entry}\n"
        )
        if tps_block:
            text += f"{tps_block}\n"
        text += (
            f"{sl_line}\n"
            f"\n"
            f"⚙️ Hävstång ({_lev_class(lev_type)}): x{leverage}\n"
            f"💰 IM: {im:.2f} USDT (Bybit confirmed)\n"
            f"📌 Försök: {trade.reentry_count}/{max_reentries}\n"
            f"🔑 Order-ID BOT: {trade.id}\n"
            f"🔑 Order-ID Bybit: {bybit_ids}"
        )
        return await self._send_notify(text)

    async def reentry_completed(
        self,
        trade,
        exit: float,
        qty: float,
        result_pct: float,
        result_usdt: float,
    ) -> str:
        """♻️ RE-ENTRY / ÅTERINTRÄDE AVSLUTAD per Meddelande spec."""
        signal = trade.signal
        lev_type = signal.signal_type if signal else "dynamic"
        bybit_ids = ', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'
        leverage = trade.leverage if trade.leverage else 0.0
        text = (
            f"♻️ RE-ENTRY / ÅTERINTRÄDE AVSLUTAD\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"💥 Exit: {exit}\n"
            f"⚙️ Hävstång ({_lev_class(lev_type)}): x{leverage}\n"
            f"💵 Stängd kvantitet: {qty} (100% av positionen)\n"
            f"📊 Resultat: {_pct(result_pct)} with leverage\n"
            f"💰 Resultat: {_pnl_sign(result_usdt)} USDT\n"
            f"\n"
            f"🔑 Order-ID BOT: {trade.id}\n"
            f"🔑 Order-ID Bybit: {bybit_ids}"
        )
        return await self._send_notify(text)

    async def reentry_exhausted(
        self,
        trade,
        max_reentries: int = 2,
    ) -> str:
        """⛔ RE-ENTRY AVSTÄNGT — all attempts used up. Per spec the
        message ends with 'Väntar på ny extern signal'."""
        signal = trade.signal
        lev_type = signal.signal_type if signal else "dynamic"
        text = (
            f"⛔ RE-ENTRY AVSTÄNGT ({max_reentries}/{max_reentries} försök gjorda)\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"📌 Väntar på ny extern signal"
        )
        return await self._send_notify(text)

    # ===================================================================
    # SL / CLOSE TEMPLATES
    # ===================================================================

    async def position_closed(
        self,
        trade,
        exit_price: float,
        qty: float,
        result_pct_total: float,
        result_usdt_total: float,
        close_source: str = "",
    ) -> str:
        """Full position closed.

        ``close_source`` is the human-readable suffix shown in the
        header — driven by the Bybit fill event that closed the
        position (client 2026-04-28: "POSITION CLOSED - stop loss",
        "POSITION CLOSED - TP3", "POSITION CLOSED - trailing stop",
        "POSITION CLOSED - liquidation", "POSITION CLOSED - external
        close"). The bot never invents this value — it comes from
        ``stopOrderType`` / ``execType`` on the order-update event.
        """
        signal = trade.signal
        lev_type = signal.signal_type if signal else "dynamic"
        bybit_ids = ', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'
        header_suffix = f"   by {close_source}" if close_source else ""
        text = (
            f"✅ POSITION STÄNGD{header_suffix}\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"💵 Stängd kvantitet: {qty} (100%)\n"
            f"📚 Underlag ink. alla delsteg (BOT/Bybit): {trade.id} / {bybit_ids}\n"
            f"📍 Exit: {exit_price}\n"
            f"\n"
            f"📊 Resultat (prisrörelse): {_pct(result_pct_total)} with leverage\n"
            f"💰 Resultat (USDT, inkl. hävstång/notional): {_pnl_sign(result_usdt_total)} USDT\n"
            f"🔑 Order-ID BOT: {trade.id}\n"
            f"🔑 Order-ID Bybit: {bybit_ids}"
        )
        return await self._send_notify(text)

    async def take_profit_hit(
        self,
        trade,
        tp_level: int,
        tp_price: float,
        tp_pct: float,
        closed_qty: float,
        closed_pct: float,
        result_pct: float,
        result_usdt: float,
    ) -> str:
        """TAKE PROFIT {N} TAGEN — per-TP partial-close notification.

        Lists every TP from the signal with ``✅`` for the levels that
        have already filled (TP1..tp_level) and ``🎯`` for the ones
        still pending. Per Meddelande telegram.docx (client 2026-04-28
        update — TP1 ✅ stays ticked when later levels also fill).
        """
        signal = trade.signal
        lev_type = signal.signal_type if signal else "dynamic"
        bybit_ids = ', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'
        entry = trade.avg_entry or (signal.entry if signal else 0)
        leverage = trade.leverage if trade.leverage else 0.0

        # Build the TP list with ✅ (hit) / 🎯 (pending) markers.
        all_tps = (
            getattr(signal, "tps", None)
            or getattr(signal, "tp_list", None)
            or []
        ) if signal else []
        tp_lines: list[str] = []
        for i, tp in enumerate(all_tps, start=1):
            if not tp:
                continue
            if entry and entry > 0:
                if signal and signal.direction == "LONG":
                    pct = (tp - entry) / entry * 100.0
                else:
                    pct = (entry - tp) / entry * 100.0
            else:
                pct = 0.0
            marker = "✅" if i <= tp_level else "🎯"
            tp_lines.append(f"{marker} TP{i}: {tp} ({_pct(pct)})")

        tps_block = "\n".join(tp_lines) if tp_lines else f"🎯 TP{tp_level}: {tp_price} ({_pct(tp_pct)})"

        text = (
            f"✅ TAKE PROFIT {tp_level} TAGEN\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"💥 Entry: {entry}\n"
            f"⚙️ Hävstång ({_lev_class(lev_type)}): x{leverage}\n"
            f"\n"
            f"{tps_block}\n"
            f"\n"
            f"💵 Stängd kvantitet: {closed_qty} ({closed_pct:.1f}% av positionen)\n"
            f"📊 Resultat: {_pct(result_pct)} with leverage\n"
            f"💰 Resultat: {_pnl_sign(result_usdt)} USDT\n"
            f"\n"
            f"🔑 Order-ID BOT: {trade.id}\n"
            f"🔑 Order-ID Bybit: {bybit_ids}"
        )
        return await self._send_notify(text)

    async def trailing_stop_activated(
        self,
        trade,
        activation_price: float,
        trailing_distance: float,
        activation_pct: float,
        distance_pct: float,
    ) -> str:
        """TRAILING STOP AKTIVERAD — fired when the bot arms Bybit's
        native trailing stop on the position at trade open. Per
        Meddelande telegram.docx (client 2026-04-28)."""
        signal = trade.signal
        lev_type = signal.signal_type if signal else "dynamic"
        bybit_ids = ', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'
        text = (
            f"🔄 TRAILING STOP AKTIVERAD\n"
            f"🕒 Tid: {_ts()}\n"
            f"📢 Från kanal: {_chan(signal.channel_name)}\n"
            f"📊 Symbol: {_sym(signal.symbol)}\n"
            f"📈 Riktning: {signal.direction}\n"
            f"📍 Typ: {lev_type}\n"
            f"\n"
            f"📍 Trigger: {_pct(activation_pct)} (aktiveringspris {activation_price})\n"
            f"📍 Avstånd: {distance_pct:+.2f}% bakom pris ({trailing_distance})\n"
            f"\n"
            f"🔑 Order-ID BOT: {trade.id}\n"
            f"🔑 Order-ID Bybit: {bybit_ids}"
        )
        return await self._send_notify(text)

    # ===================================================================
    # SPECIAL TEMPLATES
    # ===================================================================

    async def auto_sl_applied(
        self,
        trade,
        auto_sl_price: float,
        locked_leverage: float,
    ) -> str:
        """Auto stop-loss and locked leverage applied (no SL in signal)."""
        signal = trade.signal
        text = (
            f"<b>🛡️ AUTO-SL OCH HÄVSTÅNG LÅST</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Riktning: {signal.direction}\n"
            f"   Auto-SL: {auto_sl_price} (-3 % från entry)\n"
            f"   Hävstång låst: x{locked_leverage}\n"
            f"\n"
            f"   🔑 Order-ID BOT: {trade.id}\n"
            f"   🔑 Order-ID Bybit: {', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'}"
        )
        return await self._send_notify(text)

    async def partial_fill(
        self,
        trade,
        qty_filled: float,
        qty_total: float,
        avg_fill: float,
    ) -> str:
        """Order partially filled."""
        signal = trade.signal
        fill_pct = (qty_filled / qty_total * 100) if qty_total > 0 else 0
        text = (
            f"<b>🧩 ORDER DELVIS FYLLD</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Riktning: {signal.direction}\n"
            f"   Fylld: {qty_filled} / {qty_total} ({fill_pct:.1f} %)\n"
            f"   Avg Fill: {avg_fill}\n"
            f"\n"
            f"   🔑 Order-ID BOT: {trade.id}\n"
            f"   🔑 Order-ID Bybit: {', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'}"
        )
        return await self._send_notify(text)

    async def order_timeout_deleted(
        self,
        trade,
        timeout_hours: int,
    ) -> str:
        """Unfilled order deleted after timeout."""
        signal = trade.signal
        text = (
            f"<b>🗑️ ORDER RADERAD (Timeout {timeout_hours} hours)</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Riktning: {signal.direction}\n"
            f"   Timeout: {timeout_hours} timmar\n"
            f"\n"
            f"   Order ej fylld inom tidsgränsen — raderad.\n"
            f"\n"
            f"   🔑 Order-ID BOT: {trade.id}\n"
            f"   🔑 Order-ID Bybit: {', '.join(trade.bybit_order_ids) if trade.bybit_order_ids else 'N/A'}"
        )
        return await self._send_notify(text)

    async def signal_queued(
        self,
        signal,
        leverage: float,
        im: float,
    ) -> str:
        """Signal queued (limit above/below current price)."""
        tp_block = _tp_lines(signal.tps)
        sl_line = f"   SL: {signal.sl}" if signal.sl else "   SL: Auto (-3 %)"

        text = (
            f"<b>📬 SIGNAL KÖAD (Limit Above/Below)</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Riktning: {signal.direction}\n"
            f"   Entry: {signal.entry}\n"
            f"{tp_block}\n"
            f"{sl_line}\n"
            f"   Hävstång: x{leverage}\n"
            f"   IM: {im:.2f} USDT\n"
            f"\n"
            f"   Väntar på att priset når entry-nivån."
        )
        return await self._send_notify(text)

    # ===================================================================
    # ERROR TEMPLATES
    # ===================================================================

    async def error_signal_invalid(
        self,
        signal,
        error_detail: str,
    ) -> str:
        """Signal could not be parsed or validated."""
        text = (
            f"<b>❌ SIGNAL OGILTIG</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Fel: {error_detail}\n"
            f"\n"
            f"   Signalen kunde inte tolkas korrekt."
        )
        return await self._send_notify(text)

    async def error_order_failed(
        self,
        signal,
        error_detail: str,
    ) -> str:
        """Order placement failed on Bybit."""
        text = (
            f"<b>❌ ORDER MISSLYCKADES, SIGNAL OGILTIG</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Riktning: {signal.direction}\n"
            f"   Fel: {error_detail}"
        )
        return await self._send_notify(text)

    async def error_capacity_reached(
        self,
        active_count: int,
        max_count: int,
    ) -> str:
        """Maximum active trade capacity reached."""
        text = (
            f"<b>❌ KAPACITET NÅDD</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Aktiva positioner: {active_count} / {max_count}\n"
            f"\n"
            f"   Nya signaler ignoreras tills en position stängs."
        )
        return await self._send_notify(text)

    async def system_reconnected(self) -> str:
        """System reconnected after a disconnection."""
        text = (
            f"<b>🔁 SYSTEM ÅTERANSLUTET</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"\n"
            f"   Anslutningen till Telegram och Bybit har återupprättats."
        )
        return await self._send_notify(text)

    async def state_restored(
        self,
        positions_verified: int,
        sl_tp_restored: int,
    ) -> str:
        """State restored after restart."""
        text = (
            f"<b>🧷 ÅTERSTÄLLNING GENOMFÖRD</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Positioner verifierade: {positions_verified}\n"
            f"   SL/TP återställda: {sl_tp_restored}\n"
            f"\n"
            f"   Alla aktiva positioner har synkroniserats med Bybit."
        )
        return await self._send_notify(text)

    async def orderloop_protection(
        self,
        symbol: str,
        block_window: int,
    ) -> str:
        """Order-loop safety protection triggered."""
        text = (
            f"<b>🛡️ SKYDD TRIGGAT (Orderloop)</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Symbol: {_sym(symbol)}\n"
            f"   Blockfönster: {block_window} sekunder\n"
            f"\n"
            f"   För många order på kort tid — symbol blockerad tillfälligt."
        )
        return await self._send_notify(text)

    async def error_position_not_opened(
        self,
        signal,
        error_detail: str,
    ) -> str:
        """Position could not be opened."""
        text = (
            f"<b>❌ POSITION EJ ÖPPNAD</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Riktning: {signal.direction}\n"
            f"   Fel: {error_detail}"
        )
        return await self._send_notify(text)

    async def error_position_not_closed(
        self,
        signal,
        error_detail: str,
    ) -> str:
        """Position could not be closed."""
        text = (
            f"<b>❌ POSITION EJ STÄNGD</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Riktning: {signal.direction}\n"
            f"   Fel: {error_detail}"
        )
        return await self._send_notify(text)

    async def error_no_money(self) -> str:
        """Insufficient funds."""
        text = (
            f"<b>❌ SLUT PÅ PENGAR</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"\n"
            f"   Otillräckligt saldo för att öppna ny position.\n"
            f"   Kontrollera wallet-balans på Bybit."
        )
        return await self._send_notify(text)

    async def error_tp_not_executed(
        self,
        signal,
        error_detail: str,
    ) -> str:
        """Take-profit order could not be placed."""
        text = (
            f"<b>❌ TP EJ UTFÖRD</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Fel: {error_detail}"
        )
        return await self._send_notify(text)

    async def error_sl_not_executed(
        self,
        signal,
        error_detail: str,
    ) -> str:
        """Stop-loss order could not be placed."""
        text = (
            f"<b>❌ SL EJ UTFÖRD</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"   Kanal: {_chan(signal.channel_name)}\n"
            f"   Symbol: {_sym(signal.symbol)}\n"
            f"   Fel: {error_detail}"
        )
        return await self._send_notify(text)

    async def error_telegram_api(self) -> str:
        """Telegram API error."""
        text = (
            f"<b>❌ API FEL TELEGRAM</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"\n"
            f"   Kunde inte kommunicera med Telegram API.\n"
            f"   Kontrollera nätverksanslutning och API-nycklar."
        )
        return await self._send_notify(text)

    async def error_bybit_api(self) -> str:
        """Bybit API error."""
        text = (
            f"<b>❌ API FEL BYBIT</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"\n"
            f"   Kunde inte kommunicera med Bybit API.\n"
            f"   Kontrollera nätverksanslutning och API-nycklar."
        )
        return await self._send_notify(text)

    async def error_system(self) -> str:
        """Generic system error."""
        text = (
            f"<b>❌ SYSTEM FEL BOT</b>\n"
            f"\n"
            f"   Tid: {_ts()}\n"
            f"\n"
            f"   Ett oväntat systemfel har inträffat.\n"
            f"   Kontrollera loggar för detaljer."
        )
        return await self._send_notify(text)

    async def error_health_check(
        self,
        component: str,
        check_name: str,
        error_msg: str,
        env: str,
        version: str,
        trace_id: str,
        session_id: str,
    ) -> str:
        """Health check failed during system startup."""
        text = (
            f"<b>❌ SYSTEMSTART MISSLYCKADES — Hälsokontroll fel</b>\n"
            f"\n"
            f"   Tid: {_ts()} (Europe/Stockholm)\n"
            f"   Komponent: {component}\n"
            f"   Kontroll: {check_name}\n"
            f"   Felmeddelande: {error_msg}\n"
            f"\n"
            f"   Miljö: {env}\n"
            f"   Version: {version}\n"
            f"   Trace-ID: {trace_id}\n"
            f"   Session-ID: {session_id}\n"
            f"\n"
            f"   Systemet kunde inte starta.\n"
            f"   Åtgärda felet och starta om."
        )
        return await self._send_notify(text)
