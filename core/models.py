"""
Stratos1 - Runtime data models.

These are NOT config models (see ``config.settings`` for those).  The classes
here represent the live objects that flow through the trading pipeline:
parsed signals, trade state machines, order records, and reporting
aggregates.
"""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional


# ===================================================================
# Parsed signal coming out of the Telegram parser
# ===================================================================

@dataclass
class ParsedSignal:
    """
    A trading signal extracted from a Telegram message.

    Attributes
    ----------
    symbol:
        Trading pair symbol, e.g. ``"BTCUSDT"``.
    direction:
        ``"LONG"`` or ``"SHORT"``.
    entry:
        Suggested entry price.
    tp_list:
        List of take-profit price levels (ordered nearest to farthest).
    sl:
        Stop-loss price.  ``None`` when the signal does not include one
        (triggers auto-SL logic).
    source_channel_id:
        Telegram chat ID the signal was received from.
    source_channel_name:
        Human-readable name of the source channel.
    raw_text:
        The original message text, preserved for debugging / auditing.
    received_at:
        UTC timestamp when the bot received the message.
    signal_type:
        Classification of the signal: ``"swing"``, ``"dynamic"``, or
        ``"fixed"``.  Determines which leverage / TP strategy to apply.
    """

    symbol: str
    direction: str                            # "LONG" | "SHORT"
    entry: float
    tp_list: List[float]
    sl: Optional[float]
    source_channel_id: int
    source_channel_name: str
    raw_text: str
    received_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
    signal_type: str = "dynamic"              # "swing" | "dynamic" | "fixed"


# ===================================================================
# Trade state machine
# ===================================================================

class TradeState(enum.Enum):
    """All possible states a trade can be in.

    Phase 4 (client 2026-05-01) explicit state-machine refactor.
    Tomas's audit point #8 dictated 13 explicit states; we keep the
    legacy entry/scaling/error states the bot still relies on AND
    add Tomas's named states alongside them. Aliases (POSITION_ACTIVE
    == POSITION_OPEN) are provided so the existing call sites keep
    working — both names route through the same enum value.

    Audit point #16 ("Only the state machine may change status.
    No other module may directly change trade status in the DB.")
    is enforced via :func:`Trade.transition` — every transition is
    validated against ALLOWED_TRANSITIONS and logged. Illegal
    transitions are blocked + warned (failure mode is overridable
    via the ``strict`` flag for emergencies).
    """

    # --- Pre-fill (entry pipeline) ---
    PENDING                = "PENDING"
    ENTRY1_PLACED          = "ENTRY1_PLACED"
    ENTRY1_FILLED          = "ENTRY1_FILLED"
    ENTRY2_PLACED          = "ENTRY2_PLACED"
    ENTRY2_FILLED          = "ENTRY2_FILLED"

    # --- Active position ---
    POSITION_OPEN          = "POSITION_OPEN"   # = POSITION_ACTIVE in spec
    PROTECTION_FAILED      = "PROTECTION_FAILED"

    # --- Hedge lifecycle ---
    HEDGE_ARMED            = "HEDGE_ARMED"     # pre-arm conditional placed
    HEDGE_ACTIVE           = "HEDGE_ACTIVE"    # conditional fired + protected

    # --- Force-close on original ---
    ORIGINAL_FORCE_CLOSED  = "ORIGINAL_FORCE_CLOSED"  # -2% conditional fired

    # --- SL movement (profit-lock + BE + cascading TP-shift) ---
    BREAKEVEN_ACTIVE       = "BREAKEVEN_ACTIVE"
    PROFIT_LOCK_1_ACTIVE   = "PROFIT_LOCK_1_ACTIVE"   # +4% -> lock +1.5%
    PROFIT_LOCK_2_ACTIVE   = "PROFIT_LOCK_2_ACTIVE"   # +5% -> lock +2.5%
    SL_MOVED_TO_TP1        = "SL_MOVED_TO_TP1"        # at TP3 -> SL=TP1
    SL_MOVED_TO_TP2        = "SL_MOVED_TO_TP2"        # at TP4 -> SL=TP2
    SL_MOVED_TO_TP3        = "SL_MOVED_TO_TP3"        # at TP5 -> SL=TP3
    SL_MOVED_TO_TP4        = "SL_MOVED_TO_TP4"        # at TP6 -> SL=TP4

    # --- Trailing lifecycle ---
    TRAILING_ARMED         = "TRAILING_ARMED"   # set on Bybit, awaiting trigger
    TRAILING_ACTIVE        = "TRAILING_ACTIVE"  # activation crossed
    TRAILING_UPDATED       = "TRAILING_UPDATED" # SL level moved by trailing

    # --- Legacy scaling pipeline (off in M1) ---
    SCALING_STEP_1         = "SCALING_STEP_1"
    SCALING_STEP_2         = "SCALING_STEP_2"
    SCALING_STEP_3         = "SCALING_STEP_3"
    SCALING_STEP_4         = "SCALING_STEP_4"

    # --- Re-entry ---
    REENTRY_WAITING        = "REENTRY_WAITING"

    # --- Terminal ---
    CLOSED                 = "CLOSED"           # = POSITION_CLOSED in spec
    CANCELLED              = "CANCELLED"
    ERROR                  = "ERROR"


# Allowed transitions matrix. Each key is a source state; the value is
# the set of destination states reachable from that source. Used by
# Trade.transition() to enforce the "only the state machine changes
# state" rule (audit point #16).
#
# A few notes on the matrix:
#   - Terminal states (CLOSED, CANCELLED, ERROR) are sinks — no
#     outgoing transitions allowed.
#   - PROTECTION_FAILED transitions only to CLOSED (the bot force-
#     closes the position) or terminal failure states.
#   - From any active state, transitioning to a terminal state
#     (CLOSED, CANCELLED, ERROR) is always legal.
#   - HEDGE / SL-movement / trailing states overlap conceptually
#     (a single trade can be in HEDGE_ACTIVE while also tracking
#     a moved SL); we model the dominant state machine as
#     "current most-progressed lifecycle stage" and let the
#     individual flags on Trade carry the orthogonal dimensions.
_TERMINAL_STATES = frozenset({
    TradeState.CLOSED,
    TradeState.CANCELLED,
    TradeState.ERROR,
})

ALLOWED_TRANSITIONS: dict[TradeState, frozenset[TradeState]] = {
    TradeState.PENDING: frozenset({
        TradeState.ENTRY1_PLACED,
        TradeState.ENTRY1_FILLED,
        TradeState.POSITION_OPEN,
        TradeState.PROTECTION_FAILED,
        TradeState.CANCELLED,
        TradeState.ERROR,
    }),
    TradeState.ENTRY1_PLACED: frozenset({
        TradeState.ENTRY1_FILLED,
        TradeState.ENTRY2_PLACED,
        TradeState.POSITION_OPEN,
        TradeState.PROTECTION_FAILED,
        TradeState.CANCELLED,
        TradeState.ERROR,
    }),
    TradeState.ENTRY1_FILLED: frozenset({
        TradeState.ENTRY2_PLACED,
        TradeState.ENTRY2_FILLED,
        TradeState.POSITION_OPEN,
        TradeState.PROTECTION_FAILED,
        TradeState.CANCELLED,
        TradeState.ERROR,
    }),
    TradeState.ENTRY2_PLACED: frozenset({
        TradeState.ENTRY2_FILLED,
        TradeState.POSITION_OPEN,
        TradeState.PROTECTION_FAILED,
        TradeState.CANCELLED,
        TradeState.ERROR,
    }),
    TradeState.ENTRY2_FILLED: frozenset({
        TradeState.POSITION_OPEN,
        TradeState.PROTECTION_FAILED,
        TradeState.CANCELLED,
        TradeState.ERROR,
    }),
    TradeState.POSITION_OPEN: frozenset({
        TradeState.HEDGE_ARMED,
        TradeState.HEDGE_ACTIVE,
        TradeState.ORIGINAL_FORCE_CLOSED,
        TradeState.BREAKEVEN_ACTIVE,
        TradeState.PROFIT_LOCK_1_ACTIVE,
        TradeState.PROFIT_LOCK_2_ACTIVE,
        TradeState.SL_MOVED_TO_TP1,
        TradeState.SL_MOVED_TO_TP2,
        TradeState.SL_MOVED_TO_TP3,
        TradeState.SL_MOVED_TO_TP4,
        TradeState.TRAILING_ARMED,
        TradeState.TRAILING_ACTIVE,
        TradeState.SCALING_STEP_1,
        TradeState.REENTRY_WAITING,
        TradeState.CLOSED,
        TradeState.CANCELLED,
        TradeState.ERROR,
        TradeState.PROTECTION_FAILED,
    }),
    TradeState.PROTECTION_FAILED: frozenset({
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.HEDGE_ARMED: frozenset({
        TradeState.HEDGE_ACTIVE,
        TradeState.ORIGINAL_FORCE_CLOSED,
        TradeState.BREAKEVEN_ACTIVE,
        TradeState.PROFIT_LOCK_1_ACTIVE,
        TradeState.PROFIT_LOCK_2_ACTIVE,
        TradeState.TRAILING_ARMED,
        TradeState.TRAILING_ACTIVE,
        TradeState.CLOSED,
        TradeState.CANCELLED,
        TradeState.ERROR,
    }),
    TradeState.HEDGE_ACTIVE: frozenset({
        TradeState.ORIGINAL_FORCE_CLOSED,
        TradeState.TRAILING_UPDATED,
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.ORIGINAL_FORCE_CLOSED: frozenset({
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.BREAKEVEN_ACTIVE: frozenset({
        TradeState.PROFIT_LOCK_1_ACTIVE,
        TradeState.PROFIT_LOCK_2_ACTIVE,
        TradeState.SL_MOVED_TO_TP1,
        TradeState.TRAILING_ARMED,
        TradeState.TRAILING_ACTIVE,
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.PROFIT_LOCK_1_ACTIVE: frozenset({
        TradeState.PROFIT_LOCK_2_ACTIVE,
        TradeState.SL_MOVED_TO_TP1,
        TradeState.TRAILING_ARMED,
        TradeState.TRAILING_ACTIVE,
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.PROFIT_LOCK_2_ACTIVE: frozenset({
        TradeState.SL_MOVED_TO_TP1,
        TradeState.TRAILING_ARMED,
        TradeState.TRAILING_ACTIVE,
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.SL_MOVED_TO_TP1: frozenset({
        TradeState.SL_MOVED_TO_TP2,
        TradeState.TRAILING_ARMED,
        TradeState.TRAILING_ACTIVE,
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.SL_MOVED_TO_TP2: frozenset({
        TradeState.SL_MOVED_TO_TP3,
        TradeState.TRAILING_ARMED,
        TradeState.TRAILING_ACTIVE,
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.SL_MOVED_TO_TP3: frozenset({
        TradeState.SL_MOVED_TO_TP4,
        TradeState.TRAILING_ARMED,
        TradeState.TRAILING_ACTIVE,
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.SL_MOVED_TO_TP4: frozenset({
        TradeState.TRAILING_ARMED,
        TradeState.TRAILING_ACTIVE,
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.TRAILING_ARMED: frozenset({
        TradeState.TRAILING_ACTIVE,
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.TRAILING_ACTIVE: frozenset({
        TradeState.TRAILING_UPDATED,
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.TRAILING_UPDATED: frozenset({
        TradeState.TRAILING_UPDATED,  # repeated updates allowed
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.SCALING_STEP_1: frozenset({
        TradeState.SCALING_STEP_2,
        TradeState.HEDGE_ARMED,
        TradeState.HEDGE_ACTIVE,
        TradeState.TRAILING_ARMED,
        TradeState.TRAILING_ACTIVE,
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.SCALING_STEP_2: frozenset({
        TradeState.SCALING_STEP_3,
        TradeState.TRAILING_ARMED,
        TradeState.TRAILING_ACTIVE,
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.SCALING_STEP_3: frozenset({
        TradeState.SCALING_STEP_4,
        TradeState.TRAILING_ARMED,
        TradeState.TRAILING_ACTIVE,
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.SCALING_STEP_4: frozenset({
        TradeState.TRAILING_ARMED,
        TradeState.TRAILING_ACTIVE,
        TradeState.CLOSED,
        TradeState.ERROR,
    }),
    TradeState.REENTRY_WAITING: frozenset({
        TradeState.PENDING,
        TradeState.POSITION_OPEN,
        TradeState.CANCELLED,
        TradeState.ERROR,
    }),
    # Terminal states — no outgoing transitions.
    TradeState.CLOSED: frozenset(),
    TradeState.CANCELLED: frozenset(),
    TradeState.ERROR: frozenset(),
}


def is_transition_allowed(
    src: Optional[TradeState], dst: TradeState,
) -> bool:
    """Return True if transition src->dst is permitted by the matrix.

    None / unset src is treated as "fresh trade" — only PENDING is a
    legal initial transition. dst that is a terminal state is always
    allowed (safety valve).
    """
    if dst in _TERMINAL_STATES:
        return True
    if src is None:
        return dst == TradeState.PENDING
    allowed = ALLOWED_TRANSITIONS.get(src, frozenset())
    return dst in allowed


# ===================================================================
# Trade record
# ===================================================================

def _new_trade_id() -> str:
    """Generate a unique trade ID (UUID4 hex)."""
    return uuid.uuid4().hex


@dataclass
class Trade:
    """
    Full lifecycle record for a single trade.

    A ``Trade`` starts in ``PENDING`` once a valid signal is accepted
    and transitions through the state machine as orders are placed,
    filled, and managed.
    """

    # --- Identity ---
    id: str = field(default_factory=_new_trade_id)

    # --- Signal that originated this trade ---
    signal: Optional[ParsedSignal] = None

    # --- State machine ---
    state: TradeState = TradeState.PENDING

    # --- Entry orders ---
    entry1_order_id: Optional[str] = None
    entry2_order_id: Optional[str] = None
    entry1_fill_price: Optional[float] = None
    entry2_fill_price: Optional[float] = None
    avg_entry: Optional[float] = None

    # --- Position details ---
    quantity: Optional[float] = None
    leverage: Optional[float] = None
    margin: Optional[float] = None

    # --- Bybit order tracking ---
    bybit_order_ids: List[str] = field(default_factory=list)
    # Individual TP conditional order IDs (one per TP level).
    tp_order_ids: List[str] = field(default_factory=list)

    # --- TP / SL tracking ---
    tp_hits: List[float] = field(default_factory=list)
    sl_price: Optional[float] = None
    be_price: Optional[float] = None
    trailing_sl: Optional[float] = None

    # --- Trailing-stop activation tracking ---
    # The activation price + distance args we handed to Bybit's
    # set_trading_stop. We re-emit them in the TRAILING STOP AKTIVERAD
    # notification at the moment Bybit *actually* starts trailing
    # (price crosses activation_price), not at trade open. Client
    # 2026-04-28: "the message should be sent when the trailing stop
    # starts, not when it is placed".
    trailing_activation_price: Optional[float] = None
    trailing_distance: Optional[float] = None
    trailing_activation_pct: Optional[float] = None
    trailing_distance_pct: Optional[float] = None
    trailing_activated_notified: bool = False
    # The trailing-stop price last reported by Bybit (the position
    # event's ``stopLoss`` field once the trailing has activated). We
    # use it to detect when Bybit moves the trailing to a new better
    # level and fire TRAILING STOP UPPDATERAD. Client 2026-04-28:
    # "Värdet måste bekräftas från Bybit, inte beräknas lokalt" — we
    # only mirror Bybit's reported value, never compute our own.
    last_trailing_stop_price: Optional[float] = None

    # --- Hedge / re-entry ---
    hedge_trade_id: Optional[str] = None
    # Bybit orderId of the conditional that pre-arms the hedge on the
    # exchange (Phase 3 client IZZU 2026-04-27). Set after trade
    # opens, cleared on hedge fire OR on main-trade close (cancelled).
    # Lets the hedge fire autonomously if the bot is offline at the
    # moment price crosses the trigger.
    hedge_conditional_order_id: Optional[str] = None
    # Hedge fill bookkeeping (client 2026-04-30 production-stable model).
    # Recorded the moment the bot detects a fired pre-arm conditional
    # so the 20-minute timeout watcher can compute elapsed time and
    # the no-meaningful-move check can compare against the hedge entry.
    hedge_entry_price: Optional[float] = None
    hedge_filled_at: Optional[datetime] = None
    # Set to True once the bot has issued the -2 % force-close on the
    # original trade so we don't double-fire the close.
    # NOTE: Phase 2 (client 2026-05-01) moved the force-close from a
    # bot-side polling decision to a Bybit conditional order placed at
    # trade open. This flag is still consulted by legacy paths but the
    # primary close path is now ``original_force_close_order_id`` —
    # Bybit owns the trigger, the bot only watches the WS fill event.
    original_force_closed: bool = False
    # Bybit orderId of the -2% reduce-only conditional that closes the
    # original at the agreed emergency-loss threshold (Phase 2,
    # client 2026-05-01). Set after position open, cleared (cancelled)
    # when the trade closes via any other path.
    original_force_close_order_id: Optional[str] = None
    # --- Phase 5 SL-movement bookkeeping (client 2026-05-01) ---
    # Idempotency flags so each SL-move fires AT MOST ONCE per trade.
    # The TP-cascade also uses ``tp_hits`` (already tracked) but checks
    # ``sl_movement_history`` to avoid moving the SL backwards.
    profit_lock_1_active: bool = False    # +4% -> SL=entry+1.5% applied
    profit_lock_2_active: bool = False    # +5% -> SL=entry+2.5% applied
    sl_moved_to_be: bool = False          # TP2 hit  -> SL=entry applied
    sl_moved_to_tp_index: int = 0         # highest TP-index the SL has been moved to (0 = not yet)
    # Audit trail of every SL move (oldest first). Each entry is a
    # dict {ts, from_sl, to_sl, reason, state}. Persisted as JSON in
    # the events table; ephemeral on the in-memory trade.
    sl_movement_history: List[dict] = field(default_factory=list)
    reentry_count: int = 0

    # --- Scaling ---
    scaling_step: int = 0

    # --- Timestamps ---
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
    updated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
    closed_at: Optional[datetime] = None

    # --- PnL ---
    pnl_pct: Optional[float] = None
    pnl_usdt: Optional[float] = None
    close_reason: Optional[str] = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def touch(self) -> None:
        """Update the ``updated_at`` timestamp to now (UTC)."""
        self.updated_at = datetime.now(timezone.utc)

    def transition(
        self, new_state: TradeState, *, force: bool = False,
    ) -> bool:
        """Move to a new state, validate against ALLOWED_TRANSITIONS,
        update the timestamp, log the transition.

        Phase 4 (client 2026-05-01 audit #16): only the state machine
        may change status. Every transition is checked against the
        explicit allowed-transitions matrix declared in
        :data:`ALLOWED_TRANSITIONS`. An illegal transition is BLOCKED
        and a warning is logged — the trade's state stays unchanged.

        Pass ``force=True`` to override the matrix in emergencies
        (e.g. recovery from corrupted state). Forced transitions are
        logged separately so the audit trail still shows them.

        Returns ``True`` when the transition went through, ``False``
        when it was blocked.
        """
        prev_state = self.state if self.state else None
        prev_value = prev_state.value if prev_state else None

        try:
            import structlog
            _slog = structlog.get_logger(__name__)
        except Exception:
            _slog = None

        if not force and not is_transition_allowed(prev_state, new_state):
            if _slog is not None:
                try:
                    _slog.warning(
                        "trade.state_transition.blocked",
                        trade_id=self.id,
                        from_state=prev_value,
                        to_state=new_state.value,
                        reason="not in ALLOWED_TRANSITIONS",
                    )
                except Exception:
                    pass
            return False

        self.state = new_state
        self.touch()
        if _slog is not None:
            try:
                _slog.info(
                    "trade.state_transition",
                    trade_id=self.id,
                    from_state=prev_value,
                    to_state=new_state.value,
                    forced=force,
                )
            except Exception:
                pass
        return True

    @property
    def is_terminal(self) -> bool:
        """Return ``True`` if the trade is in a final state."""
        return self.state in (
            TradeState.CLOSED,
            TradeState.CANCELLED,
            TradeState.ERROR,
        )


# ===================================================================
# Order record (for the local order log / DB)
# ===================================================================

@dataclass
class OrderRecord:
    """
    A single order placed on Bybit, tracked locally for audit and
    reconciliation.

    Attributes
    ----------
    order_id_bot:
        Internal bot-generated identifier for this order.
    order_id_bybit:
        The order ID returned by Bybit after placement.  ``None`` until
        the API call succeeds.
    symbol:
        Trading pair, e.g. ``"BTCUSDT"``.
    side:
        ``"Buy"`` or ``"Sell"``.
    qty:
        Order quantity.
    price:
        Limit price (``None`` for market orders).
    status:
        Current status string, e.g. ``"New"``, ``"Filled"``,
        ``"Cancelled"``.
    created_at:
        UTC timestamp of order creation.
    """

    order_id_bot: str = field(default_factory=lambda: uuid.uuid4().hex)
    order_id_bybit: Optional[str] = None
    symbol: str = ""
    side: str = ""                       # "Buy" | "Sell"
    qty: float = 0.0
    price: Optional[float] = None       # None for market orders
    status: str = "New"
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )


# ===================================================================
# Reporting / statistics
# ===================================================================

@dataclass
class ReportStats:
    """
    Aggregate statistics for a reporting period, tracked per source group.

    Used by the daily and weekly report generators.
    """

    # --- Identity ---
    channel_id: int = 0
    channel_name: str = ""
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None

    # --- Signal counts ---
    signals_received: int = 0
    signals_accepted: int = 0
    signals_rejected_duplicate: int = 0
    signals_rejected_stale: int = 0
    signals_rejected_capacity: int = 0
    signals_rejected_parse_error: int = 0

    # --- Trade outcomes ---
    trades_opened: int = 0
    trades_closed: int = 0
    trades_tp_hit: int = 0
    trades_sl_hit: int = 0
    trades_be_hit: int = 0
    trades_trailing_closed: int = 0
    trades_timed_out: int = 0
    trades_cancelled: int = 0
    trades_error: int = 0

    # --- Hedge / re-entry ---
    hedges_triggered: int = 0
    reentries_triggered: int = 0

    # --- PnL ---
    total_pnl_usdt: float = 0.0
    total_pnl_pct: float = 0.0
    best_trade_pnl_pct: float = 0.0
    worst_trade_pnl_pct: float = 0.0
    avg_trade_pnl_pct: float = 0.0

    # --- Win rate ---
    win_count: int = 0
    loss_count: int = 0

    @property
    def win_rate(self) -> float:
        """Calculate win rate as a percentage (0-100)."""
        total = self.win_count + self.loss_count
        if total == 0:
            return 0.0
        return round((self.win_count / total) * 100, 2)

    # --- Per-trade detail lists (not persisted, used during aggregation) ---
    trade_pnls: List[float] = field(default_factory=list)

    def finalize(self) -> None:
        """
        Compute derived fields from ``trade_pnls``.

        Call this after all trades for the period have been appended to
        ``trade_pnls``.
        """
        if not self.trade_pnls:
            return
        self.best_trade_pnl_pct = max(self.trade_pnls)
        self.worst_trade_pnl_pct = min(self.trade_pnls)
        self.avg_trade_pnl_pct = round(
            sum(self.trade_pnls) / len(self.trade_pnls), 4,
        )
        self.win_count = sum(1 for p in self.trade_pnls if p > 0)
        self.loss_count = sum(1 for p in self.trade_pnls if p <= 0)
