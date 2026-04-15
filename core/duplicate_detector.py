"""
Stratos1 - Duplicate Signal Detector
-------------------------------------
Prevents the bot from opening multiple positions on the same symbol
when several Telegram groups broadcast near-identical signals.

Logic (from client requirement):
  1. Same symbol + entry within threshold_pct (default 5%)
     -> BLOCK the signal entirely (duplicate).

  2. Same symbol + entry BEYOND threshold_pct
     -> Do NOT open a new trade. Instead UPDATE the existing trade's
        TP and SL levels, and verify liquidation safety.

  3. No active trade for this symbol
     -> Allow (new trade).

Return codes:
  "new"      -> no existing trade, proceed normally
  "blocked"  -> duplicate within threshold, reject
  "update"   -> entry differs >5%, update existing trade TP/SL
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import structlog

if TYPE_CHECKING:
    from core.signal_parser import ParsedSignal

log = structlog.get_logger(__name__)


class DuplicateCheckResult:
    """Result of a duplicate check with action and context."""

    def __init__(
        self,
        action: str,             # "new", "blocked", "update"
        reason: str = "",
        existing_trade: Optional[dict] = None,
    ) -> None:
        self.action = action
        self.reason = reason
        self.existing_trade = existing_trade

    @property
    def is_blocked(self) -> bool:
        return self.action == "blocked"

    @property
    def is_update(self) -> bool:
        return self.action == "update"

    @property
    def is_new(self) -> bool:
        return self.action == "new"


class DuplicateDetector:
    """
    Check incoming signals against active trades to determine the
    correct action: new trade, block, or update existing.

    Parameters:
        db:              Database instance with async query support.
        threshold_pct:   Maximum entry-price difference (%) to consider
                         two signals as duplicates.  Default 5.0.
        lookback_hours:  How far back (hours) to search for active trades.
    """

    def __init__(
        self,
        db,                           # persistence.database.Database
        threshold_pct: float = 5.0,
        lookback_hours: int = 24,
    ) -> None:
        self._db = db
        self._threshold_pct = threshold_pct
        self._lookback_hours = lookback_hours

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def check(self, signal: "ParsedSignal") -> DuplicateCheckResult:
        """
        Determine what to do with an incoming signal.

        Returns a DuplicateCheckResult with:
          action="new"      -> no conflict, open a new trade
          action="blocked"  -> duplicate within 5%, reject
          action="update"   -> diff >5%, update existing trade's TP/SL
        """
        symbol = signal.symbol
        new_entry = signal.entry

        if new_entry <= 0:
            return DuplicateCheckResult("new")

        # Fetch active trades for this symbol.
        active_trades = await self._fetch_active_trades(symbol)

        if not active_trades:
            log.debug(
                "duplicate_check.no_active",
                symbol=symbol,
                new_entry=new_entry,
            )
            return DuplicateCheckResult("new")

        # Compare the new signal's entry against each active trade.
        for trade in active_trades:
            existing_entry = trade.get("entry_price", 0.0)
            if existing_entry <= 0:
                continue

            diff_pct = abs(new_entry - existing_entry) / existing_entry * 100

            if diff_pct <= self._threshold_pct:
                # --------------------------------------------------
                # WITHIN 5%: block as duplicate
                # --------------------------------------------------
                reason = (
                    f"Duplicate within {self._threshold_pct}% of active "
                    f"{symbol} trade at {existing_entry} "
                    f"(diff {diff_pct:.2f}%)"
                )
                log.info(
                    "duplicate_check.blocked",
                    symbol=symbol,
                    new_entry=new_entry,
                    existing_entry=existing_entry,
                    diff_pct=round(diff_pct, 2),
                    trade_id=trade.get("id"),
                )
                return DuplicateCheckResult(
                    action="blocked",
                    reason=reason,
                    existing_trade=trade,
                )
            else:
                # --------------------------------------------------
                # BEYOND 5%: update existing trade's TP/SL
                # --------------------------------------------------
                reason = (
                    f"Same symbol {symbol} with {diff_pct:.2f}% entry "
                    f"difference (>{self._threshold_pct}%). "
                    f"Updating TP/SL on existing trade at {existing_entry}."
                )
                log.info(
                    "duplicate_check.update_existing",
                    symbol=symbol,
                    new_entry=new_entry,
                    existing_entry=existing_entry,
                    diff_pct=round(diff_pct, 2),
                    trade_id=trade.get("id"),
                )
                return DuplicateCheckResult(
                    action="update",
                    reason=reason,
                    existing_trade=trade,
                )

        # No active trade with a comparable entry was found.
        log.debug(
            "duplicate_check.passed",
            symbol=symbol,
            new_entry=new_entry,
            active_count=len(active_trades),
        )
        return DuplicateCheckResult("new")

    # ------------------------------------------------------------------
    # Legacy API (backwards-compatible tuple return)
    # ------------------------------------------------------------------

    async def check_legacy(self, signal: "ParsedSignal") -> tuple[bool, str]:
        """
        Legacy check returning (is_duplicate, reason) tuple.
        Treats "update" as not-duplicate (caller handles update separately).
        """
        result = await self.check(signal)
        if result.is_blocked:
            return True, result.reason
        return False, result.reason

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _fetch_active_trades(self, symbol: str) -> list[dict]:
        """
        Query the database for active/open trades matching *symbol*
        within the configured lookback window.
        """
        try:
            trades = await self._db.get_trades_by_symbol(symbol)
        except Exception:
            log.exception("duplicate_check.db_error", symbol=symbol)
            return []

        # Filter to active trades only.
        active_states = {
            "PENDING", "ENTRY1_PLACED", "ENTRY1_FILLED",
            "ENTRY2_PLACED", "ENTRY2_FILLED", "POSITION_OPEN",
            "BREAKEVEN_ACTIVE", "SCALING_STEP_1", "SCALING_STEP_2",
            "SCALING_STEP_3", "SCALING_STEP_4", "TRAILING_ACTIVE",
            "HEDGE_ACTIVE", "REENTRY_WAITING",
        }

        result = []
        for row in trades:
            if hasattr(row, "_asdict"):
                d = row._asdict()
            elif isinstance(row, dict):
                d = row
            else:
                d = {
                    "id": row[0],
                    "signal_id": row[1],
                    "state": row[2],
                    "entry_price": row[3] if len(row) > 3 else 0.0,
                }

            state = d.get("state", "")
            if state in active_states:
                # Use avg_entry if available, otherwise fall back to
                # the signal's entry price via join.
                entry_price = d.get("avg_entry") or d.get("entry_price", 0.0)
                d["entry_price"] = entry_price
                result.append(d)

        return result
