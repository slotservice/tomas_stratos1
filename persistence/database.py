"""
persistence/database.py - SQLite persistence layer for Stratos1 trading bot.

Stores ALL state needed for restart recovery: signals, trades, orders,
events, and aggregated reporting/error statistics. Uses aiosqlite for
fully async I/O so the event loop is never blocked by disk access.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiosqlite
import structlog

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# SQL: Table definitions
# ---------------------------------------------------------------------------

_CREATE_TABLES = """
-- Raw parsed signals from Telegram channels
CREATE TABLE IF NOT EXISTS signals (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol               TEXT    NOT NULL,
    direction            TEXT    NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
    entry_price          REAL    NOT NULL,
    sl_price             REAL,
    tp_prices            TEXT,              -- JSON array of take-profit levels
    source_channel_id    INTEGER,
    source_channel_name  TEXT,
    signal_type          TEXT,
    raw_text             TEXT,
    received_at          TEXT,              -- ISO 8601
    status               TEXT    DEFAULT 'active'
);

-- Active and historical trades
CREATE TABLE IF NOT EXISTS trades (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id            INTEGER REFERENCES signals(id),
    state                TEXT    NOT NULL,
    entry1_order_id_bot  TEXT,
    entry1_order_id_bybit TEXT,
    entry2_order_id_bot  TEXT,
    entry2_order_id_bybit TEXT,
    entry1_fill_price    REAL,
    entry2_fill_price    REAL,
    avg_entry            REAL,
    quantity             REAL,
    leverage             REAL,
    margin               REAL,
    sl_price             REAL,
    be_price             REAL,
    trailing_sl          REAL,
    hedge_trade_id       INTEGER,
    hedge_conditional_order_id TEXT,           -- Bybit orderId of pre-armed hedge conditional (Phase 3)
    original_force_close_order_id TEXT,        -- Bybit orderId of -2% force-close conditional (Phase 2 / 2026-05-01)
    tp_order_ids        TEXT,                  -- JSON array of Bybit orderIds for the partial-TP conditionals (Phase 5b mutual-exclusion needs these post-restart)
    reentry_count        INTEGER DEFAULT 0,
    scaling_step         INTEGER DEFAULT 0,
    tp_hits              TEXT,              -- JSON array of hit TP indices
    close_reason         TEXT,
    pnl_pct              REAL,
    pnl_usdt             REAL,
    created_at           TEXT,
    updated_at           TEXT,
    closed_at            TEXT
);

-- All order records (limit, market, SL, TP, etc.)
CREATE TABLE IF NOT EXISTS orders (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id             INTEGER REFERENCES trades(id),
    order_id_bot         TEXT    UNIQUE,
    order_id_bybit       TEXT,
    symbol               TEXT,
    side                 TEXT,
    order_type           TEXT,
    qty                  REAL,
    price                REAL,
    fill_price           REAL,
    fill_qty             REAL,
    status               TEXT,
    created_at           TEXT,
    updated_at           TEXT
);

-- Event log for auditing and debugging
CREATE TABLE IF NOT EXISTS events (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id             INTEGER,
    event_type           TEXT,
    details              TEXT,              -- JSON blob
    created_at           TEXT
);

-- Aggregated per-group per-day report statistics
CREATE TABLE IF NOT EXISTS report_stats (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id                INTEGER,
    group_name              TEXT,
    date                    TEXT,
    signals_count           INTEGER DEFAULT 0,
    active_count            INTEGER DEFAULT 0,
    blocked_count           INTEGER DEFAULT 0,
    wins                    INTEGER DEFAULT 0,
    losses                  INTEGER DEFAULT 0,
    tp_count                INTEGER DEFAULT 0,
    sl_count                INTEGER DEFAULT 0,
    pyramid_counts          TEXT,           -- JSON: {"1": N, "2": N, ...}
    trailing_stop_count     INTEGER DEFAULT 0,
    hedge_count             INTEGER DEFAULT 0,
    reentry_count           INTEGER DEFAULT 0,
    timeout_deleted         INTEGER DEFAULT 0,
    invalid_signals         INTEGER DEFAULT 0,
    api_failures            INTEGER DEFAULT 0,
    max_profit              REAL    DEFAULT 0,
    min_profit              REAL    DEFAULT 0,
    total_profit            REAL    DEFAULT 0,
    UNIQUE(group_id, date)
);

-- Error counts per group per day
CREATE TABLE IF NOT EXISTS error_stats (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id                INTEGER,
    group_name              TEXT,
    date                    TEXT,
    order_failed            INTEGER DEFAULT 0,
    order_rejected          INTEGER DEFAULT 0,
    position_not_opened     INTEGER DEFAULT 0,
    position_not_closed     INTEGER DEFAULT 0,
    insufficient_im         INTEGER DEFAULT 0,
    insufficient_balance    INTEGER DEFAULT 0,
    api_errors              INTEGER DEFAULT 0,
    system_errors           INTEGER DEFAULT 0,
    invalid_signals         INTEGER DEFAULT 0,
    sl_tp_not_executed      INTEGER DEFAULT 0,
    no_money                INTEGER DEFAULT 0,
    timeout_deleted         INTEGER DEFAULT 0,
    UNIQUE(group_id, date)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_signals_symbol_received
    ON signals(symbol, received_at);
CREATE INDEX IF NOT EXISTS idx_trades_state
    ON trades(state);
CREATE INDEX IF NOT EXISTS idx_trades_signal_id
    ON trades(signal_id);
CREATE INDEX IF NOT EXISTS idx_orders_status_created
    ON orders(status, created_at);
CREATE INDEX IF NOT EXISTS idx_orders_trade_id
    ON orders(trade_id);
CREATE INDEX IF NOT EXISTS idx_events_trade_id
    ON events(trade_id);
CREATE INDEX IF NOT EXISTS idx_report_stats_group_date
    ON report_stats(group_id, date);
CREATE INDEX IF NOT EXISTS idx_error_stats_group_date
    ON error_stats(group_id, date);
"""


def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _row_to_dict(row: aiosqlite.Row) -> dict[str, Any]:
    """Convert an aiosqlite Row (with .description) into a plain dict."""
    return dict(row)


class Database:
    """Async SQLite persistence for the Stratos1 trading bot.

    Usage::

        db = Database("stratos1.db")
        await db.initialize()
        # ... use db methods ...
        await db.close()
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Open the database connection and create tables if they don't exist."""
        logger.info("database.initialize", path=self.db_path)
        self._db = await aiosqlite.connect(self.db_path)
        # Return rows as sqlite3.Row so we can access columns by name.
        self._db.row_factory = aiosqlite.Row
        # Enable WAL mode for better concurrent read/write performance.
        await self._db.execute("PRAGMA journal_mode=WAL")
        # Enforce foreign-key constraints.
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._db.executescript(_CREATE_TABLES)
        # Lightweight migrations — add new columns on existing DBs
        # without dropping data. Each one is idempotent: try to add,
        # ignore "duplicate column" errors.
        for ddl in (
            "ALTER TABLE trades ADD COLUMN hedge_conditional_order_id TEXT",
            "ALTER TABLE trades ADD COLUMN original_force_close_order_id TEXT",
            "ALTER TABLE trades ADD COLUMN tp_order_ids TEXT",
        ):
            try:
                await self._db.execute(ddl)
            except Exception as e:
                # 'duplicate column name' is the expected case on
                # already-migrated DBs. Anything else is logged but
                # not fatal — startup continues.
                msg = str(e).lower()
                if "duplicate column" not in msg and "already exists" not in msg:
                    logger.warning(
                        "database.migration_skipped",
                        ddl=ddl, error=str(e)[:120],
                    )
        await self._db.commit()
        logger.info("database.ready")

    async def close(self) -> None:
        """Gracefully close the database connection."""
        if self._db is not None:
            await self._db.close()
            self._db = None
            logger.info("database.closed")

    async def stop(self) -> None:
        """Alias for :meth:`close` — main.py's graceful-shutdown loop
        calls ``comp.stop()`` uniformly across every registered
        component. Without this alias, shutdown logged AttributeError
        for the Database every time and systemd ended up SIGKILL'ing
        the process after the 30 s SIGTERM timeout."""
        await self.close()

    @property
    def _conn(self) -> aiosqlite.Connection:
        """Return the active connection, raising if not initialised."""
        if self._db is None:
            raise RuntimeError(
                "Database not initialised - call await db.initialize() first"
            )
        return self._db

    # ------------------------------------------------------------------
    # Signals
    # ------------------------------------------------------------------

    async def save_signal(self, signal: dict[str, Any]) -> int:
        """Persist a parsed signal and return its row id.

        Expected keys mirror the *signals* table columns (minus ``id``).
        ``tp_prices`` may be a Python list; it will be JSON-encoded automatically.
        ``received_at`` defaults to now if omitted.
        """
        tp = signal.get("tp_prices")
        if isinstance(tp, (list, tuple)):
            tp = json.dumps(tp)

        received_at = signal.get("received_at") or _now_iso()

        cursor = await self._conn.execute(
            """
            INSERT INTO signals
                (symbol, direction, entry_price, sl_price, tp_prices,
                 source_channel_id, source_channel_name, signal_type,
                 raw_text, received_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal["symbol"],
                signal["direction"],
                signal["entry_price"],
                signal.get("sl_price"),
                tp,
                signal.get("source_channel_id"),
                signal.get("source_channel_name"),
                signal.get("signal_type"),
                signal.get("raw_text"),
                received_at,
                signal.get("status", "active"),
            ),
        )
        await self._conn.commit()
        signal_id: int = cursor.lastrowid  # type: ignore[assignment]
        logger.info(
            "signal.saved",
            signal_id=signal_id,
            symbol=signal["symbol"],
            direction=signal["direction"],
        )
        return signal_id

    async def get_recent_signals(
        self, symbol: str, hours: float = 1.0
    ) -> list[dict[str, Any]]:
        """Return signals for *symbol* received within the last *hours*.

        Useful for duplicate / rapid-fire signal detection.
        """
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=hours)
        ).isoformat()
        cursor = await self._conn.execute(
            """
            SELECT * FROM signals
            WHERE symbol = ? AND received_at >= ?
            ORDER BY received_at DESC
            """,
            (symbol, cutoff),
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Trades
    # ------------------------------------------------------------------

    async def save_trade(self, trade: dict[str, Any]) -> int:
        """Insert a new trade row and return its id.

        ``tp_hits`` may be a Python list; it will be JSON-encoded.
        ``created_at`` / ``updated_at`` default to now if omitted.
        """
        tp_hits = trade.get("tp_hits")
        if isinstance(tp_hits, (list, tuple)):
            tp_hits = json.dumps(tp_hits)

        now = _now_iso()
        created = trade.get("created_at") or now
        updated = trade.get("updated_at") or now

        cursor = await self._conn.execute(
            """
            INSERT INTO trades
                (signal_id, state, entry1_order_id_bot, entry1_order_id_bybit,
                 entry2_order_id_bot, entry2_order_id_bybit,
                 entry1_fill_price, entry2_fill_price, avg_entry,
                 quantity, leverage, margin, sl_price, be_price, trailing_sl,
                 hedge_trade_id, reentry_count, scaling_step, tp_hits,
                 close_reason, pnl_pct, pnl_usdt,
                 created_at, updated_at, closed_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                trade.get("signal_id"),
                trade["state"],
                trade.get("entry1_order_id_bot"),
                trade.get("entry1_order_id_bybit"),
                trade.get("entry2_order_id_bot"),
                trade.get("entry2_order_id_bybit"),
                trade.get("entry1_fill_price"),
                trade.get("entry2_fill_price"),
                trade.get("avg_entry"),
                trade.get("quantity"),
                trade.get("leverage"),
                trade.get("margin"),
                trade.get("sl_price"),
                trade.get("be_price"),
                trade.get("trailing_sl"),
                trade.get("hedge_trade_id"),
                trade.get("reentry_count", 0),
                trade.get("scaling_step", 0),
                tp_hits,
                trade.get("close_reason"),
                trade.get("pnl_pct"),
                trade.get("pnl_usdt"),
                created,
                updated,
                trade.get("closed_at"),
            ),
        )
        await self._conn.commit()
        trade_id: int = cursor.lastrowid  # type: ignore[assignment]
        logger.info("trade.saved", trade_id=trade_id, state=trade["state"])
        return trade_id

    async def update_trade(self, trade_id: int, **fields: Any) -> None:
        """Update arbitrary columns on a trade row.

        ``tp_hits`` values that are lists are JSON-encoded automatically.
        ``updated_at`` is set to now unless explicitly provided.
        """
        if not fields:
            return

        # Auto-encode JSON list fields.
        for key in ("tp_hits", "tp_order_ids"):
            if key in fields and isinstance(fields[key], (list, tuple)):
                fields[key] = json.dumps(fields[key])

        fields.setdefault("updated_at", _now_iso())

        set_clause = ", ".join(f"{col} = ?" for col in fields)
        values = list(fields.values()) + [trade_id]

        await self._conn.execute(
            f"UPDATE trades SET {set_clause} WHERE id = ?",  # noqa: S608
            values,
        )
        await self._conn.commit()
        logger.debug("trade.updated", trade_id=trade_id, fields=list(fields))

    async def get_trade(self, trade_id: int) -> Optional[dict[str, Any]]:
        """Fetch a single trade by id, or ``None`` if not found."""
        cursor = await self._conn.execute(
            "SELECT * FROM trades WHERE id = ?", (trade_id,)
        )
        row = await cursor.fetchone()
        return _row_to_dict(row) if row else None

    async def get_active_trades(self) -> list[dict[str, Any]]:
        """Return all trades whose state is not a terminal state.

        Terminal states: ``closed``, ``cancelled``, ``error``.
        Everything else (``pending``, ``entry1_placed``, ``open``, etc.)
        is considered active and will be recovered on restart.
        """
        cursor = await self._conn.execute(
            """
            SELECT * FROM trades
            WHERE state NOT IN ('closed', 'cancelled', 'error')
            ORDER BY created_at ASC
            """
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]

    async def get_trades_by_symbol(self, symbol: str) -> list[dict[str, Any]]:
        """Return all trades (active and historical) for a given symbol.

        Joins via signal_id so the result also carries the signal's
        ``symbol``, ``direction``, and ``entry_price`` — used by the
        duplicate detector for direction-aware blocking (client
        2026-04-28: trade only one direction per symbol).
        """
        cursor = await self._conn.execute(
            """
            SELECT t.*,
                   s.symbol         AS symbol,
                   s.direction      AS direction,
                   s.entry_price    AS entry_price
            FROM trades t
            JOIN signals s ON t.signal_id = s.id
            WHERE s.symbol = ?
            ORDER BY t.created_at DESC
            """,
            (symbol,),
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    async def save_order(self, order: dict[str, Any]) -> int:
        """Insert an order record and return its row id."""
        now = _now_iso()

        cursor = await self._conn.execute(
            """
            INSERT INTO orders
                (trade_id, order_id_bot, order_id_bybit, symbol, side,
                 order_type, qty, price, fill_price, fill_qty, status,
                 created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                order.get("trade_id"),
                order.get("order_id_bot"),
                order.get("order_id_bybit"),
                order.get("symbol"),
                order.get("side"),
                order.get("order_type"),
                order.get("qty"),
                order.get("price"),
                order.get("fill_price"),
                order.get("fill_qty"),
                order.get("status"),
                order.get("created_at") or now,
                order.get("updated_at") or now,
            ),
        )
        await self._conn.commit()
        order_id: int = cursor.lastrowid  # type: ignore[assignment]
        logger.info(
            "order.saved",
            order_id=order_id,
            bot_id=order.get("order_id_bot"),
            symbol=order.get("symbol"),
        )
        return order_id

    async def update_order(self, order_id_bot: str, **fields: Any) -> None:
        """Update arbitrary columns on an order identified by its bot-assigned id."""
        if not fields:
            return

        fields.setdefault("updated_at", _now_iso())

        set_clause = ", ".join(f"{col} = ?" for col in fields)
        values = list(fields.values()) + [order_id_bot]

        await self._conn.execute(
            f"UPDATE orders SET {set_clause} WHERE order_id_bot = ?",  # noqa: S608
            values,
        )
        await self._conn.commit()
        logger.debug(
            "order.updated", order_id_bot=order_id_bot, fields=list(fields)
        )

    async def delete_order(self, order_id_bot: str) -> None:
        """Delete an order by its bot-assigned id (e.g. after timeout cleanup)."""
        await self._conn.execute(
            "DELETE FROM orders WHERE order_id_bot = ?", (order_id_bot,)
        )
        await self._conn.commit()
        logger.info("order.deleted", order_id_bot=order_id_bot)

    async def get_unfilled_orders(
        self, older_than_hours: float = 1.0
    ) -> list[dict[str, Any]]:
        """Return orders that are still pending and were created more than
        *older_than_hours* ago -- candidates for timeout cleanup.
        """
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
        ).isoformat()
        cursor = await self._conn.execute(
            """
            SELECT * FROM orders
            WHERE status IN ('pending', 'new', 'created')
              AND created_at < ?
            ORDER BY created_at ASC
            """,
            (cutoff,),
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    async def log_event(
        self,
        trade_id: Optional[int],
        event_type: str,
        details: Any = None,
    ) -> None:
        """Append an audit event.

        *details* can be a dict/list (JSON-encoded) or a string.
        """
        if isinstance(details, (dict, list)):
            details = json.dumps(details)

        await self._conn.execute(
            """
            INSERT INTO events (trade_id, event_type, details, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (trade_id, event_type, details, _now_iso()),
        )
        await self._conn.commit()
        logger.debug(
            "event.logged", trade_id=trade_id, event_type=event_type
        )

    # ------------------------------------------------------------------
    # Report stats
    # ------------------------------------------------------------------

    async def get_report_stats(
        self, group_id: int, date_from: str, date_to: str
    ) -> list[dict[str, Any]]:
        """Return report_stats rows for *group_id* within the date range (inclusive)."""
        cursor = await self._conn.execute(
            """
            SELECT * FROM report_stats
            WHERE group_id = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
            """,
            (group_id, date_from, date_to),
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]

    async def increment_report_stat(
        self,
        group_id: int,
        group_name: str,
        date: str,
        field: str,
        amount: int = 1,
    ) -> None:
        """Atomically increment a single integer counter in report_stats.

        Creates the row (via upsert) if it doesn't exist yet for
        the given *group_id* + *date* pair.
        """
        # Whitelist to prevent SQL injection through the field name.
        _ALLOWED_FIELDS = {
            "signals_count",
            "active_count",
            "blocked_count",
            "wins",
            "losses",
            "tp_count",
            "sl_count",
            "trailing_stop_count",
            "hedge_count",
            "reentry_count",
            "timeout_deleted",
            "invalid_signals",
            "api_failures",
        }
        if field not in _ALLOWED_FIELDS:
            raise ValueError(
                f"Field '{field}' is not an allowed report_stats counter"
            )

        await self._conn.execute(
            f"""
            INSERT INTO report_stats (group_id, group_name, date, {field})
            VALUES (?, ?, ?, ?)
            ON CONFLICT(group_id, date) DO UPDATE
                SET {field} = {field} + ?,
                    group_name = excluded.group_name
            """,  # noqa: S608
            (group_id, group_name, date, amount, amount),
        )
        await self._conn.commit()
        logger.debug(
            "report_stat.incremented",
            group_id=group_id,
            date=date,
            field=field,
            amount=amount,
        )

    async def update_report_profit(
        self,
        group_id: int,
        group_name: str,
        date: str,
        profit: float,
    ) -> None:
        """Update the profit-related aggregate fields in report_stats.

        Adds *profit* to ``total_profit`` and adjusts ``max_profit`` /
        ``min_profit`` if this trade sets a new extreme.
        """
        await self._conn.execute(
            """
            INSERT INTO report_stats (group_id, group_name, date,
                                      total_profit, max_profit, min_profit)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_id, date) DO UPDATE
                SET total_profit = total_profit + ?,
                    max_profit   = MAX(max_profit, ?),
                    min_profit   = MIN(min_profit, ?),
                    group_name   = excluded.group_name
            """,
            (
                group_id, group_name, date, profit, profit, profit,
                profit, profit, profit,
            ),
        )
        await self._conn.commit()

    async def get_all_group_stats(
        self, date_from: str, date_to: str
    ) -> list[dict[str, Any]]:
        """Return aggregated report stats for ALL groups within a date range.

        Useful for group-ranking / comparison reports.
        """
        cursor = await self._conn.execute(
            """
            SELECT
                group_id,
                group_name,
                SUM(signals_count)       AS signals_count,
                SUM(active_count)        AS active_count,
                SUM(blocked_count)       AS blocked_count,
                SUM(wins)                AS wins,
                SUM(losses)              AS losses,
                SUM(tp_count)            AS tp_count,
                SUM(sl_count)            AS sl_count,
                SUM(trailing_stop_count) AS trailing_stop_count,
                SUM(hedge_count)         AS hedge_count,
                SUM(reentry_count)       AS reentry_count,
                SUM(timeout_deleted)     AS timeout_deleted,
                SUM(invalid_signals)     AS invalid_signals,
                SUM(api_failures)        AS api_failures,
                MAX(max_profit)          AS max_profit,
                MIN(min_profit)          AS min_profit,
                SUM(total_profit)        AS total_profit
            FROM report_stats
            WHERE date >= ? AND date <= ?
            GROUP BY group_id
            ORDER BY total_profit DESC
            """,
            (date_from, date_to),
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Error stats
    # ------------------------------------------------------------------

    async def get_error_stats(
        self, group_id: int, date_from: str, date_to: str
    ) -> list[dict[str, Any]]:
        """Return error_stats rows for *group_id* within the date range."""
        cursor = await self._conn.execute(
            """
            SELECT * FROM error_stats
            WHERE group_id = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
            """,
            (group_id, date_from, date_to),
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]

    async def increment_error_stat(
        self,
        group_id: int,
        group_name: str,
        date: str,
        field: str,
        amount: int = 1,
    ) -> None:
        """Atomically increment a single error counter for a group + date.

        Creates the row if it doesn't exist yet (upsert).
        """
        _ALLOWED_FIELDS = {
            "order_failed",
            "order_rejected",
            "position_not_opened",
            "position_not_closed",
            "insufficient_im",
            "insufficient_balance",
            "api_errors",
            "system_errors",
            "invalid_signals",
            "sl_tp_not_executed",
            "no_money",
            "timeout_deleted",
        }
        if field not in _ALLOWED_FIELDS:
            raise ValueError(
                f"Field '{field}' is not an allowed error_stats counter"
            )

        await self._conn.execute(
            f"""
            INSERT INTO error_stats (group_id, group_name, date, {field})
            VALUES (?, ?, ?, ?)
            ON CONFLICT(group_id, date) DO UPDATE
                SET {field} = {field} + ?,
                    group_name = excluded.group_name
            """,  # noqa: S608
            (group_id, group_name, date, amount, amount),
        )
        await self._conn.commit()
        logger.debug(
            "error_stat.incremented",
            group_id=group_id,
            date=date,
            field=field,
            amount=amount,
        )
