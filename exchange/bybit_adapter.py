"""
Stratos1 - Bybit V5 REST + WebSocket adapter.

Uses the official ``pybit`` SDK to interact with Bybit V5 unified trading
API.  Supports both Demo and Live modes, hedge-mode positions, and
real-time order/position/execution streaming via private WebSocket.

All public methods are async-safe and designed for use within an asyncio
event loop.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional, Set

import structlog
from pybit.unified_trading import HTTP, WebSocket

from config.settings import BybitSettings

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Bybit V5 constants
# ---------------------------------------------------------------------------
CATEGORY = "linear"  # USDT perpetual futures

DEMO_REST_URL = "https://api-demo.bybit.com"
LIVE_REST_URL = "https://api.bybit.com"

DEMO_WS_PRIVATE_URL = "wss://stream-demo.bybit.com/v5/private"
DEMO_WS_PUBLIC_URL = "wss://stream-demo.bybit.com/v5/public/linear"
LIVE_WS_PRIVATE_URL = "wss://stream.bybit.com/v5/private"
LIVE_WS_PUBLIC_URL = "wss://stream.bybit.com/v5/public/linear"

# Known Bybit error codes
ERR_ORDER_NOT_EXIST = 110001
ERR_POSITION_NOT_EXIST = 110003
ERR_POSITION_MODE_NOT_SUPPORTED = 110020
ERR_INSUFFICIENT_BALANCE = 110025
ERR_LEVERAGE_NOT_CHANGED = 110043
ERR_RATE_LIMIT = 10006

# Retry configuration
MAX_RETRIES = 3
RETRY_BASE_DELAY = 0.5  # seconds
RETRY_RATE_LIMIT_DELAY = 2.0  # seconds

# Instrument cache TTL
INSTRUMENT_CACHE_TTL = 300  # 5 minutes

# WebSocket deduplication window
WS_DEDUP_MAX_SIZE = 5000


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class BybitAdapterError(Exception):
    """Base exception for all adapter errors."""

    def __init__(self, message: str, ret_code: int = -1, ret_msg: str = ""):
        super().__init__(message)
        self.ret_code = ret_code
        self.ret_msg = ret_msg


class BybitRateLimitError(BybitAdapterError):
    """Raised when the API rate limit is hit."""
    pass


class BybitInsufficientBalanceError(BybitAdapterError):
    """Raised when the account has insufficient balance."""
    pass


# ---------------------------------------------------------------------------
# LRU-style dedup set
# ---------------------------------------------------------------------------

class _BoundedDedup:
    """Bounded set for deduplicating WebSocket event IDs (FIFO eviction)."""

    def __init__(self, max_size: int = WS_DEDUP_MAX_SIZE):
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._max_size = max_size

    def check_and_add(self, key: str) -> bool:
        """Return True if *key* is new (not a duplicate)."""
        if key in self._seen:
            self._seen.move_to_end(key)
            return False
        self._seen[key] = None
        while len(self._seen) > self._max_size:
            self._seen.popitem(last=False)
        return True


# ---------------------------------------------------------------------------
# Instrument cache entry
# ---------------------------------------------------------------------------

class _CachedInstrument:
    """Holds instrument info with a TTL."""

    def __init__(self, data: dict, fetched_at: float):
        self.data = data
        self.fetched_at = fetched_at

    @property
    def expired(self) -> bool:
        return (time.monotonic() - self.fetched_at) > INSTRUMENT_CACHE_TTL


# ---------------------------------------------------------------------------
# Main adapter
# ---------------------------------------------------------------------------

class BybitAdapter:
    """
    Async adapter wrapping pybit's synchronous HTTP/WebSocket clients.

    All blocking I/O is dispatched to a thread-pool executor so the
    asyncio event loop is never blocked.
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        settings: BybitSettings,
        on_order_update: Optional[Callable] = None,
        on_position_update: Optional[Callable] = None,
        on_execution_update: Optional[Callable] = None,
    ):
        self._settings = settings
        self._on_order_update = on_order_update
        self._on_position_update = on_position_update
        self._on_execution_update = on_execution_update

        self._demo = settings.demo
        self._api_key = settings.api_key
        self._api_secret = settings.api_secret

        # Clients (initialised in start())
        self._rest: Optional[HTTP] = None
        self._ws_private: Optional[WebSocket] = None
        self._ws_public: Optional[WebSocket] = None

        # Instrument info cache: symbol -> _CachedInstrument
        self._instrument_cache: Dict[str, _CachedInstrument] = {}

        # WebSocket deduplication
        self._order_dedup = _BoundedDedup()
        self._execution_dedup = _BoundedDedup()

        # State flags
        self._started = False
        self._stopping = False

        self._log = logger.bind(adapter="bybit", demo=self._demo)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Initialise REST client and connect private + public WebSockets."""
        if self._started:
            self._log.warning("adapter_already_started")
            return

        loop = asyncio.get_running_loop()

        # --- Sync clock offset with Bybit server ---
        # VPNs can cause system clock drift. We measure the offset and
        # pass recv_window to pybit so authenticated requests don't fail
        # with ErrCode 10002 (timestamp mismatch).
        try:
            import requests as _req, time as _time
            base = DEMO_REST_URL if self._demo else LIVE_REST_URL
            r = _req.get(f"{base}/v5/market/time", timeout=10)
            server_ts = int(r.json()["result"]["timeSecond"])
            local_ts = int(_time.time())
            self._time_offset = server_ts - local_ts
            self._log.info(
                "clock_offset_detected",
                offset_seconds=self._time_offset,
            )
        except Exception:
            self._time_offset = 0
            self._log.warning("clock_offset_check_failed")

        # Monkey-patch pybit's timestamp generator to use server time.
        # This is the most reliable fix for clock drift caused by VPN.
        if abs(self._time_offset) > 5:
            import pybit._helpers as _pybit_helpers
            _original_ts = _pybit_helpers.generate_timestamp
            offset_ms = self._time_offset * 1000

            def _patched_timestamp():
                return _original_ts() + int(offset_ms)

            _pybit_helpers.generate_timestamp = _patched_timestamp
            self._log.info(
                "clock_patched",
                offset_ms=int(offset_ms),
            )

        # Use a generous recv_window as additional safety.
        recv_window = max(10000, abs(self._time_offset) * 1000 + 15000)

        # --- REST client ---
        self._rest = HTTP(
            api_key=self._api_key,
            api_secret=self._api_secret,
            demo=self._demo,
            recv_window=recv_window,
        )
        self._log.info("rest_client_initialised",
                       base_url=DEMO_REST_URL if self._demo else LIVE_REST_URL,
                       recv_window=recv_window)

        # --- Private WebSocket (order, position, execution streams) ---
        try:
            self._ws_private = WebSocket(
                api_key=self._api_key,
                api_secret=self._api_secret,
                testnet=False,
                demo=self._demo,
                channel_type="private",
            )
            # Subscribe to private topics
            self._ws_private.order_stream(callback=self._raw_order_callback)
            self._ws_private.position_stream(callback=self._raw_position_callback)
            self._ws_private.execution_stream(callback=self._raw_execution_callback)
            self._log.info("private_ws_connected")
        except Exception:
            self._log.exception("private_ws_connect_failed")
            # Non-fatal: we can still operate via REST polling
            self._ws_private = None

        # --- Public WebSocket (for ticker / kline if needed later) ---
        try:
            self._ws_public = WebSocket(
                testnet=False,
                demo=self._demo,
                channel_type="linear",
            )
            self._log.info("public_ws_connected")
        except Exception:
            self._log.exception("public_ws_connect_failed")
            self._ws_public = None

        self._started = True
        self._log.info("adapter_started")

    async def stop(self) -> None:
        """Gracefully close all connections."""
        self._stopping = True
        self._log.info("adapter_stopping")

        for ws_name, ws in [("private", self._ws_private),
                            ("public", self._ws_public)]:
            if ws is not None:
                try:
                    ws.exit()
                    self._log.info("ws_closed", ws=ws_name)
                except Exception:
                    self._log.exception("ws_close_error", ws=ws_name)

        self._ws_private = None
        self._ws_public = None
        self._rest = None
        self._started = False
        self._stopping = False
        self._log.info("adapter_stopped")

    # ------------------------------------------------------------------
    # Internal: run blocking pybit calls in executor
    # ------------------------------------------------------------------

    async def _run(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute a synchronous pybit call in the default thread executor."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, lambda: func(*args, **kwargs)
        )

    # ------------------------------------------------------------------
    # Internal: retry wrapper
    # ------------------------------------------------------------------

    async def _call_with_retry(
        self,
        func: Callable,
        *args: Any,
        max_retries: int = MAX_RETRIES,
        **kwargs: Any,
    ) -> dict:
        """
        Call a pybit REST method with retry and error handling.

        Returns the full response dict on success.
        Raises BybitAdapterError on non-retryable failures.
        """
        # Known non-retryable error codes - fail fast, don't retry.
        NON_RETRYABLE = {
            110001,  # Order not exists or too late to cancel
            110003,  # Position not exist
            110020,  # Position mode not supported
            110025,  # Position mode is not modified (already correct)
            110043,  # Leverage not changed (already correct)
            10001,   # Invalid parameter (qty, price, etc.)
            10004,   # API key/signature/timestamp invalid
            170140,  # Order quantity exceeded risk limit
            34040,   # TP/SL not modified (already at target value)
        }
        last_exc: Optional[Exception] = None

        for attempt in range(max_retries + 1):
            try:
                resp = await self._run(func, *args, **kwargs)
                ret_code = resp.get("retCode", -1)

                if ret_code == 0:
                    return resp

                ret_msg = resp.get("retMsg", "")

                # Rate-limited: back off and retry
                if ret_code == ERR_RATE_LIMIT:
                    delay = RETRY_RATE_LIMIT_DELAY * (2 ** attempt)
                    self._log.warning("rate_limited", delay=delay,
                                      attempt=attempt)
                    await asyncio.sleep(delay)
                    continue

                raise BybitAdapterError(
                    f"Bybit API error {ret_code}: {ret_msg}",
                    ret_code=ret_code,
                    ret_msg=ret_msg,
                )

            except BybitAdapterError:
                raise
            except Exception as exc:
                last_exc = exc
                err_str = str(exc)

                # Extract ret_code from pybit error message if possible.
                ret_code = None
                import re
                m = re.search(r"ErrCode:\s*(\d+)", err_str)
                if m:
                    ret_code = int(m.group(1))

                # Non-retryable: raise immediately without retrying
                if ret_code and ret_code in NON_RETRYABLE:
                    self._log.info(
                        "api_call.non_retryable",
                        ret_code=ret_code,
                        error=err_str[:120],
                    )
                    raise BybitAdapterError(
                        err_str,
                        ret_code=ret_code,
                    )

                delay = RETRY_BASE_DELAY * (2 ** attempt)
                self._log.warning("api_call_error",
                                  error=err_str,
                                  attempt=attempt,
                                  delay=delay)
                if attempt < max_retries:
                    await asyncio.sleep(delay)

        raise BybitAdapterError(
            f"API call failed after {max_retries + 1} attempts: {last_exc}"
        )

    # ------------------------------------------------------------------
    # Account Setup
    # ------------------------------------------------------------------

    async def setup_hedge_mode(self, symbol: str) -> None:
        """
        Ensure hedge mode (``BothSide``) is active for the given symbol.

        Bybit V5 uses ``/v5/position/switch-mode``.  If already in hedge
        mode the API returns an error which we silently absorb.
        """
        try:
            resp = await self._call_with_retry(
                self._rest.switch_position_mode,
                category=CATEGORY,
                symbol=symbol,
                mode=3,  # 3 = BothSide (hedge)
            )
            self._log.info("hedge_mode_set", symbol=symbol, resp=resp)
        except BybitAdapterError as exc:
            # 110025 or 110020: already in hedge mode or not supported
            if exc.ret_code in (ERR_POSITION_MODE_NOT_SUPPORTED, 110025):
                self._log.info("hedge_mode_already_active", symbol=symbol)
            else:
                raise

    async def set_leverage(
        self, symbol: str, leverage: float, side: str
    ) -> None:
        """
        Set leverage for a symbol.

        Parameters
        ----------
        symbol : str
            e.g. ``"BTCUSDT"``
        leverage : float
            Desired leverage multiplier.
        side : str
            ``"Buy"`` or ``"Sell"`` (informational; Bybit sets both sides).
        """
        lev_str = str(leverage)
        try:
            resp = await self._call_with_retry(
                self._rest.set_leverage,
                category=CATEGORY,
                symbol=symbol,
                buyLeverage=lev_str,
                sellLeverage=lev_str,
            )
            self._log.info("leverage_set", symbol=symbol, leverage=leverage,
                           side=side)
        except BybitAdapterError as exc:
            if exc.ret_code == ERR_LEVERAGE_NOT_CHANGED:
                self._log.info("leverage_already_set",
                               symbol=symbol, leverage=leverage)
            else:
                self._log.error("set_leverage_failed",
                                symbol=symbol, leverage=leverage,
                                error=str(exc))
                raise

    async def get_wallet_balance(self) -> dict:
        """
        Return USDT wallet balance details.

        Returns a dict with keys: ``totalEquity``, ``availableBalance``,
        ``walletBalance``, ``unrealisedPnl``, etc.
        """
        resp = await self._call_with_retry(
            self._rest.get_wallet_balance,
            accountType="UNIFIED",
        )
        accounts = resp.get("result", {}).get("list", [])
        for acct in accounts:
            coins = acct.get("coin", [])
            for coin in coins:
                if coin.get("coin") == "USDT":
                    return {
                        "totalEquity": float(acct.get("totalEquity", 0)),
                        "availableBalance": float(
                            coin.get("availableToWithdraw", 0)
                        ),
                        "walletBalance": float(
                            coin.get("walletBalance", 0)
                        ),
                        "unrealisedPnl": float(
                            coin.get("unrealisedPnl", 0)
                        ),
                        "cumRealisedPnl": float(
                            coin.get("cumRealisedPnl", 0)
                        ),
                    }
        return {
            "totalEquity": 0.0,
            "availableBalance": 0.0,
            "walletBalance": 0.0,
            "unrealisedPnl": 0.0,
            "cumRealisedPnl": 0.0,
        }

    async def get_position(self, symbol: str, side: str) -> dict:
        """
        Get current position details for *symbol* and *side*.

        Parameters
        ----------
        side : str
            ``"Buy"`` or ``"Sell"``.

        Returns
        -------
        dict
            Position data including ``size``, ``avgPrice``, ``unrealisedPnl``,
            ``positionIdx``, etc.  Empty dict if no position found.
        """
        resp = await self._call_with_retry(
            self._rest.get_positions,
            category=CATEGORY,
            symbol=symbol,
        )
        positions = resp.get("result", {}).get("list", [])
        for pos in positions:
            if pos.get("side") == side:
                return {
                    "symbol": pos.get("symbol", ""),
                    "side": pos.get("side", ""),
                    "size": float(pos.get("size", 0)),
                    "avgPrice": float(pos.get("avgPrice", 0)),
                    "leverage": float(pos.get("leverage", 0)),
                    "unrealisedPnl": float(pos.get("unrealisedPnl", 0)),
                    "positionIdx": int(pos.get("positionIdx", 0)),
                    "positionValue": float(pos.get("positionValue", 0)),
                    "takeProfit": pos.get("takeProfit", ""),
                    "stopLoss": pos.get("stopLoss", ""),
                    "trailingStop": pos.get("trailingStop", ""),
                    "liqPrice": pos.get("liqPrice", ""),
                    "markPrice": float(pos.get("markPrice", 0)),
                    "positionIM": float(pos.get("positionIM", 0)),
                    "positionMM": float(pos.get("positionMM", 0)),
                    "createdTime": pos.get("createdTime", ""),
                    "updatedTime": pos.get("updatedTime", ""),
                }
        return {}

    # ------------------------------------------------------------------
    # Order Placement
    # ------------------------------------------------------------------

    async def place_market_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        position_idx: int,
        reduce_only: bool = False,
        close_on_trigger: bool = False,
    ) -> dict:
        """
        Place a market order on Bybit V5.

        Parameters
        ----------
        symbol : str
            e.g. ``"BTCUSDT"``
        side : str
            ``"Buy"`` or ``"Sell"``
        qty : float
            Order quantity in base asset units.
        position_idx : int
            1 = Buy-side hedge, 2 = Sell-side hedge.
        reduce_only : bool
            True to reduce an existing position only.
        close_on_trigger : bool
            True for conditional close-on-trigger orders.

        Returns
        -------
        dict
            Order response with ``orderId``, ``orderLinkId``, etc.
        """
        qty_str = str(self.round_qty(qty, symbol))

        self._log.info("placing_market_order",
                       symbol=symbol, side=side, qty=qty_str,
                       position_idx=position_idx, reduce_only=reduce_only)

        resp = await self._call_with_retry(
            self._rest.place_order,
            category=CATEGORY,
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=qty_str,
            positionIdx=position_idx,
            reduceOnly=reduce_only,
            closeOnTrigger=close_on_trigger,
        )
        result = resp.get("result", {})
        self._log.info("market_order_placed",
                       symbol=symbol, side=side,
                       order_id=result.get("orderId"),
                       order_link_id=result.get("orderLinkId"))
        return result

    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        position_idx: int,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> dict:
        """
        Place a limit order on Bybit V5.

        Parameters
        ----------
        price : float
            Limit price.
        post_only : bool
            If True, the order is PostOnly (rejected if it would take).

        Returns
        -------
        dict
            Order response with ``orderId``, ``orderLinkId``, etc.
        """
        qty_str = str(self.round_qty(qty, symbol))
        price_str = str(self.round_price(price, symbol))
        time_in_force = "PostOnly" if post_only else "GTC"

        self._log.info("placing_limit_order",
                       symbol=symbol, side=side, qty=qty_str,
                       price=price_str, position_idx=position_idx,
                       reduce_only=reduce_only, post_only=post_only)

        resp = await self._call_with_retry(
            self._rest.place_order,
            category=CATEGORY,
            symbol=symbol,
            side=side,
            orderType="Limit",
            qty=qty_str,
            price=price_str,
            positionIdx=position_idx,
            reduceOnly=reduce_only,
            timeInForce=time_in_force,
        )
        result = resp.get("result", {})
        self._log.info("limit_order_placed",
                       symbol=symbol, side=side,
                       order_id=result.get("orderId"),
                       price=price_str)
        return result

    async def cancel_order(self, symbol: str, order_id: str) -> dict:
        """Cancel an open order by ``orderId``."""
        self._log.info("cancelling_order", symbol=symbol, order_id=order_id)
        try:
            resp = await self._call_with_retry(
                self._rest.cancel_order,
                category=CATEGORY,
                symbol=symbol,
                orderId=order_id,
            )
            result = resp.get("result", {})
            self._log.info("order_cancelled", order_id=order_id)
            return result
        except BybitAdapterError as exc:
            if exc.ret_code == ERR_ORDER_NOT_EXIST:
                self._log.warning("cancel_order_not_found",
                                  order_id=order_id)
                return {"orderId": order_id, "status": "not_found"}
            raise

    async def get_order(self, symbol: str, order_id: str) -> dict:
        """Fetch a single order by ``orderId``."""
        resp = await self._call_with_retry(
            self._rest.get_open_orders,
            category=CATEGORY,
            symbol=symbol,
            orderId=order_id,
        )
        orders = resp.get("result", {}).get("list", [])
        if orders:
            return orders[0]

        # Try order history if not in open orders
        resp = await self._call_with_retry(
            self._rest.get_order_history,
            category=CATEGORY,
            symbol=symbol,
            orderId=order_id,
        )
        orders = resp.get("result", {}).get("list", [])
        return orders[0] if orders else {}

    async def get_open_orders(self, symbol: str = None) -> list:
        """Fetch all open orders, optionally filtered by symbol."""
        kwargs: dict = {"category": CATEGORY}
        if symbol:
            kwargs["symbol"] = symbol

        resp = await self._call_with_retry(
            self._rest.get_open_orders,
            **kwargs,
        )
        return resp.get("result", {}).get("list", [])

    # ------------------------------------------------------------------
    # Position Management (Trading Stop)
    # ------------------------------------------------------------------

    async def set_trading_stop(
        self,
        symbol: str,
        position_idx: int,
        take_profit: Optional[float] = None,
        stop_loss: Optional[float] = None,
        trailing_stop: Optional[float] = None,
        active_price: Optional[float] = None,
        tp_trigger_by: str = "MarkPrice",
        sl_trigger_by: str = "MarkPrice",
    ) -> dict:
        """
        Set TP / SL / trailing stop on an existing position via
        ``/v5/position/trading-stop``.

        This is the canonical way to manage TP/SL/trailing on Bybit
        derivatives.  Do NOT use separate conditional orders.

        Parameters
        ----------
        position_idx : int
            1 = Buy hedge, 2 = Sell hedge.
        trailing_stop : float, optional
            Trailing stop distance in price terms (NOT percentage).
        active_price : float, optional
            Activation price for the trailing stop.
        """
        kwargs: Dict[str, Any] = {
            "category": CATEGORY,
            "symbol": symbol,
            "positionIdx": position_idx,
            "tpTriggerBy": tp_trigger_by,
            "slTriggerBy": sl_trigger_by,
        }

        if take_profit is not None:
            kwargs["takeProfit"] = str(self.round_price(take_profit, symbol))
        if stop_loss is not None:
            kwargs["stopLoss"] = str(self.round_price(stop_loss, symbol))
        if trailing_stop is not None:
            kwargs["trailingStop"] = str(
                self.round_price(trailing_stop, symbol)
            )
        if active_price is not None:
            kwargs["activePrice"] = str(
                self.round_price(active_price, symbol)
            )

        self._log.info("setting_trading_stop", **kwargs)

        try:
            resp = await self._call_with_retry(
                self._rest.set_trading_stop,
                **kwargs,
            )
            self._log.info("trading_stop_set", symbol=symbol,
                           position_idx=position_idx)
            return resp.get("result", {})
        except BybitAdapterError as exc:
            # 34040: TP/SL not modified (already at target value) - treat as success.
            if exc.ret_code == 34040:
                self._log.info("trading_stop_already_set",
                               symbol=symbol, position_idx=position_idx)
                return {}
            raise

    async def add_margin(
        self, symbol: str, position_idx: int, margin: float
    ) -> dict:
        """
        Add margin to an isolated-margin position via
        ``/v5/position/add-margin``.

        Parameters
        ----------
        margin : float
            Amount of USDT to add.
        """
        self._log.info("adding_margin",
                       symbol=symbol, position_idx=position_idx,
                       margin=margin)

        resp = await self._call_with_retry(
            self._rest.add_or_reduce_margin,
            category=CATEGORY,
            symbol=symbol,
            margin=str(margin),
            positionIdx=position_idx,
        )
        self._log.info("margin_added", symbol=symbol, margin=margin)
        return resp.get("result", {})

    # ------------------------------------------------------------------
    # Market Data
    # ------------------------------------------------------------------

    async def get_ticker(self, symbol: str) -> dict:
        """
        Fetch current ticker data for *symbol*.

        Returns a dict with keys: ``lastPrice``, ``markPrice``,
        ``indexPrice``, ``bid1Price``, ``ask1Price``, ``volume24h``, etc.
        """
        resp = await self._call_with_retry(
            self._rest.get_tickers,
            category=CATEGORY,
            symbol=symbol,
        )
        tickers = resp.get("result", {}).get("list", [])
        if not tickers:
            raise BybitAdapterError(
                f"No ticker data for {symbol}", ret_code=-1
            )
        t = tickers[0]
        return {
            "symbol": t.get("symbol", ""),
            "lastPrice": float(t.get("lastPrice", 0)),
            "markPrice": float(t.get("markPrice", 0)),
            "indexPrice": float(t.get("indexPrice", 0)),
            "bid1Price": float(t.get("bid1Price", 0)),
            "ask1Price": float(t.get("ask1Price", 0)),
            "volume24h": float(t.get("volume24h", 0)),
            "turnover24h": float(t.get("turnover24h", 0)),
            "highPrice24h": float(t.get("highPrice24h", 0)),
            "lowPrice24h": float(t.get("lowPrice24h", 0)),
            "prevPrice24h": float(t.get("prevPrice24h", 0)),
            "price24hPcnt": float(t.get("price24hPcnt", 0)),
            "fundingRate": t.get("fundingRate", ""),
            "nextFundingTime": t.get("nextFundingTime", ""),
        }

    async def get_instrument_info(self, symbol: str) -> dict:
        """
        Fetch instrument specification for *symbol* (with caching).

        Returns a dict with ``minOrderQty``, ``maxOrderQty``,
        ``qtyStep``, ``tickSize``, ``minPrice``, ``maxPrice``, etc.
        """
        cached = self._instrument_cache.get(symbol)
        if cached and not cached.expired:
            return cached.data

        resp = await self._call_with_retry(
            self._rest.get_instruments_info,
            category=CATEGORY,
            symbol=symbol,
        )
        instruments = resp.get("result", {}).get("list", [])
        if not instruments:
            raise BybitAdapterError(
                f"No instrument info for {symbol}", ret_code=-1
            )

        raw = instruments[0]
        lot_filter = raw.get("lotSizeFilter", {})
        price_filter = raw.get("priceFilter", {})
        leverage_filter = raw.get("leverageFilter", {})

        data = {
            "symbol": raw.get("symbol", ""),
            "baseCoin": raw.get("baseCoin", ""),
            "quoteCoin": raw.get("quoteCoin", ""),
            "status": raw.get("status", ""),
            "minOrderQty": float(lot_filter.get("minOrderQty", 0)),
            "maxOrderQty": float(lot_filter.get("maxOrderQty", 0)),
            "qtyStep": float(lot_filter.get("qtyStep", 0)),
            "minNotionalValue": float(
                lot_filter.get("minNotionalValue", 0)
            ),
            "tickSize": float(price_filter.get("tickSize", 0)),
            "minPrice": float(price_filter.get("minPrice", 0)),
            "maxPrice": float(price_filter.get("maxPrice", 0)),
            "minLeverage": float(leverage_filter.get("minLeverage", 1)),
            "maxLeverage": float(leverage_filter.get("maxLeverage", 1)),
            "leverageStep": float(
                leverage_filter.get("leverageStep", 0)
            ),
        }

        self._instrument_cache[symbol] = _CachedInstrument(
            data=data, fetched_at=time.monotonic()
        )
        self._log.debug("instrument_info_cached", symbol=symbol)
        return data

    # ------------------------------------------------------------------
    # Symbol Utilities
    # ------------------------------------------------------------------

    async def get_min_order_qty(self, symbol: str) -> float:
        """Return the minimum order quantity for *symbol*."""
        info = await self.get_instrument_info(symbol)
        return info["minOrderQty"]

    async def get_price_precision(self, symbol: str) -> int:
        """
        Return the number of decimal places for price on *symbol*.

        Derived from the instrument's ``tickSize``.
        """
        info = await self.get_instrument_info(symbol)
        return self._decimals_from_step(info["tickSize"])

    async def get_qty_precision(self, symbol: str) -> int:
        """
        Return the number of decimal places for quantity on *symbol*.

        Derived from the instrument's ``qtyStep``.
        """
        info = await self.get_instrument_info(symbol)
        return self._decimals_from_step(info["qtyStep"])

    def round_price(self, price: float, symbol: str) -> float:
        """
        Round *price* to the symbol's tick-size precision.

        Uses cached instrument info.  If no cache entry exists yet,
        returns *price* unchanged (caller should ensure cache is warm).
        """
        cached = self._instrument_cache.get(symbol)
        if not cached:
            return price
        tick = cached.data["tickSize"]
        if tick <= 0:
            return price
        precision = self._decimals_from_step(tick)
        # Round down to nearest tick
        return round(math.floor(price / tick) * tick, precision)

    def round_qty(self, qty: float, symbol: str) -> float:
        """
        Round *qty* down to the symbol's qty-step precision.

        Uses cached instrument info.
        """
        cached = self._instrument_cache.get(symbol)
        if not cached:
            return qty
        step = cached.data["qtyStep"]
        if step <= 0:
            return qty
        precision = self._decimals_from_step(step)
        return round(math.floor(qty / step) * step, precision)

    def calculate_order_qty(
        self,
        margin: float,
        leverage: float,
        price: float,
        symbol: str,
    ) -> float:
        """
        Compute order quantity from margin, leverage and current price.

        ``qty = (margin * leverage) / price``, rounded down to the
        symbol's qty precision.
        """
        if price <= 0:
            raise ValueError("Price must be positive")
        raw_qty = (margin * leverage) / price
        return self.round_qty(raw_qty, symbol)

    # ------------------------------------------------------------------
    # WebSocket callbacks (called from pybit's background threads)
    # ------------------------------------------------------------------

    def _raw_order_callback(self, message: dict) -> None:
        """
        Raw callback invoked by pybit's private WS on order updates.

        Deduplicates and dispatches to the user-provided callback.
        """
        if self._stopping:
            return
        try:
            data_list = message.get("data", [])
            for order_data in data_list:
                order_id = order_data.get("orderId", "")
                update_time = order_data.get("updatedTime", "")
                dedup_key = f"{order_id}:{update_time}"

                if not self._order_dedup.check_and_add(dedup_key):
                    self._log.debug("order_update_dedup", order_id=order_id)
                    continue

                self._log.info(
                    "order_update",
                    order_id=order_id,
                    symbol=order_data.get("symbol"),
                    side=order_data.get("side"),
                    status=order_data.get("orderStatus"),
                    qty=order_data.get("qty"),
                    filled_qty=order_data.get("cumExecQty"),
                    avg_price=order_data.get("avgPrice"),
                )

                if self._on_order_update:
                    self._on_order_update(order_data)
        except Exception:
            self._log.exception("order_callback_error")

    def _raw_position_callback(self, message: dict) -> None:
        """
        Raw callback invoked by pybit's private WS on position updates.
        """
        if self._stopping:
            return
        try:
            data_list = message.get("data", [])
            for pos_data in data_list:
                self._log.info(
                    "position_update",
                    symbol=pos_data.get("symbol"),
                    side=pos_data.get("side"),
                    size=pos_data.get("size"),
                    avg_price=pos_data.get("avgPrice"),
                    unrealised_pnl=pos_data.get("unrealisedPnl"),
                    position_idx=pos_data.get("positionIdx"),
                )

                if self._on_position_update:
                    self._on_position_update(pos_data)
        except Exception:
            self._log.exception("position_callback_error")

    def _raw_execution_callback(self, message: dict) -> None:
        """
        Raw callback invoked by pybit's private WS on execution/fill
        updates.
        """
        if self._stopping:
            return
        try:
            data_list = message.get("data", [])
            for exec_data in data_list:
                exec_id = exec_data.get("execId", "")
                if not self._execution_dedup.check_and_add(exec_id):
                    self._log.debug("execution_dedup", exec_id=exec_id)
                    continue

                self._log.info(
                    "execution_update",
                    exec_id=exec_id,
                    order_id=exec_data.get("orderId"),
                    symbol=exec_data.get("symbol"),
                    side=exec_data.get("side"),
                    exec_qty=exec_data.get("execQty"),
                    exec_price=exec_data.get("execPrice"),
                    exec_type=exec_data.get("execType"),
                )

                if self._on_execution_update:
                    self._on_execution_update(exec_data)
        except Exception:
            self._log.exception("execution_callback_error")

    # ------------------------------------------------------------------
    # Helpers (private)
    # ------------------------------------------------------------------

    @staticmethod
    def _decimals_from_step(step: float) -> int:
        """
        Determine the number of decimal places implied by a step value.

        Examples: 0.01 -> 2, 0.001 -> 3, 1.0 -> 0, 0.5 -> 1
        """
        if step <= 0:
            return 0
        step_str = f"{step:.10f}".rstrip("0")
        if "." not in step_str:
            return 0
        return len(step_str.split(".")[1])

    def invalidate_instrument_cache(self, symbol: Optional[str] = None) -> None:
        """
        Clear the instrument info cache.

        If *symbol* is given, only that entry is cleared.  Otherwise the
        entire cache is wiped.
        """
        if symbol:
            self._instrument_cache.pop(symbol, None)
        else:
            self._instrument_cache.clear()
        self._log.debug("instrument_cache_invalidated", symbol=symbol)

    @property
    def is_connected(self) -> bool:
        """Return ``True`` if the adapter is started and REST client exists."""
        return self._started and self._rest is not None

    @property
    def has_private_ws(self) -> bool:
        """Return ``True`` if the private WebSocket is connected."""
        return self._ws_private is not None

    @property
    def has_public_ws(self) -> bool:
        """Return ``True`` if the public WebSocket is connected."""
        return self._ws_public is not None
