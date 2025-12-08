import os
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
import alpaca_trade_api as tradeapi
from .broker_interface import BrokerInterface, AccountInfo, Position

logger = logging.getLogger("AlpacaAdapter")

class AlpacaAdapter(BrokerInterface):
    def __init__(self):
        self.api_key = os.getenv("ALPACA_KEY")
        self.secret_key = os.getenv("ALPACA_SECRET")
        
        # --- DEFENSIVE FIX START ---
        # Get URL, default to paper, and forcefully strip trailing slashes or versions
        raw_url = os.getenv("ALPACA_ENDPOINT", "https://paper-api.alpaca.markets")
        self.base_url = raw_url.replace("/v2", "").rstrip("/")
        # --- DEFENSIVE FIX END ---
        
        if not self.api_key or not self.secret_key:
            raise ValueError("Alpaca API credentials missing in .env")

        # Initialize the synchronous REST API
        self.api = tradeapi.REST(
            self.api_key, 
            self.secret_key, 
            self.base_url, 
            api_version='v2'  # This adds the single required '/v2'
        )
        
        # Executor for non-blocking I/O
        self._executor = ThreadPoolExecutor(max_workers=4)
        logger.info(f"Alpaca Adapter initialized at: {self.base_url}")

    async def _run_sync(self, func, *args):
        """Helper to run blocking REST calls in a separate thread."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, func, *args)

    async def get_account(self) -> AccountInfo:
        """Async wrapper for api.get_account()"""
        try:
            acct = await self._run_sync(self.api.get_account)
            return AccountInfo(
                equity=float(acct.equity),
                buying_power=float(acct.buying_power),
                cash=float(acct.cash)
            )
        except Exception as e:
            logger.error(f"Error fetching account: {e}")
            raise e

    async def get_positions(self) -> list[Position]:
        """Async wrapper for api.list_positions()"""
        try:
            alpaca_positions = await self._run_sync(self.api.list_positions)
            clean_positions = []
            for p in alpaca_positions:
                clean_positions.append(Position(
                    symbol=p.symbol,
                    qty=float(p.qty),
                    current_price=float(p.current_price),
                    market_value=float(p.market_value),
                    unrealized_pl=float(p.unrealized_pl)
                ))
            return clean_positions
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            raise e

    async def submit_order(self, symbol: str, qty: float, side: str, 
                         order_type: str = "market", 
                         limit_price: float = None) -> dict:
        try:
            # Construct args
            order_args = {
                "symbol": symbol,
                "qty": qty,
                "side": side,
                "type": order_type,
                "time_in_force": "gtc"
            }
            if limit_price and order_type == 'limit':
                order_args["limit_price"] = limit_price

            order = await self._run_sync(self.api.submit_order, **order_args)
            logger.info(f"Order Submitted: {side} {qty} {symbol}")
            return order._raw
        except Exception as e:
            logger.error(f"Order Failed: {e}")
            raise e

    async def close_all_positions(self):
        """The Nuclear Option for the Risk Manager."""
        logger.warning("CLOSING ALL POSITIONS...")
        await self._run_sync(self.api.close_all_positions)
        await self._run_sync(self.api.cancel_all_orders)
        logger.info("All positions closed and orders cancelled.")