import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from datetime import datetime
import pytz # Required for timezone handling

from core.risk_manager import RiskManager, RiskException
from execution.broker_interface import BrokerInterface
from data.timescale_repo import TimescaleRepo

logger = logging.getLogger("BaseStrategy")

class BaseStrategy(ABC):
    """
    The Abstract Base Class for all trading strategies.
    Enforces Risk Management checks before every execution.
    """
    def __init__(self, name: str, broker: BrokerInterface, risk_manager: RiskManager, db: TimescaleRepo):
        self.name = name
        self.broker = broker
        self.risk_manager = risk_manager
        self.db = db
        self.is_running = False
        
        # Cache for active positions to reduce API calls
        self.positions: Dict[str, float] = {} 

    def is_market_open(self) -> bool:
        """
        Checks if the NYSE is currently open (09:30 - 16:00 EST).
        """
        tz = pytz.timezone('America/New_York')
        now = datetime.now(tz)
        
        # Weekends
        if now.weekday() > 4:
            return False
            
        # Market Hours
        market_start = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_end = now.replace(hour=16, minute=0, second=0, microsecond=0)
        
        return market_start <= now <= market_end

    async def run(self):
        logger.info(f"Starting Strategy: {self.name}")
        self.is_running = True
        
        try:
            await self.sync_state()
            
            while self.is_running:
                # 1. TIME CHECK (Timezone Aware)
                tz = pytz.timezone('America/New_York')
                now = datetime.now(tz)

                # --- NEW: HARD EXIT AT 3:50 PM ---
                # This prevents holding over the weekend or trading into illiquid closings
                if now.weekday() <= 4 and now.hour == 15 and now.minute >= 50:
                    if self.positions:
                        logger.warning("END OF DAY DETECTED (3:50 PM). FLATTENING BOOK.")
                        await self.broker.close_all_positions()
                        await self.sync_state()
                        # Sleep until tomorrow morning to avoid re-entering
                        logger.info("Sleeping until market open...")
                        await asyncio.sleep(60 * 60) # Sleep 1 hour
                        continue
                # ---------------------------------

                if not self.is_market_open():
                    logger.info("Market Closed. Sleeping for 60s...")
                    await asyncio.sleep(60)
                    continue

                # A. Risk Check
                await self._check_global_risk()
                
                # B. Strategy Logic
                await self.calculate_signals()
                
                # C. Heartbeat
                await asyncio.sleep(60) 
                
        except Exception as e:
            logger.error(f"Strategy Crash: {e}")
            await self.emergency_stop()

    async def sync_state(self):
        """Syncs local state with Broker."""
        account = await self.broker.get_account()
        
        # Push Equity to Risk Manager to update Drawdown calculations
        self.risk_manager.update_pnl(current_equity=account.equity)
        
        # Update local position cache
        positions = await self.broker.get_positions()
        self.positions = {p.symbol: p.qty for p in positions}
        logger.info(f"State Synced. Equity: {account.equity}")

    async def _check_global_risk(self):
        """Runs the Daily PnL check."""
        account = await self.broker.get_account()
        self.risk_manager.update_pnl(current_equity=account.equity)

    async def execute_order(self, symbol: str, qty: float, side: str, order_type: str = "market"):
        """
        The ONLY way a strategy is allowed to place an order.
        Wraps the Broker call in a Risk Check.
        """
        if not self.risk_manager.can_execute_trade(trade_size_notional=qty * 100): # Approximation for now
             logger.warning(f"Order blocked by Risk Manager: {symbol} {qty}")
             return

        # 2. Execute
        await self.broker.submit_order(symbol, qty, side, order_type)
        
        # 3. Post-Trade Sync
        await self.sync_state()

    async def emergency_stop(self):
        """Stops the loop and liquidates if required."""
        self.is_running = False
        logger.critical("EMERGENCY STOP TRIGGERED.")
        # Always attempt to flatten on emergency stop for safety
        await self.broker.close_all_positions()

    @abstractmethod
    async def calculate_signals(self):
        """
        The Logic. Must be implemented by the child class.
        """
        pass