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
        Checks if the NYSE is currently open (09:30 - 16:00 EST, Mon-Fri).
        """
        tz = pytz.timezone('America/New_York')
        now = datetime.now(tz)
        
        # 0=Monday, 4=Friday, 5=Saturday, 6=Sunday
        if now.weekday() > 4:
            return False
            
        # Market Hours: 09:30 to 16:00
        market_start = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_end = now.replace(hour=16, minute=0, second=0, microsecond=0)
        
        return market_start <= now <= market_end

    async def run(self):
        """
        The Main Event Loop.
        """
        logger.info(f"Starting Strategy: {self.name}")
        self.is_running = True
        
        try:
            # 1. Initial State Sync
            await self.sync_state()
            
            # 2. Main Loop
            while self.is_running:
                # --- MARKET HOURS CHECK ---
                if not self.is_market_open():
                    if self.positions:
                        logger.warning("MARKET CLOSED with Open Positions! Triggering Emergency Flatten.")
                        await self.broker.close_all_positions()
                        await self.sync_state()
                    
                    logger.info("Market Closed. Sleeping for 60s...")
                    await asyncio.sleep(60)
                    continue
                # --------------------------

                # A. Check System Health (Risk)
                await self._check_global_risk()
                
                # B. Execute Strategy Logic (The "Brain")
                await self.calculate_signals()
                
                # C. Wait for next heartbeat (e.g., 1 minute bars)
                await asyncio.sleep(60) 
                
        except RiskException as e:
            logger.critical(f"STRATEGY HALTED BY RISK MANAGER: {e}")
            await self.emergency_stop()
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