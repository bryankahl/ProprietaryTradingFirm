import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from datetime import datetime
import pytz 

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
        self.positions: Dict[str, float] = {} 

    def is_market_open(self) -> bool:
        """
        Checks if the NYSE is currently open (09:30 - 16:00 EST).
        """
        tz = pytz.timezone('America/New_York')
        now = datetime.now(tz)
        
        # Weekends (Saturday=5, Sunday=6)
        if now.weekday() > 4:
            return False
            
        # Market Hours
        market_start = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_end = now.replace(hour=16, minute=0, second=0, microsecond=0)
        
        return market_start <= now <= market_end

    async def run(self):
        logger.info(f"Starting Strategy: {self.name} (Professional Pace)")
        self.is_running = True
        
        try:
            await self.sync_state()
            
            while self.is_running:
                tz = pytz.timezone('America/New_York')
                now = datetime.now(tz)

                # 1. END OF DAY EXIT (3:50 PM)
                if now.weekday() <= 4 and now.hour == 15 and now.minute >= 50:
                    if self.positions:
                        logger.warning("END OF DAY DETECTED (3:50 PM). FLATTENING BOOK.")
                        await self.broker.close_all_positions()
                        await self.sync_state()
                        # Sleep 1 hour so we push past 4:00 PM
                        logger.info("Positions closed. Sleeping until market close...")
                        await asyncio.sleep(60 * 60)
                        continue

                # 2. MARKET HOURS CHECK
                if not self.is_market_open():
                    # Check every 5 minutes if the market has opened
                    logger.info("Market is Closed. Sleeping for 5 mins...")
                    await asyncio.sleep(300) 
                    continue

                # 3. LIVE TRADING
                await self._check_global_risk()
                await self.calculate_signals()
                
                # --- HEARTBEAT SET TO 60 SECONDS ---
                # This aligns the execution speed with the math (Mean Reversion).
                await asyncio.sleep(60) 
                
        except Exception as e:
            logger.error(f"Strategy Crash: {e}")
            await self.emergency_stop()

    async def sync_state(self):
        account = await self.broker.get_account()
        self.risk_manager.update_pnl(current_equity=account.equity)
        positions = await self.broker.get_positions()
        self.positions = {p.symbol: p.qty for p in positions}

    async def _check_global_risk(self):
        account = await self.broker.get_account()
        self.risk_manager.update_pnl(current_equity=account.equity)

    async def execute_order(self, symbol: str, qty: float, side: str, order_type: str = "market"):
        if not self.risk_manager.can_execute_trade(trade_size_notional=qty * 100):
             logger.warning(f"Order blocked by Risk Manager: {symbol} {qty}")
             return
        await self.broker.submit_order(symbol, qty, side, order_type)
        await self.sync_state()

    async def emergency_stop(self):
        self.is_running = False
        logger.critical("EMERGENCY STOP TRIGGERED.")
        await self.broker.close_all_positions()

    @abstractmethod
    async def calculate_signals(self):
        pass