import logging
import asyncio
import numpy as np
from .base_strategy import BaseStrategy

logger = logging.getLogger("StatArbPairs")

class StatArbPairs(BaseStrategy):
    # Added 'ingestor' to the arguments vvv
    def __init__(self, broker, risk_manager, db, ingestor, symbol_a: str, symbol_b: str, window: int = 30):
        super().__init__(f"Pairs-{symbol_a}", broker, risk_manager, db)
        self.symbol_a = symbol_a 
        self.symbol_b = symbol_b 
        self.ingestor = ingestor 
        
        self.lookback_window = window 
        self.z_score_threshold = 2.1 
        self.trade_qty = 10        

    async def calculate_signals(self):
        logger.info(f"Analyzing Market ({self.symbol_a}/{self.symbol_b})...")
        
        # --- SIP LIVE DATA ---
        await self.ingestor.update_live_data([self.symbol_a, self.symbol_b])
        # ---------------------

        # 1. Fetch bars from DB
        bars_a = await self.db.get_latest_bars(self.symbol_a, limit=self.lookback_window)
        bars_b = await self.db.get_latest_bars(self.symbol_b, limit=self.lookback_window)

        if len(bars_a) < self.lookback_window or len(bars_b) < self.lookback_window:
            logger.warning("Not enough data to calculate Z-Score. Waiting...")
            return

        # 2. Math Logic
        closes_a = np.array([float(b['close']) for b in bars_a])
        closes_b = np.array([float(b['close']) for b in bars_b])
        
        spread = closes_a - closes_b
        mean_spread = np.mean(spread)
        std_spread = np.std(spread)
        
        if std_spread == 0: return

        current_spread = spread[-1]
        z_score = (current_spread - mean_spread) / std_spread
        
        logger.info(f"Spread: {current_spread:.2f} | Z-Score: {z_score:.2f}")

        # --- 3. CATASTROPHE CHECK (Broken Correlation) ---
        if abs(z_score) > 4.5:
            logger.critical(f"BROKEN CORRELATION DETECTED! Z: {z_score:.2f}. FLATTENING BOOK.")
            await self.broker.close_all_positions()
            return # Stop processing
        # --------------------------------------------------

        # 4. Execution
        if z_score > self.z_score_threshold:
            if self.symbol_a not in self.positions:
                logger.info("ENTRY SIGNAL: Short A / Long B")
                await self.execute_order(self.symbol_a, self.trade_qty, "sell")
                await self.execute_order(self.symbol_b, self.trade_qty, "buy")

        elif z_score < -self.z_score_threshold:
            if self.symbol_a not in self.positions:
                logger.info("ENTRY SIGNAL: Long A / Short B")
                await self.execute_order(self.symbol_a, self.trade_qty, "buy")
                await self.execute_order(self.symbol_b, self.trade_qty, "sell")
        
        elif abs(z_score) < 0.5:
            if self.symbol_a in self.positions:
                logger.info("EXIT SIGNAL: Mean Reversion Reached")
                await self.broker.close_all_positions()