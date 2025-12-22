import logging
import asyncio
import numpy as np
from collections import deque
from .base_strategy import BaseStrategy

logger = logging.getLogger("StatArbPairs")

class StatArbPairs(BaseStrategy):
    def __init__(self, broker, risk_manager, db, ingestor, symbol_a: str, symbol_b: str, window: int = 30):
        super().__init__(f"Pairs-{symbol_a}", broker, risk_manager, db)
        self.symbol_a = symbol_a 
        self.symbol_b = symbol_b 
        self.ingestor = ingestor 
        
        self.lookback_window = window 
        self.z_score_threshold = 2.1 
        
        # IN-MEMORY HISTORY (The "Speed" Fix)
        self.spread_history = deque(maxlen=window)
        self.initialized = False
        
        # SIZING CONFIG ($5k per leg)
        self.target_position_value = 5000.0

    async def warm_up_data(self):
        logger.info("Warming up data from DB...")
        bars_a = await self.db.get_latest_bars(self.symbol_a, limit=self.lookback_window)
        bars_b = await self.db.get_latest_bars(self.symbol_b, limit=self.lookback_window)
        
        min_len = min(len(bars_a), len(bars_b))
        bars_a = bars_a[:min_len]
        bars_b = bars_b[:min_len]

        for i in range(min_len):
            price_a = float(bars_a[i]['close'])
            price_b = float(bars_b[i]['close'])
            self.spread_history.append(price_a - price_b)
            
        self.initialized = True
        logger.info(f"Warmup Complete. History Length: {len(self.spread_history)}")

    async def calculate_signals(self):
        # 1. Warm up once
        if not self.initialized:
            await self.warm_up_data()
            return

        # 2. Get INSTANT Prices
        price_a = await self.broker.get_last_price(self.symbol_a)
        price_b = await self.broker.get_last_price(self.symbol_b)

        if price_a == 0 or price_b == 0:
            return

        # 3. Update Stats
        current_spread = price_a - price_b
        self.spread_history.append(current_spread)
        
        if len(self.spread_history) < self.lookback_window:
            return

        # Calculate Z-Score
        spreads = np.array(self.spread_history)
        mean_spread = np.mean(spreads)
        std_spread = np.std(spreads)
        
        if std_spread == 0: return
        
        z_score = (current_spread - mean_spread) / std_spread
        
        logger.info(f"Spread: {current_spread:.2f} | Z: {z_score:.2f} | A: ${price_a} B: ${price_b}")

        # --- CATASTROPHE CHECK ---
        if abs(z_score) > 4.0:
            if self.positions:
                logger.critical(f"BROKEN CORRELATION (Z={z_score:.2f}). Emergency Exit.")
                await self.broker.close_all_positions()
                self.positions.clear()
            return

        # --- EXECUTION LOGIC (DOLLAR NEUTRAL) ---
        qty_a = int(self.target_position_value // price_a)
        qty_b = int(self.target_position_value // price_b)

        if z_score > self.z_score_threshold:
            # SELL A / BUY B
            if self.symbol_a not in self.positions:
                logger.info(f"ENTRY SHORT: Sell {qty_a} {self.symbol_a} / Buy {qty_b} {self.symbol_b}")
                await asyncio.gather(
                    self.execute_order(self.symbol_a, qty_a, "sell"),
                    self.execute_order(self.symbol_b, qty_b, "buy")
                )

        elif z_score < -self.z_score_threshold:
            # BUY A / SELL B
            if self.symbol_a not in self.positions:
                logger.info(f"ENTRY LONG: Buy {qty_a} {self.symbol_a} / Sell {qty_b} {self.symbol_b}")
                await asyncio.gather(
                    self.execute_order(self.symbol_a, qty_a, "buy"),
                    self.execute_order(self.symbol_b, qty_b, "sell")
                )
        
        elif abs(z_score) < 0.5:
            # EXIT (Mean Reversion)
            if self.symbol_a in self.positions or self.symbol_b in self.positions:
                logger.info("EXIT SIGNAL: Mean Reversion. Closing all.")
                await self.broker.close_all_positions()
                self.positions.clear()