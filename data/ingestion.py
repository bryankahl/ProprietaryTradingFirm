import os
import logging
from datetime import datetime, timedelta, timezone
import alpaca_trade_api as tradeapi
from .timescale_repo import TimescaleRepo

logger = logging.getLogger("DataIngestion")

class DataIngestion:
    def __init__(self, db: TimescaleRepo):
        self.db = db
        # We use a dedicated REST client for data fetching
        self.api = tradeapi.REST(
            os.getenv("ALPACA_KEY"),
            os.getenv("ALPACA_SECRET"),
            # Defensive fix: Ensure URL is clean for the library
            os.getenv("ALPACA_ENDPOINT", "https://paper-api.alpaca.markets").replace("/v2", "").rstrip("/"),
            api_version='v2'
        )

    async def backfill_bars(self, symbols: list[str], days: int = 2):
        """
        Fetches historical minute bars and pushes them to TimescaleDB.
        """
        logger.info(f"--- STARTING BACKFILL ({days} days) ---")
        
        # Calculate time range (UTC)
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days)
        
        # ISO Format for Alpaca
        start_str = start_dt.isoformat()
        end_str = end_dt.isoformat()

        for symbol in symbols:
            logger.info(f"Fetching IEX data for: {symbol}...")
            try:
                # --- FIX APPLIED HERE: feed='iex' ---
                bars = self.api.get_bars(
                    symbol, 
                    tradeapi.TimeFrame.Minute, 
                    start=start_str, 
                    end=end_str,
                    adjustment='raw',
                    feed='iex'  # <--- CRITICAL: Use the Free Data Feed
                ).df
                
                if bars.empty:
                    logger.warning(f"No data returned for {symbol}")
                    continue

                # Convert DataFrame to List of Tuples for asyncpg
                data_to_insert = []
                for timestamp, row in bars.iterrows():
                    record = (
                        timestamp.to_pydatetime(), 
                        symbol,
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['close']),
                        float(row['volume'])
                    )
                    data_to_insert.append(record)

                # Batch Insert
                if data_to_insert:
                    await self.db.insert_bars(data_to_insert)
                    logger.info(f"Stored {len(data_to_insert)} bars for {symbol}")

            except Exception as e:
                logger.error(f"Failed to ingest {symbol}: {e}")


    # --- ADD THIS TO data/ingestion.py ---
    async def update_live_data(self, symbols: list[str]):
        """Fetches the latest 5 minutes of data to ensure DB is fresh."""
        # Look back 5 minutes to capture the latest closed bar
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(minutes=5)
        
        start_str = start_dt.isoformat()
        end_str = end_dt.isoformat()

        for symbol in symbols:
            try:
                bars = self.api.get_bars(
                    symbol, 
                    tradeapi.TimeFrame.Minute, 
                    start=start_str, 
                    end=end_str,
                    adjustment='raw',
                    feed='iex' 
                ).df
                
                if bars.empty: continue

                data_to_insert = []
                for timestamp, row in bars.iterrows():
                    record = (
                        timestamp.to_pydatetime(), 
                        symbol,
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['close']),
                        float(row['volume'])
                    )
                    data_to_insert.append(record)

                if data_to_insert:
                    # This will ignore duplicates and only add new bars
                    await self.db.insert_bars(data_to_insert)
                    
            except Exception as e:
                logger.error(f"Live Ingest Error {symbol}: {e}")