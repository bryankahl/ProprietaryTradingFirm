import os
import asyncpg
import logging
from datetime import datetime
from typing import List, Tuple

logger = logging.getLogger("TimescaleRepo")

class TimescaleRepo:
    """
    Async wrapper for TimescaleDB interactions.
    Handles connection pooling and schema initialization.
    """
    def __init__(self):
        self.user = os.getenv("DB_USER", "sniper_user")
        self.password = os.getenv("DB_PASS", "sniper_password")
        self.database = os.getenv("DB_NAME", "sniper_db")
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        self.pool = None

    async def connect(self):
        """Initializes the connection pool."""
        if not self.pool:
            try:
                self.pool = await asyncpg.create_pool(
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    host=self.host,
                    port=self.port,
                    min_size=2,
                    max_size=10
                )
                logger.info("Connected to TimescaleDB.")
                await self._init_schema()
            except Exception as e:
                logger.critical(f"Failed to connect to DB: {e}")
                raise e

    async def disconnect(self):
        """Closes the connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("Disconnected from TimescaleDB.")

    async def _init_schema(self):
        """
        Creates necessary tables and hypertables if they don't exist.
        Schema: Standard OHLCV for 1-minute bars.
        """
        schema_sql = """
        -- 1. Create standard table
        CREATE TABLE IF NOT EXISTS market_bars (
            time TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            PRIMARY KEY (time, symbol)
        );

        -- 2. Convert to Hypertable (Timescale magic)
        -- We suppress error if it already exists
        SELECT create_hypertable('market_bars', 'time', if_not_exists => TRUE);
        """
        async with self.pool.acquire() as conn:
            await conn.execute(schema_sql)
            logger.info("Database schema initialized.")

    async def insert_bars(self, bars: List[Tuple]):
        """
        Batch insert bars.
        bars format: [(time, symbol, o, h, l, c, v), ...]
        """
        query = """
        INSERT INTO market_bars (time, symbol, open, high, low, close, volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (time, symbol) DO NOTHING;
        """
        async with self.pool.acquire() as conn:
            await conn.executemany(query, bars)

    async def get_latest_bars(self, symbol: str, limit: int = 100):
        """Fetch recent data for strategy calculation."""
        query = """
        SELECT time, open, high, low, close, volume 
        FROM market_bars 
        WHERE symbol = $1 
        ORDER BY time DESC 
        LIMIT $2;
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, symbol, limit)
            return rows