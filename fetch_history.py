import asyncio
import logging
import os
from dotenv import load_dotenv

# Load Environment Variables
load_dotenv()

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeepFetch")

# Import your existing modules
from data.timescale_repo import TimescaleRepo
from data.ingestion import DataIngestion

async def run_deep_fetch():
    print("--- STARTING 6-MONTH DATA BACKFILL ---")
    print("This may take 1-2 minutes. Please wait...")

    # 1. Connect to DB
    db = TimescaleRepo()
    await db.connect()

    # 2. Initialize Ingestor
    ingestor = DataIngestion(db)

    # 3. Fetch 180 Days (6 Months)
    # Tech, Staples, Energy, Banks
    symbols = ["NVDA", "AMD", "KO", "PEP", "XOM", "CVX", "JPM", "BAC"]
    await ingestor.backfill_bars(symbols, days=180)

    print("\n[SUCCESS] 6 Months of data stored in TimescaleDB.")
    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(run_deep_fetch())