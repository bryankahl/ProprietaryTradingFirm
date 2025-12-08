import asyncio
import logging
import os
from dotenv import load_dotenv
from decimal import Decimal

# Import Components
from core.risk_manager import RiskManager, RiskConfig
from data.timescale_repo import TimescaleRepo
from execution.alpaca_adapter import AlpacaAdapter
from strategies.stat_arb_pairs import StatArbPairs
from data.ingestion import DataIngestion

# Setup
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger("System")

async def main():
    logger.info("--- SYSTEM STARTUP (PORTFOLIO MODE) ---")

    # 1. Initialize Infrastructure
    try:
        # DB
        db = TimescaleRepo()
        await db.connect()
        
        # Broker
        broker = AlpacaAdapter()
        account = await broker.get_account()
        logger.info(f"Broker Connected. Equity: ${account.equity:,.2f}")

        # Risk Engine (Global Shield)
        risk_config = RiskConfig(
            max_daily_loss=Decimal(os.getenv("MAX_DAILY_LOSS", 500)),
            max_total_loss=Decimal(os.getenv("MAX_TOTAL_LOSS", 1000)),
            max_position_size=Decimal("50000.00"), 
            max_leverage=Decimal("4.0") # Increased for Portfolio
        )
        risk = RiskManager(risk_config, initial_balance=account.equity)

        # 2. Data Backfill (The Winners Only)
        # We fetch 2 days of data to prime the pump
        logger.info("Priming Data for Winning Pairs...")
        ingestor = DataIngestion(db)
        winners = ["NVDA", "AMD", "XOM", "CVX", "JPM", "BAC"]
        await ingestor.backfill_bars(winners, days=2)

        # 3. Initialize The "Triple Threat" Strategies
        # Note: We added 'ingestor=ingestor' to all of them
        
        strat_tech = StatArbPairs(
            broker=broker, risk_manager=risk, db=db, ingestor=ingestor,
            symbol_a="NVDA", symbol_b="AMD", window=60
        )
        
        strat_energy = StatArbPairs(
            broker=broker, risk_manager=risk, db=db, ingestor=ingestor,
            symbol_a="XOM", symbol_b="CVX", window=60
        )
        
        strat_banks = StatArbPairs(
            broker=broker, risk_manager=risk, db=db, ingestor=ingestor,
            symbol_a="JPM", symbol_b="BAC", window=90
        )

        logger.info("Launching Portfolio Strategies...")
        
        # 4. Run All concurrently
        await asyncio.gather(
            strat_tech.run(),
            strat_energy.run(),
            strat_banks.run()
        )

    except KeyboardInterrupt:
        logger.info("User requested shutdown.")
    except Exception as e:
        logger.critical(f"Fatal Error: {e}")
    finally:
        await db.disconnect()
        logger.info("System Shutdown Complete.")

if __name__ == "__main__":
    asyncio.run(main())