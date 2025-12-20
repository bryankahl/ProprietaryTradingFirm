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
    db = TimescaleRepo()
    
    try:
        await db.connect()
        
        broker = AlpacaAdapter()
        account = await broker.get_account()
        logger.info(f"Broker Connected. Equity: ${account.equity:,.2f}")

        risk_config = RiskConfig(
            max_daily_loss=Decimal(os.getenv("MAX_DAILY_LOSS", 500)),
            max_total_loss=Decimal(os.getenv("MAX_TOTAL_LOSS", 1000)),
            max_position_size=Decimal("50000.00"), 
            max_leverage=Decimal("4.0")
        )
        risk = RiskManager(risk_config, initial_balance=account.equity)

        # 2. Data Backfill
        logger.info("Priming Data for Winning Pairs...")
        ingestor = DataIngestion(db)
        winners = ["NVDA", "AMD", "XOM", "CVX", "JPM", "BAC"]
        await ingestor.backfill_bars(winners, days=2)

        # 3. Initialize Strategies
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
        
        # 4. Run All concurrently (Robust Failure Handling)
        # We wrap them in tasks to monitor their state individually
        tasks = [
            asyncio.create_task(strat_tech.run(), name="Tech-Strat"),
            asyncio.create_task(strat_energy.run(), name="Energy-Strat"),
            asyncio.create_task(strat_banks.run(), name="Banks-Strat")
        ]

        # Wait for any task to fail (or all to complete)
        # FIRST_EXCEPTION ensures if one crashes, we stop immediately.
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        # Check for errors in the finished tasks
        for task in done:
            if task.exception():
                logger.critical(f"CRITICAL FAILURE in {task.get_name()}: {task.exception()}")
                # Propagate the error to trigger the 'except' block below
                raise task.exception()

    except KeyboardInterrupt:
        logger.info("User requested shutdown.")
    except Exception as e:
        logger.critical(f"System Crash Detected: {e}")
    finally:
        # 5. Clean Shutdown of Pending Tasks (Killing Zombies)
        try:
            # If we are here, 'tasks' might still have running items
            # We access the local 'tasks' list if it exists
            current_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            
            if current_tasks:
                logger.warning(f"Cancelling {len(current_tasks)} active strategies...")
                for task in current_tasks:
                    task.cancel()
                
                # Allow tasks to clean up
                await asyncio.gather(*current_tasks, return_exceptions=True)
                
        except Exception as cleanup_error:
            logger.error(f"Error during task cancellation: {cleanup_error}")

        await db.disconnect()
        logger.info("System Shutdown Complete.")

if __name__ == "__main__":
    asyncio.run(main())