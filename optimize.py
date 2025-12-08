import asyncio
import pandas as pd
import numpy as np
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

async def run_optimizer():
    print("--- OPTIMIZING WINDOW SIZE (Lookback Period) ---")
    
    # 1. Fetch Data
    conn = await asyncpg.connect(
        user=os.getenv("DB_USER", "sniper_user"),
        password=os.getenv("DB_PASS", "sniper_password"),
        database=os.getenv("DB_NAME", "sniper_db"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5455")
    )
    
    # Fetch all data
    rows = await conn.fetch("""
        SELECT time, symbol, close 
        FROM market_bars 
        WHERE symbol IN ('NVDA', 'AMD') 
        ORDER BY time ASC
    """)
    await conn.close()

    # Prepare DataFrame
    df = pd.DataFrame(rows, columns=['time', 'symbol', 'close'])
    df['close'] = df['close'].astype(float)
    df_pivot = df.pivot(index='time', columns='symbol', values='close').dropna()
    
    # Define Windows to Test (Minutes)
    windows = [15, 30, 45, 60, 90, 120, 180, 240]
    fixed_threshold = 2.0  # Keeping this standard for now
    
    print(f"{'WINDOW':<10} | {'TRADES':<8} | {'TOTAL P&L':<12} | {'AVG P&L/TRADE':<15} | {'QUALITY'}")
    print("-" * 75)

    for w in windows:
        # Calculate Indicators dynamically based on Window 'w'
        df_test = df_pivot.copy()
        df_test['Spread'] = df_test['NVDA'] - df_test['AMD']
        df_test['Mean'] = df_test['Spread'].rolling(window=w).mean()
        df_test['Std'] = df_test['Spread'].rolling(window=w).std()
        df_test['Z_Score'] = (df_test['Spread'] - df_test['Mean']) / df_test['Std']

        position = 0
        entry_spread = 0
        total_pnl = 0.0
        trade_count = 0
        
        for i, row in df_test.iterrows():
            z = row['Z_Score']
            spread = row['Spread']
            
            if pd.isna(z): continue

            # Entry
            if position == 0:
                if z > fixed_threshold: 
                    position = -1
                    entry_spread = spread
                    trade_count += 1
                elif z < -fixed_threshold:
                    position = 1
                    entry_spread = spread
                    trade_count += 1
            
            # Exit (Mean Reversion)
            elif position != 0 and abs(z) < 0.5:
                pnl = 0
                if position == 1:
                    pnl = spread - entry_spread
                elif position == -1:
                    pnl = entry_spread - spread
                
                total_pnl += (pnl * 10) # 10 share lot
                position = 0
        
        avg_pnl = total_pnl / trade_count if trade_count > 0 else 0
        
        # Quality Check
        quality = "✅ SNIPER" if avg_pnl > 1.50 else "⚠️ NOISY"
        if avg_pnl < 0.50: quality = "❌ THIN"

        print(f"{w:<10} | {trade_count:<8} | ${total_pnl:<11.2f} | ${avg_pnl:<14.2f} | {quality}")

if __name__ == "__main__":
    asyncio.run(run_optimizer())