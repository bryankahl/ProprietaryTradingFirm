import asyncio
import pandas as pd
import numpy as np
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

async def run_final_stress_test():
    print("--- FINAL PORTFOLIO STRESS TEST (Optimized Windows) ---")
    
    conn = await asyncpg.connect(
        user=os.getenv("DB_USER", "sniper_user"),
        password=os.getenv("DB_PASS", "sniper_password"),
        database=os.getenv("DB_NAME", "sniper_db"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5455")
    )

    # 3 WINNING PAIRS with SPECIFIC WINDOWS
    # Format: (Symbol A, Symbol B, Window Size)
    portfolio_config = [
        ("NVDA", "AMD", 60),  # Tech: Optimized to 60
        ("XOM",  "CVX", 60),  # Energy: Optimized to 60
        ("JPM",  "BAC", 90)   # Banks: Optimized to 90
    ]

    grand_total_pnl = 0.0
    grand_total_trades = 0

    print(f"{'PAIR':<12} | {'WINDOW':<6} | {'TRADES':<8} | {'P&L (10 Shares)':<15} | {'VERDICT'}")
    print("-" * 70)

    for sym_a, sym_b, window in portfolio_config:
        # Fetch Data
        rows = await conn.fetch("""
            SELECT time, symbol, close 
            FROM market_bars 
            WHERE symbol = $1 OR symbol = $2
            ORDER BY time ASC
        """, sym_a, sym_b)

        if not rows:
            print(f"{sym_a}/{sym_b} | NO DATA")
            continue

        df = pd.DataFrame(rows, columns=['time', 'symbol', 'close'])
        df['close'] = df['close'].astype(float)
        df_pivot = df.pivot(index='time', columns='symbol', values='close').dropna()

        # Strategy Logic (Using the specific 'window' for this pair)
        df_pivot['Spread'] = df_pivot[sym_a] - df_pivot[sym_b]
        df_pivot['Mean'] = df_pivot['Spread'].rolling(window=window).mean()
        df_pivot['Std'] = df_pivot['Spread'].rolling(window=window).std()
        df_pivot['Z_Score'] = (df_pivot['Spread'] - df_pivot['Mean']) / df_pivot['Std']

        position = 0
        entry_spread = 0
        pair_pnl = 0.0
        pair_trades = 0
        
        # Fixed Threshold for everyone
        threshold = 2.1 

        for i, row in df_pivot.iterrows():
            z = row['Z_Score']
            spread = row['Spread']
            if pd.isna(z): continue

            if position == 0:
                if z > threshold: position = -1; entry_spread = spread; pair_trades += 1
                elif z < -threshold: position = 1; entry_spread = spread; pair_trades += 1
            elif position != 0 and abs(z) < 0.5:
                pnl = (spread - entry_spread) if position == 1 else (entry_spread - spread)
                pair_pnl += (pnl * 10)
                position = 0

        # Output
        print(f"{sym_a}/{sym_b:<4} | {window:<6} | {pair_trades:<8} | ${pair_pnl:<14.2f} | ✅")
        
        grand_total_pnl += pair_pnl
        grand_total_trades += pair_trades

    await conn.close()
    
    print("-" * 70)
    print(f"TOTALS       |        | {grand_total_trades:<8} | ${grand_total_pnl:<14.2f} |")
    print("-" * 70)

if __name__ == "__main__":
    asyncio.run(run_final_stress_test())