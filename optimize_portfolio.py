import asyncio
import pandas as pd
import numpy as np
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

async def run_portfolio_optimizer():
    print("--- TUNING PORTFOLIO SETTINGS ---")
    
    conn = await asyncpg.connect(
        user=os.getenv("DB_USER", "sniper_user"),
        password=os.getenv("DB_PASS", "sniper_password"),
        database=os.getenv("DB_NAME", "sniper_db"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5455")
    )

    # The 3 Winners
    pairs = [
        ("NVDA", "AMD", "Tech"),
        ("XOM",  "CVX", "Energy"),
        ("JPM",  "BAC", "Banks")
    ]
    
    windows = [30, 45, 60, 90]
    
    print(f"{'PAIR':<8} | {'WINDOW':<6} | {'TRADES':<6} | {'AVG P&L':<8} | {'VERDICT'}")
    print("-" * 55)

    for sym_a, sym_b, sector in pairs:
        # Fetch Data
        rows = await conn.fetch("""
            SELECT time, symbol, close 
            FROM market_bars 
            WHERE symbol = $1 OR symbol = $2
            ORDER BY time ASC
        """, sym_a, sym_b)
        
        df = pd.DataFrame(rows, columns=['time', 'symbol', 'close'])
        df['close'] = df['close'].astype(float)
        df_pivot = df.pivot(index='time', columns='symbol', values='close').dropna()

        best_window = 0
        best_avg_pnl = -999.0
        best_stats = ""

        # Test Windows
        for w in windows:
            df_test = df_pivot.copy()
            df_test['Spread'] = df_test[sym_a] - df_test[sym_b]
            df_test['Mean'] = df_test['Spread'].rolling(window=w).mean()
            df_test['Std'] = df_test['Spread'].rolling(window=w).std()
            df_test['Z_Score'] = (df_test['Spread'] - df_test['Mean']) / df_test['Std']

            position = 0
            entry_spread = 0
            total_pnl = 0.0
            trades = 0

            for i, row in df_test.iterrows():
                z = row['Z_Score']
                if pd.isna(z): continue

                # Logic (Fixed Threshold 2.1)
                if position == 0:
                    if z > 2.1: position = -1; entry_spread = row['Spread']; trades += 1
                    elif z < -2.1: position = 1; entry_spread = row['Spread']; trades += 1
                elif position != 0 and abs(z) < 0.5:
                    pnl = (row['Spread'] - entry_spread) if position == 1 else (entry_spread - row['Spread'])
                    total_pnl += (pnl * 10)
                    position = 0
            
            avg_pnl = total_pnl / trades if trades > 0 else 0
            
            # Print Every Run (Debug)
            # print(f"  > {sym_a} W:{w} T:{trades} Avg:${avg_pnl:.2f}")

            if avg_pnl > best_avg_pnl:
                best_avg_pnl = avg_pnl
                best_window = w
                best_stats = f"{trades:<6} | ${avg_pnl:<7.2f}"

        print(f"{sym_a:<4}     | {best_window:<6} | {best_stats} | ✅ WINNER")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(run_portfolio_optimizer())