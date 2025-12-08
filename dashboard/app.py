import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
import asyncio
from dotenv import load_dotenv
import alpaca_trade_api as tradeapi
import asyncpg
from datetime import datetime, timedelta

# Load Config
load_dotenv()

# Page Config
st.set_page_config(page_title="Sniper Algo Monitor", layout="wide", page_icon="🎯")

# --- AUTO-REFRESH (Every 3 seconds) ---
# This simulates a live feed
from streamlit_autorefresh import st_autorefresh # Optional, but we use simple rerun for now or manual
if st.button('🔄 Refresh Data'):
    st.rerun()

# --- CONNECTIVITY ---
def get_alpaca_api():
    return tradeapi.REST(
        os.getenv("ALPACA_KEY"),
        os.getenv("ALPACA_SECRET"),
        os.getenv("ALPACA_ENDPOINT", "https://paper-api.alpaca.markets").replace("/v2", "").rstrip("/"),
        api_version='v2'
    )

async def get_market_data():
    """Fetch recent bars from TimescaleDB to visualize the spread."""
    conn = await asyncpg.connect(
        user=os.getenv("DB_USER", "sniper_user"),
        password=os.getenv("DB_PASS", "sniper_password"),
        database=os.getenv("DB_NAME", "sniper_db"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5455")
    )
    
    # Fetch last 100 bars for NVDA and AMD
    query = """
    SELECT time, symbol, close 
    FROM market_bars 
    WHERE symbol IN ('NVDA', 'AMD') 
    ORDER BY time DESC 
    LIMIT 200;
    """
    rows = await conn.fetch(query)
    await conn.close()
    
    df = pd.DataFrame(rows, columns=['time', 'symbol', 'close'])
    df['close'] = df['close'].astype(float)
    return df

# --- DASHBOARD LAYOUT ---
st.title("⚡ Sniper Algorithm: Command Center")

# 1. SIDEBAR: ACCOUNT HEALTH
api = get_alpaca_api()
try:
    account = api.get_account()
    equity = float(account.equity)
    daily_pl = float(account.equity) - float(account.last_equity)
    
    st.sidebar.header("Risk Monitor")
    st.sidebar.metric("Total Equity", f"${equity:,.2f}")
    st.sidebar.metric("Daily P&L", f"${daily_pl:,.2f}", delta_color="normal")
    
    buying_power = float(account.buying_power)
    st.sidebar.progress(min(buying_power / (equity * 4), 1.0), text="Buying Power Usage")

except Exception as e:
    st.sidebar.error(f"Broker Disconnected: {e}")

# 2. MAIN PANEL: STRATEGY VISUALIZER
st.subheader("📡 Market Scanner (NVDA vs AMD)")

# Async wrapper to fetch DB data
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
df = loop.run_until_complete(get_market_data())

if not df.empty:
    # Pivot Data to calculate Spread
    df_pivot = df.pivot(index='time', columns='symbol', values='close').dropna()
    
    if 'NVDA' in df_pivot.columns and 'AMD' in df_pivot.columns:
        # Calculate Spread
        df_pivot['Spread'] = df_pivot['NVDA'] - df_pivot['AMD']
        
        # Calculate Z-Score (Simple Rolling)
        window = 20
        df_pivot['Mean'] = df_pivot['Spread'].rolling(window=window).mean()
        df_pivot['Std'] = df_pivot['Spread'].rolling(window=window).std()
        df_pivot['Z_Score'] = (df_pivot['Spread'] - df_pivot['Mean']) / df_pivot['Std']
        
        # Create Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Price Spread ($)")
            fig_spread = go.Figure()
            fig_spread.add_trace(go.Scatter(x=df_pivot.index, y=df_pivot['Spread'], mode='lines', name='Spread'))
            st.plotly_chart(fig_spread, use_container_width=True)
            
        with col2:
            st.markdown("### Z-Score (Signal Generator)")
            fig_z = go.Figure()
            fig_z.add_trace(go.Scatter(x=df_pivot.index, y=df_pivot['Z_Score'], mode='lines', name='Z-Score', line=dict(color='purple')))
            
            # Add Thresholds
            fig_z.add_hline(y=2.0, line_dash="dash", line_color="red", annotation_text="Short Signal")
            fig_z.add_hline(y=-2.0, line_dash="dash", line_color="green", annotation_text="Long Signal")
            st.plotly_chart(fig_z, use_container_width=True)

# 3. BOTTOM PANEL: ACTIVE POSITIONS
st.subheader("🛡️ Active Positions")
positions = api.list_positions()
if positions:
    pos_data = []
    for p in positions:
        pos_data.append({
            "Symbol": p.symbol,
            "Qty": p.qty,
            "Entry Price": f"${float(p.avg_entry_price):.2f}",
            "Current Price": f"${float(p.current_price):.2f}",
            "P&L": f"${float(p.unrealized_pl):.2f}"
        })
    st.table(pd.DataFrame(pos_data))
else:
    st.info("System is FLAT (No Active Exposure)")