"""
Real-Time Trading App with Deephaven.io
========================================
Uses the real embedded Deephaven server (deephaven-server) with:
- DynamicTableWriter for ticking price & risk data
- Deephaven table ops for derived views (last_by, agg_by)
- deephaven.ui for a reactive dashboard with ticking grids & charts

Run:  python3 -i app.py
Open: http://localhost:10000
"""

# ── 1. Start the Deephaven server FIRST (must happen before any DH imports) ─────
from deephaven_server import Server

server = Server(
    port=10000,
    jvm_args=[
        "-Xmx4g",
        "-Dprocess.info.system-info.enabled=false",   # required for Apple Silicon
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",  # no password
    ],
)
server.start()

# ── 2. Now import Deephaven packages ────────────────────────────────────────────
from deephaven import DynamicTableWriter, time_table, agg, empty_table
import deephaven.dtypes as dht
from deephaven import ui
import deephaven.plot.express as dx
import numpy as np
import threading
import time
import random
import math

# ── 3. Configuration ───────────────────────────────────────────────────────────
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "NFLX"]

BASE_PRICES = {
    "AAPL": 228.0, "GOOGL": 192.0, "MSFT": 415.0, "AMZN": 225.0,
    "TSLA": 355.0, "NVDA": 138.0, "META": 700.0, "NFLX": 1020.0,
}

# Random starting positions (shares held, can be negative = short)
POSITIONS = {sym: random.randint(-500, 500) for sym in SYMBOLS}

# ── 4. Create DynamicTableWriters for ticking data ─────────────────────────────
price_writer = DynamicTableWriter({
    "Symbol":    dht.string,
    "Price":     dht.double,
    "Bid":       dht.double,
    "Ask":       dht.double,
    "Volume":    dht.int64,
    "Change":    dht.double,
    "ChangePct": dht.double,
})

risk_writer = DynamicTableWriter({
    "Symbol":        dht.string,
    "Position":      dht.int64,
    "MarketValue":   dht.double,
    "UnrealizedPnL": dht.double,
    "Delta":         dht.double,
    "Gamma":         dht.double,
    "Theta":         dht.double,
    "Vega":          dht.double,
})

# Raw append-only ticking tables
prices_raw = price_writer.table
risk_raw = risk_writer.table

# ── 5. Derived tables using Deephaven table operations ─────────────────────────
# Latest snapshot per symbol (ticks every time a new row arrives for that symbol)
prices_live = prices_raw.last_by("Symbol")
risk_live = risk_raw.last_by("Symbol")

# Portfolio-level aggregations
portfolio_summary = risk_live.agg_by(
    [
        agg.sum_(["TotalMV=MarketValue", "TotalPnL=UnrealizedPnL", "TotalDelta=Delta"]),
        agg.avg(["AvgGamma=Gamma", "AvgTheta=Theta", "AvgVega=Vega"]),
        agg.count_("NumPositions"),
    ]
)

# ── 6. Greeks calculator (simplified Black-Scholes) ───────────────────────────
def _norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def calculate_greeks(price, strike=None, T=0.25, r=0.05, sigma=0.25):
    S = price
    K = strike or price * 1.05
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    delta = _norm_cdf(d1)
    gamma = math.exp(-d1 ** 2 / 2) / (S * sigma * math.sqrt(2 * math.pi * T))
    theta = -(S * math.exp(-d1 ** 2 / 2) * sigma) / (2 * math.sqrt(2 * math.pi * T)) \
            - r * K * math.exp(-r * T) * _norm_cdf(d2)
    vega = S * math.exp(-d1 ** 2 / 2) * math.sqrt(T) / math.sqrt(2 * math.pi)
    return delta, gamma, theta, vega

# ── 7. Background thread: simulate market data ────────────────────────────────
current_prices = dict(BASE_PRICES)
running = True

def simulate_market_data():
    global current_prices
    while running:
        try:
            for sym in SYMBOLS:
                old = current_prices[sym]
                move = random.gauss(0, 0.002)  # 0.2% std dev per tick
                new_price = old * (1 + move)
                current_prices[sym] = new_price

                spread = new_price * 0.0001
                bid = new_price - spread / 2
                ask = new_price + spread / 2
                volume = random.randint(100, 10000)
                change = new_price - old
                change_pct = (change / old) * 100

                price_writer.write_row(sym, new_price, bid, ask, volume, change, change_pct)

                pos = POSITIONS[sym]
                mv = pos * new_price
                pnl = pos * change
                delta, gamma, theta, vega = calculate_greeks(new_price)
                risk_writer.write_row(sym, pos, mv, pnl, delta * pos, gamma * pos, theta * pos, vega * pos)

            time.sleep(0.2)
        except Exception as e:
            print(f"[sim] error: {e}")
            time.sleep(1)

data_thread = threading.Thread(target=simulate_market_data, daemon=True)
data_thread.start()

# ── 8. deephaven.ui Dashboard ─────────────────────────────────────────────────
@ui.component
def trading_dashboard():
    # --- Price grid with formatting ---
    price_table = ui.table(
        prices_live,
        reverse=True,
    )

    # --- Risk grid ---
    risk_table = ui.table(
        risk_live,
        reverse=True,
    )

    # --- Portfolio summary ---
    summary_table = ui.table(portfolio_summary)

    # --- Real-time P&L chart ---
    pnl_chart = dx.bar(risk_live, x="Symbol", y="UnrealizedPnL", color="Symbol", title="Unrealized P&L by Symbol")

    # --- Market Value chart ---
    mv_chart = dx.bar(risk_live, x="Symbol", y="MarketValue", color="Symbol", title="Market Value by Symbol")

    # --- Layout ---
    return ui.dashboard(
        ui.row(
            ui.column(
                ui.stack(
                    ui.panel(price_table, title="Live Prices"),
                ),
                ui.stack(
                    ui.panel(risk_table, title="Risk Analytics"),
                ),
                width=60,
            ),
            ui.column(
                ui.stack(
                    ui.panel(summary_table, title="Portfolio Summary"),
                ),
                ui.stack(
                    ui.panel(pnl_chart, title="P&L Chart"),
                    ui.panel(mv_chart, title="Market Value Chart"),
                ),
                width=40,
            ),
        ),
    )

# Render the dashboard
dash = trading_dashboard()

print()
print("=" * 60)
print("  Real-Time Trading App is RUNNING")
print("  Open: http://localhost:10000")
print("  Ticking data: 8 symbols @ 5 updates/sec")
print("  Tables: prices_live, risk_live, portfolio_summary")
print("=" * 60)
print()
