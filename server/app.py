"""
Deephaven Trading Server
========================
Standalone data engine run by the platform/infra team.
Provides ticking market data and risk tables to all connected clients.

Run:   python3 -i app.py
Web IDE: http://localhost:10000
"""

# ── 1. Start the Deephaven server (must happen before other DH imports) ──────
from deephaven_server import Server

server = Server(
    port=10000,
    jvm_args=[
        "-Xmx4g",
        "-Dprocess.info.system-info.enabled=false",   # Apple Silicon compat
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
    ],
)
server.start()

# ── 2. Deephaven imports (available only after server.start()) ───────────────
from deephaven import DynamicTableWriter, agg
import deephaven.dtypes as dht

# ── 3. Create DynamicTableWriters — the raw ticking data sources ─────────────
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

# Raw append-only tables
prices_raw = price_writer.table
risk_raw = risk_writer.table

# ── 4. Derived tables (published to global scope for clients) ────────────────
# Latest snapshot per symbol — ticks on every update
prices_live = prices_raw.last_by("Symbol")
risk_live = risk_raw.last_by("Symbol")

# Portfolio-level aggregation
portfolio_summary = risk_live.agg_by(
    [
        agg.sum_(["TotalMV=MarketValue", "TotalPnL=UnrealizedPnL", "TotalDelta=Delta"]),
        agg.avg(["AvgGamma=Gamma", "AvgTheta=Theta", "AvgVega=Vega"]),
        agg.count_("NumPositions"),
    ]
)

# Top movers and volume leaders (always available)
top_movers = prices_live.sort_descending("ChangePct")
volume_leaders = prices_live.sort_descending("Volume")

# ── 5. Start the market data simulator ───────────────────────────────────────
from market_data import start_market_data, SYMBOLS, POSITIONS

data_thread, stop_event = start_market_data(price_writer, risk_writer)

# ── 6. Print status ─────────────────────────────────────────────────────────
print()
print("=" * 64)
print("  Deephaven Trading Server is RUNNING")
print("  Web IDE:  http://localhost:10000")
print()
print("  Published tables (available to all clients):")
print("    • prices_raw        — append-only price ticks")
print("    • prices_live       — latest price per symbol")
print("    • risk_raw          — append-only risk ticks")
print("    • risk_live         — latest risk per symbol")
print("    • portfolio_summary — aggregated portfolio metrics")
print("    • top_movers        — symbols ranked by % change")
print("    • volume_leaders    — symbols ranked by volume")
print()
print(f"  Symbols: {', '.join(SYMBOLS)}")
print(f"  Positions: {POSITIONS}")
print("  Tick rate: 5 updates/sec (200ms)")
print("=" * 64)
print()
