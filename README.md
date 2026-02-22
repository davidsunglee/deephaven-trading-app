# Deephaven Real-Time Trading Platform

A **server/client** real-time trading platform built on [Deephaven.io](https://deephaven.io). The server acts as a shared data engine (run by infra/platform team) while multiple Python clients (quants, risk analysts, PMs) connect remotely to configure their own ticking views.

## Architecture

```
┌──────────────────────────────────────────────────────┐
│  SERVER  (server/app.py)        Port 10000           │
│  • Deephaven embedded server (JVM)                   │
│  • DynamicTableWriter: prices, risk                  │
│  • Derived tables: prices_live, risk_live, etc.      │
│  • Market data simulation thread                     │
│  • Web IDE at http://localhost:10000                  │
└───────────────────────┬──────────────────────────────┘
                        │ gRPC (pydeephaven)
           ┌────────────┼────────────────┐
           │            │                │
     ┌─────▼────┐ ┌────▼─────┐   ┌──────▼─────┐
     │  Quant   │ │   Risk   │   │     PM     │
     │  Client  │ │  Client  │   │   Client   │
     └──────────┘ └──────────┘   └────────────┘
```

## Quick Start

### Prerequisites
- **Python 3.10+**
- **Java 11-21** (set `JAVA_HOME`)

### 1. Start the Server

```bash
pip install -r requirements-server.txt
cd server
python3 -i app.py
```

The server starts on **http://localhost:10000** — open this in a browser to access the Deephaven Web IDE with all shared ticking tables.

### 2. Run a Client

In a separate terminal:

```bash
pip install -r requirements-client.txt
cd client

# Pick one:
python3 quant_client.py          # Watchlists, top movers, volume leaders
python3 risk_client.py           # Large exposures, risk scoring
python3 pm_client.py             # P&L snapshots, position sizing
```

Clients connect via `pydeephaven` (lightweight — **no Java needed** on client machines). Tables created by clients are visible in the Web IDE.

## Project Structure

```
windsurf-project/
├── server/
│   ├── app.py              # Deephaven server + data engine
│   ├── market_data.py      # Market data simulation thread
│   ├── risk_engine.py      # Black-Scholes Greeks calculator
│   └── start_server.sh     # Launch script
├── client/
│   ├── base_client.py      # Reusable connection helper
│   ├── quant_client.py     # Quant: filtered views, derived tables
│   ├── risk_client.py      # Risk: exposure monitoring, alerts
│   └── pm_client.py        # PM: portfolio summary, P&L snapshots
├── requirements-server.txt
├── requirements-client.txt
└── README.md
```

## Published Server Tables

| Table | Description |
|-------|-------------|
| `prices_raw` | Append-only price ticks |
| `prices_live` | Latest price per symbol (ticking) |
| `risk_raw` | Append-only risk ticks |
| `risk_live` | Latest risk per symbol (ticking) |
| `portfolio_summary` | Aggregated portfolio metrics |

## Client Capabilities

| Feature | How |
|---------|-----|
| Read shared tables | `session.open_table("prices_live")` |
| Filter / sort | `table.where(...)`, `table.sort(...)` |
| Create server-side views | `session.run_script("...")` |
| Publish tables | `session.bind_table(name, table)` |
| Export to pandas | `table.to_arrow().to_pandas()` |
| Subscribe to ticks | `pydeephaven-ticking` listener API |

## Symbols

| Symbol | Base Price |
|--------|-----------|
| AAPL | $228 |
| GOOGL | $192 |
| MSFT | $415 |
| AMZN | $225 |
| TSLA | $355 |
| NVDA | $138 |
| META | $700 |
| NFLX | $1,020 |

## License

MIT
