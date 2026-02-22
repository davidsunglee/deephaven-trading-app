"""
Quant Client — Custom Filtered Views & Derived Tables
======================================================
Example: A quant connects to the Deephaven server, filters prices
to their watchlist, creates a server-side moving-average table,
and subscribes to ticking updates.

Usage:  python3 quant_client.py [--host localhost] [--port 10000]
"""

import argparse
import time
import sys
sys.path.insert(0, ".")

from base_client import DeephavenClient


def main(host="localhost", port=10000):
    with DeephavenClient(host=host, port=port) as client:
        # ── 1. See what tables the server publishes ──────────────────
        print("\nAvailable server tables:")
        for name in client.list_tables():
            print(f"  • {name}")

        # ── 2. Open the live prices table ────────────────────────────
        prices = client.open_table("prices_live")
        print(f"\nprices_live schema: {prices.schema}")

        # ── 3. Create server-side tables via run_script ────────────
        #    These run ON THE SERVER so they tick in real time and
        #    are visible to ALL other clients + the web IDE.
        client.run_script("""
# Quant watchlist: filtered to specific symbols
quant_watchlist = prices_live.where(["Symbol in 'AAPL', 'NVDA', 'TSLA'"])

# Spread analysis
quant_spreads = prices_live.update([
    "Spread = Ask - Bid",
    "SpreadBps = (Ask - Bid) / Price * 10000",
]).sort_descending("SpreadBps")
""")
        print("Published to server (visible to ALL clients):")
        print("  • quant_watchlist — AAPL, NVDA, TSLA only")
        print("  • quant_spreads  — spread analysis")

        # ── 4. Snapshot data to local Arrow / pandas ─────────────
        top_movers = client.open_table("top_movers")
        arrow_table = top_movers.to_arrow()
        print(f"\nTop movers snapshot ({arrow_table.num_rows} rows):")
        print(arrow_table.to_pandas().to_string(index=False))

        # ── 5. Keep alive to observe ticking in the web IDE ──────────
        print("\n✓ Tables are published. Any other client or the web IDE")
        print("  can now see quant_watchlist and quant_spreads.")
        print("  Try running risk_client.py or pm_client.py in another terminal!")
        print("\nPress Ctrl+C to disconnect.")

        try:
            while True:
                time.sleep(5)
                snapshot = client.open_table("quant_watchlist").to_arrow()
                df = snapshot.to_pandas()
                print(f"\n[{time.strftime('%H:%M:%S')}] Watchlist (ticking):")
                print(df[["Symbol", "Price", "ChangePct"]].to_string(index=False))
        except KeyboardInterrupt:
            print("\nDisconnecting...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Quant client for Deephaven Trading Server")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=10000)
    args = parser.parse_args()
    main(host=args.host, port=args.port)
