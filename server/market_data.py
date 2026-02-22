"""
Market Data Simulator
Generates realistic ticking price data and writes to DynamicTableWriter instances.
"""

import random
import time
import threading

from risk_engine import calculate_greeks


SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "NFLX"]

BASE_PRICES = {
    "AAPL": 228.0, "GOOGL": 192.0, "MSFT": 415.0, "AMZN": 225.0,
    "TSLA": 355.0, "NVDA": 138.0, "META": 700.0, "NFLX": 1020.0,
}

# Random starting positions (shares held, negative = short)
POSITIONS = {sym: random.randint(-500, 500) for sym in SYMBOLS}


def start_market_data(price_writer, risk_writer, tick_interval=0.2):
    """
    Launch a daemon thread that continuously writes simulated market data.

    Args:
        price_writer: DynamicTableWriter for price ticks
        risk_writer:  DynamicTableWriter for risk ticks
        tick_interval: seconds between update cycles (default 0.2 = 5/sec)

    Returns:
        (thread, stop_event) — call stop_event.set() to shut down cleanly
    """
    current_prices = dict(BASE_PRICES)
    stop_event = threading.Event()

    def _run():
        while not stop_event.is_set():
            try:
                for sym in SYMBOLS:
                    old = current_prices[sym]
                    move = random.gauss(0, 0.002)  # 0.2 % std dev per tick
                    new_price = old * (1 + move)
                    current_prices[sym] = new_price

                    spread = new_price * 0.0001
                    bid = new_price - spread / 2
                    ask = new_price + spread / 2
                    volume = random.randint(100, 10_000)
                    change = new_price - old
                    change_pct = (change / old) * 100

                    price_writer.write_row(
                        sym, new_price, bid, ask, volume, change, change_pct,
                    )

                    pos = POSITIONS[sym]
                    mv = pos * new_price
                    pnl = pos * change
                    delta, gamma, theta, vega = calculate_greeks(new_price)
                    risk_writer.write_row(
                        sym, pos, mv, pnl,
                        delta * pos, gamma * pos, theta * pos, vega * pos,
                    )

                time.sleep(tick_interval)
            except Exception as e:
                print(f"[market_data] error: {e}")
                time.sleep(1)

    thread = threading.Thread(target=_run, daemon=True, name="market-data-sim")
    thread.start()
    return thread, stop_event
