"""
Unit tests for the market data simulator.
Uses mock writers — no Deephaven server needed.
"""

import sys
import os
import time
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "server"))

from market_data import SYMBOLS, BASE_PRICES, POSITIONS, start_market_data


# ── Configuration tests ─────────────────────────────────────────────────────

class TestConfig:
    def test_symbols_count(self):
        assert len(SYMBOLS) == 8

    def test_base_prices_keys_match_symbols(self):
        assert set(BASE_PRICES.keys()) == set(SYMBOLS)

    def test_all_base_prices_positive(self):
        for sym, price in BASE_PRICES.items():
            assert price > 0, f"{sym} has non-positive base price: {price}"

    def test_positions_keys_match_symbols(self):
        assert set(POSITIONS.keys()) == set(SYMBOLS)

    def test_positions_are_integers(self):
        for sym, pos in POSITIONS.items():
            assert isinstance(pos, int), f"{sym} position is not int: {type(pos)}"

    def test_symbols_are_strings(self):
        for sym in SYMBOLS:
            assert isinstance(sym, str) and len(sym) > 0


# ── Mock writer for capturing write_row calls ───────────────────────────────

class MockWriter:
    """Records all write_row calls for inspection."""

    def __init__(self):
        self.rows = []

    def write_row(self, *args):
        self.rows.append(args)


# ── Simulation thread tests ─────────────────────────────────────────────────

class TestSimulationThread:
    def test_thread_starts_and_stops(self):
        pw, rw = MockWriter(), MockWriter()
        thread, stop = start_market_data(pw, rw, tick_interval=0.05)
        assert thread.is_alive()
        time.sleep(0.2)
        stop.set()
        thread.join(timeout=3)
        assert not thread.is_alive()

    def test_writes_price_rows(self):
        pw, rw = MockWriter(), MockWriter()
        thread, stop = start_market_data(pw, rw, tick_interval=0.05)
        time.sleep(0.3)
        stop.set()
        thread.join(timeout=3)
        assert len(pw.rows) > 0, "No price rows written"

    def test_writes_risk_rows(self):
        pw, rw = MockWriter(), MockWriter()
        thread, stop = start_market_data(pw, rw, tick_interval=0.05)
        time.sleep(0.3)
        stop.set()
        thread.join(timeout=3)
        assert len(rw.rows) > 0, "No risk rows written"

    def test_price_and_risk_rows_equal_count(self):
        """Each tick writes one price row and one risk row per symbol."""
        pw, rw = MockWriter(), MockWriter()
        thread, stop = start_market_data(pw, rw, tick_interval=0.05)
        time.sleep(0.3)
        stop.set()
        thread.join(timeout=3)
        assert len(pw.rows) == len(rw.rows)

    def test_all_symbols_covered(self):
        pw, rw = MockWriter(), MockWriter()
        thread, stop = start_market_data(pw, rw, tick_interval=0.05)
        time.sleep(0.3)
        stop.set()
        thread.join(timeout=3)
        written_symbols = {row[0] for row in pw.rows}
        assert written_symbols == set(SYMBOLS)


class TestPriceRowFormat:
    """Validate the shape and content of price writer rows."""

    @pytest.fixture(autouse=True)
    def run_sim(self):
        self.pw, self.rw = MockWriter(), MockWriter()
        thread, stop = start_market_data(self.pw, self.rw, tick_interval=0.05)
        time.sleep(0.3)
        stop.set()
        thread.join(timeout=3)

    def test_price_row_has_7_fields(self):
        # Symbol, Price, Bid, Ask, Volume, Change, ChangePct
        for row in self.pw.rows:
            assert len(row) == 7, f"Expected 7 fields, got {len(row)}: {row}"

    def test_symbol_is_string(self):
        for row in self.pw.rows:
            assert isinstance(row[0], str)

    def test_price_is_positive(self):
        for row in self.pw.rows:
            assert row[1] > 0, f"Price should be positive: {row}"

    def test_bid_less_than_ask(self):
        for row in self.pw.rows:
            bid, ask = row[2], row[3]
            assert bid < ask, f"Bid {bid} should be < Ask {ask}"

    def test_volume_in_range(self):
        for row in self.pw.rows:
            vol = row[4]
            assert 100 <= vol <= 10_000, f"Volume out of range: {vol}"

    def test_change_pct_is_finite(self):
        import math
        for row in self.pw.rows:
            assert math.isfinite(row[6]), f"ChangePct not finite: {row[6]}"


class TestRiskRowFormat:
    """Validate the shape and content of risk writer rows."""

    @pytest.fixture(autouse=True)
    def run_sim(self):
        self.pw, self.rw = MockWriter(), MockWriter()
        thread, stop = start_market_data(self.pw, self.rw, tick_interval=0.05)
        time.sleep(0.3)
        stop.set()
        thread.join(timeout=3)

    def test_risk_row_has_8_fields(self):
        # Symbol, Position, MV, PnL, Delta, Gamma, Theta, Vega
        for row in self.rw.rows:
            assert len(row) == 8, f"Expected 8 fields, got {len(row)}: {row}"

    def test_position_is_integer(self):
        for row in self.rw.rows:
            assert isinstance(row[1], int)

    def test_market_value_equals_position_times_price(self):
        """MV = position * price. Match by symbol."""
        for pr, rr in zip(self.pw.rows, self.rw.rows):
            assert pr[0] == rr[0]  # same symbol
            expected_mv = rr[1] * pr[1]  # position * price
            assert rr[2] == pytest.approx(expected_mv, rel=1e-9)

    def test_all_greeks_finite(self):
        import math
        for row in self.rw.rows:
            for val in row[4:]:  # Delta, Gamma, Theta, Vega
                assert math.isfinite(val), f"Non-finite greek in row: {row}"
