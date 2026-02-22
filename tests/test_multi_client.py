"""
Integration tests for multi-client / cross-session table sharing.
Requires the server to be running: cd server && python3 -i app.py

Run with: pytest tests/test_multi_client.py -v
"""

import sys
import os
import time
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "client"))

from base_client import DeephavenClient


def _connect():
    """Helper to create a client, skipping if server is down."""
    try:
        return DeephavenClient()
    except Exception as e:
        pytest.skip(f"Deephaven server not running: {e}")


# ── Cross-session table visibility ───────────────────────────────────────────

class TestCrossSessionVisibility:
    def test_client_a_publishes_client_b_sees(self):
        a = _connect()
        b = _connect()
        try:
            a.run_script('cross_test_ab = prices_live.where(["Symbol = `AAPL`"])')
            tables_b = b.list_tables()
            assert "cross_test_ab" in tables_b, \
                "Client B cannot see table published by Client A"
        finally:
            a.close()
            b.close()

    def test_client_b_reads_data_from_client_a_table(self):
        a = _connect()
        b = _connect()
        try:
            a.run_script('cross_test_data = prices_live.where(["Symbol = `TSLA`"])')
            df = b.open_table("cross_test_data").to_arrow().to_pandas()
            assert len(df) == 1
            assert df["Symbol"].iloc[0] == "TSLA"
        finally:
            a.close()
            b.close()

    def test_table_persists_after_creator_disconnects(self):
        a = _connect()
        a.run_script('cross_test_persist = prices_live.where(["Symbol = `NVDA`"])')
        a.close()

        b = _connect()
        try:
            assert "cross_test_persist" in b.list_tables()
            df = b.open_table("cross_test_persist").to_arrow().to_pandas()
            assert len(df) == 1
            assert df["Symbol"].iloc[0] == "NVDA"
        finally:
            b.close()


# ── Multiple concurrent sessions ─────────────────────────────────────────────

class TestConcurrentSessions:
    def test_three_clients_connect_simultaneously(self):
        clients = [_connect() for _ in range(3)]
        try:
            for c in clients:
                assert c.session.is_alive
                tables = c.list_tables()
                assert "prices_live" in tables
        finally:
            for c in clients:
                c.close()

    def test_all_clients_see_same_symbols(self):
        clients = [_connect() for _ in range(3)]
        try:
            symbol_sets = []
            for c in clients:
                df = c.open_table("prices_live").to_arrow().to_pandas()
                symbol_sets.append(set(df["Symbol"].tolist()))
            # All clients should see the same 8 symbols
            assert all(s == symbol_sets[0] for s in symbol_sets)
            assert len(symbol_sets[0]) == 8
        finally:
            for c in clients:
                c.close()

    def test_each_client_publishes_independently(self):
        a = _connect()
        b = _connect()
        c = _connect()
        try:
            a.run_script('multi_a = prices_live.where(["Symbol = `AAPL`"])')
            b.run_script('multi_b = prices_live.where(["Symbol = `MSFT`"])')
            c.run_script('multi_c = prices_live.where(["Symbol = `AMZN`"])')

            tables = c.list_tables()
            assert "multi_a" in tables
            assert "multi_b" in tables
            assert "multi_c" in tables

            df_a = c.open_table("multi_a").to_arrow().to_pandas()
            df_b = c.open_table("multi_b").to_arrow().to_pandas()
            df_c = c.open_table("multi_c").to_arrow().to_pandas()
            assert df_a["Symbol"].iloc[0] == "AAPL"
            assert df_b["Symbol"].iloc[0] == "MSFT"
            assert df_c["Symbol"].iloc[0] == "AMZN"
        finally:
            a.close()
            b.close()
            c.close()


# ── Script isolation ─────────────────────────────────────────────────────────

class TestScriptIsolation:
    def test_bad_script_does_not_break_other_sessions(self):
        a = _connect()
        b = _connect()
        try:
            # Client A runs a bad script
            with pytest.raises(Exception):
                a.run_script("this_will_fail = nonexistent_table.where(['x'])")

            # Client B should still work fine
            tables = b.list_tables()
            assert "prices_live" in tables
            df = b.open_table("prices_live").to_arrow().to_pandas()
            assert len(df) == 8
        finally:
            a.close()
            b.close()

    def test_overwrite_table_visible_to_others(self):
        a = _connect()
        b = _connect()
        try:
            # Create then overwrite
            a.run_script('overwrite_test = prices_live.where(["Symbol = `AAPL`"])')
            df1 = b.open_table("overwrite_test").to_arrow().to_pandas()
            assert df1["Symbol"].iloc[0] == "AAPL"

            a.run_script('overwrite_test = prices_live.where(["Symbol = `GOOGL`"])')
            df2 = b.open_table("overwrite_test").to_arrow().to_pandas()
            assert df2["Symbol"].iloc[0] == "GOOGL"
        finally:
            a.close()
            b.close()


# ── Data consistency ─────────────────────────────────────────────────────────

class TestDataConsistency:
    def test_portfolio_summary_consistent_across_clients(self):
        """Two clients reading portfolio_summary should get same structure."""
        a = _connect()
        b = _connect()
        try:
            df_a = a.open_table("portfolio_summary").to_arrow().to_pandas()
            df_b = b.open_table("portfolio_summary").to_arrow().to_pandas()
            assert list(df_a.columns) == list(df_b.columns)
            assert len(df_a) == len(df_b) == 1
        finally:
            a.close()
            b.close()

    def test_derived_tables_tick_for_all_clients(self):
        """A derived table created by one client ticks for another."""
        a = _connect()
        b = _connect()
        try:
            a.run_script('tick_test = prices_live.where(["Symbol = `META`"])')
            snap1 = b.open_table("tick_test").to_arrow().to_pandas()
            time.sleep(1.5)  # allow several tick cycles (200ms each)
            snap2 = b.open_table("tick_test").to_arrow().to_pandas()
            # Price should have changed (ticking)
            p1 = snap1["Price"].iloc[0]
            p2 = snap2["Price"].iloc[0]
            assert p1 != p2, "Derived table not ticking for other client"
        finally:
            a.close()
            b.close()
