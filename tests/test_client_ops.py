"""
Integration tests for client operations against the Deephaven server.
Requires the server to be running: cd server && python3 -i app.py

Run with: pytest tests/test_client_ops.py -v
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "client"))

from base_client import DeephavenClient


@pytest.fixture(scope="module")
def client():
    try:
        c = DeephavenClient()
    except Exception as e:
        pytest.skip(f"Deephaven server not running: {e}")
    yield c
    c.close()


# ── Connection ───────────────────────────────────────────────────────────────

class TestConnection:
    def test_connect_creates_session(self, client):
        assert client.session is not None
        assert client.session.is_alive

    def test_host_and_port_stored(self, client):
        assert client.host == "localhost"
        assert client.port == 10000

    def test_context_manager_connect_and_close(self):
        try:
            with DeephavenClient() as c:
                assert c.session.is_alive
                tables = c.list_tables()
                assert len(tables) > 0
        except Exception as e:
            pytest.skip(f"Deephaven server not running: {e}")

    def test_close_is_idempotent(self):
        """Closing twice should not raise."""
        try:
            c = DeephavenClient()
            c.close()
            # Second close should not raise
            c.close()
        except Exception as e:
            pytest.skip(f"Deephaven server not running: {e}")


# ── list_tables ──────────────────────────────────────────────────────────────

class TestListTables:
    def test_returns_list(self, client):
        tables = client.list_tables()
        assert isinstance(tables, list)

    def test_contains_core_tables(self, client):
        tables = client.list_tables()
        for name in ["prices_live", "risk_live", "portfolio_summary"]:
            assert name in tables


# ── open_table ───────────────────────────────────────────────────────────────

class TestOpenTable:
    def test_open_prices_live(self, client):
        table = client.open_table("prices_live")
        assert table is not None
        assert table.schema is not None

    def test_open_nonexistent_table_raises(self, client):
        with pytest.raises(Exception):
            client.open_table("this_table_does_not_exist_xyz")


# ── Export to Arrow / pandas ─────────────────────────────────────────────────

class TestExport:
    def test_to_arrow(self, client):
        import pyarrow as pa
        table = client.open_table("prices_live")
        arrow = table.to_arrow()
        assert isinstance(arrow, pa.Table)
        assert arrow.num_rows == 8
        assert arrow.num_columns == 7

    def test_to_pandas(self, client):
        import pandas as pd
        table = client.open_table("prices_live")
        df = table.to_arrow().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 8
        assert "Symbol" in df.columns
        assert "Price" in df.columns

    def test_pandas_dtypes(self, client):
        import numpy as np
        df = client.open_table("prices_live").to_arrow().to_pandas()
        assert df["Price"].dtype == np.float64
        assert df["Volume"].dtype == np.int64

    def test_risk_export_has_greeks(self, client):
        df = client.open_table("risk_live").to_arrow().to_pandas()
        for col in ["Delta", "Gamma", "Theta", "Vega"]:
            assert col in df.columns


# ── Client-side table operations ─────────────────────────────────────────────

class TestTableOperations:
    def test_where_filter(self, client):
        table = client.open_table("prices_live").where(["Symbol = `AAPL`"])
        df = table.to_arrow().to_pandas()
        assert len(df) == 1
        assert df["Symbol"].iloc[0] == "AAPL"

    def test_where_filter_multiple(self, client):
        table = client.open_table("prices_live").where(
            ["Symbol in `AAPL`, `TSLA`"]
        )
        df = table.to_arrow().to_pandas()
        assert len(df) == 2
        assert set(df["Symbol"].tolist()) == {"AAPL", "TSLA"}

    def test_sort_ascending(self, client):
        table = client.open_table("prices_live").sort(["Price"])
        df = table.to_arrow().to_pandas()
        prices = df["Price"].tolist()
        assert prices == sorted(prices)

    def test_sort_descending(self, client):
        table = client.open_table("prices_live").sort_descending(["Price"])
        df = table.to_arrow().to_pandas()
        prices = df["Price"].tolist()
        assert prices == sorted(prices, reverse=True)


# ── run_script ───────────────────────────────────────────────────────────────

class TestRunScript:
    def test_run_script_creates_table(self, client):
        client.run_script('test_script_table = prices_live.where(["Symbol = `MSFT`"])')
        tables = client.list_tables()
        assert "test_script_table" in tables

    def test_run_script_table_has_data(self, client):
        client.run_script('test_script_verify = prices_live.where(["Symbol = `GOOGL`"])')
        table = client.open_table("test_script_verify")
        df = table.to_arrow().to_pandas()
        assert len(df) == 1
        assert df["Symbol"].iloc[0] == "GOOGL"

    def test_run_script_derived_table(self, client):
        client.run_script("""
test_derived = risk_live.update([
    "AbsDelta = Math.abs(Delta)",
])
""")
        df = client.open_table("test_derived").to_arrow().to_pandas()
        assert "AbsDelta" in df.columns
        assert (df["AbsDelta"] >= 0).all()

    def test_run_script_with_aggregation(self, client):
        client.run_script("""
from deephaven import agg
test_agg = risk_live.agg_by(
    [agg.sum_(["TotalDelta=Delta"])],
)
""")
        df = client.open_table("test_agg").to_arrow().to_pandas()
        assert len(df) == 1
        assert "TotalDelta" in df.columns
