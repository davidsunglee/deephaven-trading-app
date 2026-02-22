"""
Integration tests for Deephaven server tables.
Requires the server to be running: cd server && python3 -i app.py

Run with: pytest tests/test_server_tables.py -v
"""

import sys
import os
import time
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "client"))

from base_client import DeephavenClient


@pytest.fixture(scope="module")
def client():
    """Single client connection reused across all tests in this module."""
    try:
        c = DeephavenClient()
    except Exception as e:
        pytest.skip(f"Deephaven server not running: {e}")
    yield c
    c.close()


# ── Table existence ──────────────────────────────────────────────────────────

EXPECTED_TABLES = [
    "prices_raw",
    "prices_live",
    "risk_raw",
    "risk_live",
    "portfolio_summary",
    "top_movers",
    "volume_leaders",
]


class TestTableExistence:
    def test_all_expected_tables_exist(self, client):
        tables = client.list_tables()
        for name in EXPECTED_TABLES:
            assert name in tables, f"Missing table: {name}"

    def test_no_unexpected_missing(self, client):
        """At minimum, the 7 core tables should be present."""
        assert len(client.list_tables()) >= len(EXPECTED_TABLES)


# ── Schema validation ────────────────────────────────────────────────────────

class TestPricesLiveSchema:
    def test_has_correct_columns(self, client):
        table = client.open_table("prices_live")
        col_names = [field.name for field in table.schema]
        expected = ["Symbol", "Price", "Bid", "Ask", "Volume", "Change", "ChangePct"]
        for col in expected:
            assert col in col_names, f"Missing column: {col}"

    def test_symbol_is_string_type(self, client):
        table = client.open_table("prices_live")
        schema_dict = {field.name: field.type for field in table.schema}
        import pyarrow as pa
        assert schema_dict["Symbol"] == pa.string()

    def test_price_is_float64(self, client):
        table = client.open_table("prices_live")
        schema_dict = {field.name: field.type for field in table.schema}
        import pyarrow as pa
        assert schema_dict["Price"] == pa.float64()

    def test_volume_is_int64(self, client):
        table = client.open_table("prices_live")
        schema_dict = {field.name: field.type for field in table.schema}
        import pyarrow as pa
        assert schema_dict["Volume"] == pa.int64()


class TestRiskLiveSchema:
    def test_has_correct_columns(self, client):
        table = client.open_table("risk_live")
        col_names = [field.name for field in table.schema]
        expected = ["Symbol", "Position", "MarketValue", "UnrealizedPnL",
                    "Delta", "Gamma", "Theta", "Vega"]
        for col in expected:
            assert col in col_names, f"Missing column: {col}"


class TestPortfolioSummarySchema:
    def test_has_aggregated_columns(self, client):
        table = client.open_table("portfolio_summary")
        col_names = [field.name for field in table.schema]
        expected = ["TotalMV", "TotalPnL", "TotalDelta", "AvgGamma",
                    "AvgTheta", "AvgVega", "NumPositions"]
        for col in expected:
            assert col in col_names, f"Missing column: {col}"


# ── Data flow ────────────────────────────────────────────────────────────────

class TestDataFlow:
    def test_prices_live_has_rows(self, client):
        df = client.open_table("prices_live").to_arrow().to_pandas()
        assert len(df) > 0, "prices_live is empty"

    def test_risk_live_has_rows(self, client):
        df = client.open_table("risk_live").to_arrow().to_pandas()
        assert len(df) > 0, "risk_live is empty"

    def test_prices_live_has_exactly_8_symbols(self, client):
        """last_by('Symbol') should yield exactly one row per symbol."""
        df = client.open_table("prices_live").to_arrow().to_pandas()
        assert len(df) == 8

    def test_risk_live_has_exactly_8_symbols(self, client):
        df = client.open_table("risk_live").to_arrow().to_pandas()
        assert len(df) == 8

    def test_portfolio_summary_has_one_row(self, client):
        df = client.open_table("portfolio_summary").to_arrow().to_pandas()
        assert len(df) == 1

    def test_all_expected_symbols_present(self, client):
        df = client.open_table("prices_live").to_arrow().to_pandas()
        expected = {"AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "NFLX"}
        assert set(df["Symbol"].tolist()) == expected

    def test_prices_are_positive(self, client):
        df = client.open_table("prices_live").to_arrow().to_pandas()
        assert (df["Price"] > 0).all()

    def test_bid_less_than_ask(self, client):
        df = client.open_table("prices_live").to_arrow().to_pandas()
        assert (df["Bid"] < df["Ask"]).all()

    def test_num_positions_is_8(self, client):
        df = client.open_table("portfolio_summary").to_arrow().to_pandas()
        assert df["NumPositions"].iloc[0] == 8


# ── Ticking data ─────────────────────────────────────────────────────────────

class TestTickingData:
    def test_prices_change_over_time(self, client):
        """Take two snapshots and verify at least one price changed."""
        snap1 = client.open_table("prices_live").to_arrow().to_pandas()
        time.sleep(1.5)  # allow several tick cycles (200ms each)
        snap2 = client.open_table("prices_live").to_arrow().to_pandas()
        prices1 = snap1.set_index("Symbol")["Price"]
        prices2 = snap2.set_index("Symbol")["Price"]
        differences = (prices1 - prices2).abs()
        assert differences.sum() > 0, "Prices did not tick"

    def test_raw_table_grows(self, client):
        """prices_raw is append-only and should grow."""
        n1 = client.open_table("prices_raw").to_arrow().num_rows
        time.sleep(0.5)
        n2 = client.open_table("prices_raw").to_arrow().num_rows
        assert n2 > n1, "prices_raw did not grow"


# ── Sorting ──────────────────────────────────────────────────────────────────

class TestSorting:
    def test_top_movers_sorted_descending(self, client):
        df = client.open_table("top_movers").to_arrow().to_pandas()
        pcts = df["ChangePct"].tolist()
        assert pcts == sorted(pcts, reverse=True), "top_movers not sorted descending"

    def test_volume_leaders_sorted_descending(self, client):
        df = client.open_table("volume_leaders").to_arrow().to_pandas()
        vols = df["Volume"].tolist()
        assert vols == sorted(vols, reverse=True), "volume_leaders not sorted descending"
