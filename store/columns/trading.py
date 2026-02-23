"""
Trading domain columns — symbol, price, quantity, side, pnl, etc.

Core columns for trade, order, and signal entities.
"""

from store.columns import REGISTRY

# ── Identifiers (dimensions) ─────────────────────────────────────

REGISTRY.define("symbol", str,
    description="Financial instrument ticker symbol",
    semantic_type="identifier",
    role="dimension",
    synonyms=["ticker", "instrument", "security", "stock"],
    sample_values=["AAPL", "GOOGL", "MSFT", "TSLA"],
    max_length=12,
    pattern=r"^[A-Z0-9./]+$",
    sensitivity="public",
    display_name="Symbol",
    category="trading",
)

REGISTRY.define("side", str,
    description="Trade direction",
    semantic_type="label",
    role="dimension",
    enum=["BUY", "SELL"],
    synonyms=["direction", "buy/sell"],
    display_name="Side",
    category="trading",
)

REGISTRY.define("direction", str,
    description="Signal direction",
    semantic_type="label",
    role="dimension",
    enum=["LONG", "SHORT"],
    synonyms=["signal direction"],
    display_name="Direction",
    category="signals",
)

REGISTRY.define("order_type", str,
    description="Order execution type",
    semantic_type="label",
    role="dimension",
    enum=["LIMIT", "MARKET", "STOP"],
    display_name="Order Type",
    category="trading",
)

REGISTRY.define("option_type", str,
    description="Option contract type",
    semantic_type="label",
    role="dimension",
    enum=["CALL", "PUT"],
    display_name="Option Type",
    category="trading",
)

REGISTRY.define("timestamp", str,
    description="Event timestamp (ISO format)",
    semantic_type="timestamp",
    role="attribute",
    nullable=True,
    category="trading",
)

# ── Measures ──────────────────────────────────────────────────────

REGISTRY.define("price", float,
    description="Trade execution price",
    semantic_type="currency_amount",
    role="measure",
    aggregation="last",
    unit="USD",
    format=",.2f",
    min_value=0,
    display_name="Price",
    category="trading",
    synonyms=["px", "execution price", "trade price"],
    sample_values=[228.50, 192.30, 415.75],
)

REGISTRY.define("quantity", int,
    description="Number of units/shares",
    semantic_type="count",
    role="measure",
    aggregation="sum",
    unit="shares",
    min_value=0,
    display_name="Qty",
    category="trading",
    synonyms=["qty", "shares", "lots", "size", "notional quantity"],
    sample_values=[100, 500, 1000],
)

REGISTRY.define("pnl", float,
    description="Profit and loss",
    semantic_type="currency_amount",
    role="measure",
    aggregation="sum",
    unit="USD",
    format="+,.2f",
    display_name="P&L",
    category="risk",
    synonyms=["profit", "loss", "profit and loss", "pl"],
)

REGISTRY.define("strength", float,
    description="Signal confidence score",
    semantic_type="score",
    role="measure",
    unit="ratio",
    aggregation="avg",
    min_value=0.0,
    max_value=1.0,
    format=".2%",
    display_name="Strength",
    category="signals",
)
