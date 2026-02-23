"""
Finance domain columns — bid, ask, strike, volatility, notional, etc.

Columns for market data, options, bonds, FX, and risk entities.
"""

from store.columns import REGISTRY

# ── Market Data ───────────────────────────────────────────────────

REGISTRY.define("bid", float,
    description="Best bid price",
    semantic_type="currency_amount",
    role="measure",
    unit="USD",
    format=",.4f",
    display_name="Bid",
    category="market_data",
    synonyms=["bid price"],
)

REGISTRY.define("ask", float,
    description="Best ask/offer price",
    semantic_type="currency_amount",
    role="measure",
    unit="USD",
    format=",.4f",
    display_name="Ask",
    category="market_data",
    synonyms=["ask price", "offer", "offer price"],
)

REGISTRY.define("last", float,
    description="Last traded price",
    semantic_type="currency_amount",
    role="measure",
    unit="USD",
    format=",.4f",
    display_name="Last",
    category="market_data",
    synonyms=["last price", "last trade"],
)

REGISTRY.define("volume", int,
    description="Trading volume",
    semantic_type="count",
    role="measure",
    unit="shares",
    aggregation="sum",
    display_name="Volume",
    category="market_data",
    synonyms=["vol", "trading volume"],
)

REGISTRY.define("rate", float,
    description="Exchange or interest rate",
    semantic_type="ratio",
    role="measure",
    unit="ratio",
    format=",.6f",
    display_name="Rate",
    category="market_data",
)

# ── Position / Portfolio ──────────────────────────────────────────

REGISTRY.define("avg_cost", float,
    description="Average cost basis per unit",
    semantic_type="currency_amount",
    role="measure",
    unit="USD",
    format=",.2f",
    display_name="Avg Cost",
    category="portfolio",
    synonyms=["average cost", "cost basis"],
)

REGISTRY.define("current_price", float,
    description="Current market price",
    semantic_type="currency_amount",
    role="measure",
    unit="USD",
    format=",.2f",
    display_name="Current Price",
    category="portfolio",
)

# ── Options ───────────────────────────────────────────────────────

REGISTRY.define("underlying_price", float,
    description="Price of the underlying instrument",
    semantic_type="currency_amount",
    role="measure",
    unit="USD",
    format=",.2f",
    display_name="Underlying Price",
    category="derivatives",
)

REGISTRY.define("strike", float,
    description="Option strike price",
    semantic_type="currency_amount",
    role="measure",
    unit="USD",
    format=",.2f",
    display_name="Strike",
    category="derivatives",
    synonyms=["strike price", "exercise price"],
)

REGISTRY.define("time_to_expiry", float,
    description="Time to option expiry",
    semantic_type="duration",
    role="measure",
    unit="years",
    format=".4f",
    display_name="Time to Expiry",
    category="derivatives",
    synonyms=["tte", "time to maturity", "tenor"],
)

REGISTRY.define("volatility", float,
    description="Annualized volatility",
    semantic_type="percentage",
    role="measure",
    unit="ratio",
    format=".2%",
    display_name="Volatility",
    category="derivatives",
    synonyms=["vol", "implied vol", "iv"],
)

REGISTRY.define("risk_free_rate", float,
    description="Risk-free interest rate",
    semantic_type="percentage",
    role="measure",
    unit="ratio",
    format=".2%",
    display_name="Risk-Free Rate",
    category="derivatives",
    synonyms=["rfr", "risk free", "rate"],
)

# ── FX ────────────────────────────────────────────────────────────

REGISTRY.define("pair", str,
    description="Currency pair (e.g. EUR/USD)",
    semantic_type="identifier",
    role="dimension",
    display_name="Pair",
    category="fx",
    synonyms=["currency pair", "ccy pair"],
)

# ── Bonds ─────────────────────────────────────────────────────────

REGISTRY.define("isin", str,
    description="International Securities Identification Number",
    semantic_type="identifier",
    role="dimension",
    max_length=12,
    display_name="ISIN",
    category="fixed_income",
)

REGISTRY.define("face_value", float,
    description="Bond face/par value",
    semantic_type="currency_amount",
    role="measure",
    unit="USD",
    format=",.2f",
    display_name="Face Value",
    category="fixed_income",
    synonyms=["par value", "nominal"],
)

REGISTRY.define("coupon_rate", float,
    description="Annual coupon rate",
    semantic_type="percentage",
    role="measure",
    unit="ratio",
    format=".2%",
    display_name="Coupon Rate",
    category="fixed_income",
    synonyms=["coupon"],
)

REGISTRY.define("yield_to_maturity", float,
    description="Yield to maturity",
    semantic_type="percentage",
    role="measure",
    unit="ratio",
    format=".2%",
    display_name="YTM",
    category="fixed_income",
    synonyms=["ytm", "yield"],
)

REGISTRY.define("years_to_maturity", float,
    description="Years until bond maturity",
    semantic_type="duration",
    role="measure",
    unit="years",
    format=".2f",
    display_name="Years to Maturity",
    category="fixed_income",
)

# ── Risk ──────────────────────────────────────────────────────────

REGISTRY.define("notional", float,
    description="Notional exposure amount",
    semantic_type="currency_amount",
    role="measure",
    unit="USD",
    format=",.2f",
    display_name="Notional",
    category="risk",
    synonyms=["notional amount", "exposure"],
)

REGISTRY.define("daily_vol", float,
    description="Daily volatility",
    semantic_type="percentage",
    role="measure",
    unit="ratio",
    format=".4f",
    display_name="Daily Vol",
    category="risk",
)

REGISTRY.define("z_score", float,
    description="Z-score for confidence interval",
    semantic_type="score",
    role="measure",
    unit="ratio",
    format=".3f",
    display_name="Z-Score",
    category="risk",
)
