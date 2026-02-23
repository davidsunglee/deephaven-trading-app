"""
Finance/trading test suite for the reactive computation layer.

Tests realistic pricing, risk, and P&L computations using domain objects
from store/models.py and additional finance-specific Storables.
"""

import math
import pytest
from dataclasses import dataclass, field
from typing import Optional

from store.base import Storable
from store.models import Trade, Order, Signal
from reactive.expr import Const, Field, BinOp, Func, If, Coalesce, IsNull, from_json
from reactive.graph import ReactiveGraph


# ---------------------------------------------------------------------------
# Additional finance domain models
# ---------------------------------------------------------------------------

@dataclass
class MarketData(Storable):
    """Live market data tick."""
    symbol: str = ""
    bid: float = 0.0
    ask: float = 0.0
    last: float = 0.0
    volume: int = 0


@dataclass
class Position(Storable):
    """A portfolio position."""
    symbol: str = ""
    quantity: int = 0
    avg_cost: float = 0.0
    current_price: float = 0.0
    side: str = "LONG"  # "LONG" or "SHORT"


@dataclass
class Option(Storable):
    """A vanilla equity option."""
    symbol: str = ""
    underlying_price: float = 0.0
    strike: float = 0.0
    time_to_expiry: float = 0.0  # years
    volatility: float = 0.0      # annualized, e.g. 0.20 = 20%
    risk_free_rate: float = 0.0  # e.g. 0.05 = 5%
    option_type: str = "CALL"    # "CALL" or "PUT"


@dataclass
class FXRate(Storable):
    """Foreign exchange rate."""
    pair: str = ""         # e.g. "EUR/USD"
    rate: float = 0.0
    bid: float = 0.0
    ask: float = 0.0


@dataclass
class Bond(Storable):
    """A fixed income bond."""
    isin: str = ""
    face_value: float = 1000.0
    coupon_rate: float = 0.0     # annual, e.g. 0.05 = 5%
    yield_to_maturity: float = 0.0
    years_to_maturity: float = 0.0
    price: float = 0.0


# ===========================================================================
# Pricing tests
# ===========================================================================

class TestMarketDataMidPrice:
    """Mid-price = (bid + ask) / 2"""

    def test_mid_price_computation(self):
        graph = ReactiveGraph()
        md = MarketData(symbol="AAPL", bid=227.50, ask=228.00, last=227.75)
        nid = graph.track(md)

        mid_expr = (Field("bid") + Field("ask")) / Const(2)
        graph.computed(nid, "mid", mid_expr)

        assert graph.get(nid, "mid") == 227.75

    def test_mid_price_updates_on_tick(self):
        graph = ReactiveGraph()
        md = MarketData(symbol="AAPL", bid=227.50, ask=228.00)
        nid = graph.track(md)

        mid_expr = (Field("bid") + Field("ask")) / Const(2)
        graph.computed(nid, "mid", mid_expr)

        graph.batch_update(nid, {"bid": 229.00, "ask": 229.50})
        assert graph.get(nid, "mid") == 229.25

    def test_spread_computation(self):
        graph = ReactiveGraph()
        md = MarketData(symbol="AAPL", bid=227.50, ask=228.00)
        nid = graph.track(md)

        spread_expr = Field("ask") - Field("bid")
        graph.computed(nid, "spread", spread_expr)

        assert graph.get(nid, "spread") == 0.50

    def test_spread_bps(self):
        """Spread in basis points = (ask - bid) / mid * 10000"""
        graph = ReactiveGraph()
        md = MarketData(symbol="AAPL", bid=227.50, ask=228.50)
        nid = graph.track(md)

        mid = (Field("bid") + Field("ask")) / Const(2)
        spread_bps = (Field("ask") - Field("bid")) / mid * Const(10000)
        graph.computed(nid, "spread_bps", spread_bps)

        expected = (228.50 - 227.50) / 228.0 * 10000
        assert abs(graph.get(nid, "spread_bps") - expected) < 0.01

    def test_to_sql(self):
        mid_expr = (Field("bid") + Field("ask")) / Const(2)
        sql = mid_expr.to_sql("data")
        assert "(data->>'bid')::float" in sql
        assert "(data->>'ask')::float" in sql

    def test_to_pure(self):
        mid_expr = (Field("bid") + Field("ask")) / Const(2)
        pure = mid_expr.to_pure("$tick")
        assert "$tick.bid" in pure
        assert "$tick.ask" in pure


class TestPositionPnL:
    """Unrealized P&L = (current_price - avg_cost) * quantity * direction_multiplier"""

    def test_long_pnl_profit(self):
        graph = ReactiveGraph()
        pos = Position(symbol="AAPL", quantity=100, avg_cost=220.0, current_price=230.0, side="LONG")
        nid = graph.track(pos)

        pnl_expr = (Field("current_price") - Field("avg_cost")) * Field("quantity")
        graph.computed(nid, "unrealized_pnl", pnl_expr)

        assert graph.get(nid, "unrealized_pnl") == 1000.0

    def test_long_pnl_loss(self):
        graph = ReactiveGraph()
        pos = Position(symbol="AAPL", quantity=100, avg_cost=230.0, current_price=220.0)
        nid = graph.track(pos)

        pnl_expr = (Field("current_price") - Field("avg_cost")) * Field("quantity")
        graph.computed(nid, "unrealized_pnl", pnl_expr)

        assert graph.get(nid, "unrealized_pnl") == -1000.0

    def test_pnl_updates_on_price_change(self):
        graph = ReactiveGraph()
        pos = Position(symbol="TSLA", quantity=50, avg_cost=180.0, current_price=180.0)
        nid = graph.track(pos)

        pnl_expr = (Field("current_price") - Field("avg_cost")) * Field("quantity")
        graph.computed(nid, "unrealized_pnl", pnl_expr)

        assert graph.get(nid, "unrealized_pnl") == 0.0

        graph.update(nid, "current_price", 200.0)
        assert graph.get(nid, "unrealized_pnl") == 1000.0

        graph.update(nid, "current_price", 170.0)
        assert graph.get(nid, "unrealized_pnl") == -500.0

    def test_pnl_percentage(self):
        graph = ReactiveGraph()
        pos = Position(symbol="MSFT", quantity=200, avg_cost=400.0, current_price=420.0)
        nid = graph.track(pos)

        pnl_pct_expr = (Field("current_price") - Field("avg_cost")) / Field("avg_cost") * Const(100)
        graph.computed(nid, "pnl_pct", pnl_pct_expr)

        assert graph.get(nid, "pnl_pct") == 5.0

    def test_market_value(self):
        graph = ReactiveGraph()
        pos = Position(symbol="GOOG", quantity=50, avg_cost=170.0, current_price=175.0)
        nid = graph.track(pos)

        mv_expr = Field("current_price") * Field("quantity")
        graph.computed(nid, "market_value", mv_expr)

        assert graph.get(nid, "market_value") == 8750.0

    def test_pnl_sql(self):
        pnl_expr = (Field("current_price") - Field("avg_cost")) * Field("quantity")
        sql = pnl_expr.to_sql("data")
        assert "(data->>'current_price')::float" in sql
        assert "(data->>'avg_cost')::float" in sql
        assert "(data->>'quantity')::float" in sql

    def test_pnl_pure(self):
        pnl_expr = (Field("current_price") - Field("avg_cost")) * Field("quantity")
        pure = pnl_expr.to_pure("$pos")
        assert "$pos.current_price" in pure
        assert "$pos.avg_cost" in pure
        assert "$pos.quantity" in pure


class TestOptionPricing:
    """Simplified Black-Scholes-style option computations."""

    def _make_option_graph(self, option):
        graph = ReactiveGraph()
        nid = graph.track(option)

        # Moneyness = underlying_price / strike
        moneyness = Field("underlying_price") / Field("strike")
        graph.computed(nid, "moneyness", moneyness)

        # Intrinsic value for CALL = max(underlying - strike, 0)
        # Using If instead of max to demonstrate conditional
        intrinsic_call = If(
            Field("underlying_price") > Field("strike"),
            Field("underlying_price") - Field("strike"),
            Const(0),
        )
        graph.computed(nid, "intrinsic_call", intrinsic_call)

        # Time value proxy = volatility * sqrt(time_to_expiry) * underlying_price
        time_value = Field("volatility") * Func("sqrt", [Field("time_to_expiry")]) * Field("underlying_price")
        graph.computed(nid, "time_value_proxy", time_value)

        return graph, nid

    def test_itm_call_intrinsic(self):
        opt = Option(underlying_price=110.0, strike=100.0, time_to_expiry=0.5, volatility=0.2)
        graph, nid = self._make_option_graph(opt)
        assert graph.get(nid, "intrinsic_call") == 10.0

    def test_otm_call_intrinsic(self):
        opt = Option(underlying_price=90.0, strike=100.0, time_to_expiry=0.5, volatility=0.2)
        graph, nid = self._make_option_graph(opt)
        assert graph.get(nid, "intrinsic_call") == 0

    def test_atm_moneyness(self):
        opt = Option(underlying_price=100.0, strike=100.0, time_to_expiry=0.5, volatility=0.2)
        graph, nid = self._make_option_graph(opt)
        assert graph.get(nid, "moneyness") == 1.0

    def test_itm_moneyness(self):
        opt = Option(underlying_price=120.0, strike=100.0, time_to_expiry=0.5, volatility=0.2)
        graph, nid = self._make_option_graph(opt)
        assert graph.get(nid, "moneyness") == 1.2

    def test_time_value_proxy(self):
        opt = Option(underlying_price=100.0, strike=100.0, time_to_expiry=1.0, volatility=0.25)
        graph, nid = self._make_option_graph(opt)
        expected = 0.25 * math.sqrt(1.0) * 100.0
        assert abs(graph.get(nid, "time_value_proxy") - expected) < 0.001

    def test_option_reacts_to_spot_move(self):
        opt = Option(underlying_price=100.0, strike=100.0, time_to_expiry=0.5, volatility=0.2)
        graph, nid = self._make_option_graph(opt)

        assert graph.get(nid, "intrinsic_call") == 0

        graph.update(nid, "underlying_price", 115.0)
        assert graph.get(nid, "intrinsic_call") == 15.0
        assert graph.get(nid, "moneyness") == 1.15

    def test_option_reacts_to_vol_change(self):
        opt = Option(underlying_price=100.0, strike=100.0, time_to_expiry=1.0, volatility=0.20)
        graph, nid = self._make_option_graph(opt)

        tv_before = graph.get(nid, "time_value_proxy")
        graph.update(nid, "volatility", 0.40)
        tv_after = graph.get(nid, "time_value_proxy")

        assert tv_after > tv_before
        assert abs(tv_after - 0.40 * 1.0 * 100.0) < 0.001

    def test_intrinsic_call_sql(self):
        expr = If(
            Field("underlying_price") > Field("strike"),
            Field("underlying_price") - Field("strike"),
            Const(0),
        )
        sql = expr.to_sql("data")
        assert "CASE WHEN" in sql
        assert "THEN" in sql
        assert "ELSE" in sql

    def test_intrinsic_call_pure(self):
        expr = If(
            Field("underlying_price") > Field("strike"),
            Field("underlying_price") - Field("strike"),
            Const(0),
        )
        pure = expr.to_pure("$opt")
        assert "if(" in pure
        assert "$opt.underlying_price" in pure
        assert "$opt.strike" in pure


class TestFXConversion:
    """FX rate conversions and cross-rate computations."""

    def test_usd_to_eur(self):
        graph = ReactiveGraph()
        fx = FXRate(pair="EUR/USD", rate=1.0850, bid=1.0848, ask=1.0852)
        nid = graph.track(fx)

        # Convert 1000 USD to EUR = amount / rate
        usd_amount = Const(1000)
        eur_expr = usd_amount / Field("rate")
        graph.computed(nid, "eur_value", eur_expr)

        expected = 1000 / 1.0850
        assert abs(graph.get(nid, "eur_value") - expected) < 0.01

    def test_fx_spread_pips(self):
        """FX spread in pips = (ask - bid) * 10000"""
        graph = ReactiveGraph()
        fx = FXRate(pair="EUR/USD", rate=1.0850, bid=1.0848, ask=1.0852)
        nid = graph.track(fx)

        pips_expr = (Field("ask") - Field("bid")) * Const(10000)
        graph.computed(nid, "spread_pips", pips_expr)

        expected = (1.0852 - 1.0848) * 10000
        assert abs(graph.get(nid, "spread_pips") - expected) < 0.01

    def test_fx_reacts_to_rate_change(self):
        graph = ReactiveGraph()
        fx = FXRate(pair="EUR/USD", rate=1.0850, bid=1.0848, ask=1.0852)
        nid = graph.track(fx)

        eur_expr = Const(10000) / Field("rate")
        graph.computed(nid, "eur_value", eur_expr)

        val_before = graph.get(nid, "eur_value")
        graph.update(nid, "rate", 1.1000)
        val_after = graph.get(nid, "eur_value")

        assert val_after < val_before  # EUR strengthened, so fewer EUR per USD


# ===========================================================================
# Risk tests
# ===========================================================================

class TestRiskMetrics:
    """Portfolio risk computations."""

    def test_notional_exposure(self):
        """Notional = quantity * current_price"""
        graph = ReactiveGraph()
        pos = Position(symbol="AAPL", quantity=500, avg_cost=220.0, current_price=230.0)
        nid = graph.track(pos)

        notional = Field("quantity") * Field("current_price")
        graph.computed(nid, "notional", notional)

        assert graph.get(nid, "notional") == 115000.0

    def test_position_weight(self):
        """Weight = notional / portfolio_nav (using Const for NAV)"""
        graph = ReactiveGraph()
        pos = Position(symbol="AAPL", quantity=100, avg_cost=220.0, current_price=230.0)
        nid = graph.track(pos)

        nav = Const(1_000_000)
        weight = (Field("quantity") * Field("current_price")) / nav * Const(100)
        graph.computed(nid, "weight_pct", weight)

        expected = (100 * 230.0) / 1_000_000 * 100
        assert abs(graph.get(nid, "weight_pct") - expected) < 0.001

    def test_var_proxy(self):
        """Simple parametric VaR proxy = notional * volatility * z_score * sqrt(horizon)"""
        graph = ReactiveGraph()

        @dataclass
        class RiskPosition(Storable):
            notional: float = 0.0
            daily_vol: float = 0.0  # daily volatility
            z_score: float = 1.645  # 95% confidence

        rp = RiskPosition(notional=100_000, daily_vol=0.02, z_score=1.645)
        nid = graph.track(rp)

        # 1-day VaR
        var_expr = Field("notional") * Field("daily_vol") * Field("z_score")
        graph.computed(nid, "var_1d", var_expr)

        expected = 100_000 * 0.02 * 1.645
        assert abs(graph.get(nid, "var_1d") - expected) < 0.01

        # 10-day VaR
        var_10d_expr = Field("notional") * Field("daily_vol") * Field("z_score") * Func("sqrt", [Const(10)])
        graph.computed(nid, "var_10d", var_10d_expr)

        expected_10d = 100_000 * 0.02 * 1.645 * math.sqrt(10)
        assert abs(graph.get(nid, "var_10d") - expected_10d) < 0.01

    def test_risk_reacts_to_vol_spike(self):
        graph = ReactiveGraph()

        @dataclass
        class RiskPosition(Storable):
            notional: float = 0.0
            daily_vol: float = 0.0
            z_score: float = 1.645

        rp = RiskPosition(notional=100_000, daily_vol=0.02, z_score=1.645)
        nid = graph.track(rp)

        var_expr = Field("notional") * Field("daily_vol") * Field("z_score")
        graph.computed(nid, "var_1d", var_expr)

        var_before = graph.get(nid, "var_1d")
        graph.update(nid, "daily_vol", 0.05)  # vol spike
        var_after = graph.get(nid, "var_1d")

        assert var_after > var_before
        assert abs(var_after / var_before - 2.5) < 0.01

    def test_stop_loss_alert(self):
        """If unrealized loss > threshold, flag for stop-loss."""
        graph = ReactiveGraph()
        pos = Position(symbol="TSLA", quantity=100, avg_cost=250.0, current_price=250.0)
        nid = graph.track(pos)

        pnl = (Field("current_price") - Field("avg_cost")) * Field("quantity")
        stop_loss_threshold = Const(-5000)

        alert = If(pnl < stop_loss_threshold, Const("STOP_LOSS"), Const("OK"))
        graph.computed(nid, "stop_loss_status", alert)

        assert graph.get(nid, "stop_loss_status") == "OK"

        graph.update(nid, "current_price", 190.0)  # -$6000 loss
        assert graph.get(nid, "stop_loss_status") == "STOP_LOSS"

    def test_position_limit_breach(self):
        """Alert if notional exceeds limit."""
        graph = ReactiveGraph()
        pos = Position(symbol="AMZN", quantity=200, avg_cost=180.0, current_price=185.0)
        nid = graph.track(pos)

        notional = Field("quantity") * Field("current_price")
        limit = Const(50000)
        breach = If(notional > limit, Const("BREACH"), Const("OK"))
        graph.computed(nid, "limit_status", breach)

        assert graph.get(nid, "limit_status") == "OK"

        graph.update(nid, "current_price", 260.0)  # 200 * 260 = 52000
        assert graph.get(nid, "limit_status") == "BREACH"


class TestSignalStrength:
    """Signal-based computations using existing Signal model."""

    def test_weighted_signal(self):
        """Signal score = strength * direction_multiplier"""
        graph = ReactiveGraph()
        sig = Signal(symbol="AAPL", direction="LONG", strength=0.85, model_name="momentum")
        nid = graph.track(sig)

        # direction as multiplier: LONG=1, SHORT=-1 via If
        dir_mult = If(
            Field("direction").contains(Const("LONG")),
            Const(1),
            Const(-1),
        )
        score = Field("strength") * dir_mult
        graph.computed(nid, "score", score)

        assert abs(graph.get(nid, "score") - 0.85) < 0.001

    def test_short_signal_score(self):
        graph = ReactiveGraph()
        sig = Signal(symbol="TSLA", direction="SHORT", strength=0.70, model_name="mean_rev")
        nid = graph.track(sig)

        dir_mult = If(
            Field("direction").contains(Const("LONG")),
            Const(1),
            Const(-1),
        )
        score = Field("strength") * dir_mult
        graph.computed(nid, "score", score)

        assert abs(graph.get(nid, "score") - (-0.70)) < 0.001

    def test_signal_confidence_label(self):
        """High/Medium/Low confidence based on strength."""
        graph = ReactiveGraph()
        sig = Signal(symbol="GOOG", direction="LONG", strength=0.45, model_name="stat_arb")
        nid = graph.track(sig)

        label = If(
            Field("strength") > Const(0.75),
            Const("HIGH"),
            If(
                Field("strength") > Const(0.5),
                Const("MEDIUM"),
                Const("LOW"),
            ),
        )
        graph.computed(nid, "confidence", label)

        assert graph.get(nid, "confidence") == "LOW"

        graph.update(nid, "strength", 0.6)
        assert graph.get(nid, "confidence") == "MEDIUM"

        graph.update(nid, "strength", 0.9)
        assert graph.get(nid, "confidence") == "HIGH"

    def test_signal_model_label(self):
        """Use string ops on model name."""
        graph = ReactiveGraph()
        sig = Signal(model_name="momentum_v2", symbol="AAPL", direction="LONG", strength=0.5)
        nid = graph.track(sig)

        graph.computed(nid, "model_upper", Field("model_name").upper())
        graph.computed(nid, "is_momentum", Field("model_name").starts_with(Const("momentum")))

        assert graph.get(nid, "model_upper") == "MOMENTUM_V2"
        assert graph.get(nid, "is_momentum") is True


# ===========================================================================
# Bond / Fixed Income tests
# ===========================================================================

class TestBondPricing:
    """Simplified bond computations."""

    def test_current_yield(self):
        """Current yield = coupon_rate * face_value / price"""
        graph = ReactiveGraph()
        bond = Bond(isin="US912828ZT09", face_value=1000, coupon_rate=0.05,
                     price=980.0, yield_to_maturity=0.052, years_to_maturity=10)
        nid = graph.track(bond)

        current_yield = (Field("coupon_rate") * Field("face_value")) / Field("price")
        graph.computed(nid, "current_yield", current_yield)

        expected = (0.05 * 1000) / 980.0
        assert abs(graph.get(nid, "current_yield") - expected) < 0.0001

    def test_annual_coupon(self):
        graph = ReactiveGraph()
        bond = Bond(face_value=1000, coupon_rate=0.05)
        nid = graph.track(bond)

        coupon = Field("face_value") * Field("coupon_rate")
        graph.computed(nid, "annual_coupon", coupon)

        assert graph.get(nid, "annual_coupon") == 50.0

    def test_duration_proxy(self):
        """Simple Macaulay duration proxy = years_to_maturity * (1 - coupon_rate)"""
        graph = ReactiveGraph()
        bond = Bond(face_value=1000, coupon_rate=0.06, years_to_maturity=5)
        nid = graph.track(bond)

        dur = Field("years_to_maturity") * (Const(1) - Field("coupon_rate"))
        graph.computed(nid, "duration_proxy", dur)

        expected = 5 * (1 - 0.06)
        assert abs(graph.get(nid, "duration_proxy") - expected) < 0.001

    def test_price_sensitivity(self):
        """Approx price change = -duration * Δyield * price"""
        graph = ReactiveGraph()
        bond = Bond(face_value=1000, coupon_rate=0.05, price=980.0,
                     yield_to_maturity=0.052, years_to_maturity=7)
        nid = graph.track(bond)

        # duration proxy
        dur = Field("years_to_maturity") * (Const(1) - Field("coupon_rate"))
        graph.computed(nid, "duration", dur)

        # price change for a 10bp yield increase
        delta_yield = Const(0.001)  # 10bps
        price_chg = -Field("price") * dur * delta_yield
        graph.computed(nid, "price_impact_10bp", price_chg)

        expected_dur = 7 * (1 - 0.05)
        expected_chg = -980.0 * expected_dur * 0.001
        assert abs(graph.get(nid, "price_impact_10bp") - expected_chg) < 0.01

    def test_bond_reacts_to_yield_change(self):
        graph = ReactiveGraph()
        bond = Bond(face_value=1000, coupon_rate=0.04, price=1020.0,
                     yield_to_maturity=0.038, years_to_maturity=5)
        nid = graph.track(bond)

        current_yield = (Field("coupon_rate") * Field("face_value")) / Field("price")
        graph.computed(nid, "current_yield", current_yield)

        cy_before = graph.get(nid, "current_yield")
        graph.update(nid, "price", 950.0)  # price drops
        cy_after = graph.get(nid, "current_yield")

        assert cy_after > cy_before  # yield rises when price drops

    def test_premium_discount_label(self):
        """Label bond as Premium / Par / Discount."""
        graph = ReactiveGraph()
        bond = Bond(face_value=1000, price=1020.0)
        nid = graph.track(bond)

        label = If(
            Field("price") > Field("face_value"),
            Const("PREMIUM"),
            If(
                Field("price") < Field("face_value"),
                Const("DISCOUNT"),
                Const("PAR"),
            ),
        )
        graph.computed(nid, "pricing_label", label)

        assert graph.get(nid, "pricing_label") == "PREMIUM"

        graph.update(nid, "price", 980.0)
        assert graph.get(nid, "pricing_label") == "DISCOUNT"

        graph.update(nid, "price", 1000.0)
        assert graph.get(nid, "pricing_label") == "PAR"


# ===========================================================================
# Effect / alert tests (trading-specific)
# ===========================================================================

class TestTradingEffects:
    """Test that effects fire correctly in trading scenarios."""

    def test_pnl_alert_effect(self):
        graph = ReactiveGraph()
        pos = Position(symbol="AAPL", quantity=100, avg_cost=220.0, current_price=220.0)
        nid = graph.track(pos)

        pnl = (Field("current_price") - Field("avg_cost")) * Field("quantity")
        graph.computed(nid, "pnl", pnl)

        alerts = []
        graph.effect(nid, "pnl", lambda name, val: alerts.append(val))
        initial = len(alerts)

        graph.update(nid, "current_price", 230.0)
        assert len(alerts) > initial
        assert alerts[-1] == 1000.0

    def test_multi_field_batch_trade(self):
        """Batch update bid+ask, effect fires once."""
        graph = ReactiveGraph()
        md = MarketData(symbol="AAPL", bid=100.0, ask=101.0)
        nid = graph.track(md)

        mid = (Field("bid") + Field("ask")) / Const(2)
        graph.computed(nid, "mid", mid)

        ticks = []
        graph.effect(nid, "mid", lambda name, val: ticks.append(val))
        initial = len(ticks)

        graph.batch_update(nid, {"bid": 105.0, "ask": 106.0})
        batch_fires = len(ticks) - initial
        assert batch_fires <= 1
        assert ticks[-1] == 105.5

    def test_multiple_positions_independent(self):
        """Two positions react independently."""
        graph = ReactiveGraph()
        aapl = Position(symbol="AAPL", quantity=100, avg_cost=220.0, current_price=220.0)
        tsla = Position(symbol="TSLA", quantity=50, avg_cost=250.0, current_price=250.0)

        nid_a = graph.track(aapl)
        nid_t = graph.track(tsla)

        pnl_expr = (Field("current_price") - Field("avg_cost")) * Field("quantity")
        graph.computed(nid_a, "pnl", pnl_expr)
        graph.computed(nid_t, "pnl", pnl_expr)

        graph.update(nid_a, "current_price", 230.0)

        assert graph.get(nid_a, "pnl") == 1000.0
        assert graph.get(nid_t, "pnl") == 0.0  # TSLA unchanged


# ===========================================================================
# Serialization roundtrip tests (finance expressions)
# ===========================================================================

class TestFinanceSerialization:
    """Ensure finance expressions survive JSON roundtrip."""

    def test_pnl_expr_roundtrip(self):
        expr = (Field("current_price") - Field("avg_cost")) * Field("quantity")
        restored = from_json(expr.to_json())
        ctx = {"current_price": 230.0, "avg_cost": 220.0, "quantity": 100}
        assert restored.eval(ctx) == 1000.0

    def test_stop_loss_roundtrip(self):
        pnl = (Field("current_price") - Field("avg_cost")) * Field("quantity")
        expr = If(pnl < Const(-5000), Const("STOP_LOSS"), Const("OK"))
        restored = from_json(expr.to_json())
        assert restored.eval({"current_price": 190.0, "avg_cost": 250.0, "quantity": 100}) == "STOP_LOSS"
        assert restored.eval({"current_price": 248.0, "avg_cost": 250.0, "quantity": 100}) == "OK"

    def test_option_intrinsic_roundtrip(self):
        expr = If(
            Field("underlying_price") > Field("strike"),
            Field("underlying_price") - Field("strike"),
            Const(0),
        )
        restored = from_json(expr.to_json())
        assert restored.eval({"underlying_price": 110.0, "strike": 100.0}) == 10.0
        assert restored.eval({"underlying_price": 90.0, "strike": 100.0}) == 0

    def test_confidence_label_roundtrip(self):
        expr = If(
            Field("strength") > Const(0.75),
            Const("HIGH"),
            If(Field("strength") > Const(0.5), Const("MEDIUM"), Const("LOW")),
        )
        restored = from_json(expr.to_json())
        assert restored.eval({"strength": 0.9}) == "HIGH"
        assert restored.eval({"strength": 0.6}) == "MEDIUM"
        assert restored.eval({"strength": 0.3}) == "LOW"

    def test_bond_yield_roundtrip(self):
        expr = (Field("coupon_rate") * Field("face_value")) / Field("price")
        restored = from_json(expr.to_json())
        ctx = {"coupon_rate": 0.05, "face_value": 1000, "price": 980.0}
        expected = (0.05 * 1000) / 980.0
        assert abs(restored.eval(ctx) - expected) < 0.0001
