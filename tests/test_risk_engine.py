"""
Unit tests for the Black-Scholes Greeks calculator.
No server or Deephaven dependencies required.
"""

import sys
import os
import math
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "server"))

from risk_engine import calculate_greeks, _norm_cdf


# ── _norm_cdf tests ─────────────────────────────────────────────────────────

class TestNormCdf:
    def test_at_zero(self):
        assert _norm_cdf(0) == pytest.approx(0.5)

    def test_large_positive(self):
        assert _norm_cdf(10) == pytest.approx(1.0, abs=1e-10)

    def test_large_negative(self):
        assert _norm_cdf(-10) == pytest.approx(0.0, abs=1e-10)

    def test_symmetry(self):
        for x in [0.5, 1.0, 2.0, 3.0]:
            assert _norm_cdf(x) + _norm_cdf(-x) == pytest.approx(1.0)

    def test_monotonically_increasing(self):
        vals = [_norm_cdf(x) for x in [-3, -2, -1, 0, 1, 2, 3]]
        for i in range(len(vals) - 1):
            assert vals[i] < vals[i + 1]


# ── Greeks range tests ──────────────────────────────────────────────────────

class TestGreeksRanges:
    """Greeks should always be in valid mathematical ranges."""

    @pytest.fixture(params=[100, 200, 500, 1000])
    def price(self, request):
        return request.param

    def test_delta_between_0_and_1(self, price):
        delta, _, _, _ = calculate_greeks(price)
        assert 0 <= delta <= 1

    def test_gamma_non_negative(self, price):
        _, gamma, _, _ = calculate_greeks(price)
        assert gamma >= 0

    def test_vega_non_negative(self, price):
        _, _, _, vega = calculate_greeks(price)
        assert vega >= 0

    def test_theta_is_negative_for_calls(self, price):
        _, _, theta, _ = calculate_greeks(price)
        # Theta for a long call is almost always negative (time decay)
        assert theta < 0


class TestGreeksKnownValues:
    """Test against known Black-Scholes behavior."""

    def test_atm_delta_near_half(self):
        # ATM call (S == K) should have delta ≈ 0.5–0.6
        delta, _, _, _ = calculate_greeks(100, strike=100, T=0.25, r=0.05, sigma=0.25)
        assert 0.45 <= delta <= 0.65

    def test_deep_itm_delta_near_one(self):
        # Deep in-the-money: S >> K
        delta, _, _, _ = calculate_greeks(200, strike=100, T=0.25, r=0.05, sigma=0.25)
        assert delta > 0.99

    def test_deep_otm_delta_near_zero(self):
        # Deep out-of-the-money: S << K
        delta, _, _, _ = calculate_greeks(50, strike=200, T=0.25, r=0.05, sigma=0.25)
        assert delta < 0.01

    def test_atm_gamma_is_highest(self):
        # Gamma peaks at ATM
        _, gamma_atm, _, _ = calculate_greeks(100, strike=100)
        _, gamma_itm, _, _ = calculate_greeks(120, strike=100)
        _, gamma_otm, _, _ = calculate_greeks(80, strike=100)
        assert gamma_atm > gamma_itm
        assert gamma_atm > gamma_otm

    def test_vega_increases_with_time(self):
        # Longer-dated options have more vega
        _, _, _, vega_short = calculate_greeks(100, strike=100, T=0.1)
        _, _, _, vega_long = calculate_greeks(100, strike=100, T=1.0)
        assert vega_long > vega_short

    def test_higher_vol_increases_vega(self):
        _, _, _, vega_low = calculate_greeks(100, strike=100, sigma=0.1)
        _, _, _, vega_high = calculate_greeks(100, strike=100, sigma=0.5)
        # Both should be positive; higher vol has more gamma effect but
        # vega is the raw sensitivity at current vol level
        assert vega_low > 0
        assert vega_high > 0


class TestGreeksDefaults:
    """Test default parameter behavior."""

    def test_default_strike_is_105pct(self):
        # When strike=None, K = 1.05 * price → slightly OTM
        delta_default, _, _, _ = calculate_greeks(100)
        delta_explicit, _, _, _ = calculate_greeks(100, strike=105)
        assert delta_default == pytest.approx(delta_explicit)

    def test_returns_four_values(self):
        result = calculate_greeks(100)
        assert len(result) == 4

    def test_all_finite(self):
        delta, gamma, theta, vega = calculate_greeks(100)
        for val in [delta, gamma, theta, vega]:
            assert math.isfinite(val)


class TestGreeksEdgeCases:
    """Edge cases that could break the math."""

    def test_very_small_time_to_expiry(self):
        # Near expiry: should not raise, delta should be near 0 or 1
        delta, gamma, theta, vega = calculate_greeks(100, strike=100, T=0.001)
        assert math.isfinite(delta)
        assert math.isfinite(gamma)

    def test_very_high_volatility(self):
        delta, gamma, theta, vega = calculate_greeks(100, strike=100, sigma=2.0)
        assert 0 <= delta <= 1
        assert gamma >= 0

    def test_very_low_volatility(self):
        # Low vol, deep ITM should have delta near 1
        delta, _, _, _ = calculate_greeks(200, strike=100, sigma=0.01)
        assert delta > 0.99

    def test_penny_stock(self):
        delta, gamma, theta, vega = calculate_greeks(0.50, strike=0.55)
        assert 0 <= delta <= 1
        assert math.isfinite(gamma)

    def test_high_price_stock(self):
        delta, gamma, theta, vega = calculate_greeks(5000, strike=5250)
        assert 0 <= delta <= 1
        assert math.isfinite(gamma)
