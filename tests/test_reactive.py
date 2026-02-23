"""
Tests for the reactive computation layer.

Uses generic @dataclass test classes (not trading-specific) to prove
the framework is domain-agnostic.
"""

import json
import math
import pytest
from dataclasses import dataclass

from store.base import Storable
from reactive.expr import Const, Field, BinOp, UnaryOp, Func, If, Coalesce, IsNull, StrOp, from_json
from reactive.graph import ReactiveGraph


# ---------------------------------------------------------------------------
# Test domain classes — intentionally NOT trading-related
# ---------------------------------------------------------------------------

@dataclass
class Sensor(Storable):
    """A generic sensor reading."""
    name: str = ""
    value: float = 0.0
    threshold: float = 100.0
    unit: str = "celsius"


@dataclass
class Rectangle(Storable):
    """A shape with dimensions."""
    width: float = 0.0
    height: float = 0.0
    label: str = ""


# ===========================================================================
# Expression tests
# ===========================================================================

class TestConst:
    def test_eval_number(self):
        assert Const(42).eval({}) == 42

    def test_eval_string(self):
        assert Const("hello").eval({}) == "hello"

    def test_eval_bool(self):
        assert Const(True).eval({}) is True

    def test_eval_none(self):
        assert Const(None).eval({}) is None

    def test_to_sql_number(self):
        assert Const(42).to_sql() == "42"

    def test_to_sql_string(self):
        assert Const("hello").to_sql() == "'hello'"

    def test_to_sql_bool(self):
        assert Const(True).to_sql() == "TRUE"
        assert Const(False).to_sql() == "FALSE"

    def test_to_sql_none(self):
        assert Const(None).to_sql() == "NULL"

    def test_to_pure_number(self):
        assert Const(42).to_pure() == "42"

    def test_to_pure_string(self):
        assert Const("hello").to_pure() == "'hello'"

    def test_to_pure_bool(self):
        assert Const(True).to_pure() == "true"
        assert Const(False).to_pure() == "false"


class TestField:
    def test_eval(self):
        assert Field("x").eval({"x": 10}) == 10

    def test_eval_string(self):
        assert Field("name").eval({"name": "alice"}) == "alice"

    def test_to_sql(self):
        assert Field("price").to_sql("data") == "(data->>'price')"

    def test_to_pure(self):
        assert Field("price").to_pure("$row") == "$row.price"


class TestBinOp:
    def test_add(self):
        expr = Field("a") + Field("b")
        assert expr.eval({"a": 3, "b": 7}) == 10

    def test_sub(self):
        expr = Field("a") - Field("b")
        assert expr.eval({"a": 10, "b": 3}) == 7

    def test_mul(self):
        expr = Field("a") * Field("b")
        assert expr.eval({"a": 4, "b": 5}) == 20

    def test_div(self):
        expr = Field("a") / Field("b")
        assert expr.eval({"a": 10, "b": 4}) == 2.5

    def test_mod(self):
        expr = Field("a") % Const(3)
        assert expr.eval({"a": 10}) == 1

    def test_pow(self):
        expr = Field("a") ** Const(2)
        assert expr.eval({"a": 5}) == 25

    def test_gt(self):
        expr = Field("a") > Const(5)
        assert expr.eval({"a": 10}) is True
        assert expr.eval({"a": 3}) is False

    def test_lt(self):
        expr = Field("a") < Const(5)
        assert expr.eval({"a": 3}) is True

    def test_ge(self):
        expr = Field("a") >= Const(5)
        assert expr.eval({"a": 5}) is True

    def test_le(self):
        expr = Field("a") <= Const(5)
        assert expr.eval({"a": 5}) is True

    def test_eq(self):
        expr = Field("a") == Const(5)
        assert expr.eval({"a": 5}) is True
        assert expr.eval({"a": 6}) is False

    def test_ne(self):
        expr = Field("a") != Const(5)
        assert expr.eval({"a": 6}) is True

    def test_and(self):
        expr = (Field("a") > Const(0)) & (Field("b") > Const(0))
        assert expr.eval({"a": 1, "b": 1}) is True
        assert expr.eval({"a": 1, "b": -1}) is False

    def test_or(self):
        expr = (Field("a") > Const(0)) | (Field("b") > Const(0))
        assert expr.eval({"a": -1, "b": 1}) is True
        assert expr.eval({"a": -1, "b": -1}) is False

    def test_nested_arithmetic(self):
        # (price - entry) * quantity
        expr = (Field("price") - Field("entry")) * Field("qty")
        assert expr.eval({"price": 230, "entry": 228, "qty": 100}) == 200

    def test_radd(self):
        expr = 10 + Field("a")
        assert expr.eval({"a": 5}) == 15

    def test_rmul(self):
        expr = 2 * Field("a")
        assert expr.eval({"a": 7}) == 14

    def test_to_sql_arithmetic(self):
        expr = Field("price") * Field("qty")
        sql = expr.to_sql("data")
        assert "(data->>'price')::float" in sql
        assert "(data->>'qty')::float" in sql
        assert "*" in sql

    def test_to_sql_comparison(self):
        expr = Field("age") > Const(18)
        sql = expr.to_sql("data")
        assert "(data->>'age')::float" in sql
        assert ">" in sql
        assert "18" in sql

    def test_to_sql_equality(self):
        expr = Field("status") == Const("active")
        sql = expr.to_sql("data")
        assert "(data->>'status')" in sql
        assert "=" in sql
        assert "'active'" in sql

    def test_to_sql_logical(self):
        expr = (Field("a") > Const(0)) & (Field("b") > Const(0))
        sql = expr.to_sql("data")
        assert "AND" in sql

    def test_to_pure_arithmetic(self):
        expr = (Field("price") - Field("entry")) * Field("qty")
        pure = expr.to_pure("$row")
        assert "$row.price" in pure
        assert "$row.entry" in pure
        assert "$row.qty" in pure

    def test_to_pure_logical(self):
        expr = (Field("a") > Const(0)) | (Field("b") > Const(0))
        pure = expr.to_pure("$row")
        assert "||" in pure


class TestUnaryOp:
    def test_neg(self):
        expr = -Field("x")
        assert expr.eval({"x": 5}) == -5

    def test_abs(self):
        expr = abs(Field("x"))
        assert expr.eval({"x": -7}) == 7

    def test_not(self):
        expr = ~(Field("x") > Const(0))
        assert expr.eval({"x": -1}) is True
        assert expr.eval({"x": 1}) is False

    def test_neg_to_sql(self):
        expr = -Field("x")
        assert "(-" in expr.to_sql("data")

    def test_abs_to_sql(self):
        expr = abs(Field("x"))
        assert "ABS(" in expr.to_sql("data")

    def test_not_to_sql(self):
        expr = ~(Field("x") > Const(0))
        assert "NOT" in expr.to_sql("data")

    def test_neg_to_pure(self):
        expr = -Field("x")
        assert "(-" in expr.to_pure("$row")

    def test_not_to_pure(self):
        expr = ~(Field("x") > Const(0))
        assert "!(" in expr.to_pure("$row")


class TestFunc:
    def test_sqrt(self):
        expr = Func("sqrt", [Field("x")])
        assert expr.eval({"x": 16}) == 4.0

    def test_ceil(self):
        expr = Func("ceil", [Field("x")])
        assert expr.eval({"x": 3.2}) == 4

    def test_floor(self):
        expr = Func("floor", [Field("x")])
        assert expr.eval({"x": 3.8}) == 3

    def test_round(self):
        expr = Func("round", [Field("x")])
        assert expr.eval({"x": 3.6}) == 4

    def test_min(self):
        expr = Func("min", [Field("a"), Field("b")])
        assert expr.eval({"a": 3, "b": 7}) == 3

    def test_max(self):
        expr = Func("max", [Field("a"), Field("b")])
        assert expr.eval({"a": 3, "b": 7}) == 7

    def test_log(self):
        expr = Func("log", [Const(math.e)])
        assert abs(expr.eval({}) - 1.0) < 1e-10

    def test_exp(self):
        expr = Func("exp", [Const(0)])
        assert expr.eval({}) == 1.0

    def test_sqrt_to_sql(self):
        expr = Func("sqrt", [Field("x")])
        assert "SQRT(" in expr.to_sql("data")
        assert "(data->>'x')::float" in expr.to_sql("data")

    def test_min_to_sql(self):
        expr = Func("min", [Field("a"), Field("b")])
        assert "LEAST(" in expr.to_sql("data")

    def test_max_to_sql(self):
        expr = Func("max", [Field("a"), Field("b")])
        assert "GREATEST(" in expr.to_sql("data")

    def test_sqrt_to_pure(self):
        expr = Func("sqrt", [Field("x")])
        assert expr.to_pure("$row") == "sqrt($row.x)"

    def test_ceil_to_pure(self):
        expr = Func("ceil", [Field("x")])
        assert "ceiling(" in expr.to_pure("$row")


class TestIf:
    def test_eval_true_branch(self):
        expr = If(Field("x") > Const(0), Field("x"), Const(0))
        assert expr.eval({"x": 5}) == 5

    def test_eval_false_branch(self):
        expr = If(Field("x") > Const(0), Field("x"), Const(0))
        assert expr.eval({"x": -3}) == 0

    def test_nested_if(self):
        expr = If(
            Field("age") < Const(18),
            Const("Minor"),
            If(Field("age") < Const(65), Const("Adult"), Const("Senior")),
        )
        assert expr.eval({"age": 10}) == "Minor"
        assert expr.eval({"age": 30}) == "Adult"
        assert expr.eval({"age": 70}) == "Senior"

    def test_to_sql(self):
        expr = If(Field("x") > Const(0), Field("x"), Const(0))
        sql = expr.to_sql("data")
        assert "CASE WHEN" in sql
        assert "THEN" in sql
        assert "ELSE" in sql
        assert "END" in sql

    def test_to_pure(self):
        expr = If(Field("x") > Const(0), Field("x"), Const(0))
        pure = expr.to_pure("$row")
        assert "if(" in pure
        assert "|" in pure


class TestCoalesce:
    def test_first_non_none(self):
        expr = Coalesce([Field("a"), Field("b"), Const(0)])
        assert expr.eval({"a": None, "b": 5}) == 5

    def test_all_none(self):
        expr = Coalesce([Field("a"), Field("b")])
        assert expr.eval({"a": None, "b": None}) is None

    def test_first_wins(self):
        expr = Coalesce([Field("a"), Field("b")])
        assert expr.eval({"a": 1, "b": 2}) == 1

    def test_to_sql(self):
        expr = Coalesce([Field("a"), Const(0)])
        sql = expr.to_sql("data")
        assert "COALESCE(" in sql

    def test_to_pure(self):
        expr = Coalesce([Field("a"), Const(0)])
        pure = expr.to_pure("$row")
        assert "isEmpty" in pure


class TestIsNull:
    def test_null(self):
        expr = IsNull(Field("x"))
        assert expr.eval({"x": None}) is True

    def test_not_null(self):
        expr = IsNull(Field("x"))
        assert expr.eval({"x": 5}) is False

    def test_to_sql(self):
        assert "IS NULL" in IsNull(Field("x")).to_sql("data")

    def test_to_pure(self):
        assert "isEmpty(" in IsNull(Field("x")).to_pure("$row")

    def test_is_null_method(self):
        expr = Field("x").is_null()
        assert expr.eval({"x": None}) is True


class TestStrOp:
    def test_length(self):
        expr = Field("name").length()
        assert expr.eval({"name": "alice"}) == 5

    def test_upper(self):
        expr = Field("name").upper()
        assert expr.eval({"name": "alice"}) == "ALICE"

    def test_lower(self):
        expr = Field("name").lower()
        assert expr.eval({"name": "ALICE"}) == "alice"

    def test_contains(self):
        expr = Field("name").contains(Const("li"))
        assert expr.eval({"name": "alice"}) is True
        assert expr.eval({"name": "bob"}) is False

    def test_starts_with(self):
        expr = Field("name").starts_with(Const("al"))
        assert expr.eval({"name": "alice"}) is True
        assert expr.eval({"name": "bob"}) is False

    def test_concat(self):
        expr = Field("first").concat(Const(" ")).concat(Field("last"))
        assert expr.eval({"first": "Jane", "last": "Doe"}) == "Jane Doe"

    def test_length_to_sql(self):
        assert "LENGTH(" in Field("name").length().to_sql("data")

    def test_upper_to_sql(self):
        assert "UPPER(" in Field("name").upper().to_sql("data")

    def test_lower_to_sql(self):
        assert "LOWER(" in Field("name").lower().to_sql("data")

    def test_contains_to_sql(self):
        sql = Field("name").contains(Const("li")).to_sql("data")
        assert "LIKE" in sql

    def test_starts_with_to_sql(self):
        sql = Field("name").starts_with(Const("al")).to_sql("data")
        assert "LIKE" in sql

    def test_concat_to_sql(self):
        sql = Field("a").concat(Field("b")).to_sql("data")
        assert "||" in sql

    def test_upper_to_pure(self):
        assert "toUpper(" in Field("name").upper().to_pure("$row")

    def test_lower_to_pure(self):
        assert "toLower(" in Field("name").lower().to_pure("$row")

    def test_contains_to_pure(self):
        pure = Field("name").contains(Const("li")).to_pure("$row")
        assert "contains(" in pure

    def test_starts_with_to_pure(self):
        pure = Field("name").starts_with(Const("al")).to_pure("$row")
        assert "startsWith(" in pure

    def test_concat_to_pure(self):
        pure = Field("a").concat(Field("b")).to_pure("$row")
        assert "+" in pure


class TestSerialization:
    def test_const_roundtrip(self):
        expr = Const(42)
        restored = from_json(expr.to_json())
        assert restored.eval({}) == 42

    def test_field_roundtrip(self):
        expr = Field("price")
        restored = from_json(expr.to_json())
        assert restored.eval({"price": 100}) == 100

    def test_binop_roundtrip(self):
        expr = Field("a") + Field("b")
        restored = from_json(expr.to_json())
        assert restored.eval({"a": 3, "b": 7}) == 10

    def test_nested_roundtrip(self):
        expr = (Field("price") - Field("entry")) * Field("qty")
        data = expr.to_json()
        restored = from_json(data)
        assert restored.eval({"price": 230, "entry": 228, "qty": 100}) == 200

    def test_if_roundtrip(self):
        expr = If(Field("x") > Const(0), Field("x"), Const(0))
        restored = from_json(expr.to_json())
        assert restored.eval({"x": 5}) == 5
        assert restored.eval({"x": -3}) == 0

    def test_func_roundtrip(self):
        expr = Func("sqrt", [Field("x")])
        restored = from_json(expr.to_json())
        assert restored.eval({"x": 16}) == 4.0

    def test_coalesce_roundtrip(self):
        expr = Coalesce([Field("a"), Const(0)])
        restored = from_json(expr.to_json())
        assert restored.eval({"a": None}) == 0

    def test_is_null_roundtrip(self):
        expr = IsNull(Field("x"))
        restored = from_json(expr.to_json())
        assert restored.eval({"x": None}) is True

    def test_strop_roundtrip(self):
        expr = Field("name").upper()
        restored = from_json(expr.to_json())
        assert restored.eval({"name": "alice"}) == "ALICE"

    def test_strop_with_arg_roundtrip(self):
        expr = Field("name").contains(Const("li"))
        restored = from_json(expr.to_json())
        assert restored.eval({"name": "alice"}) is True

    def test_json_string_roundtrip(self):
        expr = (Field("a") + Const(1)) * Field("b")
        json_str = json.dumps(expr.to_json())
        restored = from_json(json.loads(json_str))
        assert restored.eval({"a": 2, "b": 3}) == 9


# ===========================================================================
# ReactiveGraph tests
# ===========================================================================

class TestReactiveGraph:
    def test_track_creates_signals(self):
        graph = ReactiveGraph()
        sensor = Sensor(name="temp", value=25.0, threshold=100.0, unit="celsius")
        node_id = graph.track(sensor)
        assert graph.get_field(node_id, "value") == 25.0
        assert graph.get_field(node_id, "name") == "temp"

    def test_track_rejects_non_dataclass(self):
        graph = ReactiveGraph()
        with pytest.raises(TypeError):
            graph.track("not a dataclass")

    def test_computed_returns_correct_value(self):
        graph = ReactiveGraph()
        rect = Rectangle(width=10.0, height=5.0, label="test")
        node_id = graph.track(rect)
        graph.computed(node_id, "area", Field("width") * Field("height"))
        assert graph.get(node_id, "area") == 50.0

    def test_update_triggers_recomputation(self):
        graph = ReactiveGraph()
        rect = Rectangle(width=10.0, height=5.0)
        node_id = graph.track(rect)
        graph.computed(node_id, "area", Field("width") * Field("height"))
        assert graph.get(node_id, "area") == 50.0

        graph.update(node_id, "width", 20.0)
        assert graph.get(node_id, "area") == 100.0

    def test_update_syncs_underlying_object(self):
        graph = ReactiveGraph()
        sensor = Sensor(name="temp", value=25.0)
        node_id = graph.track(sensor)
        graph.update(node_id, "value", 30.0)
        assert sensor.value == 30.0

    def test_effect_fires_on_change(self):
        graph = ReactiveGraph()
        sensor = Sensor(name="temp", value=25.0, threshold=50.0)
        node_id = graph.track(sensor)
        graph.computed(node_id, "above_threshold", Field("value") > Field("threshold"))

        fired = []
        graph.effect(node_id, "above_threshold", lambda name, val: fired.append((name, val)))

        # Initial effect fires on creation
        initial_count = len(fired)

        graph.update(node_id, "value", 60.0)
        assert len(fired) > initial_count
        assert fired[-1] == ("above_threshold", True)

    def test_effect_requires_computed(self):
        graph = ReactiveGraph()
        sensor = Sensor(name="temp", value=25.0)
        node_id = graph.track(sensor)
        with pytest.raises(KeyError):
            graph.effect(node_id, "nonexistent", lambda n, v: None)

    def test_batch_update_atomic(self):
        graph = ReactiveGraph()
        rect = Rectangle(width=10.0, height=5.0)
        node_id = graph.track(rect)
        graph.computed(node_id, "area", Field("width") * Field("height"))

        fired = []
        graph.effect(node_id, "area", lambda name, val: fired.append(val))
        initial_count = len(fired)

        graph.batch_update(node_id, {"width": 20.0, "height": 10.0})

        # After batch, area should be 200 (not an intermediate value)
        assert graph.get(node_id, "area") == 200.0
        # Effect should have fired at most once for the batch (not twice)
        batch_fires = len(fired) - initial_count
        assert batch_fires <= 1

    def test_multiple_computeds_on_same_field(self):
        graph = ReactiveGraph()
        sensor = Sensor(name="temp", value=25.0, threshold=50.0)
        node_id = graph.track(sensor)

        graph.computed(node_id, "doubled", Field("value") * Const(2))
        graph.computed(node_id, "above", Field("value") > Field("threshold"))

        assert graph.get(node_id, "doubled") == 50.0
        assert graph.get(node_id, "above") is False

        graph.update(node_id, "value", 60.0)
        assert graph.get(node_id, "doubled") == 120.0
        assert graph.get(node_id, "above") is True

    def test_works_with_different_storable_types(self):
        graph = ReactiveGraph()

        sensor = Sensor(name="temp", value=25.0)
        rect = Rectangle(width=10.0, height=5.0)

        sid = graph.track(sensor)
        rid = graph.track(rect)

        graph.computed(sid, "doubled", Field("value") * Const(2))
        graph.computed(rid, "area", Field("width") * Field("height"))

        assert graph.get(sid, "doubled") == 50.0
        assert graph.get(rid, "area") == 50.0

        graph.update(sid, "value", 100.0)
        graph.update(rid, "width", 20.0)

        assert graph.get(sid, "doubled") == 200.0
        assert graph.get(rid, "area") == 100.0

    def test_remove_effect(self):
        graph = ReactiveGraph()
        sensor = Sensor(name="temp", value=25.0)
        node_id = graph.track(sensor)
        graph.computed(node_id, "doubled", Field("value") * Const(2))

        fired = []
        graph.effect(node_id, "doubled", lambda name, val: fired.append(val))
        initial_count = len(fired)

        graph.remove_effect(node_id, "doubled")

        graph.update(node_id, "value", 50.0)
        # After removal, no new fires
        assert len(fired) == initial_count

    def test_untrack_cleanup(self):
        graph = ReactiveGraph()
        sensor = Sensor(name="temp", value=25.0)
        node_id = graph.track(sensor)
        graph.computed(node_id, "doubled", Field("value") * Const(2))

        graph.untrack(node_id)

        with pytest.raises(KeyError):
            graph.get(node_id, "doubled")

    def test_computed_with_if_expression(self):
        graph = ReactiveGraph()
        sensor = Sensor(name="temp", value=25.0, threshold=50.0)
        node_id = graph.track(sensor)

        expr = If(
            Field("value") > Field("threshold"),
            Const("ALERT"),
            Const("OK"),
        )
        graph.computed(node_id, "status", expr)

        assert graph.get(node_id, "status") == "OK"
        graph.update(node_id, "value", 60.0)
        assert graph.get(node_id, "status") == "ALERT"

    def test_computed_with_coalesce(self):
        graph = ReactiveGraph()

        @dataclass
        class Config:
            override: object = None
            default: float = 10.0

        cfg = Config(override=None, default=10.0)
        node_id = graph.track(cfg)
        graph.computed(node_id, "effective", Coalesce([Field("override"), Field("default")]))

        assert graph.get(node_id, "effective") == 10.0
        graph.update(node_id, "override", 99.0)
        assert graph.get(node_id, "effective") == 99.0

    def test_computed_with_string_ops(self):
        graph = ReactiveGraph()
        rect = Rectangle(width=10.0, height=5.0, label="my rect")
        node_id = graph.track(rect)
        graph.computed(node_id, "upper_label", Field("label").upper())
        assert graph.get(node_id, "upper_label") == "MY RECT"

    def test_complex_expression(self):
        """Test a realistic multi-step computation."""
        graph = ReactiveGraph()
        sensor = Sensor(name="temp", value=72.0, threshold=100.0, unit="fahrenheit")
        node_id = graph.track(sensor)

        # Convert fahrenheit to celsius: (value - 32) * 5 / 9
        celsius_expr = (Field("value") - Const(32)) * Const(5) / Const(9)
        graph.computed(node_id, "celsius", celsius_expr)

        result = graph.get(node_id, "celsius")
        expected = (72.0 - 32) * 5 / 9
        assert abs(result - expected) < 0.001

        graph.update(node_id, "value", 212.0)
        result = graph.get(node_id, "celsius")
        expected = (212.0 - 32) * 5 / 9
        assert abs(result - expected) < 0.001


# ===========================================================================
# Cross-entity reactive tests
# ===========================================================================

@dataclass
class Position(Storable):
    symbol: str = ""
    quantity: int = 0
    price: float = 0.0


class TestGroupComputed:
    def test_sum_across_nodes(self):
        graph = ReactiveGraph()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        n1 = graph.track(p1)
        n2 = graph.track(p2)

        mv = Field("price") * Field("quantity")
        graph.computed(n1, "mv", mv)
        graph.computed(n2, "mv", mv)

        graph.group_computed("portfolio_value", [n1, n2], "mv", sum)
        assert graph.get_group("portfolio_value") == 100 * 228.0 + 50 * 192.0

    def test_max_across_nodes(self):
        graph = ReactiveGraph()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        n1 = graph.track(p1)
        n2 = graph.track(p2)

        mv = Field("price") * Field("quantity")
        graph.computed(n1, "mv", mv)
        graph.computed(n2, "mv", mv)

        graph.group_computed("max_mv", [n1, n2], "mv", max)
        assert graph.get_group("max_mv") == 100 * 228.0

    def test_recomputes_on_member_update(self):
        graph = ReactiveGraph()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        n1 = graph.track(p1)
        n2 = graph.track(p2)

        mv = Field("price") * Field("quantity")
        graph.computed(n1, "mv", mv)
        graph.computed(n2, "mv", mv)

        graph.group_computed("total", [n1, n2], "mv", sum)
        before = graph.get_group("total")

        graph.update(n1, "price", 230.0)
        after = graph.get_group("total")

        assert after == 100 * 230.0 + 50 * 192.0
        assert after != before

    def test_single_node_group(self):
        graph = ReactiveGraph()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        n1 = graph.track(p1)
        graph.computed(n1, "mv", Field("price") * Field("quantity"))

        graph.group_computed("solo", [n1], "mv", sum)
        assert graph.get_group("solo") == 100 * 228.0

    def test_empty_group(self):
        graph = ReactiveGraph()
        graph.group_computed("empty", [], "mv", sum)
        assert graph.get_group("empty") == 0

    def test_custom_reduce(self):
        graph = ReactiveGraph()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        n1 = graph.track(p1)
        n2 = graph.track(p2)

        graph.computed(n1, "mv", Field("price") * Field("quantity"))
        graph.computed(n2, "mv", Field("price") * Field("quantity"))

        # Average
        graph.group_computed("avg_mv", [n1, n2], "mv",
                            lambda vals: sum(vals) / len(vals) if vals else 0)
        expected = (100 * 228.0 + 50 * 192.0) / 2
        assert graph.get_group("avg_mv") == expected

    def test_duplicate_group_name_raises(self):
        graph = ReactiveGraph()
        graph.group_computed("x", [], "mv", sum)
        with pytest.raises(KeyError):
            graph.group_computed("x", [], "mv", sum)


class TestMultiComputed:
    def test_spread_between_nodes(self):
        graph = ReactiveGraph()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        n1 = graph.track(p1)
        n2 = graph.track(p2)

        graph.computed(n1, "mv", Field("price") * Field("quantity"))
        graph.computed(n2, "mv", Field("price") * Field("quantity"))

        graph.multi_computed("spread", lambda g: g.get(n1, "mv") - g.get(n2, "mv"))
        assert graph.get_group("spread") == (100 * 228.0) - (50 * 192.0)

    def test_reads_raw_fields(self):
        graph = ReactiveGraph()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        n1 = graph.track(p1)
        n2 = graph.track(p2)

        graph.multi_computed("total_qty",
                            lambda g: g.get_field(n1, "quantity") + g.get_field(n2, "quantity"))
        assert graph.get_group("total_qty") == 150

    def test_recomputes_on_field_update(self):
        graph = ReactiveGraph()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        n1 = graph.track(p1)
        n2 = graph.track(p2)

        graph.multi_computed("total_qty",
                            lambda g: g.get_field(n1, "quantity") + g.get_field(n2, "quantity"))

        graph.update(n1, "quantity", 200)
        assert graph.get_group("total_qty") == 250


class TestDynamicMembership:
    def test_add_to_group(self):
        graph = ReactiveGraph()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        n1 = graph.track(p1)
        n2 = graph.track(p2)

        mv = Field("price") * Field("quantity")
        graph.computed(n1, "mv", mv)
        graph.computed(n2, "mv", mv)

        graph.group_computed("total", [n1], "mv", sum)
        assert graph.get_group("total") == 100 * 228.0

        graph.add_to_group("total", n2)
        assert graph.get_group("total") == 100 * 228.0 + 50 * 192.0

    def test_remove_from_group(self):
        graph = ReactiveGraph()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        n1 = graph.track(p1)
        n2 = graph.track(p2)

        mv = Field("price") * Field("quantity")
        graph.computed(n1, "mv", mv)
        graph.computed(n2, "mv", mv)

        graph.group_computed("total", [n1, n2], "mv", sum)
        graph.remove_from_group("total", n2)
        assert graph.get_group("total") == 100 * 228.0

    def test_add_duplicate_is_noop(self):
        graph = ReactiveGraph()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        n1 = graph.track(p1)
        graph.computed(n1, "mv", Field("price") * Field("quantity"))

        graph.group_computed("total", [n1], "mv", sum)
        graph.add_to_group("total", n1)  # already in group
        assert graph.get_group("total") == 100 * 228.0

    def test_add_to_multi_computed_raises(self):
        graph = ReactiveGraph()
        graph.multi_computed("x", lambda g: 42)
        with pytest.raises(ValueError):
            graph.add_to_group("x", "fake_id")


class TestGroupEffect:
    def test_effect_fires_on_change(self):
        graph = ReactiveGraph()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        n1 = graph.track(p1)
        graph.computed(n1, "mv", Field("price") * Field("quantity"))
        graph.group_computed("total", [n1], "mv", sum)

        fired = []
        graph.group_effect("total", lambda name, val: fired.append((name, val)))

        graph.update(n1, "price", 230.0)
        assert any(v == 100 * 230.0 for _, v in fired)

    def test_effect_on_nonexistent_raises(self):
        graph = ReactiveGraph()
        with pytest.raises(KeyError):
            graph.group_effect("nope", lambda n, v: None)


class TestRemoveGroup:
    def test_remove_group_cleanup(self):
        graph = ReactiveGraph()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        n1 = graph.track(p1)
        graph.computed(n1, "mv", Field("price") * Field("quantity"))
        graph.group_computed("total", [n1], "mv", sum)

        graph.remove_group("total")
        with pytest.raises(KeyError):
            graph.get_group("total")

    def test_remove_nonexistent_is_noop(self):
        graph = ReactiveGraph()
        graph.remove_group("does_not_exist")  # should not raise

    def test_get_group_nonexistent_raises(self):
        graph = ReactiveGraph()
        with pytest.raises(KeyError):
            graph.get_group("nope")
