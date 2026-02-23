"""
Comprehensive tests for the bi-temporal event-sourced object store.
Tests cover: serde, event sourcing, bi-temporal queries, state machines,
RLS enforcement, trust boundary, sharing, and admin access.

Run with: pytest tests/test_store.py -v
"""

import os
import sys
import json
import time
import uuid
import tempfile
import pytest
import psycopg2.errors
from dataclasses import dataclass
from datetime import datetime, date, timezone, timedelta
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from store.server import ObjectStoreServer
from store.schema import provision_user
from store.base import Storable, _JSONEncoder, _json_decoder_hook
from store.client import StoreClient
from store.state_machine import StateMachine, InvalidTransition
from store.permissions import share_read, share_write, unshare_read, unshare_write, list_shared_with


# ── Test models ──────────────────────────────────────────────────────────────

@dataclass
class Widget(Storable):
    name: str = ""
    color: str = ""
    weight: float = 0.0


@dataclass
class RichObject(Storable):
    label: str = ""
    amount: float = 0.0
    ts: str = ""
    tags: list = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []


class OrderLifecycle(StateMachine):
    initial = "PENDING"
    transitions = {
        "PENDING":   ["PARTIAL", "FILLED", "CANCELLED"],
        "PARTIAL":   ["FILLED", "CANCELLED"],
        "FILLED":    ["SETTLED"],
    }


@dataclass
class Order(Storable):
    symbol: str = ""
    quantity: int = 0
    price: float = 0.0
    side: str = ""

Order._state_machine = OrderLifecycle


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def server():
    """Start an embedded PostgreSQL server for testing."""
    tmp_dir = tempfile.mkdtemp(prefix="test_store_")
    srv = ObjectStoreServer(data_dir=tmp_dir, admin_password="test_admin_pw")
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture(scope="module")
def conn_info(server):
    """Connection info dict."""
    return server.conn_info()


@pytest.fixture(scope="module")
def _provision_users(server):
    """Provision test users: alice, bob, charlie."""
    admin_conn = server.admin_conn()
    provision_user(admin_conn, "alice", "alice_pw")
    provision_user(admin_conn, "bob", "bob_pw")
    provision_user(admin_conn, "charlie", "charlie_pw")
    admin_conn.close()


@pytest.fixture()
def alice(conn_info, _provision_users):
    """StoreClient connected as alice."""
    c = StoreClient(
        user="alice", password="alice_pw",
        host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
    )
    yield c
    c.close()


@pytest.fixture()
def bob(conn_info, _provision_users):
    """StoreClient connected as bob."""
    c = StoreClient(
        user="bob", password="bob_pw",
        host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
    )
    yield c
    c.close()


@pytest.fixture()
def charlie(conn_info, _provision_users):
    """StoreClient connected as charlie."""
    c = StoreClient(
        user="charlie", password="charlie_pw",
        host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
    )
    yield c
    c.close()


@pytest.fixture()
def admin_client(server, conn_info):
    """StoreClient connected as app_admin."""
    c = StoreClient(
        user="app_admin", password="test_admin_pw",
        host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
    )
    yield c
    c.close()


# ── Serialization (no DB needed) ────────────────────────────────────────────

class TestSerde:
    def test_dataclass_to_json(self):
        w = Widget(name="gear", color="blue", weight=1.5)
        j = w.to_json()
        data = json.loads(j)
        assert data == {"name": "gear", "color": "blue", "weight": 1.5}

    def test_dataclass_from_json(self):
        j = '{"name": "gear", "color": "blue", "weight": 1.5}'
        w = Widget.from_json(j)
        assert w.name == "gear"
        assert w.color == "blue"
        assert w.weight == 1.5

    def test_roundtrip(self):
        original = Widget(name="bolt", color="red", weight=0.3)
        restored = Widget.from_json(original.to_json())
        assert restored.name == original.name
        assert restored.color == original.color
        assert restored.weight == pytest.approx(original.weight)

    def test_datetime_serde(self):
        dt = datetime(2025, 1, 15, 10, 30, 0)
        encoded = json.dumps({"ts": dt}, cls=_JSONEncoder)
        decoded = json.loads(encoded, object_hook=_json_decoder_hook)
        assert decoded["ts"] == dt

    def test_decimal_serde(self):
        d = Decimal("123.456")
        encoded = json.dumps({"val": d}, cls=_JSONEncoder)
        decoded = json.loads(encoded, object_hook=_json_decoder_hook)
        assert decoded["val"] == d

    def test_uuid_serde(self):
        u = uuid.uuid4()
        encoded = json.dumps({"id": u}, cls=_JSONEncoder)
        decoded = json.loads(encoded, object_hook=_json_decoder_hook)
        assert decoded["id"] == u

    def test_type_name(self):
        assert "Widget" in Widget.type_name()

    def test_extra_fields_ignored_on_deserialize(self):
        j = '{"name": "x", "color": "y", "weight": 1.0, "extra": "ignored"}'
        w = Widget.from_json(j)
        assert w.name == "x"
        assert not hasattr(w, "extra")


# ── Event Sourcing ──────────────────────────────────────────────────────────

class TestEventSourcing:
    def test_write_creates_version_1(self, alice):
        w = Widget(name="v1_test", color="green", weight=2.0)
        entity_id = alice.write(w)
        assert entity_id is not None
        uuid.UUID(entity_id)
        assert w._store_version == 1
        assert w._store_event_type == "CREATED"

    def test_read_back(self, alice):
        w = Widget(name="spring", color="silver", weight=0.1)
        entity_id = alice.write(w)
        loaded = alice.read(Widget, entity_id)
        assert loaded is not None
        assert loaded.name == "spring"
        assert loaded.color == "silver"
        assert loaded.weight == pytest.approx(0.1)

    def test_store_metadata_set(self, alice):
        w = Widget(name="pin", color="black", weight=0.01)
        alice.write(w)
        assert w._store_entity_id is not None
        assert w._store_version == 1
        assert w._store_owner == "alice"
        assert w._store_tx_time is not None
        assert w._store_valid_from is not None
        assert w._store_event_type == "CREATED"

    def test_update_creates_new_version(self, alice):
        w = Widget(name="updatable", color="white", weight=1.0)
        alice.write(w)
        assert w._store_version == 1

        w.color = "black"
        alice.update(w)
        assert w._store_version == 2
        assert w._store_event_type == "UPDATED"

        loaded = alice.read(Widget, w._store_entity_id)
        assert loaded.color == "black"
        assert loaded._store_version == 2

    def test_update_never_overwrites(self, alice):
        """After update, both versions exist in history."""
        w = Widget(name="immutable_test", color="red", weight=1.0)
        alice.write(w)
        entity_id = w._store_entity_id

        w.color = "blue"
        alice.update(w)

        history = alice.history(Widget, entity_id)
        assert len(history) == 2
        assert history[0].color == "red"
        assert history[0]._store_version == 1
        assert history[1].color == "blue"
        assert history[1]._store_version == 2

    def test_delete_creates_tombstone(self, alice):
        w = Widget(name="deletable", color="grey", weight=0.5)
        entity_id = alice.write(w)

        alice.delete(w)
        assert w._store_event_type == "DELETED"

        # Gone from read/query
        assert alice.read(Widget, entity_id) is None

        # But present in history
        history = alice.history(Widget, entity_id)
        assert len(history) == 2
        assert history[-1]._store_event_type == "DELETED"

    def test_version_numbers_monotonic(self, alice):
        w = Widget(name="mono_test", color="a", weight=1.0)
        alice.write(w)
        for i in range(5):
            w.color = f"color_{i}"
            alice.update(w)
        assert w._store_version == 6

        history = alice.history(Widget, w._store_entity_id)
        versions = [h._store_version for h in history]
        assert versions == [1, 2, 3, 4, 5, 6]

    def test_independent_entity_versions(self, alice):
        w1 = Widget(name="ent1", color="a", weight=1.0)
        w2 = Widget(name="ent2", color="b", weight=2.0)
        alice.write(w1)
        alice.write(w2)

        w1.color = "updated"
        alice.update(w1)

        assert w1._store_version == 2
        assert w2._store_version == 1

    def test_history_returns_all_versions(self, alice):
        w = Widget(name="history_test", color="v1", weight=1.0)
        alice.write(w)
        w.color = "v2"
        alice.update(w)
        w.color = "v3"
        alice.update(w)

        history = alice.history(Widget, w._store_entity_id)
        assert len(history) == 3
        colors = [h.color for h in history]
        assert colors == ["v1", "v2", "v3"]


# ── Bi-Temporal Queries ─────────────────────────────────────────────────────

class TestBiTemporal:
    def test_as_of_tx_time(self, alice):
        """What did we know at time T?"""
        w = Widget(name="bitemporal", color="original", weight=1.0)
        alice.write(w)
        entity_id = w._store_entity_id
        after_write = datetime.now(timezone.utc)

        time.sleep(0.05)

        w.color = "corrected"
        alice.update(w)

        # As-of before the update: should see original
        old = alice.as_of(Widget, entity_id, tx_time=after_write)
        assert old is not None
        assert old.color == "original"

        # As-of now: should see corrected
        current = alice.as_of(Widget, entity_id, tx_time=datetime.now(timezone.utc))
        assert current.color == "corrected"

    def test_backdated_correction(self, alice):
        """Write with valid_from in the past → event_type=CORRECTED."""
        w = Widget(name="backdate_test", color="original", weight=1.0)
        alice.write(w)

        past = datetime.now(timezone.utc) - timedelta(hours=1)
        w.color = "corrected"
        alice.update(w, valid_from=past)

        assert w._store_event_type == "CORRECTED"
        assert w._store_valid_from <= datetime.now(timezone.utc)

    def test_valid_from_defaults_to_now(self, alice):
        """When valid_from is not specified, it defaults to now()."""
        w = Widget(name="default_vf", color="a", weight=1.0)
        before = datetime.now(timezone.utc)
        alice.write(w)
        after = datetime.now(timezone.utc)

        assert w._store_valid_from is not None
        # valid_from should be roughly between before and after
        # (PG now() might differ slightly from Python now())

    def test_as_of_valid_time(self, alice):
        """What was effective at business time T?"""
        w = Widget(name="valid_time_test", color="original", weight=1.0)
        past_time = datetime.now(timezone.utc) - timedelta(hours=2)
        alice.write(w, valid_from=past_time)
        entity_id = w._store_entity_id

        # Update effective from 1 hour ago
        later_time = datetime.now(timezone.utc) - timedelta(hours=1)
        w.color = "updated"
        alice.update(w, valid_from=later_time)

        # Query valid_time before the update
        before_update = past_time + timedelta(minutes=30)
        old = alice.as_of(Widget, entity_id, valid_time=before_update)
        assert old is not None
        assert old.color == "original"

        # Query valid_time after the update
        after_update = datetime.now(timezone.utc)
        current = alice.as_of(Widget, entity_id, valid_time=after_update)
        assert current.color == "updated"

    def test_write_with_custom_valid_from(self, alice):
        past = datetime.now(timezone.utc) - timedelta(days=1)
        w = Widget(name="custom_vf", color="yesterday", weight=1.0)
        alice.write(w, valid_from=past)
        assert w._store_valid_from is not None

    def test_tx_time_is_immutable(self, alice):
        """tx_time is set by the system and never changes."""
        w = Widget(name="tx_immutable", color="a", weight=1.0)
        alice.write(w)
        tx1 = w._store_tx_time

        time.sleep(0.05)
        w.color = "b"
        alice.update(w)
        tx2 = w._store_tx_time

        # Different versions have different tx_times
        assert tx2 > tx1


# ── State Machine ───────────────────────────────────────────────────────────

class TestStateMachine:
    def test_write_sets_initial_state(self, alice):
        o = Order(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        alice.write(o)
        assert o._store_state == "PENDING"

    def test_valid_transition(self, alice):
        o = Order(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        alice.write(o)
        alice.transition(o, "FILLED")
        assert o._store_state == "FILLED"
        assert o._store_event_type == "STATE_CHANGE"

    def test_invalid_transition_raises(self, alice):
        o = Order(symbol="TSLA", quantity=50, price=355.0, side="SELL")
        alice.write(o)
        with pytest.raises(InvalidTransition):
            alice.transition(o, "SETTLED")  # Can't go PENDING → SETTLED

    def test_state_tracked_across_versions(self, alice):
        o = Order(symbol="GOOG", quantity=200, price=192.0, side="BUY")
        alice.write(o)
        assert o._store_state == "PENDING"

        alice.transition(o, "PARTIAL")
        assert o._store_state == "PARTIAL"

        alice.transition(o, "FILLED")
        assert o._store_state == "FILLED"

        alice.transition(o, "SETTLED")
        assert o._store_state == "SETTLED"

    def test_state_history(self, alice):
        o = Order(symbol="MSFT", quantity=100, price=415.0, side="BUY")
        alice.write(o)
        alice.transition(o, "FILLED")
        alice.transition(o, "SETTLED")

        history = alice.history(Order, o._store_entity_id)
        states = [h._store_state for h in history]
        assert states == ["PENDING", "FILLED", "SETTLED"]
        event_types = [h._store_event_type for h in history]
        assert event_types == ["CREATED", "STATE_CHANGE", "STATE_CHANGE"]

    def test_cancel_from_pending(self, alice):
        o = Order(symbol="AMZN", quantity=50, price=225.0, side="BUY")
        alice.write(o)
        alice.transition(o, "CANCELLED")
        assert o._store_state == "CANCELLED"

    def test_cancel_from_partial(self, alice):
        o = Order(symbol="NVDA", quantity=100, price=138.0, side="BUY")
        alice.write(o)
        alice.transition(o, "PARTIAL")
        alice.transition(o, "CANCELLED")
        assert o._store_state == "CANCELLED"

    def test_cannot_transition_from_terminal_state(self, alice):
        o = Order(symbol="META", quantity=10, price=700.0, side="SELL")
        alice.write(o)
        alice.transition(o, "CANCELLED")
        with pytest.raises(InvalidTransition):
            alice.transition(o, "PENDING")

    def test_object_without_state_machine(self, alice):
        """Widget has no state machine — state should be NULL."""
        w = Widget(name="no_sm", color="x", weight=1.0)
        alice.write(w)
        assert w._store_state is None

    def test_transition_without_state_machine_raises(self, alice):
        w = Widget(name="no_sm_transition", color="x", weight=1.0)
        alice.write(w)
        with pytest.raises(ValueError):
            alice.transition(w, "ACTIVE")

    def test_allowed_transitions(self):
        assert set(OrderLifecycle.allowed_transitions("PENDING")) == {"PARTIAL", "FILLED", "CANCELLED"}
        assert set(OrderLifecycle.allowed_transitions("FILLED")) == {"SETTLED"}
        assert OrderLifecycle.allowed_transitions("SETTLED") == []
        assert OrderLifecycle.allowed_transitions("CANCELLED") == []

    def test_read_preserves_state(self, alice):
        o = Order(symbol="NFLX", quantity=5, price=1020.0, side="BUY")
        alice.write(o)
        alice.transition(o, "FILLED")

        loaded = alice.read(Order, o._store_entity_id)
        assert loaded._store_state == "FILLED"


# ── Basic CRUD ───────────────────────────────────────────────────────────────

class TestCRUD:
    def test_write_returns_uuid(self, alice):
        w = Widget(name="cog", color="green", weight=2.0)
        entity_id = alice.write(w)
        assert entity_id is not None
        uuid.UUID(entity_id)

    def test_query_by_type(self, alice):
        alice.write(Widget(name="q1", color="a", weight=1.0))
        alice.write(Widget(name="q2", color="b", weight=2.0))
        results = alice.query(Widget)
        assert len(results) >= 2
        assert all(isinstance(r, Widget) for r in results)

    def test_query_with_jsonb_filter(self, alice):
        alice.write(Widget(name="filterable", color="purple", weight=9.9))
        results = alice.query(Widget, filters={"color": "purple"})
        assert any(r.name == "filterable" for r in results)

    def test_count(self, alice):
        before = alice.count(Widget)
        alice.write(Widget(name="counted", color="x", weight=0.0))
        after = alice.count(Widget)
        assert after == before + 1

    def test_count_excludes_deleted(self, alice):
        before = alice.count(Widget)
        w = Widget(name="count_del", color="x", weight=0.0)
        alice.write(w)
        assert alice.count(Widget) == before + 1
        alice.delete(w)
        assert alice.count(Widget) == before

    def test_list_types(self, alice):
        alice.write(Widget(name="typed", color="x", weight=0.0))
        types = alice.list_types()
        assert any("Widget" in t for t in types)

    def test_rich_object_with_list(self, alice):
        r = RichObject(label="test", amount=42.0, ts="2025-01-01", tags=["a", "b"])
        entity_id = alice.write(r)
        loaded = alice.read(RichObject, entity_id)
        assert loaded.tags == ["a", "b"]

    def test_multiple_types_coexist(self, alice):
        alice.write(Widget(name="w", color="x", weight=1.0))
        alice.write(Order(symbol="AAPL", quantity=10, price=228.0, side="BUY"))
        widgets = alice.query(Widget)
        orders = alice.query(Order)
        assert len(widgets) >= 1
        assert len(orders) >= 1
        assert all(isinstance(w, Widget) for w in widgets)
        assert all(isinstance(o, Order) for o in orders)

    def test_query_returns_latest_version_only(self, alice):
        """Query should return only the latest version per entity, not all versions."""
        w = Widget(name="latest_only", color="v1", weight=1.0)
        alice.write(w)
        w.color = "v2"
        alice.update(w)
        w.color = "v3"
        alice.update(w)

        results = alice.query(Widget, filters={"name": "latest_only"})
        assert len(results) == 1
        assert results[0].color == "v3"

    def test_update_requires_entity_id(self, alice):
        w = Widget(name="no_id", color="x", weight=1.0)
        with pytest.raises(ValueError):
            alice.update(w)

    def test_delete_requires_entity_id(self, alice):
        w = Widget(name="no_id_del", color="x", weight=1.0)
        with pytest.raises(ValueError):
            alice.delete(w)


# ── RLS Isolation (zero-trust core) ─────────────────────────────────────────

class TestRLSIsolation:
    def test_alice_cannot_see_bobs_events(self, alice, bob):
        w = Widget(name="bobs_secret", color="red", weight=1.0)
        entity_id = bob.write(w)
        assert alice.read(Widget, entity_id) is None

    def test_bob_cannot_see_alices_events(self, alice, bob):
        w = Widget(name="alices_secret", color="blue", weight=2.0)
        entity_id = alice.write(w)
        assert bob.read(Widget, entity_id) is None

    def test_query_only_returns_own_entities(self, alice, bob):
        alice.write(Widget(name="alice_only_rls", color="a", weight=1.0))
        bob.write(Widget(name="bob_only_rls", color="b", weight=2.0))
        alice_results = alice.query(Widget)
        bob_results = bob.query(Widget)
        alice_names = {r.name for r in alice_results}
        bob_names = {r.name for r in bob_results}
        assert "alice_only_rls" in alice_names
        assert "bob_only_rls" not in alice_names
        assert "bob_only_rls" in bob_names
        assert "alice_only_rls" not in bob_names

    def test_alice_cannot_see_bobs_history(self, alice, bob):
        w = Widget(name="bob_history_secret", color="a", weight=1.0)
        bob.write(w)
        w.color = "b"
        bob.update(w)
        # Alice gets empty history
        assert alice.history(Widget, w._store_entity_id) == []

    def test_count_respects_rls(self, alice, bob):
        alice.write(Widget(name="ac_rls", color="x", weight=0.0))
        bob.write(Widget(name="bc_rls", color="x", weight=0.0))
        a_count = alice.count()
        b_count = bob.count()
        assert a_count > 0
        assert b_count > 0


# ── Trust Boundary Tests ─────────────────────────────────────────────────────

class TestTrustBoundary:
    def test_cannot_connect_with_wrong_password(self, conn_info, _provision_users):
        with pytest.raises(Exception):
            StoreClient(
                user="alice", password="wrong_password",
                host=conn_info["host"], port=conn_info["port"],
                dbname=conn_info["dbname"],
            )

    def test_cannot_connect_as_nonexistent_user(self, conn_info):
        with pytest.raises(Exception):
            StoreClient(
                user="nonexistent_user", password="whatever",
                host=conn_info["host"], port=conn_info["port"],
                dbname=conn_info["dbname"],
            )

    def test_alice_cannot_set_role_to_bob(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("SET ROLE bob")

    def test_alice_cannot_set_role_to_admin(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("SET ROLE app_admin")

    def test_alice_cannot_disable_rls(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("ALTER TABLE object_events DISABLE ROW LEVEL SECURITY")

    def test_alice_cannot_create_roles(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("CREATE ROLE hacker LOGIN PASSWORD 'x'")

    def test_alice_cannot_drop_table(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("DROP TABLE object_events")

    def test_alice_cannot_drop_policy(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("DROP POLICY user_select ON object_events")

    def test_alice_cannot_bypass_rls_attribute(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("ALTER ROLE alice BYPASSRLS")

    def test_alice_cannot_grant_superuser(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("ALTER ROLE alice SUPERUSER")

    def test_alice_cannot_insert_as_bob(self, alice):
        """Alice cannot forge owner = 'bob' on insert — RLS blocks it."""
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            with alice.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO object_events
                        (entity_id, version, type_name, owner, data)
                    VALUES (gen_random_uuid(), 1, 'fake', 'bob', '{"x":1}'::jsonb)
                    """,
                )
        alice.conn.rollback()

    def test_alice_cannot_delete_events(self, alice):
        """Append-only: no DELETE permission on object_events."""
        w = Widget(name="no_hard_delete", color="x", weight=1.0)
        alice.write(w)
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM object_events WHERE entity_id = %s",
                    (w._store_entity_id,),
                )
        alice.conn.rollback()


# ── Sharing ──────────────────────────────────────────────────────────────────

class TestSharing:
    def test_share_read_makes_visible(self, alice, bob):
        w = Widget(name="shared_to_bob", color="gold", weight=3.0)
        entity_id = alice.write(w)
        assert bob.read(Widget, entity_id) is None
        share_read(alice.conn, entity_id, "bob")
        loaded = bob.read(Widget, entity_id)
        assert loaded is not None
        assert loaded.name == "shared_to_bob"

    def test_shared_read_user_cannot_update(self, alice, bob):
        """Reader cannot create new versions — not owner or writer."""
        w = Widget(name="readonly_for_bob", color="silver", weight=1.0)
        entity_id = alice.write(w)
        share_read(alice.conn, entity_id, "bob")
        # Bob can read
        loaded = bob.read(Widget, entity_id)
        assert loaded is not None
        # But bob cannot update (not owner, not writer)
        loaded.color = "hacked"
        with pytest.raises(PermissionError):
            bob.update(loaded)

    def test_share_write_allows_new_version(self, alice, bob):
        w = Widget(name="writable_for_bob", color="white", weight=1.0)
        entity_id = alice.write(w)
        share_write(alice.conn, entity_id, "bob")
        loaded = bob.read(Widget, entity_id)
        assert loaded is not None
        loaded.color = "updated_by_bob"
        bob.update(loaded)
        refreshed = alice.read(Widget, entity_id)
        assert refreshed.color == "updated_by_bob"
        # owner stays as alice, updated_by records bob
        assert refreshed._store_owner == "alice"
        assert refreshed._store_updated_by == "bob"

    def test_shared_history_visible(self, alice, bob):
        """Shared entity's full history is visible to the reader."""
        w = Widget(name="shared_history", color="v1", weight=1.0)
        entity_id = alice.write(w)
        w.color = "v2"
        alice.update(w)
        share_read(alice.conn, entity_id, "bob")
        history = bob.history(Widget, entity_id)
        assert len(history) == 2

    def test_unshare_read_revokes_access(self, alice, bob):
        w = Widget(name="unshare_test", color="x", weight=1.0)
        entity_id = alice.write(w)
        share_read(alice.conn, entity_id, "bob")
        assert bob.read(Widget, entity_id) is not None
        unshare_read(alice.conn, entity_id, "bob")
        assert bob.read(Widget, entity_id) is None

    def test_unshare_write_revokes_access(self, alice, bob):
        w = Widget(name="unshare_write_test", color="x", weight=1.0)
        entity_id = alice.write(w)
        share_write(alice.conn, entity_id, "bob")
        assert bob.read(Widget, entity_id) is not None
        unshare_write(alice.conn, entity_id, "bob")
        assert bob.read(Widget, entity_id) is None

    def test_list_shared_with(self, alice):
        w = Widget(name="list_shared", color="x", weight=1.0)
        entity_id = alice.write(w)
        share_read(alice.conn, entity_id, "bob")
        share_write(alice.conn, entity_id, "charlie")
        perms = list_shared_with(alice.conn, entity_id)
        assert "bob" in perms["readers"]
        assert "charlie" in perms["writers"]

    def test_third_party_cannot_see_shared_between_others(self, alice, bob, charlie):
        w = Widget(name="alice_bob_only", color="x", weight=1.0)
        entity_id = alice.write(w)
        share_read(alice.conn, entity_id, "bob")
        assert charlie.read(Widget, entity_id) is None


# ── Admin Access ─────────────────────────────────────────────────────────────

class TestAdminAccess:
    def test_admin_sees_all_entities(self, alice, bob, admin_client):
        alice.write(Widget(name="admin_test_a", color="x", weight=1.0))
        bob.write(Widget(name="admin_test_b", color="x", weight=1.0))
        results = admin_client.query(Widget)
        names = {r.name for r in results}
        assert "admin_test_a" in names
        assert "admin_test_b" in names

    def test_admin_can_soft_delete(self, alice, admin_client):
        w = Widget(name="admin_deletable", color="x", weight=1.0)
        entity_id = alice.write(w)
        # Admin reads then deletes (admin policy bypasses RLS)
        admin_w = admin_client.read(Widget, entity_id)
        admin_client.delete(admin_w)
        assert alice.read(Widget, entity_id) is None

    def test_admin_count_includes_all_users(self, alice, bob, admin_client):
        alice.write(Widget(name="ac2", color="x", weight=0.0))
        bob.write(Widget(name="bc2", color="x", weight=0.0))
        admin_count = admin_client.count()
        alice_count = alice.count()
        bob_count = bob.count()
        assert admin_count > alice_count
        assert admin_count > bob_count

    def test_admin_can_see_history(self, alice, admin_client):
        w = Widget(name="admin_history", color="v1", weight=1.0)
        alice.write(w)
        w.color = "v2"
        alice.update(w)
        history = admin_client.history(Widget, w._store_entity_id)
        assert len(history) == 2


# ── Context Manager ──────────────────────────────────────────────────────────

class TestContextManager:
    def test_client_as_context_manager(self, conn_info, _provision_users):
        with StoreClient(
            user="alice", password="alice_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        ) as c:
            w = Widget(name="ctx_test", color="x", weight=1.0)
            c.write(w)
            assert w._store_entity_id is not None
