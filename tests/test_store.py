"""
Comprehensive tests for the zero-trust PostgreSQL object store.
Tests cover: serde, CRUD, RLS enforcement, trust boundary, sharing, and admin access.

Run with: pytest tests/test_store.py -v
"""

import os
import sys
import json
import uuid
import tempfile
import pytest
import psycopg2.errors
from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from store.server import ObjectStoreServer
from store.schema import provision_user
from store.base import Storable, _JSONEncoder, _json_decoder_hook
from store.client import StoreClient
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


# ── Basic CRUD ───────────────────────────────────────────────────────────────

class TestCRUD:
    def test_write_returns_uuid(self, alice):
        w = Widget(name="cog", color="green", weight=2.0)
        obj_id = alice.write(w)
        assert obj_id is not None
        uuid.UUID(obj_id)  # validates format

    def test_read_back(self, alice):
        w = Widget(name="spring", color="silver", weight=0.1)
        obj_id = alice.write(w)
        loaded = alice.read(Widget, obj_id)
        assert loaded is not None
        assert loaded.name == "spring"
        assert loaded.color == "silver"
        assert loaded.weight == pytest.approx(0.1)

    def test_store_metadata_set(self, alice):
        w = Widget(name="pin", color="black", weight=0.01)
        alice.write(w)
        assert w._store_id is not None
        assert w._store_owner == "alice"
        assert w._store_created_at is not None

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

    def test_update(self, alice):
        w = Widget(name="updatable", color="white", weight=1.0)
        alice.write(w)
        w.color = "black"
        alice.update(w)
        loaded = alice.read(Widget, w._store_id)
        assert loaded.color == "black"

    def test_delete(self, alice):
        w = Widget(name="deletable", color="grey", weight=0.5)
        obj_id = alice.write(w)
        assert alice.delete(obj_id) is True
        assert alice.read(Widget, obj_id) is None

    def test_delete_nonexistent(self, alice):
        fake_id = str(uuid.uuid4())
        assert alice.delete(fake_id) is False

    def test_count(self, alice):
        before = alice.count(Widget)
        alice.write(Widget(name="counted", color="x", weight=0.0))
        after = alice.count(Widget)
        assert after == before + 1

    def test_list_types(self, alice):
        alice.write(Widget(name="typed", color="x", weight=0.0))
        types = alice.list_types()
        assert any("Widget" in t for t in types)

    def test_rich_object_with_list(self, alice):
        r = RichObject(label="test", amount=42.0, ts="2025-01-01", tags=["a", "b"])
        obj_id = alice.write(r)
        loaded = alice.read(RichObject, obj_id)
        assert loaded.tags == ["a", "b"]


# ── RLS Isolation (zero-trust core) ─────────────────────────────────────────

class TestRLSIsolation:
    def test_alice_cannot_see_bobs_rows(self, alice, bob):
        w = Widget(name="bobs_secret", color="red", weight=1.0)
        obj_id = bob.write(w)
        # Alice cannot read it
        assert alice.read(Widget, obj_id) is None

    def test_bob_cannot_see_alices_rows(self, alice, bob):
        w = Widget(name="alices_secret", color="blue", weight=2.0)
        obj_id = alice.write(w)
        assert bob.read(Widget, obj_id) is None

    def test_query_only_returns_own_rows(self, alice, bob):
        alice.write(Widget(name="alice_only", color="a", weight=1.0))
        bob.write(Widget(name="bob_only", color="b", weight=2.0))
        alice_results = alice.query(Widget)
        bob_results = bob.query(Widget)
        alice_names = {r.name for r in alice_results}
        bob_names = {r.name for r in bob_results}
        assert "alice_only" in alice_names
        assert "bob_only" not in alice_names
        assert "bob_only" in bob_names
        assert "alice_only" not in bob_names

    def test_alice_cannot_delete_bobs_row(self, alice, bob):
        w = Widget(name="bob_nodelete", color="x", weight=1.0)
        obj_id = bob.write(w)
        assert alice.delete(obj_id) is False
        # Bob can still see it
        assert bob.read(Widget, obj_id) is not None

    def test_alice_cannot_update_bobs_row(self, alice, bob):
        w = Widget(name="bob_noupdate", color="original", weight=1.0)
        bob.write(w)
        # Manually try to update via alice's connection
        with alice.conn.cursor() as cur:
            cur.execute(
                "UPDATE objects SET data = '{\"name\":\"hacked\"}'::jsonb WHERE id = %s RETURNING id",
                (w._store_id,),
            )
            assert cur.fetchone() is None  # RLS blocks it

    def test_count_respects_rls(self, alice, bob):
        alice.write(Widget(name="ac", color="x", weight=0.0))
        bob.write(Widget(name="bc", color="x", weight=0.0))
        # Each user's count should only include their own
        a_count = alice.count()
        b_count = bob.count()
        # They should not see each other's totals
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
                cur.execute("ALTER TABLE objects DISABLE ROW LEVEL SECURITY")

    def test_alice_cannot_create_roles(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("CREATE ROLE hacker LOGIN PASSWORD 'x'")

    def test_alice_cannot_drop_table(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("DROP TABLE objects")

    def test_alice_cannot_drop_policy(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("DROP POLICY user_select ON objects")

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
                    INSERT INTO objects (type_name, owner, data)
                    VALUES ('fake', 'bob', '{"x":1}'::jsonb)
                    """,
                )
        # Reset connection state after the error
        alice.conn.rollback()


# ── Sharing ──────────────────────────────────────────────────────────────────

class TestSharing:
    def test_share_read_makes_visible(self, alice, bob):
        w = Widget(name="shared_to_bob", color="gold", weight=3.0)
        obj_id = alice.write(w)
        # Bob cannot see it yet
        assert bob.read(Widget, obj_id) is None
        # Alice shares read access
        share_read(alice.conn, obj_id, "bob")
        # Now bob can see it
        loaded = bob.read(Widget, obj_id)
        assert loaded is not None
        assert loaded.name == "shared_to_bob"

    def test_shared_read_user_cannot_update(self, alice, bob):
        w = Widget(name="readonly_for_bob", color="silver", weight=1.0)
        obj_id = alice.write(w)
        share_read(alice.conn, obj_id, "bob")
        # Bob can read but not update
        with bob.conn.cursor() as cur:
            cur.execute(
                "UPDATE objects SET data = '{\"name\":\"hacked\"}'::jsonb WHERE id = %s RETURNING id",
                (obj_id,),
            )
            assert cur.fetchone() is None

    def test_shared_read_user_cannot_delete(self, alice, bob):
        w = Widget(name="nodelete_for_bob", color="bronze", weight=1.0)
        obj_id = alice.write(w)
        share_read(alice.conn, obj_id, "bob")
        assert bob.delete(obj_id) is False

    def test_share_write_allows_update(self, alice, bob):
        w = Widget(name="writable_for_bob", color="white", weight=1.0)
        obj_id = alice.write(w)
        share_write(alice.conn, obj_id, "bob")
        # Bob can now update
        loaded = bob.read(Widget, obj_id)
        assert loaded is not None
        loaded.color = "updated_by_bob"
        bob.update(loaded)
        refreshed = alice.read(Widget, obj_id)
        assert refreshed.color == "updated_by_bob"

    def test_writer_still_cannot_delete(self, alice, bob):
        w = Widget(name="writer_nodelete", color="x", weight=1.0)
        obj_id = alice.write(w)
        share_write(alice.conn, obj_id, "bob")
        # Writers can update but NOT delete (owner only)
        assert bob.delete(obj_id) is False

    def test_unshare_read_revokes_access(self, alice, bob):
        w = Widget(name="unshare_test", color="x", weight=1.0)
        obj_id = alice.write(w)
        share_read(alice.conn, obj_id, "bob")
        assert bob.read(Widget, obj_id) is not None
        unshare_read(alice.conn, obj_id, "bob")
        assert bob.read(Widget, obj_id) is None

    def test_unshare_write_revokes_access(self, alice, bob):
        w = Widget(name="unshare_write_test", color="x", weight=1.0)
        obj_id = alice.write(w)
        share_write(alice.conn, obj_id, "bob")
        assert bob.read(Widget, obj_id) is not None
        unshare_write(alice.conn, obj_id, "bob")
        assert bob.read(Widget, obj_id) is None

    def test_list_shared_with(self, alice):
        w = Widget(name="list_shared", color="x", weight=1.0)
        obj_id = alice.write(w)
        share_read(alice.conn, obj_id, "bob")
        share_write(alice.conn, obj_id, "charlie")
        perms = list_shared_with(alice.conn, obj_id)
        assert "bob" in perms["readers"]
        assert "charlie" in perms["writers"]

    def test_third_party_cannot_see_shared_between_others(self, alice, bob, charlie):
        w = Widget(name="alice_bob_only", color="x", weight=1.0)
        obj_id = alice.write(w)
        share_read(alice.conn, obj_id, "bob")
        # Charlie should NOT see this
        assert charlie.read(Widget, obj_id) is None


# ── Admin Access ─────────────────────────────────────────────────────────────

class TestAdminAccess:
    def test_admin_sees_all_rows(self, alice, bob, admin_client):
        alice.write(Widget(name="admin_test_a", color="x", weight=1.0))
        bob.write(Widget(name="admin_test_b", color="x", weight=1.0))
        results = admin_client.query(Widget)
        names = {r.name for r in results}
        assert "admin_test_a" in names
        assert "admin_test_b" in names

    def test_admin_can_delete_any_row(self, alice, admin_client):
        w = Widget(name="admin_deletable", color="x", weight=1.0)
        obj_id = alice.write(w)
        assert admin_client.delete(obj_id) is True
        assert alice.read(Widget, obj_id) is None

    def test_admin_count_includes_all_users(self, alice, bob, admin_client):
        alice.write(Widget(name="ac2", color="x", weight=0.0))
        bob.write(Widget(name="bc2", color="x", weight=0.0))
        admin_count = admin_client.count()
        alice_count = alice.count()
        bob_count = bob.count()
        # Admin sees everything; individual users only see their own
        assert admin_count > alice_count
        assert admin_count > bob_count


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
            assert w._store_id is not None
