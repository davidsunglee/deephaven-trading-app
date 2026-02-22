"""
StoreClient — connects to PostgreSQL as a specific user.
All operations are filtered by RLS automatically.
"""

import json
import uuid
import psycopg2
import psycopg2.extras

from store.base import Storable, _JSONEncoder, _json_decoder_hook


class StoreClient:
    """
    Connects to the object store as a specific user.
    All reads/writes are filtered by PostgreSQL RLS — no middleware needed.

    Usage:
        client = StoreClient(user="alice", password="secret", host="/tmp/pg", port=5432)
        client.write(Trade(symbol="AAPL", quantity=100, price=228.0, side="BUY"))
        trades = client.query(Trade)
        client.close()
    """

    def __init__(self, user, password, host="localhost", port=5432, dbname="postgres"):
        self.user = user
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
        self.conn.autocommit = True
        # Register UUID adapter
        psycopg2.extras.register_uuid()

    def write(self, obj):
        """
        Persist a Storable object. Returns the assigned UUID.
        RLS enforces that owner = current_user on INSERT.
        """
        json_data = obj.to_json()
        type_name = obj.type_name()

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO objects (type_name, data)
                VALUES (%s, %s::jsonb)
                RETURNING id, owner, created_at, updated_at
                """,
                (type_name, json_data),
            )
            row = cur.fetchone()
            obj._store_id = str(row[0])
            obj._store_owner = row[1]
            obj._store_created_at = row[2]
            obj._store_updated_at = row[3]
            return obj._store_id

    def read(self, cls, obj_id):
        """
        Read a single object by ID. Returns None if not found or not visible.
        RLS filters automatically — if this user can't see it, it's invisible.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id, type_name, owner, readers, writers, data, "
                "created_at, updated_at FROM objects WHERE id = %s",
                (obj_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return self._row_to_object(cls, row)

    def query(self, cls, filters=None, limit=100):
        """
        Query objects of a given type. Optional JSONB filters.

        filters: dict of key-value pairs matched against the data column.
            e.g. {"symbol": "AAPL"} → data @> '{"symbol": "AAPL"}'

        Returns list of typed objects (only those visible to current user).
        """
        type_name = cls.type_name()
        params = [type_name]
        sql = (
            "SELECT id, type_name, owner, readers, writers, data, "
            "created_at, updated_at FROM objects WHERE type_name = %s"
        )

        if filters:
            filter_json = json.dumps(filters, cls=_JSONEncoder)
            sql += " AND data @> %s::jsonb"
            params.append(filter_json)

        sql += " ORDER BY created_at DESC LIMIT %s"
        params.append(limit)

        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [self._row_to_object(cls, row) for row in rows]

    def update(self, obj):
        """
        Update an existing object's data. Requires write access (owner or writer).
        """
        if not obj._store_id:
            raise ValueError("Object has no store ID — write() it first")

        json_data = obj.to_json()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE objects SET data = %s::jsonb, updated_at = now()
                WHERE id = %s
                RETURNING updated_at
                """,
                (json_data, obj._store_id),
            )
            row = cur.fetchone()
            if row is None:
                raise PermissionError(
                    f"Cannot update object {obj._store_id} — "
                    f"not owner or writer, or object does not exist"
                )
            obj._store_updated_at = row[0]

    def delete(self, obj_id):
        """
        Delete an object by ID. RLS enforces owner-only deletion.
        Returns True if deleted, False if not found / not permitted.
        """
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM objects WHERE id = %s RETURNING id", (obj_id,))
            return cur.fetchone() is not None

    def list_types(self):
        """List distinct type_names visible to the current user."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT DISTINCT type_name FROM objects ORDER BY type_name")
            return [row[0] for row in cur.fetchall()]

    def count(self, cls=None):
        """Count objects visible to current user, optionally filtered by type."""
        with self.conn.cursor() as cur:
            if cls:
                cur.execute(
                    "SELECT COUNT(*) FROM objects WHERE type_name = %s",
                    (cls.type_name(),),
                )
            else:
                cur.execute("SELECT COUNT(*) FROM objects")
            return cur.fetchone()[0]

    def _row_to_object(self, cls, row):
        """Convert a database row to a typed Python object."""
        obj_id, type_name, owner, readers, writers, data, created_at, updated_at = row
        # data is already a dict (psycopg2 auto-parses JSONB)
        if isinstance(data, str):
            data = json.loads(data, object_hook=_json_decoder_hook)
        else:
            # Walk the dict for special types
            data = json.loads(
                json.dumps(data, cls=_JSONEncoder),
                object_hook=_json_decoder_hook,
            )

        obj = cls.from_json(json.dumps(data, cls=_JSONEncoder))
        obj._store_id = str(obj_id)
        obj._store_owner = owner
        obj._store_created_at = created_at
        obj._store_updated_at = updated_at
        return obj

    def close(self):
        """Close the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
