"""
StoreClient — bi-temporal event-sourced object store.

All operations are append-only INSERTs into object_events.
Reads return the latest non-deleted version per entity.
RLS enforces zero-trust access control automatically.
"""

import json
import uuid
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

from store.base import Storable, _JSONEncoder, _json_decoder_hook
from store.state_machine import InvalidTransition


class StoreClient:
    """
    Connects to the object store as a specific user.
    All reads/writes are filtered by PostgreSQL RLS — no middleware needed.

    Every mutation creates a new immutable event (never overwrites).
    Bi-temporal: tx_time (system) + valid_from/valid_to (business).

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
        psycopg2.extras.register_uuid()

    # ── Write operations (all append-only) ────────────────────────────

    def write(self, obj, valid_from=None):
        """
        Create a new entity (version 1). Returns the entity_id.
        If the Storable class has a state machine, initial state is set automatically.
        """
        entity_id = str(uuid.uuid4())
        json_data = obj.to_json()
        type_name = obj.type_name()
        state = None
        if obj._state_machine is not None:
            state = obj._state_machine.initial

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO object_events
                    (entity_id, version, type_name, data, state, event_type, valid_from)
                VALUES (%s, 1, %s, %s::jsonb, %s, 'CREATED', COALESCE(%s, now()))
                RETURNING event_id, entity_id, owner, updated_by, tx_time, valid_from, state
                """,
                (entity_id, type_name, json_data, state, valid_from),
            )
            row = cur.fetchone()
            obj._store_entity_id = str(row[1])
            obj._store_version = 1
            obj._store_owner = row[2]
            obj._store_updated_by = row[3]
            obj._store_tx_time = row[4]
            obj._store_valid_from = row[5]
            obj._store_valid_to = None
            obj._store_state = row[6]
            obj._store_event_type = "CREATED"
            return obj._store_entity_id

    def update(self, obj, valid_from=None):
        """
        Create a new version of an existing entity (never overwrites).
        Automatically determines event_type: UPDATED or CORRECTED (if backdated).
        """
        if not obj._store_entity_id:
            raise ValueError("Object has no entity_id — write() it first")

        next_ver = self._next_version(obj._store_entity_id)
        json_data = obj.to_json()
        type_name = obj.type_name()

        # Determine event type
        event_type = "UPDATED"
        if valid_from is not None:
            now = datetime.now(timezone.utc)
            if valid_from < now:
                event_type = "CORRECTED"

        # Carry forward state and permissions from previous version
        state = obj._store_state

        with self.conn.cursor() as cur:
            # Copy owner, readers/writers from latest version
            cur.execute(
                """
                SELECT owner, readers, writers FROM object_events
                WHERE entity_id = %s ORDER BY version DESC LIMIT 1
                """,
                (obj._store_entity_id,),
            )
            prev = cur.fetchone()
            original_owner = prev[0] if prev else self.user
            readers = prev[1] if prev else []
            writers = prev[2] if prev else []

            # Only the owner or a writer can create new versions
            if self.user != original_owner and self.user not in writers:
                raise PermissionError(
                    f"Cannot update entity {obj._store_entity_id} — "
                    f"not owner or writer"
                )

            cur.execute(
                """
                INSERT INTO object_events
                    (entity_id, version, type_name, owner, data, state, event_type,
                     readers, writers, valid_from)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, COALESCE(%s, now()))
                RETURNING event_id, tx_time, valid_from
                """,
                (obj._store_entity_id, next_ver, type_name, original_owner,
                 json_data, state, event_type, readers, writers, valid_from),
            )
            row = cur.fetchone()
            obj._store_version = next_ver
            obj._store_tx_time = row[1]
            obj._store_valid_from = row[2]
            obj._store_event_type = event_type

    def delete(self, obj):
        """
        Soft-delete: creates a DELETED tombstone event.
        The entity disappears from read()/query() but remains in history().
        """
        if not obj._store_entity_id:
            raise ValueError("Object has no entity_id — write() it first")

        next_ver = self._next_version(obj._store_entity_id)
        type_name = obj.type_name()
        json_data = obj.to_json()

        with self.conn.cursor() as cur:
            # Carry forward original owner
            cur.execute(
                """
                SELECT owner FROM object_events
                WHERE entity_id = %s ORDER BY version DESC LIMIT 1
                """,
                (obj._store_entity_id,),
            )
            prev = cur.fetchone()
            original_owner = prev[0] if prev else self.user

            cur.execute(
                """
                INSERT INTO object_events
                    (entity_id, version, type_name, owner, data, state, event_type)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, 'DELETED')
                RETURNING event_id, tx_time
                """,
                (obj._store_entity_id, next_ver, type_name, original_owner,
                 json_data, obj._store_state),
            )
            row = cur.fetchone()
            obj._store_version = next_ver
            obj._store_tx_time = row[1]
            obj._store_event_type = "DELETED"
            return row[0] is not None

    def transition(self, obj, new_state, valid_from=None):
        """
        Transition an entity to a new lifecycle state.
        Validates against the registered state machine.
        Creates a STATE_CHANGE event.
        """
        if obj._state_machine is None:
            raise ValueError(
                f"{type(obj).__name__} has no state machine registered"
            )

        current_state = obj._store_state
        obj._state_machine.validate_transition(current_state, new_state)

        next_ver = self._next_version(obj._store_entity_id)
        json_data = obj.to_json()
        type_name = obj.type_name()

        event_meta = json.dumps({
            "from_state": current_state,
            "to_state": new_state,
        })

        with self.conn.cursor() as cur:
            # Copy owner, readers/writers from latest version
            cur.execute(
                """
                SELECT owner, readers, writers FROM object_events
                WHERE entity_id = %s ORDER BY version DESC LIMIT 1
                """,
                (obj._store_entity_id,),
            )
            prev = cur.fetchone()
            original_owner = prev[0] if prev else self.user
            readers = prev[1] if prev else []
            writers = prev[2] if prev else []

            cur.execute(
                """
                INSERT INTO object_events
                    (entity_id, version, type_name, owner, data, state, event_type,
                     event_meta, readers, writers, valid_from)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, 'STATE_CHANGE',
                        %s::jsonb, %s, %s, COALESCE(%s, now()))
                RETURNING event_id, tx_time, valid_from
                """,
                (obj._store_entity_id, next_ver, type_name, original_owner,
                 json_data, new_state, event_meta, readers, writers, valid_from),
            )
            row = cur.fetchone()
            obj._store_version = next_ver
            obj._store_state = new_state
            obj._store_tx_time = row[1]
            obj._store_valid_from = row[2]
            obj._store_event_type = "STATE_CHANGE"

    # ── Read operations ───────────────────────────────────────────────

    def read(self, cls, entity_id):
        """
        Read the latest non-deleted version of an entity.
        Returns None if not found, not visible, or deleted.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, entity_id, version, type_name, owner,
                       updated_by, readers, writers, data, state, event_type,
                       tx_time, valid_from, valid_to
                FROM object_events
                WHERE entity_id = %s
                ORDER BY version DESC
                LIMIT 1
                """,
                (entity_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            # If latest version is DELETED, entity is gone
            if row[10] == "DELETED":
                return None
            return self._row_to_object(cls, row)

    def query(self, cls, filters=None, limit=100):
        """
        Query current (latest non-deleted) entities of a given type.

        filters: dict of key-value pairs matched against the data column.
            e.g. {"symbol": "AAPL"} → data @> '{"symbol": "AAPL"}'
        """
        type_name = cls.type_name()
        params = [type_name]

        sql = """
            SELECT DISTINCT ON (entity_id)
                   event_id, entity_id, version, type_name, owner,
                   updated_by, readers, writers, data, state, event_type,
                   tx_time, valid_from, valid_to
            FROM object_events
            WHERE type_name = %s
        """

        if filters:
            filter_json = json.dumps(filters, cls=_JSONEncoder)
            sql += " AND data @> %s::jsonb"
            params.append(filter_json)

        sql += " ORDER BY entity_id, version DESC"

        # Wrap to filter out DELETED and apply limit
        wrapped = f"""
            SELECT * FROM ({sql}) sub
            WHERE event_type != 'DELETED'
            ORDER BY tx_time DESC
            LIMIT %s
        """
        params.append(limit)

        with self.conn.cursor() as cur:
            cur.execute(wrapped, params)
            rows = cur.fetchall()
            return [self._row_to_object(cls, row) for row in rows]

    def history(self, cls, entity_id):
        """
        Return all versions of an entity, ordered by version ascending.
        Includes DELETED tombstones.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, entity_id, version, type_name, owner,
                       updated_by, readers, writers, data, state, event_type,
                       tx_time, valid_from, valid_to
                FROM object_events
                WHERE entity_id = %s
                ORDER BY version ASC
                """,
                (entity_id,),
            )
            rows = cur.fetchall()
            return [self._row_to_object(cls, row) for row in rows]

    def as_of(self, cls, entity_id, tx_time=None, valid_time=None):
        """
        Bi-temporal point-in-time query.

        - tx_time only: "what did we know at time T?"
        - valid_time only: "what was effective at business time T?"
        - both: "what did we know at T about business time T'?"
        """
        conditions = ["entity_id = %s"]
        params = [entity_id]

        if tx_time is not None:
            conditions.append("tx_time <= %s")
            params.append(tx_time)

        if valid_time is not None:
            conditions.append("valid_from <= %s")
            params.append(valid_time)

        where = " AND ".join(conditions)

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT event_id, entity_id, version, type_name, owner,
                       updated_by, readers, writers, data, state, event_type,
                       tx_time, valid_from, valid_to
                FROM object_events
                WHERE {where}
                ORDER BY version DESC
                LIMIT 1
                """,
                params,
            )
            row = cur.fetchone()
            if row is None:
                return None
            return self._row_to_object(cls, row)

    def count(self, cls=None):
        """Count current (latest non-deleted) entities visible to this user."""
        with self.conn.cursor() as cur:
            if cls:
                type_name = cls.type_name()
                cur.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT ON (entity_id) entity_id, event_type
                        FROM object_events
                        WHERE type_name = %s
                        ORDER BY entity_id, version DESC
                    ) sub
                    WHERE event_type != 'DELETED'
                    """,
                    (type_name,),
                )
            else:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT ON (entity_id) entity_id, event_type
                        FROM object_events
                        ORDER BY entity_id, version DESC
                    ) sub
                    WHERE event_type != 'DELETED'
                    """
                )
            return cur.fetchone()[0]

    def list_types(self):
        """List distinct type_names visible to the current user."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT type_name FROM object_events ORDER BY type_name"
            )
            return [row[0] for row in cur.fetchall()]

    # ── Internal helpers ──────────────────────────────────────────────

    def _next_version(self, entity_id):
        """Get the next version number for an entity."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 FROM object_events WHERE entity_id = %s",
                (entity_id,),
            )
            return cur.fetchone()[0]

    def _row_to_object(self, cls, row):
        """Convert a database row to a typed Python object with bi-temporal metadata."""
        (event_id, entity_id, version, type_name, owner,
         updated_by, readers, writers, data, state, event_type,
         tx_time, valid_from, valid_to) = row

        # data is already a dict (psycopg2 auto-parses JSONB)
        if isinstance(data, str):
            data = json.loads(data, object_hook=_json_decoder_hook)
        else:
            data = json.loads(
                json.dumps(data, cls=_JSONEncoder),
                object_hook=_json_decoder_hook,
            )

        obj = cls.from_json(json.dumps(data, cls=_JSONEncoder))
        obj._store_entity_id = str(entity_id)
        obj._store_version = version
        obj._store_owner = owner
        obj._store_updated_by = updated_by
        obj._store_tx_time = tx_time
        obj._store_valid_from = valid_from
        obj._store_valid_to = valid_to
        obj._store_state = state
        obj._store_event_type = event_type
        return obj

    def close(self):
        """Close the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
