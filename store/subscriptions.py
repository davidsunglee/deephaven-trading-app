"""
Event subscription system for the object store.

Tier 1: In-process EventBus — synchronous callbacks after DB writes.
Tier 2: PostgreSQL LISTEN/NOTIFY — cross-process real-time notifications.
Tier 2.5: Durable catch-up — persisted high-water mark for crash recovery.
"""

import json
import select
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Callable, List

import psycopg2


NOTIFY_CHANNEL = "object_events"


@dataclass
class ChangeEvent:
    """Notification payload for an entity change."""
    entity_id: str
    version: int
    event_type: str          # CREATED / UPDATED / DELETED / STATE_CHANGE / CORRECTED
    type_name: str
    updated_by: str
    state: Optional[str]
    tx_time: datetime


class EventBus:
    """
    In-process pub/sub for entity change events.

    Subscribe by type, by entity_id, or catch-all.
    Thread-safe for concurrent emit/subscribe.
    """

    def __init__(self):
        self._type_listeners = {}       # type_name → [callback]
        self._entity_listeners = {}     # entity_id → [callback]
        self._all_listeners = []        # [callback]
        self._lock = threading.Lock()

    def on(self, type_name, callback):
        """Subscribe to all changes for a given type_name."""
        with self._lock:
            self._type_listeners.setdefault(type_name, []).append(callback)

    def on_entity(self, entity_id, callback):
        """Subscribe to changes for a specific entity."""
        with self._lock:
            self._entity_listeners.setdefault(entity_id, []).append(callback)

    def on_all(self, callback):
        """Subscribe to all changes regardless of type or entity."""
        with self._lock:
            self._all_listeners.append(callback)

    def off(self, type_name, callback):
        """Unsubscribe a type listener."""
        with self._lock:
            listeners = self._type_listeners.get(type_name, [])
            if callback in listeners:
                listeners.remove(callback)

    def off_entity(self, entity_id, callback):
        """Unsubscribe an entity listener."""
        with self._lock:
            listeners = self._entity_listeners.get(entity_id, [])
            if callback in listeners:
                listeners.remove(callback)

    def off_all(self, callback):
        """Unsubscribe a catch-all listener."""
        with self._lock:
            if callback in self._all_listeners:
                self._all_listeners.remove(callback)

    def emit(self, event: ChangeEvent):
        """Dispatch a ChangeEvent to all matching listeners."""
        with self._lock:
            listeners = list(self._all_listeners)
            listeners += list(self._type_listeners.get(event.type_name, []))
            listeners += list(self._entity_listeners.get(event.entity_id, []))

        for cb in listeners:
            try:
                cb(event)
            except Exception:
                pass  # Don't let a bad callback break the chain


class SubscriptionListener:
    """
    Background listener for PostgreSQL LISTEN/NOTIFY with durable catch-up.

    Runs a daemon thread that:
    1. On start, catches up from the last checkpoint (or start time)
    2. Runs LISTEN on the object_events channel
    3. Dispatches notifications to the EventBus
    4. Optionally persists checkpoint to DB for crash recovery

    Args:
        event_bus: EventBus to dispatch events to
        host, port, dbname, user, password: PG connection params
        subscriber_id: Optional. If set, checkpoint is persisted to DB.
    """

    def __init__(self, event_bus, host, port, dbname, user, password,
                 subscriber_id=None):
        self.event_bus = event_bus
        self._conn_params = dict(host=host, port=port, dbname=dbname,
                                 user=user, password=password)
        self.subscriber_id = subscriber_id
        self._conn = None
        self._thread = None
        self._stop_event = threading.Event()
        self._last_tx_time = None

    def start(self):
        """Start the listener background thread."""
        self._stop_event.clear()
        self._conn = psycopg2.connect(**self._conn_params)
        self._conn.autocommit = True

        # Load checkpoint
        self._last_tx_time = self._load_checkpoint()

        # Catch up on missed events
        self._catch_up()

        # Start LISTEN
        with self._conn.cursor() as cur:
            cur.execute(f"LISTEN {NOTIFY_CHANNEL};")

        self._thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the listener and close the connection."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
            self._thread = None
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def _listen_loop(self):
        """Background loop: poll for NOTIFY, dispatch to bus."""
        while not self._stop_event.is_set():
            if self._conn is None or self._conn.closed:
                break
            # Use select to wait for notifications with timeout
            if select.select([self._conn], [], [], 0.5) != ([], [], []):
                self._conn.poll()
                while self._conn.notifies:
                    notify = self._conn.notifies.pop(0)
                    self._handle_notify(notify)

    def _handle_notify(self, notify):
        """Parse a PG notification and emit to EventBus."""
        try:
            payload = json.loads(notify.payload)
            event = ChangeEvent(
                entity_id=str(payload["entity_id"]),
                version=payload["version"],
                event_type=payload["event_type"],
                type_name=payload["type_name"],
                updated_by=payload["updated_by"],
                state=payload.get("state"),
                tx_time=datetime.fromisoformat(payload["tx_time"]) if isinstance(payload["tx_time"], str) else payload["tx_time"],
            )
            self.event_bus.emit(event)
            self._last_tx_time = event.tx_time
            self._save_checkpoint()
        except (json.JSONDecodeError, KeyError):
            pass  # Malformed notification — skip

    def _catch_up(self):
        """Replay missed events from the event log since last checkpoint."""
        if self._last_tx_time is None:
            self._last_tx_time = datetime.now(timezone.utc)
            self._save_checkpoint()
            return

        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT entity_id, version, event_type, type_name,
                       updated_by, state, tx_time
                FROM object_events
                WHERE tx_time > %s
                ORDER BY tx_time ASC
                """,
                (self._last_tx_time,),
            )
            for row in cur.fetchall():
                event = ChangeEvent(
                    entity_id=str(row[0]),
                    version=row[1],
                    event_type=row[2],
                    type_name=row[3],
                    updated_by=row[4],
                    state=row[5],
                    tx_time=row[6],
                )
                self.event_bus.emit(event)
                self._last_tx_time = event.tx_time

        self._save_checkpoint()

    def _load_checkpoint(self):
        """Load the last checkpoint from DB (if subscriber_id is set)."""
        if not self.subscriber_id:
            return None

        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT last_tx_time FROM subscription_checkpoints WHERE subscriber_id = %s",
                (self.subscriber_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def _save_checkpoint(self):
        """Persist the current checkpoint to DB (if subscriber_id is set)."""
        if not self.subscriber_id or self._last_tx_time is None:
            return
        if self._conn is None or self._conn.closed:
            return

        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO subscription_checkpoints (subscriber_id, last_tx_time)
                VALUES (%s, %s)
                ON CONFLICT (subscriber_id) DO UPDATE
                    SET last_tx_time = EXCLUDED.last_tx_time,
                        updated_at = now()
                """,
                (self.subscriber_id, self._last_tx_time),
            )
