"""
Storable base class — defines how Python objects serialize to/from JSONB.
Subclass with @dataclass to create persistable types.

Bi-temporal metadata:
- entity_id: stable identity across versions
- version: monotonic per entity
- tx_time: when this version was recorded (system, immutable)
- valid_from: when this version is effective (user, defaults to now)
- valid_to: when this version stops being effective
- state: lifecycle state (if a state machine is registered)
- event_type: CREATED, UPDATED, DELETED, STATE_CHANGE, CORRECTED
"""

import json
import uuid
import dataclasses
from datetime import datetime, date
from decimal import Decimal
from typing import Optional


class _JSONEncoder(json.JSONEncoder):
    """Handles datetime, date, Decimal, UUID, and dataclass serialization."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return {"__type__": "datetime", "value": obj.isoformat()}
        if isinstance(obj, date):
            return {"__type__": "date", "value": obj.isoformat()}
        if isinstance(obj, Decimal):
            return {"__type__": "Decimal", "value": str(obj)}
        if isinstance(obj, uuid.UUID):
            return {"__type__": "UUID", "value": str(obj)}
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return dataclasses.asdict(obj)
        return super().default(obj)


def _json_decoder_hook(d):
    """Reconstruct special types from JSONB."""
    if "__type__" in d:
        t = d["__type__"]
        v = d["value"]
        if t == "datetime":
            return datetime.fromisoformat(v)
        if t == "date":
            return date.fromisoformat(v)
        if t == "Decimal":
            return Decimal(v)
        if t == "UUID":
            return uuid.UUID(v)
    return d


class Storable:
    """
    Base class for objects stored in the bi-temporal event-sourced object store.

    Subclass as a dataclass:

        @dataclass
        class Trade(Storable):
            symbol: str
            quantity: int
            price: float
            side: str

    Then use StoreClient to persist:

        client.write(Trade(symbol="AAPL", quantity=100, price=228.0, side="BUY"))

    Every write/update creates an immutable event with bi-temporal timestamps.
    """

    # Bi-temporal metadata — set by the store after writing / reading
    _store_entity_id: Optional[str] = None
    _store_version: Optional[int] = None
    _store_owner: Optional[str] = None
    _store_updated_by: Optional[str] = None
    _store_tx_time: Optional[datetime] = None
    _store_valid_from: Optional[datetime] = None
    _store_valid_to: Optional[datetime] = None
    _store_state: Optional[str] = None
    _store_event_type: Optional[str] = None

    # Optional state machine — set on the class by the user
    _state_machine = None

    # Column registry — mandatory enforcement for all subclasses
    _registry = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls._registry is not None:
            # __init_subclass__ fires BEFORE @dataclass, so we use
            # __annotations__ (not dataclasses.fields) to check columns.
            own_annotations = {
                k: v for k, v in getattr(cls, '__annotations__', {}).items()
                if not k.startswith('_')
            }
            if own_annotations:
                cls._registry.validate_class(cls)

    def to_json(self) -> str:
        """Serialize this object to a JSON string for JSONB storage."""
        if dataclasses.is_dataclass(self):
            data = dataclasses.asdict(self)
        else:
            data = {
                k: v for k, v in self.__dict__.items()
                if not k.startswith("_store_") and not k.startswith("_state_")
            }
        return json.dumps(data, cls=_JSONEncoder)

    @classmethod
    def from_json(cls, json_str: str) -> "Storable":
        """Deserialize from a JSON string back to a typed object."""
        data = json.loads(json_str, object_hook=_json_decoder_hook)
        if dataclasses.is_dataclass(cls):
            # Filter to only fields the dataclass expects
            field_names = {f.name for f in dataclasses.fields(cls)}
            filtered = {k: v for k, v in data.items() if k in field_names}
            return cls(**filtered)
        else:
            obj = cls.__new__(cls)
            obj.__dict__.update(data)
            return obj

    @classmethod
    def type_name(cls) -> str:
        """The type identifier stored in the database."""
        return f"{cls.__module__}.{cls.__qualname__}"


# ── Wire mandatory column registry (no circular import — columns/ does not import base) ──
from store.columns import REGISTRY  # noqa: E402
Storable._registry = REGISTRY
