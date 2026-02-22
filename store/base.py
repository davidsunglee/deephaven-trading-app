"""
Storable base class — defines how Python objects serialize to/from JSONB.
Subclass with @dataclass to create persistable types.
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
    Base class for objects that can be stored in the PostgreSQL object store.

    Subclass as a dataclass:

        @dataclass
        class Trade(Storable):
            symbol: str
            quantity: int
            price: float
            side: str

    Then use StoreClient to persist:

        client.write(Trade(symbol="AAPL", quantity=100, price=228.0, side="BUY"))
    """

    # Set by the store after writing / reading
    _store_id: Optional[str] = None
    _store_owner: Optional[str] = None
    _store_created_at: Optional[datetime] = None
    _store_updated_at: Optional[datetime] = None

    def to_json(self) -> str:
        """Serialize this object to a JSON string for JSONB storage."""
        if dataclasses.is_dataclass(self):
            data = dataclasses.asdict(self)
        else:
            data = {
                k: v for k, v in self.__dict__.items()
                if not k.startswith("_store_")
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
