"""
Type mapping between Python @dataclass fields and Deephaven column types.

Provides:
- infer_dh_schema(storable_cls) → OrderedDict of {column_name: dh_type}
- extract_row(obj, column_names) → tuple of values in column order
"""

import dataclasses
from collections import OrderedDict
from datetime import datetime
from decimal import Decimal
from typing import Optional, get_type_hints, get_origin, get_args


def _get_dh_type(python_type):
    """Map a Python type annotation to a Deephaven dtype.

    Must be called after deephaven_server.Server.start().
    """
    import deephaven.dtypes as dht

    # Unwrap Optional[X] → X
    if get_origin(python_type) is type(None):
        pass
    origin = get_origin(python_type)
    if origin is not None:
        args = get_args(python_type)
        # Optional[X] is Union[X, None]
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            python_type = non_none[0]

    mapping = {
        str: dht.string,
        int: dht.int64,
        float: dht.double,
        bool: dht.bool_,
        Decimal: dht.double,
        datetime: dht.Instant,
    }
    return mapping.get(python_type, dht.string)


# Standard metadata columns prepended to every schema
_METADATA_COLUMNS = [
    ("EntityId", str),
    ("Version", int),
    ("EventType", str),
    ("State", str),
    ("UpdatedBy", str),
    ("TxTime", datetime),
]


def infer_dh_schema(storable_cls):
    """Auto-generate a Deephaven column schema from a @dataclass Storable.

    Returns an OrderedDict of {column_name: dh_type}.
    Must be called after deephaven_server.Server.start().

    Metadata columns (EntityId, Version, EventType, State, UpdatedBy, TxTime)
    are always prepended. Domain columns follow from the dataclass fields.
    """
    schema = OrderedDict()

    # Metadata columns
    for col_name, py_type in _METADATA_COLUMNS:
        schema[col_name] = _get_dh_type(py_type)

    # Domain columns from dataclass fields
    if dataclasses.is_dataclass(storable_cls):
        hints = get_type_hints(storable_cls)
        for field in dataclasses.fields(storable_cls):
            py_type = hints.get(field.name, str)
            schema[field.name] = _get_dh_type(py_type)

    return schema


def _to_dh_value(value):
    """Convert a Python value to a Deephaven-compatible value.

    Uses DH's own SDK for type conversions. The only type that
    DynamicTableWriter.write_row() can't auto-convert is
    datetime → java.time.Instant.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        from deephaven.time import to_j_instant
        return to_j_instant(value)
    if isinstance(value, Decimal):
        return float(value)
    return value


# Metadata column definitions: (column_name, store_attr, python_type)
_META_COLUMNS = [
    ("EntityId", "_store_entity_id", str),
    ("Version", "_store_version", int),
    ("EventType", "_store_event_type", str),
    ("State", "_store_state", str),
    ("UpdatedBy", "_store_updated_by", str),
    ("TxTime", "_store_tx_time", datetime),
]

_META_ATTR_MAP = {name: attr for name, attr, _ in _META_COLUMNS}


def extract_row(obj, column_names):
    """Extract values from a Storable instance in the given column order.

    Args:
        obj: A Storable instance with _store_* metadata populated.
        column_names: Iterable of column names matching the schema.

    Returns:
        Tuple of DH-compatible values in the same order as column_names.
    """
    values = []
    for col in column_names:
        if col in _META_ATTR_MAP:
            raw = getattr(obj, _META_ATTR_MAP[col], None)
        else:
            raw = getattr(obj, col, None)
        values.append(_to_dh_value(raw))
    return tuple(values)
