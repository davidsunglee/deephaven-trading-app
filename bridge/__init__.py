"""
Deephaven ↔ Store Bridge — streams object store events into ticking tables.

Users import StoreBridge and optionally the type_mapping helpers.
The bridge is a library, not a service — embed it wherever makes sense.
"""

from bridge.store_bridge import StoreBridge
from bridge.type_mapping import infer_dh_schema, extract_row

__all__ = ["StoreBridge", "infer_dh_schema", "extract_row"]
