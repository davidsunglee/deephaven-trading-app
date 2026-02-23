"""
General-purpose columns — name, label, title, color, weight, etc.

Used across test entities and general-purpose Storables.
"""

from datetime import datetime

from store.columns import REGISTRY

# ── Identifiers / Labels ─────────────────────────────────────────

REGISTRY.define("name", str,
    description="Entity or person name",
    semantic_type="person_name",
    role="dimension",
    synonyms=["person", "individual"],
    sensitivity="pii",
    allowed_prefixes=["trader", "salesperson", "client", "approver"],
)

REGISTRY.define("label", str,
    description="Short descriptive label",
    semantic_type="label",
    role="dimension",
    synonyms=["tag", "descriptor"],
)

REGISTRY.define("title", str,
    description="Title or heading",
    semantic_type="label",
    role="dimension",
    synonyms=["heading", "subject"],
)

REGISTRY.define("color", str,
    description="Color identifier",
    semantic_type="label",
    role="dimension",
    synonyms=["colour"],
)

REGISTRY.define("status", str,
    description="Current status label",
    semantic_type="label",
    role="dimension",
    display_name="Status",
)

REGISTRY.define("notes", str,
    description="Free text notes or comments",
    semantic_type="free_text",
    role="attribute",
    nullable=True,
)

REGISTRY.define("tags", list,
    description="List of classification tags",
    semantic_type="label",
    role="attribute",
    nullable=True,
)

# ── Measures (general) ───────────────────────────────────────────

REGISTRY.define("weight", float,
    description="Weight measurement",
    semantic_type="count",
    role="measure",
    unit="units",
    display_name="Weight",
)

REGISTRY.define("amount", float,
    description="Monetary amount",
    semantic_type="currency_amount",
    role="measure",
    unit="USD",
    display_name="Amount",
)

REGISTRY.define("value", float,
    description="Numeric value or measurement",
    semantic_type="count",
    role="measure",
    unit="units",
    display_name="Value",
)

REGISTRY.define("width", float,
    description="Width dimension",
    semantic_type="count",
    role="measure",
    unit="units",
)

REGISTRY.define("height", float,
    description="Height dimension",
    semantic_type="count",
    role="measure",
    unit="units",
)

REGISTRY.define("threshold", float,
    description="Threshold value for triggering",
    semantic_type="count",
    role="measure",
    unit="units",
)

# ── Attributes ────────────────────────────────────────────────────

REGISTRY.define("unit", str,
    description="Unit of measurement label",
    semantic_type="label",
    role="attribute",
)

REGISTRY.define("ts", str,
    description="Timestamp string (ISO format)",
    semantic_type="timestamp",
    role="attribute",
    nullable=True,
)

REGISTRY.define("active", bool,
    description="Active/inactive flag",
    semantic_type="boolean_flag",
    role="attribute",
)

REGISTRY.define("created", datetime,
    description="Creation timestamp",
    semantic_type="timestamp",
    role="attribute",
    nullable=True,
)


REGISTRY.define("model_name", str,
    description="Model identifier",
    semantic_type="identifier",
    role="dimension",
    synonyms=["model", "algorithm"],
)
