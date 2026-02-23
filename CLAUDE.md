# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Running Tests

```bash
# All tests (requires venv activated)
pytest tests/ -v

# Single test file
pytest tests/test_store.py -v
pytest tests/test_reactive.py -v
pytest tests/test_workflow.py -v

# Single test by name
pytest tests/test_store.py -v -k "test_optimistic_concurrency"
```

### Starting the Deephaven Server

```bash
pip install -r requirements-server.txt
cd server && python3 -i app.py
# Web IDE at http://localhost:10000
```

### Running Clients

```bash
pip install -r requirements-client.txt
cd client && python3 quant_client.py   # or risk_client.py / pm_client.py
```

### Installing Store/Reactive Dependencies

```bash
pip install -r requirements-store.txt
# Installs: reaktiv, psycopg2-binary, pgserver, dbos
```

## Architecture

Four independent layers that can be used separately:

### 1. Deephaven Server (`server/`)
Embeds a Deephaven JVM server (requires Java 11-21). `app.py` creates ticking tables (`prices_raw`, `prices_live`, `risk_raw`, `risk_live`, `portfolio_summary`) using `DynamicTableWriter`. `market_data.py` drives price simulation in a background thread. `risk_engine.py` computes Black-Scholes Greeks.

Clients connect via gRPC (`pydeephaven`) — no Java needed on client machines. Client operations: `session.open_table()`, `table.where()`, `session.run_script()`, `session.bind_table()`.

### 2. Object Store (`store/`)
Embedded PostgreSQL (via `pgserver`) with append-only bi-temporal event sourcing. Single table: `object_events`. Never overwrites — every mutation is a new row.

**Key classes:**
- `Storable` (`base.py`): Base dataclass with `_store_*` metadata fields. Subclass with `@dataclass` to make persistable types.
- `StoreClient` (`client.py`): All operations append events. Constructor: `StoreClient(user, password, host, port, dbname, event_bus=None)`.
- `ObjectStoreServer` (`server.py`): Bootstraps embedded PG. Used in tests.
- `StateMachine` / `Transition` (`state_machine.py`): Attach to a `Storable` class via `MyClass._state_machine = MyLifecycle`.

**Bi-temporal columns:** `tx_time` (system, immutable) + `valid_from`/`valid_to` (business, user-settable). Backdated corrections set `valid_from` in the past — `event_type` becomes `"CORRECTED"` automatically.

**RLS:** Every user has their own PG role. The `owner` and `readers`/`writers` arrays on each event enforce zero-trust access. `app_admin` role bypasses RLS.

**Exceptions:** `VersionConflict` (optimistic concurrency), `InvalidTransition`, `GuardFailure`, `TransitionNotPermitted`.

### 3. Reactive Layer (`reactive/`)
Two components:

**`expr.py` — Expression tree:** Builds computation graphs at definition time; never evaluates until `.eval(ctx)` is called. Leaf nodes: `Field(name)`, `Const(value)`. Operator overloading builds `BinOp`/`UnaryOp`/`StrOp` nodes. Higher-level: `If(cond, then, else)`, `Coalesce([...])`, `IsNull(expr)`, `Func(name, args)`. Compiles to three targets: `eval(ctx: dict)` → Python, `to_sql(col)` → PostgreSQL JSONB, `to_pure(var)` → Legend Pure. Fully serializable via `to_json()` / `from_json()`.

**`graph.py` — ReactiveGraph:** Wraps `reaktiv` (Signal/Computed/Effect). `graph.track(obj)` creates one `Signal` per dataclass field. `graph.computed(node_id, name, expr)` wires an `Expr` as a `Computed`. `graph.group_computed(name, node_ids, field, agg_fn)` aggregates across nodes. `graph.multi_computed(name, fn)` is a free-form cross-node function.

**`bridge.py`:** Factory for auto-persist `Effect` that writes back to the store on reactive changes.

### 4. Workflow Engine (`workflow/`)
`WorkflowEngine` ABC (`engine.py`) is the only interface application code touches. The DBOS-backed implementation (`dbos_engine.py`) is injected — never imported directly. Provides durable steps, queues, sleep, and inter-workflow messaging. `engine.run()` is synchronous (any args); `engine.workflow()` is async (must be serializable). `engine.step()` gives exactly-once semantics on crash recovery.

## Key Patterns

**Adding a new domain type:**
```python
@dataclass
class MyEntity(Storable):
    field_a: str = ""
    field_b: float = 0.0
# Optionally attach a state machine:
MyEntity._state_machine = MyLifecycle
```

**Type identity** is stored as `module.ClassName` (from `Storable.type_name()`). Renaming classes or moving modules breaks existing DB records.

**State machine hook order:** `on_exit` → `action` → `on_enter`.

**Logical operators in expressions use `&`, `|`, `~`** (not `and`/`or`/`not`) because Python's boolean operators can't be overridden.

**Tests use embedded PG:** `ObjectStoreServer` spins up a temporary PostgreSQL instance per test session. Tests in `tests/test_store.py` cover the full stack (serde, bi-temporal queries, RLS, state machines, subscriptions). `tests/test_reactive.py` and `tests/test_reactive_finance.py` test the expression tree and graph. `tests/test_workflow.py` tests the DBOS engine.
