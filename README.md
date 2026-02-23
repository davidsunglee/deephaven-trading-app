# Deephaven Real-Time Trading Platform

A **server/client** real-time trading platform built on [Deephaven.io](https://deephaven.io), with a **reactive object store** backed by embedded PostgreSQL and a **domain-agnostic expression language** that compiles to Python, SQL, and Legend Pure.

## Architecture

```
┌──────────────────────────────────────────────────────┐
│  SERVER  (server/app.py)        Port 10000           │
│  • Deephaven embedded server (JVM)                   │
│  • DynamicTableWriter: prices, risk                  │
│  • Derived tables: prices_live, risk_live, etc.      │
│  • Market data simulation thread                     │
│  • Web IDE at http://localhost:10000                  │
└───────────────────────┬──────────────────────────────┘
                        │ gRPC (pydeephaven)
           ┌────────────┼────────────────┐
           │            │                │
     ┌─────▼────┐ ┌────▼─────┐   ┌──────▼─────┐
     │  Quant   │ │   Risk   │   │     PM     │
     │  Client  │ │  Client  │   │   Client   │
     └──────────┘ └──────────┘   └────────────┘

┌──────────────────────────────────────────────────────┐
│  OBJECT STORE  (store/)                              │
│  • Embedded PostgreSQL with Row-Level Security       │
│  • Zero-trust: one role per user, RLS-enforced       │
│  • Bi-temporal event sourcing (tx_time + valid_time) │
│  • Append-only: never overwrite, full audit trail    │
│  • Declarative state machines for lifecycle mgmt     │
│  • owner + updated_by on every event for audit       │
└───────────────────────┬──────────────────────────────┘
                        │
┌───────────────────────▼──────────────────────────────┐
│  REACTIVE LAYER  (reactive/)                         │
│  • Expression tree: eval() → Python, to_sql() → PG, │
│    to_pure() → Legend Pure                           │
│  • ReactiveGraph: field Signals → Computed → Effects │
│  • Auto-persist bridge to object store               │
└──────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites
- **Python 3.10+**
- **Java 11-21** (set `JAVA_HOME`) — only for Deephaven server

### 1. Start the Deephaven Server

```bash
pip install -r requirements-server.txt
cd server
python3 -i app.py
```

The server starts on **http://localhost:10000** — open this in a browser to access the Deephaven Web IDE with all shared ticking tables.

### 2. Run a Client

In a separate terminal:

```bash
pip install -r requirements-client.txt
cd client

# Pick one:
python3 quant_client.py          # Watchlists, top movers, volume leaders
python3 risk_client.py           # Large exposures, risk scoring
python3 pm_client.py             # P&L snapshots, position sizing
```

Clients connect via `pydeephaven` (lightweight — **no Java needed** on client machines). Tables created by clients are visible in the Web IDE.

### 3. Object Store + Reactive Layer

```bash
pip install -r requirements-store.txt
```

```python
from dataclasses import dataclass
from store.base import Storable
from store.state_machine import StateMachine
from reactive import Field, Const, If, Func, ReactiveGraph

# Define any domain object
@dataclass
class Position(Storable):
    symbol: str = ""
    quantity: int = 0
    avg_cost: float = 0.0
    current_price: float = 0.0

# Build reactive computations
graph = ReactiveGraph()
pos = Position(symbol="AAPL", quantity=100, avg_cost=220.0, current_price=230.0)
node_id = graph.track(pos)

# Expression tree — never computes at definition time
pnl = (Field("current_price") - Field("avg_cost")) * Field("quantity")
graph.computed(node_id, "pnl", pnl)

print(graph.get(node_id, "pnl"))        # 1000.0

# React to market data updates
graph.update(node_id, "current_price", 235.0)
print(graph.get(node_id, "pnl"))        # 1500.0

# Same expression compiles to SQL and Legend Pure
print(pnl.to_sql("data"))
# ((data->>'current_price')::float - (data->>'avg_cost')::float) * (data->>'quantity')::float

print(pnl.to_pure("$pos"))
# (($pos.current_price - $pos.avg_cost) * $pos.quantity)
```

## Event Subscriptions

Two-tier notification system — zero external infrastructure:

```python
from store.subscriptions import EventBus, SubscriptionListener

# Tier 1: In-process EventBus — synchronous callbacks after DB writes
bus = EventBus()
bus.on("Order", lambda e: print(f"{e.event_type} on {e.entity_id}"))
bus.on_entity(entity_id, lambda e: recalc_risk(e))
bus.on_all(lambda e: audit_log(e))

client = StoreClient(user="alice", ..., event_bus=bus)
client.write(order)            # → bus fires ChangeEvent(event_type="CREATED")
client.transition(order, "FILLED")  # → bus fires ChangeEvent(event_type="STATE_CHANGE")

# Tier 2: Cross-process LISTEN/NOTIFY — real-time PG notifications
listener = SubscriptionListener(
    event_bus=bus,
    host=host, port=port, dbname=dbname,
    user="bob", password="bob_pw",
    subscriber_id="risk_engine",       # optional: persists checkpoint for crash recovery
)
listener.start()   # background thread: LISTEN + catch-up from checkpoint
# ... any client on any connection that writes triggers notification ...
listener.stop()
```

**Durable catch-up**: if the listener is down when events happen, it replays missed events from the append-only log on reconnect. With `subscriber_id`, the checkpoint persists to DB — survives crashes.

## Bi-Temporal Event Sourcing

The object store is **append-only** — every write, update, or state change creates an immutable event. Nothing is ever overwritten or deleted.

Every event carries two time dimensions:

| Column | Meaning | Who sets it |
|--------|---------|-------------|
| `tx_time` | When we recorded this fact | System (`now()`, immutable) |
| `valid_from` | When this fact becomes effective | User (defaults to `now()`) |
| `owner` | Entity owner (controls RLS visibility) | System (from first write) |
| `updated_by` | Who made this specific change | System (`current_user`) |

### Four Query Modes

```python
# Current state (default)
trade = client.read(Trade, entity_id)

# Full history — every version, including tombstones
versions = client.history(Trade, entity_id)

# As-of transaction time: "what did we know at noon?"
old = client.as_of(Trade, entity_id, tx_time=noon)

# As-of valid time: "what was effective at 10am?"
eff = client.as_of(Trade, entity_id, valid_time=ten_am)

# Full bi-temporal: "what did we know at noon about 10am?"
snap = client.as_of(Trade, entity_id, tx_time=noon, valid_time=ten_am)
```

### Backdated Corrections

```python
# Discover at 3pm that a 10am trade had wrong price
trade.price = 151.25
client.update(trade, valid_from=datetime(2026, 2, 22, 10, 0, tzinfo=timezone.utc))
# event_type automatically set to "CORRECTED"
```

### Optimistic Concurrency (Automatic)

```python
# Framework tracks versions automatically — no user action needed
trade = client.read(Trade, entity_id)          # _store_version=3
trade.price = 152.0
client.update(trade)                           # checks version 3 → succeeds → now version 4

# If someone else updated in between, raises VersionConflict
stale = client.read(Trade, entity_id)          # _store_version=4
# ... someone else updates to version 5 ...
stale.price = 999.0
client.update(stale)                           # raises VersionConflict (expected 4, actual 5)
```

### Bulk Operations

```python
# Atomic batch write — all-or-nothing
orders = [Order(symbol=s, quantity=100, price=0.0, side="BUY") for s in ["AAPL", "GOOG", "MSFT"]]
entity_ids = client.write_many(orders)         # single transaction

# Atomic batch update — version checks are automatic
client.update_many([o1, o2, o3])
```

### Pagination

```python
# Cursor-based pagination
page1 = client.query(Trade, filters={"side": "BUY"}, limit=50)
for trade in page1:
    process(trade)

if page1.next_cursor:
    page2 = client.query(Trade, filters={"side": "BUY"}, limit=50, cursor=page1.next_cursor)
```

### Audit Log

```python
# Full audit trail: who changed what, when
trail = client.audit(entity_id)
for entry in trail:
    print(f"v{entry['version']} {entry['event_type']} by {entry['updated_by']} at {entry['tx_time']}")
# v1 CREATED by alice at 2026-02-22 10:00:00
# v2 UPDATED by bob at 2026-02-22 10:05:00
# v3 STATE_CHANGE by alice at 2026-02-22 10:10:00
```

### Event Types

| Type | Meaning |
|------|--------|
| `CREATED` | Entity first written |
| `UPDATED` | Data changed |
| `DELETED` | Soft-delete tombstone |
| `STATE_CHANGE` | Lifecycle state transition |
| `CORRECTED` | Backdated correction (`valid_from` in the past) |

## State Machines

Declarative lifecycle management with **guards**, **actions**, **hooks**, and **per-transition permissions**:

```python
from store.state_machine import StateMachine, Transition
from reactive.expr import Field, Const

class OrderLifecycle(StateMachine):
    initial = "PENDING"
    transitions = [
        Transition("PENDING", "PARTIAL"),
        Transition("PENDING", "FILLED",
                   guard=Field("quantity") > Const(0)),       # Expr guard
        Transition("PENDING", "CANCELLED",
                   allowed_by=["risk_manager"]),               # Permission
        Transition("PARTIAL", "FILLED"),
        Transition("PARTIAL", "CANCELLED"),
        Transition("FILLED", "SETTLED",
                   guard=Field("price") > Const(0),
                   action=lambda obj, f, t: book_settlement(obj)),
    ]
    on_enter = {"FILLED": [lambda obj, f, t: notify_risk(obj)]}
    on_exit  = {"PENDING": [lambda obj, f, t: log_departure(obj)]}

@dataclass
class Order(Storable):
    symbol: str = ""
    quantity: int = 0
    price: float = 0.0
    side: str = ""

Order._state_machine = OrderLifecycle
```

### Guards, Actions, Permissions

| Feature | Description |
|---------|------------|
| **Guard** | `Expr` evaluated against object data. Raises `GuardFailure` if False. |
| **Action** | `callable(obj, from_state, to_state)` fired after transition. |
| **allowed_by** | List of usernames permitted to trigger. Raises `TransitionNotPermitted`. |
| **on_enter/on_exit** | Hooks fired when entering/leaving any state. |

Hooks fire in order: `on_exit` → `action` → `on_enter`.

```python
client.write(order)                    # state = "PENDING"
client.transition(order, "FILLED")     # guard passes, on_exit + on_enter fire
client.transition(order, "SETTLED")    # guard passes, action fires
client.transition(order, "PENDING")    # raises InvalidTransition

# Full state history with audit trail
for v in client.history(Order, order._store_entity_id):
    print(f"v{v._store_version}: {v._store_state} by {v._store_updated_by}")
```

## Cross-Entity Reactive Computations

Aggregate computations that span multiple tracked objects and auto-recompute when any member changes:

```python
from reactive.graph import ReactiveGraph
from reactive.expr import Field

graph = ReactiveGraph()
n1 = graph.track(position_aapl)   # quantity=100, price=228
n2 = graph.track(position_goog)   # quantity=50, price=192

# Per-entity computeds
mv = Field("price") * Field("quantity")
graph.computed(n1, "mv", mv)
graph.computed(n2, "mv", mv)

# group_computed — aggregate across nodes
graph.group_computed("portfolio_value", [n1, n2], "mv", sum)
print(graph.get_group("portfolio_value"))   # 32400.0

# multi_computed — arbitrary cross-node function
graph.multi_computed("spread", lambda g: g.get(n1, "mv") - g.get(n2, "mv"))

# Dynamic membership — add/remove nodes
n3 = graph.track(position_tsla)
graph.computed(n3, "mv", mv)
graph.add_to_group("portfolio_value", n3)

# Updates propagate through the entire graph
graph.update(n1, "price", 230.0)           # portfolio_value auto-recomputes

# group_effect — fire on group changes
graph.group_effect("portfolio_value", lambda name, val: alert_if_low(val))
```

## Reactive Expression Language

A typed expression tree that builds at definition time and compiles to three targets:

| Target | Method | Use case |
|--------|--------|----------|
| **Python** | `expr.eval(ctx)` | Powers reaktiv Computed values |
| **PostgreSQL** | `expr.to_sql(col)` | JSONB push-down queries |
| **Legend Pure** | `expr.to_pure(var)` | FINOS Legend Engine integration |

### Supported Operations

| Category | Operations |
|----------|-----------|
| **Arithmetic** | `+`, `-`, `*`, `/`, `%`, `**`, negation, abs |
| **Comparison** | `>`, `<`, `>=`, `<=`, `==`, `!=` |
| **Logical** | `&` (AND), `\|` (OR), `~` (NOT) |
| **Conditionals** | `If(cond, then, else)` → `CASE WHEN` in SQL |
| **Null handling** | `Coalesce([...])`, `IsNull(expr)`, `.is_null()` |
| **Functions** | `sqrt`, `ceil`, `floor`, `round`, `log`, `exp`, `min`, `max` |
| **String** | `.length()`, `.upper()`, `.lower()`, `.contains()`, `.starts_with()`, `.concat()` |

Expressions are fully serializable via `to_json()` / `from_json()` for persistence and inspection.

### Example: Risk Alert

```python
from reactive import Field, Const, If, Func

# Stop-loss: alert if unrealized loss exceeds threshold
pnl = (Field("current_price") - Field("avg_cost")) * Field("quantity")
alert = If(pnl < Const(-5000), Const("STOP_LOSS"), Const("OK"))

# Nested conditionals
confidence = If(
    Field("strength") > Const(0.75), Const("HIGH"),
    If(Field("strength") > Const(0.5), Const("MEDIUM"), Const("LOW")),
)

# Option intrinsic value
intrinsic = If(
    Field("underlying_price") > Field("strike"),
    Field("underlying_price") - Field("strike"),
    Const(0),
)
```

## Project Structure

```
windsurf-project/
├── server/
│   ├── app.py              # Deephaven server + data engine
│   ├── market_data.py      # Market data simulation thread
│   ├── risk_engine.py      # Black-Scholes Greeks calculator
│   └── start_server.sh     # Launch script
├── client/
│   ├── base_client.py      # Reusable connection helper
│   ├── quant_client.py     # Quant: filtered views, derived tables
│   ├── risk_client.py      # Risk: exposure monitoring, alerts
│   └── pm_client.py        # PM: portfolio summary, P&L snapshots
├── store/
│   ├── base.py             # Storable base class + bi-temporal metadata
│   ├── models.py           # Domain models: Trade, Order, Signal
│   ├── server.py           # Embedded PG server bootstrap
│   ├── client.py           # StoreClient (event-sourced, bi-temporal)
│   ├── schema.py           # DDL: object_events table + RLS policies
│   ├── state_machine.py    # Declarative StateMachine + InvalidTransition
│   ├── permissions.py      # Share/unshare entities between users
│   └── subscriptions.py    # EventBus + SubscriptionListener + checkpoints
├── reactive/
│   ├── expr.py             # Expression tree (eval/to_sql/to_pure)
│   ├── graph.py            # ReactiveGraph (Signal/Computed/Effect/Groups)
│   └── bridge.py           # Auto-persist effect factory
├── tests/
│   ├── test_store.py       # Bi-temporal + state machine + RLS + subs tests (127)
│   ├── test_reactive.py    # Expression + graph + cross-entity tests (137)
│   └── test_reactive_finance.py  # Finance domain tests (49)
├── requirements-server.txt
├── requirements-client.txt
├── requirements-store.txt  # reaktiv, psycopg2-binary, pgserver
└── README.md
```

## Published Server Tables

| Table | Description |
|-------|-------------|
| `prices_raw` | Append-only price ticks |
| `prices_live` | Latest price per symbol (ticking) |
| `risk_raw` | Append-only risk ticks |
| `risk_live` | Latest risk per symbol (ticking) |
| `portfolio_summary` | Aggregated portfolio metrics |

## Client Capabilities

| Feature | How |
|---------|-----|
| Read shared tables | `session.open_table("prices_live")` |
| Filter / sort | `table.where(...)`, `table.sort(...)` |
| Create server-side views | `session.run_script("...")` |
| Publish tables | `session.bind_table(name, table)` |
| Export to pandas | `table.to_arrow().to_pandas()` |
| Subscribe to ticks | `pydeephaven-ticking` listener API |

## Symbols

| Symbol | Base Price |
|--------|-----------|
| AAPL | $228 |
| GOOGL | $192 |
| MSFT | $415 |
| AMZN | $225 |
| TSLA | $355 |
| NVDA | $138 |
| META | $700 |
| NFLX | $1,020 |

## License

MIT
