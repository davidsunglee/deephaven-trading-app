"""
ReactiveGraph — wires Storable objects to reaktiv Signals/Computed/Effects.

Any @dataclass Storable can be tracked: its fields become Signals,
expressions become Computed values, and Effects fire on change.
"""

import uuid
import asyncio
import dataclasses
from reaktiv import Signal, Computed, Effect, batch


class ReactiveGraph:
    """
    Reactive computation graph for Storable objects.

    Usage:
        graph = ReactiveGraph()
        node_id = graph.track(my_trade)
        graph.computed(node_id, "market_value", Field("price") * Field("quantity"))
        graph.effect(node_id, "market_value", lambda name, val: print(f"{name}={val}"))
        graph.update(node_id, "price", 230.5)
        print(graph.get(node_id, "market_value"))
    """

    def __init__(self):
        self._nodes = {}       # node_id → _TrackedNode
        self._groups = {}      # name → _GroupNode

    def track(self, obj) -> str:
        """
        Register a Storable object. Each dataclass field becomes a Signal.
        Returns a node_id string for referencing this object in the graph.
        """
        if not dataclasses.is_dataclass(obj):
            raise TypeError(f"{type(obj).__name__} is not a dataclass")

        node_id = str(uuid.uuid4())
        signals = {}
        for f in dataclasses.fields(obj):
            if f.name.startswith("_store_"):
                continue
            value = getattr(obj, f.name)
            signals[f.name] = Signal(value)

        self._nodes[node_id] = _TrackedNode(
            obj=obj,
            signals=signals,
            computeds={},
            effects={},
        )
        return node_id

    def computed(self, node_id: str, name: str, expr) -> None:
        """
        Define a computed value on a tracked object.
        `expr` is an Expr from reactive.expr — it will be evaluated
        against the object's current field values whenever they change.
        """
        node = self._get_node(node_id)

        def make_compute_fn():
            """Create the compute function that reads signals and evaluates the expr."""
            def compute():
                ctx = {}
                for field_name, sig in node.signals.items():
                    ctx[field_name] = sig()
                return expr.eval(ctx)
            return compute

        computed_signal = Computed(make_compute_fn())
        node.computeds[name] = computed_signal

    def effect(self, node_id: str, name: str, callback) -> None:
        """
        Attach a side-effect that fires when a computed value changes.
        callback(name, value) is called with the computed's name and new value.

        Effects run inside an asyncio event loop managed by the graph.
        """
        node = self._get_node(node_id)
        if name not in node.computeds:
            raise KeyError(f"No computed '{name}' on node {node_id}")

        computed_signal = node.computeds[name]
        effect_name = name  # capture for closure

        def effect_fn():
            value = computed_signal()
            callback(effect_name, value)

        eff = Effect(effect_fn)
        node.effects[name] = eff
        # Run the effect once immediately so it registers its dependencies
        self._tick()

    def update(self, node_id: str, field: str, value) -> None:
        """Update a single field's Signal, triggering recomputation cascade."""
        node = self._get_node(node_id)
        if field not in node.signals:
            raise KeyError(f"No field '{field}' on node {node_id}")
        node.signals[field].set(value)
        # Also update the underlying object
        setattr(node.obj, field, value)
        # Run effects
        self._tick()

    def batch_update(self, node_id: str, updates: dict) -> None:
        """
        Update multiple fields atomically.
        Effects fire only once after all fields are set (not per-field).
        """
        node = self._get_node(node_id)
        with batch():
            for field, value in updates.items():
                if field not in node.signals:
                    raise KeyError(f"No field '{field}' on node {node_id}")
                node.signals[field].set(value)
                setattr(node.obj, field, value)
        self._tick()

    def get(self, node_id: str, name: str):
        """Read the current value of a computed signal."""
        node = self._get_node(node_id)
        if name not in node.computeds:
            raise KeyError(f"No computed '{name}' on node {node_id}")
        return node.computeds[name]()

    def get_field(self, node_id: str, field: str):
        """Read the current value of a field signal."""
        node = self._get_node(node_id)
        if field not in node.signals:
            raise KeyError(f"No field '{field}' on node {node_id}")
        return node.signals[field]()

    def remove_effect(self, node_id: str, name: str) -> None:
        """Remove an effect from a tracked node."""
        node = self._get_node(node_id)
        if name in node.effects:
            eff = node.effects.pop(name)
            eff.dispose()

    def untrack(self, node_id: str) -> None:
        """Remove a node from the graph, cleaning up all signals/computeds/effects."""
        node = self._nodes.pop(node_id, None)
        if node is None:
            return
        for eff in node.effects.values():
            eff.dispose()
        node.effects.clear()
        node.computeds.clear()
        node.signals.clear()

    # ── Cross-entity group computations ──────────────────────────────

    def group_computed(self, name, node_ids, computed_name, reduce_fn):
        """
        Aggregate a named computed value across multiple nodes.

        Args:
            name: unique name for this group computation
            node_ids: list of node_id strings to aggregate
            computed_name: name of the per-node computed to aggregate
            reduce_fn: callable that takes a list of values (e.g. sum, max)

        The group auto-recomputes when any member's computed changes
        or when nodes are added/removed via add_to_group/remove_from_group.
        """
        if name in self._groups:
            raise KeyError(f"Group '{name}' already exists")

        # Signal holding the member list — mutating it triggers recomputation
        ids_signal = Signal(list(node_ids))

        graph_ref = self

        def compute():
            members = ids_signal()
            values = []
            for nid in members:
                if nid in graph_ref._nodes:
                    node = graph_ref._nodes[nid]
                    if computed_name in node.computeds:
                        values.append(node.computeds[computed_name]())
            return reduce_fn(values)

        group = _GroupNode(
            computed=Computed(compute),
            node_ids_signal=ids_signal,
            computed_name=computed_name,
            reduce_fn=reduce_fn,
        )
        self._groups[name] = group

    def multi_computed(self, name, fn):
        """
        Define an arbitrary cross-node computed.

        fn(graph) is called with the ReactiveGraph instance.
        Use graph.get(node_id, name) and graph.get_field(node_id, field)
        inside fn to read from any tracked node — dependencies are
        automatically tracked by the signal graph.

        Example:
            graph.multi_computed("spread", lambda g: g.get(n1, "mv") - g.get(n2, "mv"))
        """
        if name in self._groups:
            raise KeyError(f"Group '{name}' already exists")

        graph_ref = self

        def compute():
            return fn(graph_ref)

        group = _GroupNode(computed=Computed(compute), fn=fn)
        self._groups[name] = group

    def get_group(self, name):
        """Read the current value of a group or multi computed."""
        if name not in self._groups:
            raise KeyError(f"No group '{name}'")
        return self._groups[name].computed()

    def group_effect(self, name, callback):
        """
        Attach a side-effect that fires when a group computed changes.
        callback(name, value) is called with the group name and new value.
        """
        if name not in self._groups:
            raise KeyError(f"No group '{name}'")

        group = self._groups[name]
        group_name = name

        def effect_fn():
            value = group.computed()
            callback(group_name, value)

        eff = Effect(effect_fn)
        group.effects[f"_effect_{len(group.effects)}"] = eff
        self._tick()

    def add_to_group(self, name, node_id):
        """Dynamically add a node to a group_computed. Triggers recomputation."""
        if name not in self._groups:
            raise KeyError(f"No group '{name}'")
        group = self._groups[name]
        if group.node_ids_signal is None:
            raise ValueError(f"Group '{name}' is a multi_computed — use group_computed for dynamic membership")
        current = list(group.node_ids_signal())
        if node_id not in current:
            current.append(node_id)
            group.node_ids_signal.set(current)
            self._tick()

    def remove_from_group(self, name, node_id):
        """Dynamically remove a node from a group_computed. Triggers recomputation."""
        if name not in self._groups:
            raise KeyError(f"No group '{name}'")
        group = self._groups[name]
        if group.node_ids_signal is None:
            raise ValueError(f"Group '{name}' is a multi_computed — use group_computed for dynamic membership")
        current = list(group.node_ids_signal())
        if node_id in current:
            current.remove(node_id)
            group.node_ids_signal.set(current)
            self._tick()

    def remove_group(self, name):
        """Tear down a group computed and its effects."""
        group = self._groups.pop(name, None)
        if group is None:
            return
        for eff in group.effects.values():
            eff.dispose()
        group.effects.clear()

    def _get_node(self, node_id: str):
        if node_id not in self._nodes:
            raise KeyError(f"Node {node_id} not tracked")
        return self._nodes[node_id]

    def _tick(self):
        """Process pending effects by running the event loop briefly."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is None or loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(asyncio.sleep(0))


class _TrackedNode:
    """Internal state for a tracked object."""

    __slots__ = ("obj", "signals", "computeds", "effects")

    def __init__(self, obj, signals, computeds, effects):
        self.obj = obj
        self.signals = signals
        self.computeds = computeds
        self.effects = effects


class _GroupNode:
    """Internal state for a cross-entity group computation."""

    __slots__ = ("computed", "effects", "node_ids_signal", "computed_name", "reduce_fn", "fn")

    def __init__(self, computed, node_ids_signal=None, computed_name=None, reduce_fn=None, fn=None):
        self.computed = computed
        self.effects = {}
        self.node_ids_signal = node_ids_signal
        self.computed_name = computed_name
        self.reduce_fn = reduce_fn
        self.fn = fn
