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
