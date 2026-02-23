"""
Bridge between the reactive graph and the object store.

Provides an effect factory that auto-persists objects to the store
whenever computed values change.
"""


def auto_persist_effect(graph, node_id, store_client, obj):
    """
    Create an effect that writes `obj` back to the store whenever
    any computed value on `node_id` changes.

    Args:
        graph: ReactiveGraph instance
        node_id: ID returned by graph.track()
        store_client: StoreClient instance (must have write access)
        obj: The Storable object being tracked

    Returns:
        List of effect names created (one per computed on this node).
    """
    node = graph._get_node(node_id)
    effect_names = []

    for name in list(node.computeds.keys()):
        effect_key = f"_persist_{name}"

        def make_callback(computed_name):
            def callback(name, value):
                # Sync the computed value back to the object if it has that attr
                if hasattr(obj, computed_name):
                    setattr(obj, computed_name, value)
                store_client.update(obj)
            return callback

        graph.effect(node_id, name, make_callback(name))
        effect_names.append(name)

    return effect_names
