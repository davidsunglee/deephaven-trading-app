"""
Declarative state machines for Storable lifecycle management.

Define valid states and transitions, register on a Storable class:

    class OrderLifecycle(StateMachine):
        initial = "PENDING"
        transitions = {
            "PENDING":   ["PARTIAL", "FILLED", "CANCELLED"],
            "PARTIAL":   ["FILLED", "CANCELLED"],
            "FILLED":    ["SETTLED"],
        }

    Order._state_machine = OrderLifecycle
"""


class InvalidTransition(Exception):
    """Raised when a state transition is not allowed."""

    def __init__(self, from_state, to_state, allowed):
        self.from_state = from_state
        self.to_state = to_state
        self.allowed = allowed
        super().__init__(
            f"Cannot transition from '{from_state}' to '{to_state}'. "
            f"Allowed: {allowed}"
        )


class StateMachine:
    """
    Base class for declarative state machines.

    Subclass and define:
        initial: str          — the starting state
        transitions: dict     — {state: [allowed_next_states]}
    """

    initial: str = None
    transitions: dict = {}

    @classmethod
    def validate_transition(cls, from_state, to_state):
        """
        Check if transitioning from from_state to to_state is allowed.
        Raises InvalidTransition if not.
        """
        allowed = cls.allowed_transitions(from_state)
        if to_state not in allowed:
            raise InvalidTransition(from_state, to_state, allowed)

    @classmethod
    def allowed_transitions(cls, from_state):
        """Return the list of valid next states from from_state."""
        return list(cls.transitions.get(from_state, []))
