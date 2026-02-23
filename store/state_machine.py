"""
Declarative state machines for Storable lifecycle management.

Supports guards (Expr-based), actions (callables), on_enter/on_exit hooks,
and per-transition user permissions.

    from store.state_machine import StateMachine, Transition
    from reactive.expr import Field, Const

    class OrderLifecycle(StateMachine):
        initial = "PENDING"
        transitions = [
            Transition("PENDING", "PARTIAL"),
            Transition("PENDING", "FILLED",
                       guard=Field("quantity") > Const(0)),
            Transition("PENDING", "CANCELLED",
                       allowed_by=["risk_manager"]),
            Transition("FILLED", "SETTLED",
                       guard=Field("price") > Const(0),
                       action=lambda obj, f, t: book_settlement(obj)),
        ]
        on_enter = {
            "FILLED": [lambda obj, f, t: log_fill(obj)],
        }

    Order._state_machine = OrderLifecycle
"""

from dataclasses import dataclass, field
from typing import Optional, Callable, List


@dataclass
class Transition:
    """
    A single state machine edge with optional guard, action, and permissions.

    - guard: Expr that must evaluate to truthy against the object's data dict.
    - action: callable(obj, from_state, to_state) fired after the transition.
    - allowed_by: list of usernames who can trigger this transition.
      If None, anyone with write access can trigger. Owner is always allowed.
    """
    from_state: str
    to_state: str
    guard: object = None            # Optional[Expr] — avoid circular import
    action: Optional[Callable] = None
    allowed_by: Optional[List[str]] = None


class InvalidTransition(Exception):
    """Raised when the transition edge does not exist."""

    def __init__(self, from_state, to_state, allowed):
        self.from_state = from_state
        self.to_state = to_state
        self.allowed = allowed
        super().__init__(
            f"Cannot transition from '{from_state}' to '{to_state}'. "
            f"Allowed: {allowed}"
        )


class GuardFailure(Exception):
    """Raised when a transition edge exists but the guard evaluates to False."""

    def __init__(self, from_state, to_state, guard):
        self.from_state = from_state
        self.to_state = to_state
        self.guard = guard
        super().__init__(
            f"Guard failed for transition '{from_state}' → '{to_state}'"
        )


class TransitionNotPermitted(Exception):
    """Raised when the user is not authorized to trigger a transition."""

    def __init__(self, from_state, to_state, user, allowed_by):
        self.from_state = from_state
        self.to_state = to_state
        self.user = user
        self.allowed_by = allowed_by
        super().__init__(
            f"User '{user}' not permitted for transition "
            f"'{from_state}' → '{to_state}'. Allowed: {allowed_by}"
        )


class StateMachine:
    """
    Base class for declarative state machines.

    Subclass and define:
        initial: str                    — the starting state
        transitions: list[Transition]   — list of Transition edges
        on_enter: dict[str, list]       — {state: [callbacks]} fired on entry
        on_exit: dict[str, list]        — {state: [callbacks]} fired on exit
    """

    initial: str = None
    transitions: list = []
    on_enter: dict = {}
    on_exit: dict = {}

    @classmethod
    def get_transition(cls, from_state, to_state):
        """Return the Transition object for this edge, or None."""
        for t in cls.transitions:
            if t.from_state == from_state and t.to_state == to_state:
                return t
        return None

    @classmethod
    def validate_transition(cls, from_state, to_state, context=None, user=None):
        """
        Validate and return the Transition object.

        Raises:
            InvalidTransition — edge doesn't exist
            GuardFailure — guard evaluated to False
            TransitionNotPermitted — user not authorized
        """
        t = cls.get_transition(from_state, to_state)
        if t is None:
            allowed = cls.allowed_transitions(from_state)
            raise InvalidTransition(from_state, to_state, allowed)

        # Check guard
        if t.guard is not None and context is not None:
            if not t.guard.eval(context):
                raise GuardFailure(from_state, to_state, t.guard)

        # Check permissions
        if t.allowed_by is not None and user is not None:
            if user not in t.allowed_by:
                raise TransitionNotPermitted(from_state, to_state, user, t.allowed_by)

        return t

    @classmethod
    def allowed_transitions(cls, from_state):
        """Return list of valid next state names from from_state."""
        return [t.to_state for t in cls.transitions if t.from_state == from_state]

    @classmethod
    def fire_on_exit(cls, state, obj, from_state, to_state):
        """Fire on_exit hooks for the given state."""
        for callback in cls.on_exit.get(state, []):
            callback(obj, from_state, to_state)

    @classmethod
    def fire_on_enter(cls, state, obj, from_state, to_state):
        """Fire on_enter hooks for the given state."""
        for callback in cls.on_enter.get(state, []):
            callback(obj, from_state, to_state)
