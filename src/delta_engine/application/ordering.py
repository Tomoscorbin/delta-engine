"""
Deterministic ordering for action plans.

Orders by execution phase, then by subject name within a phase. Both the phase
and the subject are declared by each action type (see :class:`Action`), so this
module stays agnostic of concrete action types -- a new action orders itself.
"""

from delta_engine.domain.plan import Action


def action_sort_key(action: Action) -> tuple[int, str]:
    """
    Return a sortable key for deterministic action ordering.

    Orders by the action's execution phase, then by its subject name.
    """
    return (action.phase, action.subject)
