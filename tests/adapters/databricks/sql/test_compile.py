import inspect

from delta_engine.adapters.databricks.sql.compile import _compile_action, compile_plan
from delta_engine.domain.model import Column, Integer, QualifiedName
import delta_engine.domain.plan.actions as actions_module
from delta_engine.domain.plan.actions import Action, ActionPlan, AddColumn

_TARGET = QualifiedName("cat", "sch", "tbl")


def _concrete_action_types():
    """
    Return every concrete Action subclass the actions module exposes.

    Discovered from the module namespace rather than ``Action.__subclasses__()``
    because ``@dataclass(slots=True)`` leaves a stale pre-slots class object in
    ``__subclasses__()``; the module binds each name to the real class.
    """
    return [
        obj
        for _, obj in inspect.getmembers(actions_module, inspect.isclass)
        if issubclass(obj, Action)
        and obj is not Action
        and not getattr(obj, "__abstractmethods__", False)
    ]


def _compile_single(action) -> str:
    """Compile a one-action plan and return the single SQL statement."""
    (statement,) = compile_plan(ActionPlan(target=_TARGET, actions=(action,)))
    return statement


def test_add_column_with_comment_includes_comment_clause():
    # Given a new column that carries a comment
    action = AddColumn(Column("age", Integer(), comment="user age"))

    # When compiling the add
    statement = _compile_single(action)

    # Then the ADD COLUMN carries the comment
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl` ADD COLUMN `age` INT COMMENT 'user age'"
    )


def test_add_column_without_comment_omits_comment_clause():
    # Given a new column with no comment (the common case)
    action = AddColumn(Column("age", Integer()))

    # When compiling the add
    statement = _compile_single(action)

    # Then no empty COMMENT clause is emitted
    assert statement == "ALTER TABLE `cat`.`sch`.`tbl` ADD COLUMN `age` INT"


def test_every_action_type_has_a_registered_compiler():
    # Given every concrete Action subclass the domain defines
    fallback = _compile_action.dispatch(object)

    # Then each one resolves to a dedicated compiler, not the raising fallback
    # (so a newly added action cannot silently lack SQL until it hits a plan)
    unregistered = [
        action_type.__name__
        for action_type in _concrete_action_types()
        if _compile_action.dispatch(action_type) is fallback
    ]
    assert unregistered == []
