import pytest

from delta_engine.application.failures import ExecutionFailure
from delta_engine.application.ports import (
    CompiledAction,
    CompiledPlan,
    ExecutionResult,
)
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan import ActionPlan, SetTableComment
from tests.builders import build_compiled_comment_plan

_TARGET = QualifiedName("cat", "schema", "table")


def _failed_exec(idx=0, preview="ALTER TABLE ...", exc="ValueError", msg="boom"):
    return ExecutionFailure(
        statement_index=idx,
        exception_type=exc,
        message=msg,
        statement=preview,
    )


def _compiled(*statements: str) -> CompiledPlan:
    return build_compiled_comment_plan(_TARGET, *statements)


def test_compiled_action_rejects_an_empty_statement():
    # Given an action whose compilation produced only whitespace
    action = SetTableComment(desired_comment="new", observed_comment="old")

    # Then pairing them is rejected
    with pytest.raises(ValueError):
        CompiledAction(action=action, statement="  ")


def test_compiled_plan_rejects_an_omitted_action():
    # Given a plan with one action and no compiled statements
    plan = ActionPlan(
        target=_TARGET,
        actions=(SetTableComment(desired_comment="new", observed_comment="old"),),
    )

    # Then the mismatch is rejected — statements must correspond exactly to actions
    with pytest.raises(ValueError):
        CompiledPlan(plan=plan, compiled_actions=())


def test_compiled_plan_copies_mutable_compiled_actions_to_a_tuple():
    # Given a compiled plan built from a mutable list
    action = SetTableComment(desired_comment="new", observed_comment="old")
    plan = ActionPlan(target=_TARGET, actions=(action,))
    compiled_action = CompiledAction(action=action, statement="SQL")
    compiled_actions = [compiled_action]

    compiled = CompiledPlan(
        plan=plan,
        compiled_actions=compiled_actions,  # type: ignore[arg-type]
    )

    # When the caller mutates its list afterwards
    compiled_actions.clear()

    # Then the compiled plan is unaffected
    assert compiled.compiled_actions == (compiled_action,)


def test_execution_result_success_covers_the_complete_plan():
    # Given a two-statement plan fully applied
    compiled = _compiled("SQL 0", "SQL 1")

    result = ExecutionResult(compiled_plan=compiled, applied_count=2)

    # Then the history carries no failure
    assert result.failures == ()
    assert result.applied_count == 2


def test_execution_result_exposes_its_single_failure():
    # Given an execution that stopped at its second statement
    compiled = _compiled("SQL 0", "SQL 1", "SQL 2")

    result = ExecutionResult(
        compiled_plan=compiled,
        applied_count=1,
        failure=_failed_exec(1, "SQL 1", msg="bang"),
    )

    # Then the one failure flattens for reports, after the applied prefix
    assert tuple(f.message for f in result.failures) == ("bang",)
    assert result.applied_count == 1


def test_execution_result_accepts_complete_execution_of_an_empty_plan():
    # Given an empty compiled plan
    result = ExecutionResult(compiled_plan=_compiled())

    # Then zero applied statements is a complete, failure-free history
    assert result.failures == ()
    assert result.applied_count == 0


def test_execution_result_rejects_a_successful_partial_history():
    # When a failure-free history covers only part of the plan
    # Then construction fails — success must cover every statement
    with pytest.raises(ValueError):
        ExecutionResult(compiled_plan=_compiled("SQL 0", "SQL 1"), applied_count=1)


def test_execution_result_rejects_an_applied_count_outside_the_plan():
    # When the applied count lies outside the compiled plan, then construction fails
    with pytest.raises(ValueError):
        ExecutionResult(compiled_plan=_compiled("SQL 0"), applied_count=2)
    with pytest.raises(ValueError):
        ExecutionResult(compiled_plan=_compiled("SQL 0"), applied_count=-1)


def test_execution_result_rejects_a_failure_away_from_the_applied_prefix():
    # When the failure does not sit immediately after the applied prefix
    # Then construction fails — execution stops at its first failure
    with pytest.raises(ValueError):
        ExecutionResult(
            compiled_plan=_compiled("SQL 0", "SQL 1"),
            applied_count=0,
            failure=_failed_exec(1, "SQL 1"),
        )


def test_execution_result_rejects_a_failure_for_a_different_statement():
    # When the failure carries a statement the plan never compiled
    # Then construction fails
    with pytest.raises(ValueError):
        ExecutionResult(
            compiled_plan=_compiled("SQL 0"),
            applied_count=0,
            failure=_failed_exec(0, "OTHER SQL"),
        )


def test_execution_result_rejects_a_failure_beyond_a_fully_applied_plan():
    # When a failure is claimed after every statement already applied
    # Then construction fails
    with pytest.raises(ValueError):
        ExecutionResult(
            compiled_plan=_compiled("SQL 0"),
            applied_count=1,
            failure=_failed_exec(1, "SQL 0"),
        )
