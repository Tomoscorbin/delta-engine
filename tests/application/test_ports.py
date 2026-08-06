import pytest

from delta_engine.application.failures import ExecutionFailure
from delta_engine.application.ports import (
    CompiledAction,
    CompiledPlan,
    ExecutionResult,
)
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan import ActionPlan, SetTableComment
from tests.builders import build_compiled_plan


def _failed_exec(idx=0, preview="ALTER TABLE ...", exc="ValueError", msg="boom"):
    return ExecutionFailure(
        statement_index=idx,
        exception_type=exc,
        message=msg,
        statement=preview,
    )


def _compiled(*statements: str) -> CompiledPlan:
    plan = ActionPlan(
        target=QualifiedName("cat", "schema", "table"),
        actions=tuple(
            SetTableComment(desired_comment=f"new {index}", observed_comment=f"old {index}")
            for index in range(len(statements))
        ),
    )
    return build_compiled_plan(plan, statements)


def test_compiled_action_rejects_an_empty_statement():
    action = SetTableComment(desired_comment="new", observed_comment="old")

    with pytest.raises(ValueError, match="SetTableComment compiled to an empty statement"):
        CompiledAction(action=action, statement="  ")


def test_compiled_plan_rejects_an_omitted_action():
    plan = ActionPlan(
        target=QualifiedName("cat", "schema", "table"),
        actions=(SetTableComment(desired_comment="new", observed_comment="old"),),
    )

    with pytest.raises(ValueError, match="correspond exactly"):
        CompiledPlan(plan=plan, compiled_actions=())


def test_compiled_plan_copies_mutable_compiled_actions_to_a_tuple():
    action = SetTableComment(desired_comment="new", observed_comment="old")
    plan = ActionPlan(
        target=QualifiedName("cat", "schema", "table"),
        actions=(action,),
    )
    compiled_action = CompiledAction(action=action, statement="SQL")
    compiled_actions = [compiled_action]

    compiled = CompiledPlan(
        plan=plan,
        compiled_actions=compiled_actions,  # type: ignore[arg-type]
    )
    compiled_actions.clear()

    assert compiled.compiled_actions == (compiled_action,)


def test_execution_result_success_covers_the_complete_plan():
    compiled = _compiled("SQL 0", "SQL 1")

    result = ExecutionResult(compiled_plan=compiled, applied_count=2)

    assert result.failures == ()
    assert result.applied_count == 2


def test_execution_result_exposes_its_single_failure():
    compiled = _compiled("SQL 0", "SQL 1", "SQL 2")

    result = ExecutionResult(
        compiled_plan=compiled,
        applied_count=1,
        failure=_failed_exec(1, "SQL 1", msg="bang"),
    )

    assert tuple(f.message for f in result.failures) == ("bang",)
    assert result.applied_count == 1


def test_execution_result_accepts_complete_execution_of_an_empty_plan():
    result = ExecutionResult(compiled_plan=_compiled())

    assert result.failures == ()
    assert result.applied_count == 0


def test_execution_result_rejects_a_successful_partial_history():
    with pytest.raises(ValueError, match="complete compiled plan"):
        ExecutionResult(compiled_plan=_compiled("SQL 0", "SQL 1"), applied_count=1)


def test_execution_result_rejects_an_applied_count_outside_the_plan():
    with pytest.raises(ValueError, match="within the compiled plan"):
        ExecutionResult(compiled_plan=_compiled("SQL 0"), applied_count=2)
    with pytest.raises(ValueError, match="within the compiled plan"):
        ExecutionResult(compiled_plan=_compiled("SQL 0"), applied_count=-1)


def test_execution_result_rejects_a_failure_away_from_the_applied_prefix():
    with pytest.raises(ValueError, match="first failure"):
        ExecutionResult(
            compiled_plan=_compiled("SQL 0", "SQL 1"),
            applied_count=0,
            failure=_failed_exec(1, "SQL 1"),
        )


def test_execution_result_rejects_a_failure_for_a_different_statement():
    with pytest.raises(ValueError, match="compiled statement"):
        ExecutionResult(
            compiled_plan=_compiled("SQL 0"),
            applied_count=0,
            failure=_failed_exec(0, "OTHER SQL"),
        )


def test_execution_result_rejects_a_failure_beyond_a_fully_applied_plan():
    with pytest.raises(ValueError, match="within the compiled plan"):
        ExecutionResult(
            compiled_plan=_compiled("SQL 0"),
            applied_count=1,
            failure=_failed_exec(1, "SQL 0"),
        )
