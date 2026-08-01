import pytest

from delta_engine.application.failures import ExecutionFailure
from delta_engine.application.ports import (
    CompiledAction,
    CompiledPlan,
    ExecutionSucceeded,
    ExecutionSummary,
)
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan import ActionPlan, SetTableComment
from tests.builders import build_compiled_plan


def _ok_exec(idx=0, preview="ALTER TABLE ..."):
    return ExecutionSucceeded(statement_index=idx, statement=preview)


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


def test_execution_summary_copies_mutable_results_to_a_tuple():
    compiled = _compiled("SQL")
    succeeded = _ok_exec(0, "SQL")
    results = [succeeded]

    summary = ExecutionSummary(
        compiled_plan=compiled,
        results=results,  # type: ignore[arg-type]
    )
    results.clear()

    assert summary.results == (succeeded,)


def test_execution_summary_reports_no_failure_when_every_statement_succeeds():
    # Given a run whose statements all executed
    compiled = _compiled("SQL 0", "SQL 1")
    summary = ExecutionSummary(
        compiled_plan=compiled,
        results=(_ok_exec(0, "SQL 0"), _ok_exec(1, "SQL 1")),
    )

    # Then the summary reports success with no failures
    assert not summary.failures
    assert summary.applied_count == 2


def test_execution_summary_exposes_the_failures_among_mixed_results():
    # Given a run whose second statement failed, leaving the third unattempted
    compiled = _compiled("SQL 0", "SQL 1", "SQL 2")
    summary = ExecutionSummary(
        compiled_plan=compiled,
        results=(_ok_exec(0, "SQL 0"), _failed_exec(1, "SQL 1", msg="bang")),
    )

    # Then the summary surfaces the single failure and the applied count
    assert tuple(f.message for f in summary.failures) == ("bang",)
    assert summary.applied_count == 1


def test_execution_summary_accepts_complete_execution_of_an_empty_plan():
    summary = ExecutionSummary(compiled_plan=_compiled())

    # Then it is an empty, non-failing summary
    assert summary.results == ()
    assert not summary.failures
    assert summary.applied_count == 0


def test_execution_summary_rejects_non_contiguous_statement_indexes():
    compiled = _compiled("SQL 0")

    with pytest.raises(ValueError, match="contiguous"):
        ExecutionSummary(compiled_plan=compiled, results=(_ok_exec(1, "SQL 0"),))


def test_execution_summary_rejects_results_after_a_failure():
    compiled = _compiled("SQL 0", "SQL 1")

    with pytest.raises(ValueError, match="first failure"):
        ExecutionSummary(
            compiled_plan=compiled,
            results=(_failed_exec(0, "SQL 0"), _ok_exec(1, "SQL 1")),
        )


def test_execution_summary_rejects_a_successful_partial_history():
    compiled = _compiled("SQL 0", "SQL 1")

    with pytest.raises(ValueError, match="complete compiled plan"):
        ExecutionSummary(
            compiled_plan=compiled,
            results=(_ok_exec(0, "SQL 0"),),
        )


def test_execution_summary_rejects_a_statement_outside_the_compiled_plan():
    compiled = _compiled("SQL 0")

    with pytest.raises(ValueError, match="compiled statement prefix"):
        ExecutionSummary(
            compiled_plan=compiled,
            results=(_ok_exec(0, "OTHER SQL"),),
        )
