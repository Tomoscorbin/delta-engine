import dataclasses

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.ports import TablePresent
from delta_engine.application.rendering import action_diff_line, render_diff_block
from delta_engine.application.report import TableRunReport
from delta_engine.domain.model import Column, DesiredTable, Integer, ObservedTable, QualifiedName
from delta_engine.domain.plan.actions import (
    ActionPlan,
    SetColumnTag,
    SetTableTag,
    UnsetColumnTag,
    UnsetTableTag,
)

# ---------- tag diff lines ----------


def test_set_table_tag_renders_a_tilde_tag_line():
    # Given a SetTableTag action
    line = action_diff_line(SetTableTag(name="env", value="prod"))

    # Then it renders as a change line naming the tag and its value
    assert line == "~ tag env = 'prod'"


def test_unset_table_tag_renders_a_minus_tag_line():
    # Given an UnsetTableTag action
    line = action_diff_line(UnsetTableTag(name="env"))

    # Then it renders as a removal line naming the tag
    assert line == "- tag env"


# ---------- column tag diff lines ----------


def test_set_column_tag_renders_a_tilde_column_tag_line():
    # Given a SetColumnTag action
    line = action_diff_line(SetColumnTag(column_name="email", name="pii", value="true"))

    # Then it renders as a change line naming the column, tag, and value
    assert line == "~ column tag email.pii = 'true'"


def test_unset_column_tag_renders_a_minus_column_tag_line():
    # Given an UnsetColumnTag action
    line = action_diff_line(UnsetColumnTag(column_name="email", name="pii"))

    # Then it renders as a removal line naming the column and tag
    assert line == "- column tag email.pii"


# ---------- diff block with failures hint ----------


def _report_with_empty_plan_and_failure() -> TableRunReport:
    qualified_name = QualifiedName("dev", "silver", "orders")
    desired = DesiredTable(qualified_name=qualified_name, columns=(Column("id", Integer()),))
    observed = ObservedTable(qualified_name=qualified_name, columns=(Column("id", Integer()),))
    return TableRunReport(
        qualified_name=qualified_name,
        desired=desired,
        read=TablePresent(table=observed),
        plan=ActionPlan(),
        failures=(ValidationFailure(rule_name="UnsupportedColumnTypeChange", message="nope"),),
        execution=None,
    )


def test_diff_block_points_to_failures_when_plan_is_empty_but_failures_exist():
    # Given a table whose only drift is unsupported — empty plan, failed validation
    block = render_diff_block(_report_with_empty_plan_and_failure())

    # Then the block does not read as a healthy no-op
    assert "(no changes — see failures)" in block


def test_diff_block_shows_plain_no_changes_when_nothing_failed():
    # Given a fully in-sync table
    report = _report_with_empty_plan_and_failure()
    healthy = dataclasses.replace(report, failures=())

    block = render_diff_block(healthy)

    assert "(no changes)" in block
    assert "see failures" not in block
