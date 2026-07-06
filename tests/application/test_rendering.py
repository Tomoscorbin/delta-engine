import dataclasses
from datetime import datetime

import pytest

from delta_engine.application.failures import ReadFailure, ValidationFailure
from delta_engine.application.ports import ReadFailed, TablePresent
from delta_engine.application.rendering import (
    action_diff_line,
    render_diff_block,
    render_grid,
    run_summary_footer,
)
from delta_engine.application.report import SyncReport, TableRunReport
from delta_engine.domain.model import (
    Column,
    DesiredTable,
    Integer,
    ObservedTable,
    QualifiedName,
    String,
)
from delta_engine.domain.plan.actions import (
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    SetColumnComment,
    SetColumnNullability,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    UnsetColumnTag,
    UnsetProperty,
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


def test_every_action_type_has_a_registered_diff_line():
    # Given every concrete Action subclass the plan vocabulary defines
    import inspect

    from delta_engine.domain.plan import actions as actions_module
    from delta_engine.domain.plan.actions import Action

    concrete_action_types = [
        obj
        for _, obj in inspect.getmembers(actions_module, inspect.isclass)
        if issubclass(obj, Action) and obj is not Action
    ]

    # Then each dispatches to a real arm, not the NotImplementedError fallback
    fallback = action_diff_line.dispatch(object)
    for action_type in concrete_action_types:
        assert action_diff_line.dispatch(action_type) is not fallback, (
            f"No diff line registered for {action_type.__name__}"
        )


def test_set_property_renders_plus_for_first_write():
    # Given a property being written for the first time (absent from catalog)
    line = action_diff_line(SetProperty(name="delta.enableChangeDataFeed", value="true"))

    assert line == "+ property delta.enableChangeDataFeed = 'true'"


def test_set_property_renders_tilde_with_was_for_update():
    # Given a property whose catalog value is stale
    line = action_diff_line(
        SetProperty(name="delta.enableChangeDataFeed", value="true", observed_value="false")
    )

    assert line == "~ property delta.enableChangeDataFeed = 'true' (was 'false')"


def test_unset_property_renders_minus():
    line = action_diff_line(UnsetProperty(name="delta.logRetentionDuration"))

    assert line == "- property delta.logRetentionDuration"


# ---------- structural action diff lines ----------


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        (
            CreateTable(
                table=DesiredTable(
                    qualified_name=QualifiedName("cat", "sch", "orders"),
                    columns=(Column("id", Integer()), Column("name", String())),
                )
            ),
            "+ create table (columns: id, name)",
        ),
        (AddColumn(Column("age", Integer())), "+ column age Integer"),
        (AddColumn(Column("age", Integer(), nullable=False)), "+ column age Integer NOT NULL"),
        (DropColumn(column_name="legacy"), "- column legacy"),
        (SetColumnNullability(column_name="id", nullable=True), "~ column id drop NOT NULL"),
        (SetColumnNullability(column_name="id", nullable=False), "~ column id set NOT NULL"),
        (SetColumnComment(column_name="id", comment="primary key"), "~ comment on column id"),
        (SetColumnComment(column_name="id", comment=""), "~ comment on column id (unset)"),
        (SetTableComment(comment="core table"), "~ comment on table"),
        (
            SetPrimaryKey(columns=("id", "tenant_id"), constraint_name="tbl_pk"),
            "+ primary key (id, tenant_id)",
        ),
        (DropPrimaryKey(), "- primary key"),
        (
            SetForeignKey(
                local_columns=("customer_id",),
                referenced_table=QualifiedName("cat", "sch", "customers"),
                referenced_columns=("id",),
                constraint_name="orders_customer_id_fk",
            ),
            "+ foreign key (customer_id) → cat.sch.customers",
        ),
        (
            DropForeignKey(constraint_name="orders_customer_id_fk"),
            "- foreign key orders_customer_id_fk",
        ),
    ],
)
def test_action_diff_line_renders_expected_text(action, expected):
    # Then each structural action renders as its documented +/-/~ diff line
    assert action_diff_line(action) == expected


# ---------- grid rendering ----------


def _grid_report(name, *, plan=None, failures=()):
    qualified_name = QualifiedName("cat", "sch", name)
    columns = (Column("id", Integer()),)
    return TableRunReport(
        qualified_name=qualified_name,
        desired=DesiredTable(qualified_name=qualified_name, columns=columns),
        read=TablePresent(table=ObservedTable(qualified_name=qualified_name, columns=columns)),
        plan=plan if plan is not None else ActionPlan(),
        failures=failures,
    )


def test_grid_detail_lists_action_type_names_for_a_changed_table():
    # Given a table with a two-action plan and no failures
    report = _grid_report(
        "orders",
        plan=ActionPlan(
            (SetTableComment(comment="c"), SetProperty(name="delta.appendOnly", value="true"))
        ),
    )

    # When rendering the single-row grid
    data_row = render_grid((report,)).splitlines()[1]

    # Then the DETAIL cell lists the action type names in plan order
    assert data_row.endswith("SetProperty, SetTableComment")


def test_grid_detail_shows_first_failure_and_extra_count_when_multiple():
    # Given a table with two validation failures
    report = _grid_report(
        "orders",
        failures=(
            ValidationFailure(rule_name="R1", message="first"),
            ValidationFailure(rule_name="R2", message="second"),
        ),
    )

    # When rendering the grid
    data_row = render_grid((report,)).splitlines()[1]

    # Then DETAIL shows the first failure and a count of the rest
    assert "Validation failed: R1 - first" in data_row
    assert data_row.endswith("(+1 more)")


def test_grid_detail_truncates_an_overlong_detail_with_an_ellipsis():
    # Given a failure whose rendered line exceeds the detail width
    report = _grid_report(
        "orders",
        failures=(ValidationFailure(rule_name="R", message="x" * 80),),
    )

    # When rendering the grid
    data_row = render_grid((report,)).splitlines()[1]

    # Then the DETAIL cell is truncated with an ellipsis
    assert data_row.endswith("…")


def test_grid_aligns_the_status_column_across_header_and_rows():
    # Given two tables whose names differ in length
    changed = _grid_report("a", plan=ActionPlan((SetTableComment(comment="c"),)))
    failed = _grid_report(
        "a_much_longer_name",
        failures=(ValidationFailure(rule_name="R", message="m"),),
    )

    # When rendering a multi-row grid
    header, changed_row, failed_row = render_grid((changed, failed)).splitlines()

    # Then the STATUS column starts at the same offset in every line
    status_offset = header.index("STATUS")
    assert changed_row.index("SUCCESS") == status_offset
    assert failed_row.index("VALIDATION_FAILED") == status_offset


# ---------- run summary footer ----------


def test_run_summary_footer_counts_changed_unchanged_and_failed():
    # Given a run over one changed, one unchanged, and one failed table
    changed = _grid_report("a", plan=ActionPlan((SetTableComment(comment="c"),)))
    unchanged = _grid_report("b")
    failed = _grid_report("c", failures=(ValidationFailure(rule_name="R", message="m"),))
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_reports=(changed, unchanged, failed),
    )

    # When rendering the footer
    footer = run_summary_footer(sync)

    # Then each table is classified by its outcome
    assert footer == "3 tables: 1 changed, 1 unchanged, 1 failed (3.0s)"


# ---------- diff block ----------


def test_diff_block_lists_one_indented_line_per_planned_action():
    # Given a table whose plan holds two actions
    report = _grid_report(
        "orders",
        plan=ActionPlan((SetTableComment(comment="c"), AddColumn(Column("age", Integer())))),
    )

    # When rendering the diff block
    lines = render_diff_block(report).splitlines()

    # Then the header names the table and each action is one indented line, in plan order
    assert lines[0] == "cat.sch.orders"
    assert lines[1:] == ["  + column age Integer", "  ~ comment on table"]


def test_diff_block_reports_a_read_failure_instead_of_a_diff():
    # Given a table whose catalog read failed
    qualified_name = QualifiedName("cat", "sch", "orders")
    report = TableRunReport(
        qualified_name=qualified_name,
        desired=DesiredTable(qualified_name=qualified_name, columns=(Column("id", Integer()),)),
        read=ReadFailed(ReadFailure("IOError", "boom")),
    )

    # When rendering the diff block
    block = render_diff_block(report)

    # Then it says the table could not be read rather than showing a diff
    assert block == "cat.sch.orders\n  (could not read — no diff)"
