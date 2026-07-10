import dataclasses
from datetime import datetime

import pytest

from delta_engine.application.diff_entries import (
    DiffCategory,
    DiffEntry,
    action_entries,
)
from delta_engine.application.failures import ExecutionFailure, ReadFailure, ValidationFailure
from delta_engine.application.ports import (
    ExecutionFailed,
    ExecutionSucceeded,
    ExecutionSummary,
    ReadFailed,
    TablePresent,
)
from delta_engine.application.rendering import (
    render_diff,
    render_diff_block,
    render_grid,
    render_report,
    run_summary_footer,
)
from delta_engine.application.report import SyncReport, TableRunReport
from delta_engine.domain.model import (
    Column,
    Decimal,
    DesiredTable,
    Integer,
    Long,
    ObservedTable,
    PrimaryKeyConstraint,
    QualifiedName,
    String,
)
from delta_engine.domain.plan.actions import (
    ActionPlan,
    AddColumn,
    AlterClustering,
    AlterColumnType,
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

# ---------- action diff entries ----------


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        (
            AddColumn(Column("age", Integer())),
            (DiffEntry(DiffCategory.COLUMNS, "+", ("age", "Integer")),),
        ),
        (
            AddColumn(Column("age", Integer(), nullable=False)),
            (DiffEntry(DiffCategory.COLUMNS, "+", ("age", "Integer", "NOT NULL")),),
        ),
        (
            DropColumn(column_name="legacy"),
            (DiffEntry(DiffCategory.COLUMNS, "-", ("legacy",)),),
        ),
        (
            SetColumnNullability(column_name="id", nullable=False),
            (DiffEntry(DiffCategory.COLUMNS, "~", ("id", "set NOT NULL (was nullable)")),),
        ),
        (
            SetColumnNullability(column_name="id", nullable=True),
            (DiffEntry(DiffCategory.COLUMNS, "~", ("id", "drop NOT NULL (was NOT NULL)")),),
        ),
        (
            AlterColumnType(column_name="id", data_type=Long(), observed_type=Integer()),
            (DiffEntry(DiffCategory.COLUMNS, "~", ("id", "Integer → Long")),),
        ),
        # Decimal renders its parameters — the bare class name would hide a
        # precision widen.
        (
            AlterColumnType(
                column_name="amount", data_type=Decimal(12, 2), observed_type=Decimal(10, 2)
            ),
            (DiffEntry(DiffCategory.COLUMNS, "~", ("amount", "Decimal(10,2) → Decimal(12,2)")),),
        ),
        (
            SetPrimaryKey(columns=("id", "tenant_id"), constraint_name="tbl_pk"),
            (DiffEntry(DiffCategory.KEYS, "+", ("primary key (id, tenant_id)",)),),
        ),
        (
            DropPrimaryKey(),
            (DiffEntry(DiffCategory.KEYS, "-", ("primary key",)),),
        ),
        (
            SetForeignKey(
                local_columns=("customer_id",),
                referenced_table=QualifiedName("cat", "sch", "customers"),
                referenced_columns=("id",),
                constraint_name="orders_customer_id_fk",
            ),
            (
                DiffEntry(
                    DiffCategory.KEYS, "+", ("foreign key (customer_id) → cat.sch.customers",)
                ),
            ),
        ),
        (
            DropForeignKey(constraint_name="orders_customer_id_fk"),
            (DiffEntry(DiffCategory.KEYS, "-", ("foreign key orders_customer_id_fk",)),),
        ),
        (
            SetProperty(name="delta.enableChangeDataFeed", value="true"),
            (DiffEntry(DiffCategory.PROPERTIES, "+", ("delta.enableChangeDataFeed = 'true'",)),),
        ),
        (
            SetProperty(name="delta.enableChangeDataFeed", value="true", observed_value="false"),
            (
                DiffEntry(
                    DiffCategory.PROPERTIES,
                    "~",
                    ("delta.enableChangeDataFeed = 'true' (was 'false')",),
                ),
            ),
        ),
        (
            UnsetProperty(name="delta.logRetentionDuration"),
            (DiffEntry(DiffCategory.PROPERTIES, "-", ("delta.logRetentionDuration",)),),
        ),
        (
            SetTableTag(name="env", value="prod"),
            (DiffEntry(DiffCategory.TAGS, "~", ("env = 'prod'",)),),
        ),
        (
            UnsetTableTag(name="env"),
            (DiffEntry(DiffCategory.TAGS, "-", ("env",)),),
        ),
        (
            SetColumnTag(column_name="email", name="pii", value="true"),
            (DiffEntry(DiffCategory.TAGS, "~", ("column email.pii = 'true'",)),),
        ),
        (
            UnsetColumnTag(column_name="email", name="pii"),
            (DiffEntry(DiffCategory.TAGS, "-", ("column email.pii",)),),
        ),
        (
            SetColumnComment(column_name="id", comment="the key"),
            (DiffEntry(DiffCategory.COMMENTS, "~", ("column id: 'the key'",)),),
        ),
        (
            SetColumnComment(column_name="id", comment=""),
            (DiffEntry(DiffCategory.COMMENTS, "~", ("column id comment (unset)",)),),
        ),
        (
            SetTableComment(comment="core table"),
            (DiffEntry(DiffCategory.COMMENTS, "~", ("table: 'core table'",)),),
        ),
        (
            SetTableComment(comment=""),
            (DiffEntry(DiffCategory.COMMENTS, "~", ("table comment (unset)",)),),
        ),
        (
            AlterClustering(columns=("region", "day")),
            (
                DiffEntry(
                    DiffCategory.CLUSTERING,
                    "~",
                    ("clustering (region, day) — run OPTIMIZE FULL to recluster existing data",),
                ),
            ),
        ),
        # Removal carries no OPTIMIZE hint: OPTIMIZE FULL errors on a table
        # without clustering columns.
        (
            AlterClustering(columns=()),
            (DiffEntry(DiffCategory.CLUSTERING, "-", ("clustering",)),),
        ),
    ],
)
def test_action_entries_render_expected(action, expected):
    # Then each action lowers to its category-tagged diff entries
    assert action_entries(action) == expected


def test_create_table_entries_list_columns_with_types_and_primary_key():
    # Given a CreateTable for a table with a not-null id, a nullable name, and a PK
    action = CreateTable(
        table=DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "orders"),
            columns=(
                Column("id", Integer(), nullable=False),
                Column("name", String()),
            ),
            primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
        )
    )

    # Then it expands to one columns entry per column (with types) plus the primary key
    assert action_entries(action) == (
        DiffEntry(DiffCategory.COLUMNS, "+", ("id", "Integer", "NOT NULL")),
        DiffEntry(DiffCategory.COLUMNS, "+", ("name", "String")),
        DiffEntry(DiffCategory.KEYS, "+", ("primary key (id)",)),
    )


def test_create_table_entries_include_clustering_without_optimize_hint():
    # Given a CREATE TABLE that declares clustering keys
    action = CreateTable(
        table=DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "tbl"),
            columns=(Column("id", Integer()), Column("region", String())),
            clustered_by=("region",),
        )
    )
    # When rendering its diff entries
    entries = action_entries(action)
    # Then a clustering line is present with no OPTIMIZE hint (new table, no data)
    clustering = [e for e in entries if e.category is DiffCategory.CLUSTERING]
    assert clustering == [DiffEntry(DiffCategory.CLUSTERING, "+", ("clustering (region)",))]


def test_every_action_type_has_registered_diff_entries():
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
    fallback = action_entries.dispatch(object)
    for action_type in concrete_action_types:
        assert action_entries.dispatch(action_type) is not fallback, (
            f"No diff entries registered for {action_type.__name__}"
        )


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


# ---------- diff block rendering ----------


def test_diff_block_groups_lines_under_category_headings_in_plan_order():
    # Given a table whose plan sets the table comment and adds a column
    report = _grid_report(
        "orders",
        plan=ActionPlan((SetTableComment(comment="c"), AddColumn(Column("age", Integer())))),
    )

    # When rendering the diff block
    lines = render_diff_block(report).splitlines()

    # Then lines sit under 2-space category headings, entries indented 4 spaces,
    # with categories in DiffCategory order (columns before comments)
    assert lines[0] == "cat.sch.orders"
    assert "  columns" in lines
    assert "    + age  Integer" in lines
    assert "  comments" in lines
    assert "    ~ table: 'c'" in lines
    assert lines.index("  columns") < lines.index("  comments")


def test_diff_block_marks_a_create_in_the_header():
    # Given a plan that creates a table
    report = _grid_report(
        "orders",
        plan=ActionPlan((CreateTable(table=_grid_report("orders").desired),)),
    )

    # Then the block header flags the table as newly created
    assert render_diff_block(report).splitlines()[0] == "cat.sch.orders  (CREATE)"


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


def _execution(*, applied: int, failed: int) -> ExecutionSummary:
    succeeded = tuple(
        ExecutionSucceeded(action_index=index, statement_preview="SQL") for index in range(applied)
    )
    failures = tuple(
        ExecutionFailed(
            failure=ExecutionFailure(
                action_index=applied + index,
                exception_type="AnalysisException",
                message="boom",
                statement_preview="SQL",
            ),
        )
        for index in range(failed)
    )
    return ExecutionSummary(results=succeeded + failures)


def test_grid_detail_summarizes_changes_by_category_not_class_names():
    # Given a changed table with two column adds and one property set
    report = _grid_report(
        "orders",
        plan=ActionPlan(
            (
                AddColumn(Column("a", Integer())),
                AddColumn(Column("b", Integer())),
                SetProperty(name="delta.appendOnly", value="true"),
            )
        ),
    )

    # When rendering the single-row grid
    data_row = render_grid((report,)).splitlines()[1]

    # Then DETAIL counts by category and does not leak class names
    assert "2 columns, 1 property" in data_row
    assert "AddColumn" not in data_row


def test_grid_actions_cell_shows_applied_over_total_on_partial_failure():
    # Given a three-action plan where two applied and one failed during execution
    plan = ActionPlan(
        (
            AddColumn(Column("a", Integer())),
            AddColumn(Column("b", Integer())),
            AddColumn(Column("c", Integer())),
        )
    )
    report = dataclasses.replace(
        _grid_report("orders", plan=plan),
        execution=_execution(applied=2, failed=1),
        failures=(ExecutionFailure(2, "AnalysisException", "boom", "SQL"),),
    )

    # When rendering the grid row
    data_row = render_grid((report,)).splitlines()[1]

    # Then the ACTIONS cell reads applied/total
    assert "2/3" in data_row


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

    # Then DETAIL shows the first failure's headline (no detail message) and a count of the rest
    assert "Validation failed: R1" in data_row
    assert " - first" not in data_row
    assert data_row.endswith("(+1 more)")


def test_grid_detail_truncates_an_overlong_detail_with_an_ellipsis():
    # Given a failure whose headline exceeds the detail width
    report = _grid_report(
        "orders",
        failures=(ValidationFailure(rule_name="R" * 80, message="short"),),
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


# ---------- whole-report rendering ----------


def test_render_report_is_the_status_grid_followed_by_the_summary_footer():
    # Given a run over one changed and one failed table
    changed = _grid_report("a", plan=ActionPlan((SetTableComment(comment="c"),)))
    failed = _grid_report("b", failures=(ValidationFailure(rule_name="R", message="m"),))
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_reports=(changed, failed),
    )

    # When rendering the whole report
    rendered = render_report(sync)

    # Then it opens with the SYNC REPORT title, shows the grid header, and ends with the footer
    lines = rendered.splitlines()
    assert lines[0] == "SYNC REPORT"
    grid_header = next(line for line in lines if line.startswith("TABLE"))
    assert grid_header.split() == ["TABLE", "STATUS", "ACTIONS", "DETAIL"]
    assert rendered.endswith("2 tables: 1 changed, 0 unchanged, 1 failed (3.0s)")


def test_render_report_of_an_empty_run_is_a_header_and_zero_footer():
    # Given a run over no tables
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_reports=(),
    )

    # When rendering the whole report
    rendered = render_report(sync)

    # Then the title, grid header, and a zero-count footer are shown -- no empty-run sentinel
    lines = rendered.splitlines()
    assert lines[0] == "SYNC REPORT"
    grid_header = next(line for line in lines if line.startswith("TABLE"))
    assert grid_header.split() == ["TABLE", "STATUS", "ACTIONS", "DETAIL"]
    assert rendered.endswith("0 tables: 0 changed, 0 unchanged, 0 failed (3.0s)")


def test_render_report_shows_a_full_failures_section_for_failed_tables():
    # Given a run with a table that failed validation twice
    failed = _grid_report(
        "orders",
        failures=(
            ValidationFailure(rule_name="R1", message="first"),
            ValidationFailure(rule_name="R2", message="second"),
        ),
    )
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_reports=(failed,),
    )

    rendered = render_report(sync)

    # Then a Failures section lists every failure in full (not '+N more')
    assert "Failures" in rendered
    assert "Validation failed: R1 - first" in rendered
    assert "Validation failed: R2 - second" in rendered


def test_render_report_failures_section_has_an_underlined_header():
    # Given a run with a failed table
    failed = _grid_report("orders", failures=(ValidationFailure(rule_name="R", message="m"),))
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_reports=(failed,),
    )

    lines = render_report(sync).splitlines()

    # Then the Failures section is introduced by an underlined header
    index = lines.index("Failures")
    assert lines[index + 1] == "-" * len("Failures")


def test_render_report_has_no_failures_section_when_all_succeed():
    # Given a run where every table succeeds
    changed = _grid_report("a", plan=ActionPlan((SetTableComment(comment="c"),)))
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_reports=(changed,),
    )

    # Then no Failures section is rendered
    assert "Failures" not in render_report(sync)


def test_render_report_shows_dry_run_banner_only_for_dry_runs():
    # Given the same run rendered as a dry run and as an applied run
    changed = _grid_report("a", plan=ActionPlan((SetTableComment(comment="c"),)))
    base = dict(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_reports=(changed,),
    )

    dry = render_report(SyncReport(**base, dry_run=True))
    applied = render_report(SyncReport(**base, dry_run=False))

    # Then the banner appears (below the title) for a dry run and is absent otherwise
    assert "DRY RUN — no changes applied" in dry.splitlines()
    assert "DRY RUN" not in applied


def test_render_report_is_titled():
    # Given any run
    changed = _grid_report("a", plan=ActionPlan((SetTableComment(comment="c"),)))
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_reports=(changed,),
    )

    lines = render_report(sync).splitlines()

    # Then a SYNC REPORT title, underlined with a rule, heads the output
    assert lines[0] == "SYNC REPORT"
    assert lines[1] == "=" * len("SYNC REPORT")


def test_render_diff_is_titled():
    # Given any run
    first = _grid_report("a", plan=ActionPlan((SetTableComment(comment="c"),)))
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_reports=(first,),
    )

    lines = render_diff(sync).splitlines()

    # Then a DIFF title, underlined with a rule, heads the output
    assert lines[0] == "DIFF"
    assert lines[1] == "=" * len("DIFF")


def test_render_diff_joins_each_tables_change_block_in_report_order():
    # Given a run over two tables with plans
    first = _grid_report("a", plan=ActionPlan((SetTableComment(comment="c"),)))
    second = _grid_report("b", plan=ActionPlan((AddColumn(Column("age", Integer())),)))
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_reports=(first, second),
    )

    # When rendering the run's diff
    rendered = render_diff(sync)

    # Then each table's change block appears, in report order
    assert rendered.index("cat.sch.a") < rendered.index("cat.sch.b")
    assert "~ table: 'c'" in rendered
    assert "+ age  Integer" in rendered
