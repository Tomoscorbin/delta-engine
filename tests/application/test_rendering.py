from datetime import datetime

import pytest

from delta_engine.application.diff_entries import (
    DiffCategory,
    DiffEntry,
    DiffOperation,
    action_entries,
    unresolvable_entries,
)
from delta_engine.application.failures import (
    ExecutionFailure,
    ForeignKeyFailure,
    ForeignKeyFailureReason,
    ReadFailure,
    ValidationFailure,
)
from delta_engine.application.planning import PlanningAccepted, PlanningRejected
from delta_engine.application.ports import (
    ExecutionResult,
    TablePresent,
)
from delta_engine.application.relationships import TableResolution
from delta_engine.application.rendering import (
    render_diff,
    render_diff_block,
    render_grid,
    render_report,
    run_summary_footer,
)
from delta_engine.application.report import SyncReport, TableRun
from delta_engine.domain.model import (
    Array,
    Decimal,
    DesiredColumn,
    DesiredTable,
    ForeignKeyConstraint,
    Integer,
    Long,
    Map,
    ObservedColumn,
    ObservedTable,
    PrimaryKeyConstraint,
    QualifiedName,
    String,
    Struct,
    StructField,
    TableFeature,
)
from delta_engine.domain.plan import (
    ColumnCaseDrift,
    ColumnRenameConflict,
    PartitioningChanged,
    PropertyUndeclared,
    TableCreation,
    diff_table,
)
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    AlterClustering,
    AlterColumnType,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    EnableTableFeature,
    RenameColumn,
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
from tests.builders import build_compiled_plan


def _primary_key(
    columns: tuple[str, ...] = ("id",), constraint_name: str = "tbl_pk"
) -> PrimaryKeyConstraint:
    return PrimaryKeyConstraint(columns, constraint_name)


def _foreign_key(constraint_name: str = "orders_customer_id_fk") -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("cat", "sch", "customers"),
        referenced_columns=("id",),
        constraint_name=constraint_name,
    )


def _plan(name: str, *actions: Action) -> ActionPlan:
    return ActionPlan(
        target=QualifiedName("cat", "sch", name),
        actions=actions,
    )


# ---------- action diff entries ----------


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        (
            AddColumn(DesiredColumn("age", Integer())),
            (DiffEntry(DiffCategory.COLUMNS, DiffOperation.ADD, "age", ("Integer",)),),
        ),
        (
            AddColumn(DesiredColumn("age", Integer(), nullable=False)),
            (DiffEntry(DiffCategory.COLUMNS, DiffOperation.ADD, "age", ("Integer", "NOT NULL")),),
        ),
        (
            DropColumn(column=ObservedColumn("legacy", Integer())),
            (DiffEntry(DiffCategory.COLUMNS, DiffOperation.REMOVE, "legacy"),),
        ),
        (
            SetColumnNullability(column_name="id", desired_nullable=False, observed_nullable=True),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.CHANGE,
                    "id",
                    ("set NOT NULL (was nullable)",),
                ),
            ),
        ),
        (
            SetColumnNullability(column_name="id", desired_nullable=True, observed_nullable=False),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.CHANGE,
                    "id",
                    ("drop NOT NULL (was NOT NULL)",),
                ),
            ),
        ),
        (
            AlterColumnType(column_name="id", desired_type=Long(), observed_type=Integer()),
            (DiffEntry(DiffCategory.COLUMNS, DiffOperation.CHANGE, "id", ("Integer → Long",)),),
        ),
        # Decimal renders its parameters — the bare class name would hide a
        # precision widen.
        (
            AlterColumnType(
                column_name="amount",
                desired_type=Decimal(12, 2),
                observed_type=Decimal(10, 2),
            ),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.CHANGE,
                    "amount",
                    ("Decimal(10,2) → Decimal(12,2)",),
                ),
            ),
        ),
        # A nested type spells its own structure, so its delimiters have to
        # nest too — unbalanced ones leave a reader unable to tell where an
        # inner type ends.
        (
            AddColumn(
                DesiredColumn(
                    "payload",
                    Struct(
                        (
                            StructField("id", Integer()),
                            StructField("labels", Map(String(), Array(String()))),
                        )
                    ),
                )
            ),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.ADD,
                    "payload",
                    ("Struct<id: Integer, labels: Map<String, Array<String>>>",),
                ),
            ),
        ),
        (
            SetPrimaryKey(primary_key=_primary_key(("id", "tenant_id"))),
            (
                DiffEntry(
                    DiffCategory.KEYS,
                    DiffOperation.ADD,
                    "primary key tbl_pk",
                    ("(id, tenant_id)",),
                ),
            ),
        ),
        (
            DropPrimaryKey("legacy_pk"),
            (DiffEntry(DiffCategory.KEYS, DiffOperation.REMOVE, "primary key legacy_pk"),),
        ),
        (
            SetForeignKey(constraint=_foreign_key()),
            (
                DiffEntry(
                    DiffCategory.KEYS,
                    DiffOperation.ADD,
                    "foreign key orders_customer_id_fk",
                    ("(customer_id) → cat.sch.customers",),
                ),
            ),
        ),
        (
            DropForeignKey(constraint=_foreign_key()),
            (
                DiffEntry(
                    DiffCategory.KEYS, DiffOperation.REMOVE, "foreign key orders_customer_id_fk"
                ),
            ),
        ),
        (
            SetProperty(
                name="delta.enableChangeDataFeed", desired_value="true", observed_value=None
            ),
            (
                DiffEntry(
                    DiffCategory.PROPERTIES,
                    DiffOperation.ADD,
                    "delta.enableChangeDataFeed",
                    ("= 'true'",),
                ),
            ),
        ),
        # The old value trails as its own phrase, so it aligns down the group.
        (
            SetProperty(
                name="delta.enableChangeDataFeed",
                desired_value="true",
                observed_value="false",
            ),
            (
                DiffEntry(
                    DiffCategory.PROPERTIES,
                    DiffOperation.CHANGE,
                    "delta.enableChangeDataFeed",
                    ("= 'true'", "(was 'false')"),
                ),
            ),
        ),
        (
            UnsetProperty(name="delta.logRetentionDuration", observed_value="old"),
            (
                DiffEntry(
                    DiffCategory.PROPERTIES, DiffOperation.REMOVE, "delta.logRetentionDuration"
                ),
            ),
        ),
        (
            SetTableTag(name="env", desired_value="prod", observed_value=None),
            (DiffEntry(DiffCategory.TAGS, DiffOperation.ADD, "env", ("= 'prod'",)),),
        ),
        (
            SetTableTag(name="env", desired_value="prod", observed_value="dev"),
            (
                DiffEntry(
                    DiffCategory.TAGS, DiffOperation.CHANGE, "env", ("= 'prod'", "(was 'dev')")
                ),
            ),
        ),
        (
            UnsetTableTag(name="env"),
            (DiffEntry(DiffCategory.TAGS, DiffOperation.REMOVE, "env"),),
        ),
        (
            SetColumnTag(
                column_name="email", name="pii", desired_value="true", observed_value=None
            ),
            (DiffEntry(DiffCategory.TAGS, DiffOperation.ADD, "column email.pii", ("= 'true'",)),),
        ),
        (
            SetColumnTag(
                column_name="email",
                name="pii",
                desired_value="true",
                observed_value="false",
            ),
            (
                DiffEntry(
                    DiffCategory.TAGS,
                    DiffOperation.CHANGE,
                    "column email.pii",
                    ("= 'true'", "(was 'false')"),
                ),
            ),
        ),
        (
            UnsetColumnTag(column_name="email", name="pii"),
            (DiffEntry(DiffCategory.TAGS, DiffOperation.REMOVE, "column email.pii"),),
        ),
        # The subject names what carries the comment; alignment separates it
        # from the text, so no colon is needed.
        (
            SetColumnComment(column_name="id", desired_comment="the key", observed_comment=""),
            (DiffEntry(DiffCategory.COMMENTS, DiffOperation.CHANGE, "column id", ("'the key'",)),),
        ),
        (
            SetColumnComment(column_name="id", desired_comment="", observed_comment="old"),
            (DiffEntry(DiffCategory.COMMENTS, DiffOperation.CHANGE, "column id", ("(unset)",)),),
        ),
        (
            SetTableComment(desired_comment="core table", observed_comment=""),
            (DiffEntry(DiffCategory.COMMENTS, DiffOperation.CHANGE, "table", ("'core table'",)),),
        ),
        (
            SetTableComment(desired_comment="", observed_comment="old"),
            (DiffEntry(DiffCategory.COMMENTS, DiffOperation.CHANGE, "table", ("(unset)",)),),
        ),
        (
            AlterClustering(desired_clustering=("region", "day"), observed_clustering=()),
            (
                DiffEntry(
                    DiffCategory.CLUSTERING,
                    DiffOperation.CHANGE,
                    "clustering",
                    ("(region, day)", "— run OPTIMIZE FULL to recluster existing data"),
                ),
            ),
        ),
        # Removal carries no OPTIMIZE hint: OPTIMIZE FULL errors on a table
        # without clustering columns.
        (
            AlterClustering(desired_clustering=(), observed_clustering=("region",)),
            (DiffEntry(DiffCategory.CLUSTERING, DiffOperation.REMOVE, "clustering"),),
        ),
        (
            RenameColumn(old_name="customer_nm", new_name="customer_name"),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.CHANGE,
                    "customer_nm",
                    ("renamed → customer_name",),
                ),
            ),
        ),
    ],
)
def test_action_entries_render_expected(action, expected):
    # Then each action lowers to its category-tagged diff entries
    assert action_entries(action) == expected


def test_create_table_entries_include_clustering_without_optimize_hint():
    # Given a CREATE TABLE that declares clustering keys
    action = CreateTable(
        table=DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "tbl"),
            columns=(DesiredColumn("id", Integer()), DesiredColumn("region", String())),
            clustered_by=("region",),
        )
    )
    # When rendering its diff entries
    entries = action_entries(action)
    # Then a clustering line is present with no OPTIMIZE hint (new table, no data)
    clustering = [e for e in entries if e.category is DiffCategory.CLUSTERING]
    assert clustering == [
        DiffEntry(DiffCategory.CLUSTERING, DiffOperation.ADD, "clustering", ("(region)",))
    ]


def test_create_table_entries_include_all_state_embedded_in_create():
    # Given a CREATE TABLE carrying structural, layout, property, and comment state
    action = CreateTable(
        table=DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "tbl"),
            columns=(
                DesiredColumn("id", Integer(), nullable=False, comment="identifier"),
                DesiredColumn("day", String(), comment="partition date"),
            ),
            comment="daily orders",
            properties={
                "delta.appendOnly": "true",
                "delta.logRetentionDuration": None,
            },
            partitioned_by=("day",),
            primary_key=PrimaryKeyConstraint(("id",), "tbl_pk"),
        )
    )

    # Then reporting states every fact that CREATE TABLE establishes. A None
    # property asserts absence and is therefore not a creation change.
    assert action_entries(action) == (
        DiffEntry(DiffCategory.COLUMNS, DiffOperation.ADD, "id", ("Integer", "NOT NULL")),
        DiffEntry(DiffCategory.COLUMNS, DiffOperation.ADD, "day", ("String",)),
        DiffEntry(DiffCategory.KEYS, DiffOperation.ADD, "primary key tbl_pk", ("(id)",)),
        DiffEntry(DiffCategory.PARTITIONING, DiffOperation.ADD, "partitioning", ("(day)",)),
        DiffEntry(DiffCategory.PROPERTIES, DiffOperation.ADD, "delta.appendOnly", ("= 'true'",)),
        DiffEntry(DiffCategory.COMMENTS, DiffOperation.ADD, "column id", ("'identifier'",)),
        DiffEntry(DiffCategory.COMMENTS, DiffOperation.ADD, "column day", ("'partition date'",)),
        DiffEntry(DiffCategory.COMMENTS, DiffOperation.ADD, "table", ("'daily orders'",)),
    )


def test_every_category_names_itself_in_singular_and_plural():
    # Given every diff category
    # Then each names itself both ways, so a new one cannot reach a report unnamed
    for category in DiffCategory:
        assert category.plural
        assert category.counted(1).startswith("1 ")
        assert category.counted(2).startswith("2 ")


@pytest.mark.parametrize(
    ("category", "count", "expected"),
    [
        (DiffCategory.COLUMNS, 1, "1 column"),
        (DiffCategory.COLUMNS, 3, "3 columns"),
        (DiffCategory.KEYS, 1, "1 key"),
        # Some nouns do not inflect: "1 clustering" and "2 clustering" both read
        # correctly, where "2 clusterings" would not.
        (DiffCategory.CLUSTERING, 2, "2 clustering"),
        (DiffCategory.FEATURES, 1, "1 table feature"),
        (DiffCategory.FEATURES, 2, "2 table features"),
    ],
)
def test_a_category_counts_itself_with_the_right_noun(category, count, expected):
    assert category.counted(count) == expected


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


def test_every_unresolvable_type_has_registered_diff_entries():
    # Given every member of the Unresolvable union
    import typing

    from delta_engine.domain.plan.unresolvable import Unresolvable

    # Then each dispatches to a real arm, not the NotImplementedError fallback
    fallback = unresolvable_entries.dispatch(object)
    for unresolvable_type in typing.get_args(Unresolvable.__value__):
        assert unresolvable_entries.dispatch(unresolvable_type) is not fallback, (
            f"No diff entries registered for {unresolvable_type.__name__}"
        )


# ---------- diff block with failures hint ----------


def _report_with_empty_plan_and_failure() -> TableRun:
    qualified_name = QualifiedName("dev", "silver", "orders")
    desired = DesiredTable(qualified_name=qualified_name, columns=(DesiredColumn("id", Integer()),))
    observed = ObservedTable(
        qualified_name=qualified_name, columns=(ObservedColumn("id", Integer()),)
    )
    return TableRun(
        read=TablePresent(table=observed),
        planning=PlanningRejected(
            diff=diff_table(desired, observed),
            failures=(ValidationFailure(rule_name="UnsupportedColumnTypeChange", message="nope"),),
        ),
        compiled=None,
        resolution=TableResolution(desired, (), ()),
        execution=None,
    )


def test_diff_block_points_to_failures_when_no_plan_exists_and_failures_exist():
    # Given a table whose only drift is unsupported — no plan, failed validation
    block = render_diff_block(_report_with_empty_plan_and_failure())

    # Then the block does not read as a healthy no-op
    assert "(no changes — see failures)" in block


def test_diff_block_shows_plain_no_changes_when_nothing_failed():
    # Given a fully in-sync table
    report = _report_with_empty_plan_and_failure()
    plan = ActionPlan(target=report.desired.qualified_name)
    healthy = TableRun(
        read=report.read,
        planning=PlanningAccepted(diff=TableCreation(report.desired), plan=plan),
        compiled=build_compiled_plan(plan, ()),
        resolution=report.resolution,
        execution=None,
    )

    block = render_diff_block(healthy)

    assert "(no changes)" in block
    assert "see failures" not in block


# ---------- diff block rendering ----------


def test_diff_block_groups_lines_under_category_headings_in_plan_order():
    # Given a table whose plan sets the table comment and adds a column
    report = _grid_report(
        "orders",
        plan=_plan(
            "orders",
            SetTableComment(desired_comment="c", observed_comment=""),
            AddColumn(DesiredColumn("age", Integer())),
        ),
    )

    # When rendering the diff block
    lines = render_diff_block(report).splitlines()

    # Then lines sit under 2-space category headings, entries indented 4 spaces,
    # with categories in DiffCategory order (columns before comments)
    assert lines[0] == "cat.sch.orders"
    assert "  columns" in lines
    assert "    + age  Integer" in lines
    assert "  comments" in lines
    assert "    ~ table  'c'" in lines
    assert lines.index("  columns") < lines.index("  comments")


def test_diff_block_marks_a_create_in_the_header():
    # Given a plan that creates a table
    report = _grid_report(
        "orders",
        plan=_plan("orders", CreateTable(table=_grid_report("orders").desired)),
    )

    # Then the block header flags the table as newly created
    assert render_diff_block(report).splitlines()[0] == "cat.sch.orders  (CREATE)"


def test_diff_block_reports_a_read_failure_instead_of_a_diff():
    # Given a table whose catalog read failed
    qualified_name = QualifiedName("cat", "sch", "orders")
    failure = ReadFailure("IOError", "boom")
    report = TableRun(
        read=failure,
        planning=None,
        compiled=None,
        resolution=TableResolution(
            DesiredTable(qualified_name=qualified_name, columns=(DesiredColumn("id", Integer()),)),
            (),
            (),
        ),
        execution=None,
    )

    # When rendering the diff block
    block = render_diff_block(report)

    # Then it says the table could not be read rather than showing a diff
    assert block == "cat.sch.orders\n  (could not read — no diff)"


# ---------- grid rendering ----------


def _grid_report(name, *, plan=None, failures=(), execution=None, blocked_failures=()):
    qualified_name = QualifiedName("cat", "sch", name)
    columns = (DesiredColumn("id", Integer()),)
    desired = DesiredTable(qualified_name=qualified_name, columns=columns)
    if plan is None and not failures:
        plan = ActionPlan(target=qualified_name)
    planning_failures = tuple(
        failure for failure in failures if isinstance(failure, ValidationFailure)
    )
    planning = (
        PlanningRejected(diff=TableCreation(desired), failures=planning_failures)
        if planning_failures
        else PlanningAccepted(diff=TableCreation(desired), plan=plan)
    )
    statements = tuple(f"SQL {index}" for index in range(len(plan) if plan is not None else 0))
    if planning_failures:
        compiled = None
    elif execution is not None:
        compiled = execution.compiled_plan
    else:
        assert plan is not None
        compiled = build_compiled_plan(plan, statements)
    return TableRun(
        read=TablePresent(
            table=ObservedTable(
                qualified_name=qualified_name, columns=(ObservedColumn("id", Integer()),)
            )
        ),
        planning=planning,
        compiled=compiled,
        resolution=TableResolution(desired, (), ()),
        execution=execution,
        blocked_failures=blocked_failures,
    )


def _failed_execution(plan: ActionPlan, *, applied: int) -> ExecutionResult:
    statements = tuple(f"SQL {index}" for index in range(len(plan)))
    return ExecutionResult(
        compiled_plan=build_compiled_plan(plan, statements),
        applied_count=applied,
        failure=ExecutionFailure(
            statement_index=applied,
            exception_type="AnalysisException",
            message="boom",
            statement=f"SQL {applied}",
        ),
    )


def _successful_execution(plan: ActionPlan) -> ExecutionResult:
    statements = tuple(f"SQL {index}" for index in range(len(plan)))
    return ExecutionResult(
        compiled_plan=build_compiled_plan(plan, statements),
        applied_count=len(statements),
    )


def _blocked_failure(name: str) -> ForeignKeyFailure:
    return ForeignKeyFailure(
        table=QualifiedName("cat", "sch", name),
        local_columns=("parent_id",),
        references=QualifiedName("cat", "sch", "parent"),
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
    )


def test_grid_detail_summarizes_changes_by_category_not_class_names():
    # Given a changed table with two column adds and one property set
    report = _grid_report(
        "orders",
        plan=_plan(
            "orders",
            AddColumn(DesiredColumn("a", Integer())),
            AddColumn(DesiredColumn("b", Integer())),
            SetProperty(name="delta.appendOnly", desired_value="true", observed_value=None),
        ),
    )

    # When rendering the single-row grid
    data_row = render_grid((report,)).splitlines()[1]

    # Then DETAIL counts by category and does not leak class names
    assert "2 columns, 1 property" in data_row
    assert "AddColumn" not in data_row


def test_grid_statements_cell_shows_applied_over_planned_on_partial_failure():
    # Given a three-statement plan where two applied and one failed during execution
    plan = _plan(
        "orders",
        AddColumn(DesiredColumn("a", Integer())),
        AddColumn(DesiredColumn("b", Integer())),
        AddColumn(DesiredColumn("c", Integer())),
    )
    report = _grid_report(
        "orders",
        plan=plan,
        execution=_failed_execution(plan, applied=2),
    )

    # When rendering the grid row
    data_row = render_grid((report,)).splitlines()[1]

    # Then the STATEMENTS cell reads applied/planned
    assert "2/3" in data_row


def test_grid_statement_counts_distinguish_blocking_from_pre_compilation_failure():
    # Given a compiled two-statement plan blocked in real and dry runs, plus a rejected plan
    blocked = _grid_report(
        "blocked",
        plan=_plan(
            "blocked",
            AddColumn(DesiredColumn("a", Integer())),
            AddColumn(DesiredColumn("b", Integer())),
        ),
        blocked_failures=(_blocked_failure("blocked"),),
    )
    rejected = _grid_report(
        "rejected",
        failures=(ValidationFailure(rule_name="R", message="m"),),
    )
    started_at = datetime(2025, 1, 1, 0, 0, 0)
    ended_at = datetime(2025, 1, 1, 0, 0, 3)
    real_run = SyncReport(
        started_at=started_at,
        ended_at=ended_at,
        table_runs=(blocked, rejected),
    )
    dry_run = SyncReport(
        started_at=started_at,
        ended_at=ended_at,
        table_runs=(blocked,),
        dry_run=True,
    )

    # When rendering both reports
    real_lines = render_report(real_run).splitlines()
    dry_lines = render_report(dry_run).splitlines()
    blocked_row = next(line for line in real_lines if line.startswith("cat.sch.blocked"))
    rejected_row = next(line for line in real_lines if line.startswith("cat.sch.rejected"))
    dry_blocked_row = next(line for line in dry_lines if line.startswith("cat.sch.blocked"))

    # Then only the real blocked work is counted; rejected and previewed work stay unknown
    assert "0/2" in blocked_row
    assert "—" in rejected_row
    assert "—" in dry_blocked_row


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


def test_grid_detail_names_the_subject_a_rule_rejected():
    report = _grid_report(
        "orders",
        failures=(
            ValidationFailure(
                rule_name="NonNullableColumnAdd",
                message="Operation not allowed: cannot add non-nullable column 'email'.",
                subject="email",
            ),
        ),
    )

    data_row = render_grid((report,)).splitlines()[1]

    assert "NonNullableColumnAdd (email)" in data_row


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
    changed = _grid_report(
        "a", plan=_plan("a", SetTableComment(desired_comment="c", observed_comment=""))
    )
    failed = _grid_report(
        "a_much_longer_name",
        failures=(ValidationFailure(rule_name="R", message="m"),),
    )

    # When rendering a multi-row grid
    header, changed_row, failed_row = render_grid((changed, failed)).splitlines()

    # Then the STATUS column starts at the same offset in every line
    status_offset = header.index("STATUS")
    assert changed_row.index("SUCCESS") == status_offset
    assert failed_row.index("PLANNING_FAILED") == status_offset


# ---------- run summary footer ----------


def test_run_summary_footer_counts_changed_unchanged_and_failed():
    # Given a run over one changed, one unchanged, and one failed table
    changed = _grid_report(
        "a", plan=_plan("a", SetTableComment(desired_comment="c", observed_comment=""))
    )
    unchanged = _grid_report("b")
    failed = _grid_report("c", failures=(ValidationFailure(rule_name="R", message="m"),))
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_runs=(changed, unchanged, failed),
        dry_run=True,
    )

    # When rendering the footer
    footer = run_summary_footer(sync)

    # Then each table is classified by its outcome
    assert footer == "3 tables: 1 changed, 1 unchanged, 1 failed (3.0s)"


def test_real_run_footer_counts_each_catalog_change_outcome():
    # Given a real run containing every possible non-preview catalog outcome
    applied_plan = _plan("applied", AddColumn(DesiredColumn("a", Integer())))
    applied = _grid_report(
        "applied",
        plan=applied_plan,
        execution=_successful_execution(applied_plan),
    )
    partial_plan = _plan(
        "partial",
        AddColumn(DesiredColumn("a", Integer())),
        AddColumn(DesiredColumn("b", Integer())),
    )
    partial = _grid_report(
        "partial",
        plan=partial_plan,
        execution=_failed_execution(partial_plan, applied=1),
    )
    blocked = _grid_report(
        "blocked",
        plan=_plan("blocked", AddColumn(DesiredColumn("a", Integer()))),
        blocked_failures=(_blocked_failure("blocked"),),
    )
    unchanged = _grid_report("unchanged")
    not_planned = _grid_report(
        "not_planned",
        failures=(ValidationFailure(rule_name="R", message="m"),),
    )
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_runs=(applied, partial, blocked, unchanged, not_planned),
    )

    # When rendering the real-run footer
    footer = run_summary_footer(sync)

    # Then it reports catalog outcomes rather than planned-change and failure counts
    assert footer == (
        "5 tables: 1 applied, 1 partially applied, 1 not applied, 1 unchanged, 1 not planned (3.0s)"
    )


def test_a_single_table_run_uses_the_singular_noun():
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_runs=(_grid_report("orders"),),
    )

    assert run_summary_footer(sync).startswith("1 table:")


# ---------- whole-report rendering ----------


def test_render_report_is_the_status_grid_followed_by_the_summary_footer():
    # Given a run over one changed and one failed table
    changed = _grid_report(
        "a", plan=_plan("a", SetTableComment(desired_comment="c", observed_comment=""))
    )
    failed = _grid_report("b", failures=(ValidationFailure(rule_name="R", message="m"),))
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_runs=(changed, failed),
        dry_run=True,
    )

    # When rendering the whole report
    rendered = render_report(sync)

    # Then it opens with the SYNC REPORT title, shows the grid header, and ends with the footer
    lines = rendered.splitlines()
    assert lines[0] == "SYNC REPORT"
    grid_header = next(line for line in lines if line.startswith("TABLE"))
    assert grid_header.split() == ["TABLE", "STATUS", "STATEMENTS", "DETAIL"]
    assert rendered.endswith("2 tables: 1 changed, 0 unchanged, 1 failed (3.0s)")


def test_render_report_of_an_empty_run_is_a_header_and_zero_footer():
    # Given a run over no tables
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_runs=(),
    )

    # When rendering the whole report
    rendered = render_report(sync)

    # Then the title, grid header, and an outcome-free footer are shown -- no empty-run sentinel
    lines = rendered.splitlines()
    assert lines[0] == "SYNC REPORT"
    grid_header = next(line for line in lines if line.startswith("TABLE"))
    assert grid_header.split() == ["TABLE", "STATUS", "STATEMENTS", "DETAIL"]
    assert rendered.endswith("0 tables (3.0s)")


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
        table_runs=(failed,),
    )

    rendered = render_report(sync)

    # Then a Failures section lists every failure in full (not '+N more')
    assert "Failures" in rendered
    assert "Validation failed: R1 - first" in rendered
    assert "Validation failed: R2 - second" in rendered


def test_failures_section_nests_supporting_detail_under_its_error_line():
    # Given a table whose execution failed, so its failure carries the SQL
    plan = _plan("orders", AddColumn(DesiredColumn("age", Integer())))
    failed = _grid_report(
        "orders",
        plan=plan,
        execution=_failed_execution(plan, applied=0),
    )
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_runs=(failed,),
    )

    lines = render_report(sync).splitlines()

    # Then the error line sits under its table, and the SQL nests below it —
    # depth is the renderer's decision, so the failure itself carries none
    assert "    Execution failed at statement 1: AnalysisException - boom" in lines
    assert "        SQL: SQL 0" in lines


def test_render_report_failures_section_has_an_underlined_header():
    # Given a run with a failed table
    failed = _grid_report("orders", failures=(ValidationFailure(rule_name="R", message="m"),))
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_runs=(failed,),
    )

    lines = render_report(sync).splitlines()

    # Then the Failures section is introduced by an underlined header
    index = lines.index("Failures")
    assert lines[index + 1] == "-" * len("Failures")


def test_render_report_has_no_failures_section_when_all_succeed():
    # Given a run where every table succeeds
    changed = _grid_report(
        "a", plan=_plan("a", SetTableComment(desired_comment="c", observed_comment=""))
    )
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_runs=(changed,),
        dry_run=True,
    )

    # When rendering the complete report
    rendered = render_report(sync)

    # Then no Failures section is rendered
    assert "Failures" not in rendered


def test_render_report_shows_dry_run_banner_only_for_dry_runs():
    # Given the same planned change in a dry run and a completed real run
    plan = _plan("a", SetTableComment(desired_comment="c", observed_comment=""))
    planned = _grid_report("a", plan=plan)
    applied = _grid_report("a", plan=plan, execution=_successful_execution(plan))
    base = dict(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
    )

    # When rendering both valid aggregates
    dry_rendered = render_report(SyncReport(**base, table_runs=(planned,), dry_run=True))
    applied_rendered = render_report(SyncReport(**base, table_runs=(applied,)))

    # Then the banner appears (below the title) for a dry run and is absent otherwise
    assert "PLAN — no planned SQL executed" in dry_rendered.splitlines()
    assert "PLAN — no planned SQL executed" not in applied_rendered


def test_render_report_is_titled():
    # Given any run
    changed = _grid_report(
        "a", plan=_plan("a", SetTableComment(desired_comment="c", observed_comment=""))
    )
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_runs=(changed,),
        dry_run=True,
    )

    # When rendering the complete report
    lines = render_report(sync).splitlines()

    # Then a SYNC REPORT title, underlined with a rule, heads the output
    assert lines[0] == "SYNC REPORT"
    assert lines[1] == "=" * len("SYNC REPORT")


def test_render_diff_is_titled():
    # Given any run
    first = _grid_report(
        "a", plan=_plan("a", SetTableComment(desired_comment="c", observed_comment=""))
    )
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_runs=(first,),
        dry_run=True,
    )

    # When rendering the run's diff
    lines = render_diff(sync).splitlines()

    # Then a DIFF title, underlined with a rule, heads the output
    assert lines[0] == "DIFF"
    assert lines[1] == "=" * len("DIFF")


def test_render_diff_joins_each_tables_change_block_in_report_order():
    # Given a run over two tables with plans
    first = _grid_report(
        "a", plan=_plan("a", SetTableComment(desired_comment="c", observed_comment=""))
    )
    second = _grid_report("b", plan=_plan("b", AddColumn(DesiredColumn("age", Integer()))))
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_runs=(first, second),
        dry_run=True,
    )

    # When rendering the run's diff
    rendered = render_diff(sync)

    # Then each table's change block appears, in report order
    assert rendered.index("cat.sch.a") < rendered.index("cat.sch.b")
    assert "cat.sch.a" in rendered.splitlines()
    assert "cat.sch.b" in rendered.splitlines()
    assert "~ table  'c'" in rendered
    assert "+ age  Integer" in rendered


def test_render_diff_marks_unapplied_real_run_changes_in_their_headers():
    # Given a real run with applied, blocked, and partially applied plans
    applied_plan = _plan("applied", AddColumn(DesiredColumn("a", Integer())))
    applied = _grid_report(
        "applied",
        plan=applied_plan,
        execution=_successful_execution(applied_plan),
    )
    blocked = _grid_report(
        "blocked",
        plan=_plan("blocked", AddColumn(DesiredColumn("a", Integer()))),
        blocked_failures=(_blocked_failure("blocked"),),
    )
    partial_plan = _plan(
        "partial",
        AddColumn(DesiredColumn("a", Integer())),
        AddColumn(DesiredColumn("b", Integer())),
    )
    partial = _grid_report(
        "partial",
        plan=partial_plan,
        execution=_failed_execution(partial_plan, applied=1),
    )
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_runs=(applied, blocked, partial),
    )

    # When rendering the real run's diff
    lines = render_diff(sync).splitlines()

    # Then only incomplete outcomes are marked with how much reached the catalog
    assert "cat.sch.applied" in lines
    assert "cat.sch.blocked  (not applied)" in lines
    assert "cat.sch.partial  (partially applied, 1/2)" in lines


def test_enable_table_feature_renders_a_permanent_features_entry():
    # Given a planned table-feature enablement
    action = EnableTableFeature(feature=TableFeature.TIMESTAMP_NTZ)

    # When projecting the action into public diff entries
    [entry] = action_entries(action)

    # Then it is presented as a permanent feature addition
    assert entry.category is DiffCategory.FEATURES
    assert entry.symbol == "+"
    assert entry.subject == "timestampNtz"
    assert "permanent" in entry.detail[0]


@pytest.mark.parametrize(
    ("unresolvable", "expected"),
    [
        (
            ColumnCaseDrift(declared_name="SKU", observed_name="sku"),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.CHANGE,
                    "SKU",
                    ("spelled 'sku' in the catalog",),
                ),
            ),
        ),
        (
            ColumnRenameConflict(old_name="old_id", new_name="id"),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.CHANGE,
                    "old_id",
                    ("renamed → id, but both columns exist",),
                ),
            ),
        ),
        (
            PropertyUndeclared(name="delta.enableChangeDataFeed", observed_value="true"),
            (
                DiffEntry(
                    DiffCategory.PROPERTIES,
                    DiffOperation.CHANGE,
                    "delta.enableChangeDataFeed",
                    ("= 'true'", "(set on the table, undeclared)"),
                ),
            ),
        ),
        (
            PartitioningChanged(
                desired_partitioning=("region",), observed_partitioning=("country",)
            ),
            (
                DiffEntry(
                    DiffCategory.PARTITIONING,
                    DiffOperation.CHANGE,
                    "partitioning",
                    ("(country) → (region)",),
                ),
            ),
        ),
    ],
)
def test_unresolvable_differences_describe_themselves(unresolvable, expected):
    assert unresolvable_entries(unresolvable) == expected


def test_a_rejected_table_shows_the_drift_that_was_refused():
    # Given a table whose declaration adds a NOT NULL column and drops another,
    # both refused by validation
    qualified_name = QualifiedName("cat", "sch", "orders")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(
            DesiredColumn("id", Integer(), nullable=False),
            DesiredColumn("email", String(), nullable=False),
        ),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(
            ObservedColumn("id", Integer(), nullable=False),
            ObservedColumn("obsolete", String()),
        ),
    )
    report = TableRun(
        read=TablePresent(table=observed),
        planning=PlanningRejected(
            diff=diff_table(desired, observed),
            failures=(ValidationFailure(rule_name="NonNullableColumnAdd", message="nope"),),
        ),
        compiled=None,
        resolution=TableResolution(desired, (), ()),
        execution=None,
    )

    # When the rejected report is rendered as part of a real run
    block = render_diff(
        SyncReport(
            started_at=datetime(2025, 1, 1, 0, 0, 0),
            ended_at=datetime(2025, 1, 1, 0, 0, 3),
            table_runs=(report,),
        )
    )

    # Then it names the refused changes rather than claiming there were none
    assert "(no changes" not in block
    assert "cat.sch.orders  (REJECTED — no SQL planned)" in block.splitlines()
    assert "+ email" in block
    assert "- obsolete" in block


def test_a_rejected_table_shows_the_differences_no_action_could_close():
    # Given a table rejected over drift an action cannot express
    qualified_name = QualifiedName("cat", "sch", "orders")
    columns = (("id", Integer()), ("region", String()), ("country", String()))
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=tuple(DesiredColumn(name, data_type) for name, data_type in columns),
        partitioned_by=("region",),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=tuple(ObservedColumn(name, data_type) for name, data_type in columns),
        partitioned_by=("country",),
    )
    report = TableRun(
        read=TablePresent(table=observed),
        planning=PlanningRejected(
            diff=diff_table(desired, observed),
            failures=(ValidationFailure(rule_name="PartitioningIsImmutable", message="nope"),),
        ),
        compiled=None,
        resolution=TableResolution(desired, (), ()),
        execution=None,
    )

    # When the block is rendered
    block = render_diff_block(report)

    # Then the unresolvable difference is shown alongside any refused actions
    assert "~ partitioning  (country) → (region)" in block


def test_a_table_with_no_diff_at_all_still_points_at_its_failures():
    # Given a report that carries failures but no diff (a hand-built report,
    # or a table that failed before the diff phase)
    block = render_diff_block(_report_with_empty_plan_and_failure())

    # Then the old wording stands — there is genuinely nothing to show
    assert "(no changes — see failures)" in block
