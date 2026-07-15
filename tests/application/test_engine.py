from hypothesis import given, strategies as st
import pytest

from delta_engine.application.engine import Engine
from delta_engine.application.errors import DuplicateTableDefinitionError, SyncFailedError
from delta_engine.application.failures import (
    ExecutionFailure,
    ForeignKeyFailure,
    ForeignKeyFailureReason,
    ReadFailure,
)
from delta_engine.application.ports import (
    CatalogState,
    ExecutionFailed,
    ExecutionResult,
    ExecutionSucceeded,
    ExecutionSummary,
    ReadFailed,
    TableAbsent,
    TablePresent,
)
from delta_engine.application.report import (
    SyncReport,
    TableRunReport,
    TableRunStatus,
)
from delta_engine.domain.model import ObservedColumn, ObservedTable, QualifiedName
from delta_engine.domain.model.constraints import PrimaryKeyConstraint
from delta_engine.domain.plan import ActionPlan
from delta_engine.domain.plan.actions import (
    CreateTable,
    SetColumnComment,
    SetColumnTag,
    SetTableComment,
    SetTableTag,
    UnsetColumnTag,
    UnsetTableTag,
)
from delta_engine.schema import Column, DeltaTable, ForeignKey, String
from tests.builders import as_observed_columns

# ---------------------------------------------------------------------------
# Helpers and fakes
# ---------------------------------------------------------------------------


def _qualified_name(fqn: str) -> QualifiedName:
    return QualifiedName.parse(fqn)


def _split_fqn(fqn: str) -> tuple[str, str, str]:
    catalog, schema, table_name = fqn.split(".")
    return catalog, schema, table_name


def _spec(fqn: str) -> DeltaTable:
    """Build a minimal table declaration with a single-column primary key."""
    catalog, schema, table_name = _split_fqn(fqn)

    return DeltaTable(
        catalog,
        schema,
        table_name,
        columns=(
            Column(
                "id",
                String(),
                nullable=False,
            ),
        ),
        primary_key=["id"],
    )


def _spec_without_pk(fqn: str) -> DeltaTable:
    """Build a table declaration with a plain id column and no primary key."""
    catalog, schema, table_name = _split_fqn(fqn)

    return DeltaTable(
        catalog,
        schema,
        table_name,
        columns=(Column("id", String()),),
    )


def _referenced_spec(fqn: str) -> DeltaTable:
    """Build a minimal table declaration for use as an FK target."""
    return _spec(fqn)


def _spec_with_fk(fqn: str, references: str) -> DeltaTable:
    """Build a table declaration with a single FK to another table."""
    catalog, schema, table_name = _split_fqn(fqn)

    return DeltaTable(
        catalog,
        schema,
        table_name,
        columns=(
            Column("id", String(), nullable=False),
            Column("ref_id", String()),
        ),
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(
                columns={"ref_id": "id"},
                references=_referenced_spec(references),
            )
        ],
    )


def _spec_adding_not_null(fqn: str) -> DeltaTable:
    """
    Build a spec that adds a NOT NULL column to an existing id-only table.

    Diffed against _existing_id_table(), this trips a real validation rule.
    """
    catalog, schema, table_name = _split_fqn(fqn)

    return DeltaTable(
        catalog,
        schema,
        table_name,
        columns=(
            Column("id", String(), nullable=False),
            Column("order_id", String(), nullable=False),
        ),
        primary_key=["id"],
    )


def _existing_id_table(fqn: str) -> TablePresent:
    """Build an observed table with one existing id column."""
    catalog, schema, table_name = _split_fqn(fqn)

    return TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName(catalog, schema, table_name),
            columns=(ObservedColumn("id", String()),),
        )
    )


def _existing_id_table_synced(fqn: str) -> TablePresent:
    """Build an observed table that already matches _spec(fqn)."""
    catalog, schema, table_name = _split_fqn(fqn)

    return TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName(catalog, schema, table_name),
            columns=(ObservedColumn("id", String(), nullable=False),),
            primary_key=PrimaryKeyConstraint.generate(
                table_name=table_name,
                columns=("id",),
            ),
        )
    )


def _existing_fk_table_synced(fqn: str, references: str) -> TablePresent:
    """Build an observed table that already matches _spec_with_fk(fqn, references)."""
    desired = _spec_with_fk(fqn, references).to_desired_table()

    return TablePresent(
        table=ObservedTable(
            qualified_name=desired.qualified_name,
            columns=as_observed_columns(desired.columns),
            primary_key=desired.primary_key,
            foreign_keys=desired.foreign_keys,
        )
    )


def _metadata_scoped_spec(fqn: str) -> DeltaTable:
    """Build a metadata-scoped declaration with table and column comments."""
    catalog, schema, table_name = _split_fqn(fqn)

    return DeltaTable(
        catalog,
        schema,
        table_name,
        columns=(Column("id", String(), comment="surrogate key"),),
        comment="orders table",
        scope="metadata",
    )


def _tag_scoped_spec(fqn: str) -> DeltaTable:
    """Build a tag-scoped declaration that manages only tags."""
    catalog, schema, table_name = _split_fqn(fqn)

    return DeltaTable(
        catalog,
        schema,
        table_name,
        columns=(Column("id", String(), tags={"pii": "false"}),),
        tags={"domain": "events"},
        scope="tags",
    )


def _existing_matching_table(fqn: str) -> TablePresent:
    """Build an observed table whose schema matches _metadata_scoped_spec."""
    catalog, schema, table_name = _split_fqn(fqn)

    return TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName(catalog, schema, table_name),
            columns=(ObservedColumn("id", String()),),
        )
    )


def _existing_tag_drifted_table(fqn: str) -> TablePresent:
    """Build an observed table with tag drift against _tag_scoped_spec."""
    catalog, schema, table_name = _split_fqn(fqn)

    return TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName(catalog, schema, table_name),
            columns=(ObservedColumn("id", String(), tags={"stale": "true"}),),
            tags={"legacy": "yes"},
        )
    )


def _ok_exec(statement_index: int = 0) -> ExecutionResult:
    return ExecutionSucceeded(
        statement_index=statement_index,
        statement="-- ok",
    )


def _failed_exec(
    statement_index: int = 0,
    *,
    exception_type: str = "AnalysisException",
    message: str = "boom",
) -> ExecutionResult:
    return ExecutionFailed(
        failure=ExecutionFailure(
            statement_index=statement_index,
            exception_type=exception_type,
            message=message,
            statement="-- bad sql",
        ),
    )


class _RecordingReader:
    def __init__(self, mapping: dict[str, CatalogState] | None = None) -> None:
        self.mapping = mapping or {}
        self.calls: list[QualifiedName] = []

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        self.calls.append(qualified_name)
        return self.mapping.get(str(qualified_name), TableAbsent())

    @property
    def fetched_names(self) -> list[str]:
        return [str(qualified_name) for qualified_name in self.calls]


class _RecordingExecutor:
    """
    Records execution calls and returns configured results.

    If per_call_results is None, every execution succeeds. If an explicit list
    is supplied, each call consumes one item and unexpected extra calls fail
    loudly.
    """

    def __init__(
        self,
        per_call_results: list[tuple[ExecutionResult, ...]] | None = None,
    ) -> None:
        self.calls: list[tuple[str, ...]] = []
        self._per_call_results = None if per_call_results is None else list(per_call_results)

    def compile(self, qualified_name: QualifiedName, plan: ActionPlan) -> tuple[str, ...]:
        return tuple(f"STATEMENT {index} FOR {qualified_name}" for index in range(len(plan)))

    def execute(self, statements: tuple[str, ...]) -> ExecutionSummary:
        self.calls.append(statements)

        if self._per_call_results is None:
            return ExecutionSummary((_ok_exec(),))

        if not self._per_call_results:
            raise AssertionError(f"Unexpected execution call: {statements}")

        return ExecutionSummary(self._per_call_results.pop(0))

    @property
    def executed_names(self) -> list[str]:
        # compile embeds the table name in each fake statement; recover it so
        # tests can assert which tables executed, since execute takes no name.
        return [statements[0].split(" FOR ", 1)[1] for statements in self.calls]


def _reports_by_name(report: SyncReport) -> dict[str, TableRunReport]:
    return {str(table_report.qualified_name): table_report for table_report in report}


def _foreign_key_failures(table_report: TableRunReport) -> list[ForeignKeyFailure]:
    return [failure for failure in table_report.failures if isinstance(failure, ForeignKeyFailure)]


def _assert_status(
    report: SyncReport,
    fqn: str,
    status: TableRunStatus,
) -> TableRunReport:
    table_report = _reports_by_name(report)[fqn]
    assert table_report.status is status
    return table_report


def _assert_has_fk_failure(
    table_report: TableRunReport,
    *,
    reason: ForeignKeyFailureReason,
    references: str | None = None,
    local_columns: tuple[str, ...] | None = None,
) -> None:
    matching_failures = [
        failure for failure in _foreign_key_failures(table_report) if failure.reason is reason
    ]

    if references is not None:
        matching_failures = [
            failure
            for failure in matching_failures
            if failure.references == _qualified_name(references)
        ]

    if local_columns is not None:
        matching_failures = [
            failure for failure in matching_failures if failure.local_columns == local_columns
        ]

    assert matching_failures


# ---------------------------------------------------------------------------
# Successful syncs and report shape
# ---------------------------------------------------------------------------


def test_syncing_no_tables_returns_empty_report_without_reading_or_executing():
    # Given no tables to sync
    reader = _RecordingReader()
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    report = engine.sync()

    # Then an empty, non-failing report is returned
    assert isinstance(report, SyncReport)
    assert report.has_failures is False
    assert tuple(report) == ()
    assert reader.fetched_names == []
    assert executor.executed_names == []


def test_sync_returns_report_when_all_tables_succeed():
    # Given two absent tables that can be created
    reader = _RecordingReader(
        {
            "c.a.users": TableAbsent(),
            "c.b.orders": TableAbsent(),
        }
    )
    executor = _RecordingExecutor(
        [
            (_ok_exec(0),),
            (_ok_exec(0),),
        ]
    )
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    report = engine.sync(
        _spec("c.b.orders"),
        _spec("c.a.users"),
    )

    # Then both tables succeed in prepared name order
    assert isinstance(report, SyncReport)
    assert report.has_failures is False
    assert [table_report.status for table_report in report] == [
        TableRunStatus.SUCCESS,
        TableRunStatus.SUCCESS,
    ]
    assert executor.executed_names == ["c.a.users", "c.b.orders"]


@st.composite
def _distinct_qualified_names(
    draw: st.DrawFn, min_size: int = 1, max_size: int = 8
) -> list[QualifiedName]:
    """Draw a list of distinct QualifiedName instances."""
    part = st.from_regex(r"[a-z][a-z0-9]{0,9}", fullmatch=True)
    names: list[QualifiedName] = []
    seen: set[str] = set()
    size = draw(st.integers(min_value=min_size, max_value=max_size))
    attempts = 0
    while len(names) < size and attempts < size * 20:
        attempts += 1
        catalog, schema, name = draw(part), draw(part), draw(part)
        key = f"{catalog}.{schema}.{name}"
        if key not in seen:
            seen.add(key)
            names.append(QualifiedName(catalog, schema, name))
    return names


@given(_distinct_qualified_names(min_size=1, max_size=8))
def test_tables_are_always_processed_in_qualified_name_order(
    qualified_names: list[QualifiedName],
) -> None:
    # Given N distinct table declarations in arbitrary order
    specs = [_spec(str(qn)) for qn in qualified_names]
    reader = _RecordingReader()
    engine = Engine(reader=reader, executor=_RecordingExecutor())

    # When syncing
    report = engine.sync(*specs)

    # Then reads and reports are always in lexicographic qualified-name order
    assert reader.fetched_names == sorted(reader.fetched_names)
    reported = [str(table_report.qualified_name) for table_report in report]
    assert reported == sorted(reported)


def test_unchanged_table_is_reported_successful_without_execution():
    # Given the observed table already matches the desired declaration
    fqn = "c.s.same"
    reader = _RecordingReader({fqn: _existing_id_table_synced(fqn)})
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    report = engine.sync(_spec(fqn))

    # Then the table succeeds with no execution summary
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.SUCCESS
    assert table_report.execution is None
    assert executor.executed_names == []


def test_real_run_records_the_applied_plan_on_the_report():
    # Given a new table that will be created
    fqn = "c.s.new_table"
    reader = _RecordingReader({fqn: TableAbsent()})
    executor = _RecordingExecutor([(_ok_exec(0),)])
    engine = Engine(reader=reader, executor=executor)

    # When syncing for real
    report = engine.sync(_spec(fqn))

    # Then the report records the plan that was applied
    [table_report] = list(report)
    assert [type(action) for action in table_report.plan] == [CreateTable]
    assert table_report.status is TableRunStatus.SUCCESS
    assert executor.executed_names == [fqn]


def test_tag_scoped_dry_run_plans_only_tag_actions():
    # Given a tag-scoped declaration over an existing table with tag drift
    fqn = "c.s.streaming_events"
    reader = _RecordingReader({fqn: _existing_tag_drifted_table(fqn)})
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing as a dry run
    report = engine.sync(_tag_scoped_spec(fqn), dry_run=True)

    # Then the plan reconciles table and column tags, and nothing else
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.SUCCESS
    assert [type(action) for action in table_report.plan] == [
        SetTableTag,
        UnsetTableTag,
        SetColumnTag,
        UnsetColumnTag,
    ]
    assert executor.executed_names == []


def test_dry_run_is_recorded_on_the_report():
    # Given a dry run over one table
    fqn = "c.s.new_table"
    reader = _RecordingReader({fqn: TableAbsent()})
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing as a dry run
    report = engine.sync(_spec(fqn), dry_run=True)

    # Then the report records that no changes were applied
    assert report.dry_run is True
    assert executor.executed_names == []


def test_real_run_records_dry_run_false():
    # Given a normal (applied) run
    fqn = "c.s.new_table"
    reader = _RecordingReader({fqn: TableAbsent()})
    executor = _RecordingExecutor([(_ok_exec(0),)])
    engine = Engine(reader=reader, executor=executor)

    # When syncing for real
    report = engine.sync(_spec(fqn))

    # Then the report is marked as not a dry run
    assert report.dry_run is False


# ---------------------------------------------------------------------------
# Phase ordering and failure gating
# ---------------------------------------------------------------------------


def test_read_phase_attempts_all_tables_before_any_execution():
    # Given three tables where the middle read fails
    events: list[str] = []

    class _EventRecordingReader:
        def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
            events.append(f"read:{qualified_name}")
            if str(qualified_name) == "c.s.b":
                return ReadFailed(ReadFailure("IOError", "cannot read"))
            return TableAbsent()

    class _EventRecordingExecutor:
        def compile(self, qualified_name: QualifiedName, plan: ActionPlan) -> tuple[str, ...]:
            # Silent by design: the plan phase compiles every table, but this
            # test asserts read/execute event ordering, not compilation. The
            # name is embedded so execute can name the table in its event.
            return tuple(f"STATEMENT {index} FOR {qualified_name}" for index in range(len(plan)))

        def execute(self, statements: tuple[str, ...]) -> ExecutionSummary:
            events.append(f"execute:{statements[0].split(' FOR ', 1)[1]}")
            return ExecutionSummary((_ok_exec(0),))

    engine = Engine(
        reader=_EventRecordingReader(),
        executor=_EventRecordingExecutor(),
    )

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(
            _spec("c.s.a"),
            _spec("c.s.b"),
            _spec("c.s.c"),
        )

    # Then every read is attempted before execution starts
    assert events[:3] == [
        "read:c.s.a",
        "read:c.s.b",
        "read:c.s.c",
    ]
    assert events[3:] == [
        "execute:c.s.a",
        "execute:c.s.c",
    ]

    report = exc_info.value.report
    _assert_status(report, "c.s.a", TableRunStatus.SUCCESS)
    failed_table = _assert_status(report, "c.s.b", TableRunStatus.READ_FAILED)
    _assert_status(report, "c.s.c", TableRunStatus.SUCCESS)

    assert len(failed_table.failures) == 1
    assert failed_table.execution is None


def test_read_failure_is_reported_once_and_has_no_plan_or_execution():
    # Given a table whose read fails
    fqn = "c.s.read_fail"
    reader = _RecordingReader(
        {
            fqn: ReadFailed(ReadFailure("IOError", "cannot read")),
        }
    )
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(_spec(fqn))

    # Then the read failure appears once and the run has no plan or execution
    [table_report] = list(exc_info.value.report)
    assert table_report.status is TableRunStatus.READ_FAILED
    assert len(table_report.failures) == 1
    assert len(table_report.plan) == 0
    assert table_report.execution is None
    assert executor.executed_names == []

    assert str(exc_info.value).count("Read error: IOError - cannot read") == 1


def test_validation_failed_table_is_not_executed_but_independent_table_still_runs():
    # Given one table fails validation and another can be created
    reader = _RecordingReader(
        {
            "c.s.a": _existing_id_table("c.s.a"),
            "c.s.b": TableAbsent(),
        }
    )
    executor = _RecordingExecutor([(_ok_exec(0),)])
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(
            _spec_adding_not_null("c.s.a"),
            _spec("c.s.b"),
        )

    # Then the invalid table is skipped and the independent table succeeds
    report = exc_info.value.report
    table_a = _assert_status(report, "c.s.a", TableRunStatus.PLANNING_FAILED)
    table_b = _assert_status(report, "c.s.b", TableRunStatus.SUCCESS)

    assert table_a.execution is None
    assert table_b.execution is not None
    assert executor.executed_names == ["c.s.b"]


def test_execution_failure_is_reported_but_independent_later_table_still_executes():
    # Given the first table fails during execution and the second is independent
    reader = _RecordingReader(
        {
            "c.s.a": TableAbsent(),
            "c.s.b": TableAbsent(),
        }
    )
    executor = _RecordingExecutor(
        [
            (_failed_exec(0),),
            (_ok_exec(0),),
        ]
    )
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(
            _spec("c.s.a"),
            _spec("c.s.b"),
        )

    # Then the second table still executes and succeeds
    report = exc_info.value.report
    table_a = _assert_status(report, "c.s.a", TableRunStatus.EXECUTION_FAILED)
    table_b = _assert_status(report, "c.s.b", TableRunStatus.SUCCESS)

    assert table_a.execution is not None
    assert table_b.execution is not None
    assert executor.executed_names == ["c.s.a", "c.s.b"]


def test_execution_summary_is_retained_when_one_action_fails():
    # Given execution returns a mixed summary for one table
    fqn = "c.s.exec_fail"
    reader = _RecordingReader({fqn: TableAbsent()})
    executor = _RecordingExecutor(
        [
            (
                _ok_exec(0),
                _failed_exec(1),
                _ok_exec(2),
            )
        ]
    )
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(_spec(fqn))

    # Then the whole execution summary is kept on the report
    [table_report] = list(exc_info.value.report)
    assert table_report.status is TableRunStatus.EXECUTION_FAILED
    assert table_report.execution is not None
    assert len(table_report.execution.results) == 3
    assert executor.executed_names == [fqn]


# ---------------------------------------------------------------------------
# Foreign-key orchestration
# ---------------------------------------------------------------------------


def test_sync_processes_tables_in_fk_dependency_order():
    # Given orders depends on customers, but orders is passed first
    reader = _RecordingReader()
    executor = _RecordingExecutor()
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    engine.sync(
        _spec_with_fk("cat.sch.orders", "cat.sch.customers"),
        _spec("cat.sch.customers"),
    )

    # Then customers executes before orders
    assert executor.executed_names.index("cat.sch.customers") < executor.executed_names.index(
        "cat.sch.orders"
    )


def test_sync_fails_table_whose_fk_references_table_not_in_the_sync():
    # Given orders references customers, but customers is not registered
    reader = _RecordingReader()
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(_spec_with_fk("cat.sch.orders", "cat.sch.customers"))

    # Then orders is FK-failed and not executed
    [orders] = list(exc_info.value.report)
    assert orders.status is TableRunStatus.FOREIGN_KEY_FAILED
    assert orders.execution is None
    assert executor.executed_names == []

    _assert_has_fk_failure(
        orders,
        reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
        references="cat.sch.customers",
        local_columns=("ref_id",),
    )


def test_read_failure_in_upstream_blocks_fk_dependent():
    # Given a fails to read and b depends on a
    reader = _RecordingReader(
        {
            "cat.sch.a": ReadFailed(ReadFailure("IOError", "cannot read")),
        }
    )
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(
            _spec("cat.sch.a"),
            _spec_with_fk("cat.sch.b", "cat.sch.a"),
        )

    # Then b is blocked and neither table executes
    report = exc_info.value.report
    table_a = _assert_status(report, "cat.sch.a", TableRunStatus.READ_FAILED)
    table_b = _assert_status(report, "cat.sch.b", TableRunStatus.FOREIGN_KEY_FAILED)

    _assert_has_fk_failure(
        table_b,
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.a",
    )

    assert table_a.execution is None
    assert table_b.execution is None
    assert executor.executed_names == []


def test_validation_failure_in_upstream_blocks_fk_dependent():
    # Given customers fails validation and orders depends on customers
    reader = _RecordingReader(
        {
            "cat.sch.customers": _existing_id_table("cat.sch.customers"),
            "cat.sch.orders": TableAbsent(),
        }
    )
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(
            _spec_adding_not_null("cat.sch.customers"),
            _spec_with_fk("cat.sch.orders", "cat.sch.customers"),
        )

    # Then orders is blocked before execution
    report = exc_info.value.report
    customers = _assert_status(
        report,
        "cat.sch.customers",
        TableRunStatus.PLANNING_FAILED,
    )
    orders = _assert_status(
        report,
        "cat.sch.orders",
        TableRunStatus.FOREIGN_KEY_FAILED,
    )

    _assert_has_fk_failure(
        orders,
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.customers",
    )

    assert customers.execution is None
    assert orders.execution is None
    assert executor.executed_names == []


def test_sync_fails_fk_that_does_not_reference_a_primary_key():
    # Given orders references customers, but customers has no primary key
    reader = _RecordingReader()
    executor = _RecordingExecutor([(_ok_exec(0),)])
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(
            _spec_with_fk("cat.sch.orders", "cat.sch.customers"),
            _spec_without_pk("cat.sch.customers"),
        )

    # Then orders is FK-failed, but customers can still be created
    report = exc_info.value.report
    orders = _assert_status(
        report,
        "cat.sch.orders",
        TableRunStatus.FOREIGN_KEY_FAILED,
    )
    customers = _assert_status(report, "cat.sch.customers", TableRunStatus.SUCCESS)

    _assert_has_fk_failure(
        orders,
        reason=ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY,
        references="cat.sch.customers",
    )

    assert orders.execution is None
    assert customers.execution is not None
    assert executor.executed_names == ["cat.sch.customers"]


def test_execution_failure_in_fk_parent_blocks_dependent_before_execution():
    # Given orders depends on customers, and customers fails during execution
    reader = _RecordingReader()
    executor = _RecordingExecutor(
        [
            (_failed_exec(0),),  # customers
            (_ok_exec(0),),  # orders should not be reached after the engine fix
        ]
    )
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(
            _spec_with_fk("cat.sch.orders", "cat.sch.customers"),
            _spec("cat.sch.customers"),
        )

    # Then customers fails execution and orders is blocked before execution
    report = exc_info.value.report
    customers = _assert_status(
        report,
        "cat.sch.customers",
        TableRunStatus.EXECUTION_FAILED,
    )
    orders = _assert_status(
        report,
        "cat.sch.orders",
        TableRunStatus.FOREIGN_KEY_FAILED,
    )

    assert customers.execution is not None
    assert orders.execution is None
    assert executor.executed_names == ["cat.sch.customers"]

    _assert_has_fk_failure(
        orders,
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.customers",
    )


def test_execution_failure_blocks_transitively_along_fk_chain():
    # Given c -> b -> a, and a fails during execution
    reader = _RecordingReader()
    executor = _RecordingExecutor([(_failed_exec(0),)])  # a only; b and c must not run
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(
            _spec_with_fk("cat.sch.c", "cat.sch.b"),
            _spec_with_fk("cat.sch.b", "cat.sch.a"),
            _spec("cat.sch.a"),
        )

    # Then the block propagates from a through b to c before execution
    report = exc_info.value.report
    _assert_status(report, "cat.sch.a", TableRunStatus.EXECUTION_FAILED)
    table_b = _assert_status(report, "cat.sch.b", TableRunStatus.FOREIGN_KEY_FAILED)
    table_c = _assert_status(report, "cat.sch.c", TableRunStatus.FOREIGN_KEY_FAILED)

    _assert_has_fk_failure(
        table_b,
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.a",
    )
    _assert_has_fk_failure(
        table_c,
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.b",
    )

    assert table_b.execution is None
    assert table_c.execution is None
    assert executor.executed_names == ["cat.sch.a"]


def test_partial_execution_failure_in_parent_blocks_dependent():
    # Given customers' plan fails on one action among successful ones
    reader = _RecordingReader()
    executor = _RecordingExecutor([(_ok_exec(0), _failed_exec(1))])
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(
            _spec_with_fk("cat.sch.orders", "cat.sch.customers"),
            _spec("cat.sch.customers"),
        )

    # Then a partially failed parent still blocks its dependent
    report = exc_info.value.report
    customers = _assert_status(report, "cat.sch.customers", TableRunStatus.EXECUTION_FAILED)
    orders = _assert_status(report, "cat.sch.orders", TableRunStatus.FOREIGN_KEY_FAILED)

    _assert_has_fk_failure(
        orders,
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.customers",
    )

    assert customers.execution is not None
    assert orders.execution is None
    assert executor.executed_names == ["cat.sch.customers"]


def test_execution_failure_blocks_dependent_that_needed_no_changes():
    # Given orders already matches its declaration (empty plan)
    # and customers fails during execution
    reader = _RecordingReader(
        {"cat.sch.orders": _existing_fk_table_synced("cat.sch.orders", "cat.sch.customers")}
    )
    executor = _RecordingExecutor([(_failed_exec(0),)])  # customers only
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(
            _spec_with_fk("cat.sch.orders", "cat.sch.customers"),
            _spec("cat.sch.customers"),
        )

    # Then the no-op dependent is blocked too: one blocking rule everywhere
    report = exc_info.value.report
    _assert_status(report, "cat.sch.customers", TableRunStatus.EXECUTION_FAILED)
    orders = _assert_status(report, "cat.sch.orders", TableRunStatus.FOREIGN_KEY_FAILED)

    _assert_has_fk_failure(
        orders,
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.customers",
    )

    assert orders.execution is None
    assert executor.executed_names == ["cat.sch.customers"]


def test_execution_failure_blocks_diamond_dependent_with_one_failure_per_fk():
    # Given d -> b and d -> c, both b and c -> a, and a fails during execution
    spec_d = DeltaTable(
        "cat",
        "sch",
        "d",
        columns=(
            Column("id", String(), nullable=False),
            Column("b_id", String()),
            Column("c_id", String()),
        ),
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(columns={"b_id": "id"}, references=_referenced_spec("cat.sch.b")),
            ForeignKey(columns={"c_id": "id"}, references=_referenced_spec("cat.sch.c")),
        ],
    )

    reader = _RecordingReader()
    executor = _RecordingExecutor([(_failed_exec(0),)])  # a only
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(
            spec_d,
            _spec_with_fk("cat.sch.b", "cat.sch.a"),
            _spec_with_fk("cat.sch.c", "cat.sch.a"),
            _spec("cat.sch.a"),
        )

    # Then the block flows through both branches and d records both blocking FKs
    report = exc_info.value.report
    _assert_status(report, "cat.sch.a", TableRunStatus.EXECUTION_FAILED)
    _assert_status(report, "cat.sch.b", TableRunStatus.FOREIGN_KEY_FAILED)
    _assert_status(report, "cat.sch.c", TableRunStatus.FOREIGN_KEY_FAILED)
    table_d = _assert_status(report, "cat.sch.d", TableRunStatus.FOREIGN_KEY_FAILED)

    assert len(_foreign_key_failures(table_d)) == 2
    _assert_has_fk_failure(
        table_d,
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.b",
        local_columns=("b_id",),
    )
    _assert_has_fk_failure(
        table_d,
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.c",
        local_columns=("c_id",),
    )
    assert executor.executed_names == ["cat.sch.a"]


def test_synced_fk_parent_with_no_work_does_not_block_dependent():
    # Given customers already matches its declaration and orders is absent
    reader = _RecordingReader({"cat.sch.customers": _existing_id_table_synced("cat.sch.customers")})
    executor = _RecordingExecutor([(_ok_exec(0),)])  # orders only
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    report = engine.sync(
        _spec_with_fk("cat.sch.orders", "cat.sch.customers"),
        _spec("cat.sch.customers"),
    )

    # Then the healthy no-op parent does not block its dependent
    assert report.has_failures is False
    _assert_status(report, "cat.sch.customers", TableRunStatus.SUCCESS)
    _assert_status(report, "cat.sch.orders", TableRunStatus.SUCCESS)
    assert executor.executed_names == ["cat.sch.orders"]


# ---------------------------------------------------------------------------
# Duplicate declarations
# ---------------------------------------------------------------------------


def test_sync_rejects_duplicate_table_names_before_reading():
    # Given duplicate table declarations
    reader = _RecordingReader()
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(DuplicateTableDefinitionError) as excinfo:
        engine.sync(
            _spec("cat.sch.orders"),
            _spec("cat.sch.orders"),
        )

    # Then the typed error names the table and no phase has started
    assert str(excinfo.value.qualified_name) == "cat.sch.orders"
    assert reader.fetched_names == []
    assert executor.executed_names == []


# ---------------------------------------------------------------------------
# Dry run behaviour
# ---------------------------------------------------------------------------


def test_dry_run_does_not_execute_and_reports_no_execution():
    # Given two absent tables that would otherwise be created
    reader = _RecordingReader(
        {
            "c.s.a": TableAbsent(),
            "c.s.b": TableAbsent(),
        }
    )
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing in dry-run mode
    report = engine.sync(
        _spec("c.s.a"),
        _spec("c.s.b"),
        dry_run=True,
    )

    # Then plans are produced, but nothing is executed
    assert isinstance(report, SyncReport)
    assert report.has_failures is False
    assert [table_report.status for table_report in report] == [
        TableRunStatus.SUCCESS,
        TableRunStatus.SUCCESS,
    ]
    assert [table_report.execution for table_report in report] == [None, None]
    assert executor.executed_names == []


def test_dry_run_exposes_the_planned_actions_on_the_report():
    # Given a table that would be created
    fqn = "c.s.new_table"
    reader = _RecordingReader({fqn: TableAbsent()})
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing in dry-run mode
    report = engine.sync(_spec(fqn), dry_run=True)

    # Then the table report carries the plan that would have been applied
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.SUCCESS
    assert [type(action) for action in table_report.plan] == [CreateTable]
    assert table_report.execution is None
    assert executor.executed_names == []


def test_dry_run_records_the_sql_that_would_execute():
    # Given a table that would be created (dry run plans a CREATE)
    fqn = "c.s.new_table"
    reader = _RecordingReader({fqn: TableAbsent()})
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing in dry-run mode
    report = engine.sync(_spec(fqn), dry_run=True)

    # Then the report carries the compiled statements even though nothing ran
    [table_report] = list(report)
    assert table_report.has_changes is True
    assert table_report.execution is None
    assert len(table_report.planned_sql_statements) == len(table_report.plan)
    assert all("STATEMENT" in statement for statement in table_report.planned_sql_statements)


def test_failed_table_records_no_planned_sql():
    # Given a table whose read fails, so no plan is built
    fqn = "c.s.unreadable"
    reader = _RecordingReader({fqn: ReadFailed(ReadFailure("IOError", "cannot read"))})
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing in dry-run mode
    report = engine.sync(_spec(fqn), dry_run=True)

    # Then no statements are recorded for the failed table
    [table_report] = list(report)
    assert table_report.has_failures is True
    assert table_report.planned_sql_statements == ()


def test_fk_failed_table_still_reports_its_planned_sql_without_executing():
    # Given an absent table whose FK references a table not in the sync.
    # FK resolution runs after planning, so the plan and its SQL are already
    # legitimate facts about the table — only the ordering/dependency failed.
    reader = _RecordingReader()
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When performing a dry run
    report = engine.sync(_spec_with_fk("cat.sch.orders", "cat.sch.customers"), dry_run=True)

    # Then the table is FK-failed but still exposes the change it planned
    [orders] = list(report)
    assert orders.status is TableRunStatus.FOREIGN_KEY_FAILED
    assert orders.has_changes is True
    assert orders.planned_sql_statements != ()

    # And nothing executed
    assert orders.execution is None
    assert executor.executed_names == []


def test_dry_run_returns_validation_failures_without_raising_or_executing():
    # Given a table that would fail validation
    fqn = "c.s.val_fail"
    reader = _RecordingReader({fqn: _existing_id_table(fqn)})
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing in dry-run mode
    report = engine.sync(
        _spec_adding_not_null(fqn),
        dry_run=True,
    )

    # Then the failure is reported without raising or executing
    [table_report] = list(report)
    assert report.has_failures is True
    assert table_report.status is TableRunStatus.PLANNING_FAILED
    assert table_report.execution is None
    assert executor.executed_names == []


def test_dry_run_returns_fk_failures_without_raising_or_executing():
    # Given orders references customers, but customers is not registered
    reader = _RecordingReader()
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing in dry-run mode
    report = engine.sync(
        _spec_with_fk("cat.sch.orders", "cat.sch.customers"),
        dry_run=True,
    )

    # Then the FK failure is returned in the report
    [orders] = list(report)
    assert report.has_failures is True
    assert orders.status is TableRunStatus.FOREIGN_KEY_FAILED
    assert orders.execution is None
    assert executor.executed_names == []

    _assert_has_fk_failure(
        orders,
        reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
        references="cat.sch.customers",
    )


# ---------------------------------------------------------------------------
# Metadata-only sync behaviour
# ---------------------------------------------------------------------------


def test_metadata_scoped_sync_applies_metadata_when_schema_matches():
    # Given a live table whose schema matches the declaration
    fqn = "cat.sch.orders"
    reader = _RecordingReader({fqn: _existing_matching_table(fqn)})
    executor = _RecordingExecutor([(_ok_exec(0),)])
    engine = Engine(reader=reader, executor=executor)

    # When syncing a metadata-only declaration
    report = engine.sync(_metadata_scoped_spec(fqn))

    # Then the sync succeeds and metadata actions are planned
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.SUCCESS
    assert report.has_failures is False
    assert any(isinstance(action, SetColumnComment) for action in table_report.plan)
    assert any(isinstance(action, SetTableComment) for action in table_report.plan)
    assert executor.executed_names == [fqn]


def test_metadata_scoped_sync_fails_when_table_is_missing():
    # Given a metadata-only declaration for a missing table
    fqn = "cat.sch.orders"
    reader = _RecordingReader({fqn: TableAbsent()})
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(_metadata_scoped_spec(fqn))

    # Then validation fails and nothing executes
    [table_report] = list(exc_info.value.report)
    assert table_report.status is TableRunStatus.PLANNING_FAILED
    assert table_report.execution is None
    assert executor.executed_names == []
    assert any("does not exist" in failure.message for failure in table_report.failures)


def test_sync_fails_at_validation_when_dropping_column_without_column_mapping():
    # Given a live table with an extra column, and a declaration without columnMapping
    fqn = "cat.sch.orders"
    catalog, schema, name = fqn.split(".")
    reader = _RecordingReader(
        {
            fqn: TablePresent(
                table=ObservedTable(
                    qualified_name=QualifiedName(catalog, schema, name),
                    columns=(ObservedColumn("id", String()), ObservedColumn("stale", String())),
                )
            )
        }
    )
    engine = Engine(reader=reader, executor=_RecordingExecutor(per_call_results=[]))
    spec = DeltaTable(catalog, schema, name, columns=(Column("id", String()),))

    # When syncing (the plan would drop `stale`)
    with pytest.raises(SyncFailedError) as excinfo:
        engine.sync(spec)

    # Then it fails at validation, naming the property to declare
    table_report = excinfo.value.report.table_reports[0]
    assert table_report.status is TableRunStatus.PLANNING_FAILED
    assert any("delta.columnMapping.mode" in f.message for f in table_report.failures)


def test_sync_fails_loud_on_undeclared_registered_property():
    # Given a live table carrying columnMapping.mode that the spec omits
    fqn = "cat.sch.orders"
    catalog, schema, name = fqn.split(".")
    reader = _RecordingReader(
        {
            fqn: TablePresent(
                table=ObservedTable(
                    qualified_name=QualifiedName(catalog, schema, name),
                    columns=(ObservedColumn("id", String()),),
                    properties={"delta.columnMapping.mode": "name"},
                )
            )
        }
    )
    engine = Engine(reader=reader, executor=_RecordingExecutor(per_call_results=[]))
    spec = DeltaTable(catalog, schema, name, columns=(Column("id", String()),))

    # When / Then the sync stops at validation naming the key
    with pytest.raises(SyncFailedError) as excinfo:
        engine.sync(spec)
    table_report = excinfo.value.report.table_reports[0]
    assert table_report.status is TableRunStatus.PLANNING_FAILED
    assert any("delta.columnMapping.mode" in f.message for f in table_report.failures)


def test_metadata_scoped_column_removal_fails_without_drop_precondition():
    # Given a metadata-only spec over a table with an extra column (an
    # unmanaged DropColumn drift) — the user never asked to drop anything
    fqn = "cat.sch.orders"
    catalog, schema, name = fqn.split(".")
    reader = _RecordingReader(
        {
            fqn: TablePresent(
                table=ObservedTable(
                    qualified_name=QualifiedName(catalog, schema, name),
                    columns=(ObservedColumn("id", String()), ObservedColumn("extra", String())),
                )
            )
        }
    )
    engine = Engine(reader=reader, executor=_RecordingExecutor(per_call_results=[]))

    # When syncing
    with pytest.raises(SyncFailedError) as excinfo:
        engine.sync(_metadata_scoped_spec(fqn))

    # Then the single failure is the scope violation — the drop-column
    # precondition is guarded out for unmanaged column structure
    table_report = excinfo.value.report.table_reports[0]
    assert len(table_report.failures) == 1
    assert not any("ColumnMappingRequiredForDrop" in f.rule_name for f in table_report.failures)
    assert any("column structure" in f.message.lower() for f in table_report.failures)
