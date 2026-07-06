import pytest

from delta_engine.application.engine import Engine
from delta_engine.application.errors import SyncFailedError
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
from delta_engine.domain.model import ObservedTable, QualifiedName
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint
from delta_engine.domain.plan import ActionPlan
from delta_engine.domain.plan.actions import CreateTable, SetColumnComment, SetTableComment
from delta_engine.schema import Column, DeltaTable, ForeignKey, String

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
                primary_key=True,
            ),
        ),
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
            Column("id", String(), nullable=False, primary_key=True),
            Column("ref_id", String()),
        ),
        foreign_keys=[
            ForeignKey(
                local_columns=("ref_id",),
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
            Column("id", String(), nullable=False, primary_key=True),
            Column("order_id", String(), nullable=False),
        ),
    )


def _existing_id_table(fqn: str) -> TablePresent:
    """Build an observed table with one existing id column."""
    catalog, schema, table_name = _split_fqn(fqn)

    return TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName(catalog, schema, table_name),
            columns=(Column("id", String()),),
        )
    )


def _existing_id_table_synced(fqn: str) -> TablePresent:
    """Build an observed table that already matches _spec(fqn)."""
    catalog, schema, table_name = _split_fqn(fqn)

    return TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName(catalog, schema, table_name),
            columns=(Column("id", String(), nullable=False),),
            primary_key=PrimaryKeyConstraint.generate(
                table_name=table_name,
                columns=("id",),
            ),
        )
    )


def _metadata_only_spec(fqn: str) -> DeltaTable:
    """Build a metadata-only declaration with table and column comments."""
    catalog, schema, table_name = _split_fqn(fqn)

    return DeltaTable(
        catalog,
        schema,
        table_name,
        columns=(Column("id", String(), comment="surrogate key"),),
        comment="orders table",
        metadata_only=True,
    )


def _existing_matching_table(fqn: str) -> TablePresent:
    """Build an observed table whose schema matches _metadata_only_spec."""
    catalog, schema, table_name = _split_fqn(fqn)

    return TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName(catalog, schema, table_name),
            columns=(Column("id", String()),),
        )
    )


def _ok_exec(action_index: int = 0) -> ExecutionResult:
    return ExecutionSucceeded(
        action="X",
        action_index=action_index,
        statement_preview="-- ok",
    )


def _failed_exec(
    action_index: int = 0,
    *,
    exception_type: str = "AnalysisException",
    message: str = "boom",
) -> ExecutionResult:
    return ExecutionFailed(
        action="X",
        failure=ExecutionFailure(
            action_index=action_index,
            exception_type=exception_type,
            message=message,
            statement_preview="-- bad sql",
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
        self.calls: list[tuple[QualifiedName, ActionPlan]] = []
        self._per_call_results = None if per_call_results is None else list(per_call_results)

    def execute(
        self,
        qualified_name: QualifiedName,
        plan: ActionPlan,
    ) -> ExecutionSummary:
        self.calls.append((qualified_name, plan))

        if self._per_call_results is None:
            return ExecutionSummary((_ok_exec(),))

        if not self._per_call_results:
            raise AssertionError(f"Unexpected execution call for {qualified_name}")

        return ExecutionSummary(self._per_call_results.pop(0))

    @property
    def executed_names(self) -> list[str]:
        return [str(qualified_name) for qualified_name, _ in self.calls]


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
    assert report.any_failures is False
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
    assert report.any_failures is False
    assert [table_report.status for table_report in report] == [
        TableRunStatus.SUCCESS,
        TableRunStatus.SUCCESS,
    ]
    assert executor.executed_names == ["c.a.users", "c.b.orders"]


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
        def execute(
            self,
            qualified_name: QualifiedName,
            plan: ActionPlan,
        ) -> ExecutionSummary:
            events.append(f"execute:{qualified_name}")
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
    table_a = _assert_status(report, "c.s.a", TableRunStatus.VALIDATION_FAILED)
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
        TableRunStatus.VALIDATION_FAILED,
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


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Current Engine._execute() does not yet block FK dependents when a "
        "parent fails during execution. Remove this marker after fixing _execute()."
    ),
)
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


# ---------------------------------------------------------------------------
# Duplicate declarations
# ---------------------------------------------------------------------------


def test_sync_rejects_duplicate_table_names_before_reading():
    # Given duplicate table declarations
    reader = _RecordingReader()
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(ValueError):
        engine.sync(
            _spec("cat.sch.orders"),
            _spec("cat.sch.orders"),
        )

    # Then no phase has started
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
    assert report.any_failures is False
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
    assert report.any_failures is True
    assert table_report.status is TableRunStatus.VALIDATION_FAILED
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
    assert report.any_failures is True
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


def test_metadata_only_sync_applies_metadata_when_schema_matches():
    # Given a live table whose schema matches the declaration
    fqn = "cat.sch.orders"
    reader = _RecordingReader({fqn: _existing_matching_table(fqn)})
    executor = _RecordingExecutor([(_ok_exec(0),)])
    engine = Engine(reader=reader, executor=executor)

    # When syncing a metadata-only declaration
    report = engine.sync(_metadata_only_spec(fqn))

    # Then the sync succeeds and metadata actions are planned
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.SUCCESS
    assert report.any_failures is False
    assert any(isinstance(action, SetColumnComment) for action in table_report.plan)
    assert any(isinstance(action, SetTableComment) for action in table_report.plan)
    assert executor.executed_names == [fqn]


def test_metadata_only_sync_fails_when_table_is_missing():
    # Given a metadata-only declaration for a missing table
    fqn = "cat.sch.orders"
    reader = _RecordingReader({fqn: TableAbsent()})
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(_metadata_only_spec(fqn))

    # Then validation fails and nothing executes
    [table_report] = list(exc_info.value.report)
    assert table_report.status is TableRunStatus.VALIDATION_FAILED
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
                    columns=(Column("id", String()), Column("stale", String())),
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
    assert table_report.status is TableRunStatus.VALIDATION_FAILED
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
                    columns=(Column("id", String()),),
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
    assert table_report.status is TableRunStatus.VALIDATION_FAILED
    assert any("delta.columnMapping.mode" in f.message for f in table_report.failures)


def test_metadata_only_column_removal_fails_scope_only_without_drop_precondition():
    # Given a metadata-only spec over a table with an extra column (an
    # unmanaged ColumnRemoved drift) — the user never asked to drop anything
    fqn = "cat.sch.orders"
    catalog, schema, name = fqn.split(".")
    reader = _RecordingReader(
        {
            fqn: TablePresent(
                table=ObservedTable(
                    qualified_name=QualifiedName(catalog, schema, name),
                    columns=(Column("id", String()), Column("extra", String())),
                )
            )
        }
    )
    engine = Engine(reader=reader, executor=_RecordingExecutor(per_call_results=[]))

    # When syncing
    with pytest.raises(SyncFailedError) as excinfo:
        engine.sync(_metadata_only_spec(fqn))

    # Then the single failure is the scope violation — the drop-column
    # precondition is guarded out for unmanaged column structure
    table_report = excinfo.value.report.table_reports[0]
    assert len(table_report.failures) == 1
    assert not any("ColumnMappingRequiredForDrop" in f.rule_name for f in table_report.failures)
    assert any("column structure" in f.message.lower() for f in table_report.failures)
