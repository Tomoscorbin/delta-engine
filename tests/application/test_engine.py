import pytest

from delta_engine.api import Column, DeltaTable, String
from delta_engine.application.engine import Engine
from delta_engine.application.errors import SyncFailedError
from delta_engine.application.registry import Registry
from delta_engine.application.results import (
    CatalogState,
    ExecutionFailed,
    ExecutionFailure,
    ExecutionResult,
    ExecutionSucceeded,
    ExecutionSummary,
    ForeignKeyFailure,
    ForeignKeyFailureReason,
    ReadFailed,
    ReadFailure,
    SyncReport,
    TableAbsent,
    TablePresent,
    TableRunStatus,
    ValidationFailure,
)
from delta_engine.domain.model import ObservedTable, QualifiedName
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint
from delta_engine.domain.plan import ActionPlan
from delta_engine.domain.plan.actions import CreateTable

# --------- helpers/fakes


def _spec(fqn: str) -> DeltaTable:
    """Build a minimal real table definition from a 'catalog.schema.name' string."""
    catalog, schema, name = fqn.split(".")
    return DeltaTable(
        catalog, schema, name, columns=(Column("id", String(), nullable=False, primary_key=True),)
    )


def _spec_adding_not_null(fqn: str) -> DeltaTable:
    """
    Build a spec that adds a NOT NULL column on top of the baseline 'id' column.

    Diffed against an existing 'id'-only table, this trips the real
    NonNullableColumnAdd rule, so the engine drives a genuine validation failure
    instead of a faked one.
    """
    catalog, schema, name = fqn.split(".")
    return DeltaTable(
        catalog,
        schema,
        name,
        columns=(
            Column("id", String(), nullable=False, primary_key=True),
            Column("order_id", String(), nullable=False),
        ),
    )


def _existing_id_table(fqn: str) -> TablePresent:
    """Build the present-state read of an existing table with a single 'id' column."""
    catalog, schema, name = fqn.split(".")
    return TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName(catalog, schema, name),
            columns=(Column("id", String()),),
        )
    )


def _existing_id_table_synced(fqn: str) -> TablePresent:
    """
    Build the present-state read of a table that is already fully in sync with _spec.

    Includes the default Delta properties that _spec produces so that diffing
    against _spec yields an empty plan.
    """
    catalog, schema, name = fqn.split(".")
    return TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName(catalog, schema, name),
            columns=(Column("id", String(), nullable=False, primary_key=True),),
            primary_key=PrimaryKeyConstraint(columns=("id",)),
            properties={
                "delta.columnMapping.mode": "name",
                "delta.enableDeletionVectors": "true",
            },
        )
    )


class _FakeReader:
    def __init__(self, mapping: dict[str, CatalogState]) -> None:
        self.mapping = mapping

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        return self.mapping.get(str(qualified_name), TableAbsent())


class _FakeExecutor:
    def __init__(self, results: tuple[ExecutionResult, ...]) -> None:
        self.results = results

    def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
        return ExecutionSummary(self.results)


def _ok_exec(idx: int = 0) -> ExecutionResult:
    return ExecutionSucceeded(action="X", action_index=idx, statement_preview="-- ok")


def _failed_exec(idx: int = 0, exc="AnalysisException", msg="boom") -> ExecutionResult:
    return ExecutionFailed(
        action="X",
        action_index=idx,
        failure=ExecutionFailure(
            action_index=idx, exception_type=exc, message=msg, statement_preview="-- bad sql"
        ),
    )


class _SeqExecutor:
    """Returns a different result tuple on each call."""

    def __init__(self, per_call_results: list[tuple[ExecutionResult, ...]]) -> None:
        self._seq = list(per_call_results)

    def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
        return ExecutionSummary(self._seq.pop(0))


# ---------- Tests


def test_raises_when_any_table_has_read_failure():
    # Given a registry with one table and a reader that fails to read it
    t = _spec("c.s.read_fail")
    reg = Registry()
    reg.register(t)
    reader = _FakeReader({"c.s.read_fail": ReadFailed(ReadFailure("IOError", "cannot read"))})
    executor = _FakeExecutor(results=(_ok_exec(0),))  # would be fine if reached

    # When syncing
    # Then the engine raises SyncFailedError because read failed
    engine = Engine(reader=reader, executor=executor)
    with pytest.raises(SyncFailedError):
        engine.sync(reg)


def test_skips_execution_and_raises_when_validation_fails():
    # Given an existing table whose desired spec adds a NOT NULL column
    # (a real rule violation, not a faked verdict)
    reg = Registry()
    reg.register(_spec_adding_not_null("c.s.val_fail"))
    reader = _FakeReader({"c.s.val_fail": _existing_id_table("c.s.val_fail")})
    # Executor would return OK, but must not be used because validation fails
    executor = _FakeExecutor(results=(_ok_exec(0),))

    # When syncing
    # Then the engine raises SyncFailedError because validation failed
    engine = Engine(reader=reader, executor=executor)
    with pytest.raises(SyncFailedError):
        engine.sync(reg)


def test_raises_when_execution_contains_any_failure():
    # Given a table that reads & validates successfully, but execution has a failed action
    reg = Registry()
    reg.register(_spec("c.s.exec_fail"))
    reader = _FakeReader({"c.s.exec_fail": TableAbsent()})
    executor = _FakeExecutor(results=(_ok_exec(0), _failed_exec(1), _ok_exec(2)))

    # When syncing
    # Then the engine raises SyncFailedError because execution failed
    engine = Engine(reader=reader, executor=executor)
    with pytest.raises(SyncFailedError):
        engine.sync(reg)


def test_returns_report_when_all_tables_succeed():
    # Given two tables that read present/absent, validate cleanly, and execute with no failures
    reg = Registry()
    reg.register(
        _spec("c.a.users"), _spec("c.b.orders")
    )  # registry will yield in name-sorted order
    reader = _FakeReader(
        {
            "c.a.users": TableAbsent(),
            "c.b.orders": TableAbsent(),
        }
    )
    executor = _FakeExecutor(results=(_ok_exec(0), _ok_exec(1)))
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    report = engine.sync(reg)

    # Then the successful run is returned as a SyncReport for programmatic use
    assert isinstance(report, SyncReport)
    assert report.any_failures is False
    assert [tr.status for tr in report] == [
        TableRunStatus.SUCCESS,
        TableRunStatus.SUCCESS,
    ]


def test_engine_reads_all_tables_then_raises_on_any_read_failure():
    # Given two tables; first read fails, second reads OK (absent)
    reg = Registry()
    reg.register(_spec("c.s.a"), _spec("c.s.b"))
    reader = _FakeReader(
        {
            "c.s.a": ReadFailed(ReadFailure("IOError", "cannot read")),
            "c.s.b": TableAbsent(),
        }
    )
    executor = _FakeExecutor(results=(_ok_exec(0),))  # irrelevant for a; used for b
    engine = Engine(reader=reader, executor=executor)

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then both tables appear in the report; one READ_FAILED, one SUCCESS
    [tr_a, tr_b] = list(err.value.report)
    assert tr_a.status is TableRunStatus.READ_FAILED
    assert tr_b.status is TableRunStatus.SUCCESS
    # And the surviving table was actually executed, not just reported as success
    # with an empty (never-run) execution summary
    assert tr_b.execution.results != ()


def test_engine_validates_all_tables_executes_only_the_passing_ones_then_raises():
    # Given two tables that read OK; 'a' adds a NOT NULL column to an existing
    # table (a real validation failure) while 'b' is a clean creation
    reg = Registry()
    reg.register(_spec_adding_not_null("c.s.a"), _spec("c.s.b"))
    reader = _FakeReader(
        {
            "c.s.a": _existing_id_table("c.s.a"),  # existing -> add NOT NULL is rejected
            "c.s.b": TableAbsent(),  # absent -> clean create
        }
    )
    executor = _FakeExecutor(results=(_ok_exec(0), _ok_exec(1)))  # used only for b
    engine = Engine(reader=reader, executor=executor)

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then the report shows a VALIDATION_FAILED and a SUCCESS
    [tr_a, tr_b] = list(err.value.report)
    assert tr_a.status is TableRunStatus.VALIDATION_FAILED
    assert tr_a.execution is None  # a was not executed
    assert tr_b.status is TableRunStatus.SUCCESS
    assert tr_b.execution.results != ()  # b was executed


def test_engine_executes_all_tables_then_raises_if_any_execution_failed():
    # Given both tables read & validate OK; execution fails only for the second
    reg = Registry()
    reg.register(_spec("c.s.a"), _spec("c.s.b"))
    reader = _FakeReader(
        {
            "c.s.a": TableAbsent(),
            "c.s.b": TableAbsent(),
        }
    )
    executor = _SeqExecutor(
        [
            (_ok_exec(0), _ok_exec(1)),  # execution for a: all good
            (_ok_exec(0), _failed_exec(1)),  # execution for b: one failed
        ]
    )
    engine = Engine(reader=reader, executor=executor)

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then both tables are in the report; first SUCCESS, second EXECUTION_FAILED
    statuses = [tr.status for tr in err.value.report]
    assert statuses == [TableRunStatus.SUCCESS, TableRunStatus.EXECUTION_FAILED]


def test_engine_executes_remaining_tables_even_if_first_execution_fails():
    # Given both tables read & validate OK; execution fails for the FIRST table
    reg = Registry()
    reg.register(_spec("c.s.a"), _spec("c.s.b"))
    reader = _FakeReader(
        {
            "c.s.a": TableAbsent(),
            "c.s.b": TableAbsent(),
        }
    )
    executor = _SeqExecutor(
        [
            (_failed_exec(0),),  # execution for 'a' fails
            (_ok_exec(0), _ok_exec(1)),  # execution for 'b' succeeds
        ]
    )
    engine = Engine(reader=reader, executor=executor)

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then both tables appear; first is EXECUTION_FAILED, second is SUCCESS (i.e. it still executed)
    [tr_a, tr_b] = list(err.value.report)
    assert tr_a.status is TableRunStatus.EXECUTION_FAILED
    assert tr_b.status is TableRunStatus.SUCCESS
    assert tr_b.execution.results != ()  # proves 'b' actually executed


def test_syncing_an_empty_registry_returns_an_empty_report_without_raising():
    # Given a registry with no tables registered (e.g. nothing matched a filter)
    reg = Registry()
    engine = Engine(reader=_FakeReader({}), executor=_FakeExecutor(results=()))

    # When syncing
    report = engine.sync(reg)

    # Then an empty, non-failing report comes back -- no SyncFailedError
    assert isinstance(report, SyncReport)
    assert report.any_failures is False
    assert tuple(report) == ()


def test_read_phase_attempts_all_tables_before_any_execution():
    # Given three tables; the middle one fails to read
    reg = Registry()
    reg.register(_spec("c.s.a"), _spec("c.s.b"), _spec("c.s.c"))
    read_order: list[str] = []

    class _TrackingReader:
        def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
            read_order.append(str(qualified_name))
            if str(qualified_name) == "c.s.b":
                return ReadFailed(ReadFailure("IOError", "boom"))
            return TableAbsent()

    executor = _FakeExecutor(results=(_ok_exec(0), _ok_exec(0)))
    engine = Engine(_TrackingReader(), executor)

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then all three reads happened before any execution
    assert read_order == ["c.s.a", "c.s.b", "c.s.c"]
    statuses = [tr.status for tr in err.value.report]
    assert statuses == [
        TableRunStatus.SUCCESS,
        TableRunStatus.READ_FAILED,
        TableRunStatus.SUCCESS,
    ]


def test_validate_phase_validates_all_tables_before_any_execution():
    # Given two tables; 'a' fails validation, 'b' is clean
    reg = Registry()
    reg.register(_spec_adding_not_null("c.s.a"), _spec("c.s.b"))
    reader = _FakeReader(
        {
            "c.s.a": _existing_id_table("c.s.a"),
            "c.s.b": TableAbsent(),
        }
    )
    execute_order: list[str] = []

    class _TrackingExecutor:
        def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
            execute_order.append(str(qualified_name))
            return ExecutionSummary((_ok_exec(0),))

    engine = Engine(reader, _TrackingExecutor())

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then only 'b' was executed (validation failed for 'a')
    # and execution only started after both tables were validated
    assert execute_order == ["c.s.b"]
    [tr_a, tr_b] = list(err.value.report)
    assert tr_a.status is TableRunStatus.VALIDATION_FAILED
    assert tr_b.status is TableRunStatus.SUCCESS


def _spec_with_fk(fqn: str, references: str) -> DeltaTable:
    """Build a table spec with a single FK to another table."""
    catalog, schema, name = fqn.split(".")
    return DeltaTable(
        catalog,
        schema,
        name,
        columns=(
            Column("id", String(), nullable=False, primary_key=True),
            Column("ref_id", String()),
        ),
        foreign_keys=[
            ForeignKeyConstraint(
                local_columns=("ref_id",),
                references=references,
                referenced_columns=("id",),
            )
        ],
    )


def test_sync_report_has_no_fk_failures_when_no_fks_declared():
    # Given a registry with no FKs
    registry = Registry()
    registry.register(_spec("cat.sch.tbl"))
    engine = Engine(_FakeReader({}), _FakeExecutor((_ok_exec(),)))

    # When
    report = engine.sync(registry)

    # Then the single table has no pre-execution failures and succeeds
    [tr] = list(report)
    assert tr.pre_execution_failures == ()
    assert tr.status is TableRunStatus.SUCCESS


def test_sync_fails_table_whose_fk_references_table_not_in_registry():
    # Given orders references customers, but customers is not registered
    registry = Registry()
    registry.register(_spec_with_fk("cat.sch.orders", "cat.sch.customers"))
    engine = Engine(_FakeReader({}), _FakeExecutor((_ok_exec(),)))

    # When syncing
    # Then the job fails and orders is reported FOREIGN_KEY_FAILED
    with pytest.raises(SyncFailedError) as err:
        engine.sync(registry)
    [tr] = list(err.value.report)
    assert tr.status is TableRunStatus.FOREIGN_KEY_FAILED
    fk_failures = [f for f in tr.pre_execution_failures if isinstance(f, ForeignKeyFailure)]
    assert fk_failures[0].reason == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    assert tr.execution is None  # all-or-nothing: the table was not built


def test_sync_processes_tables_in_fk_dependency_order():
    # Given customers is referenced by orders; customers registered second
    registry = Registry()
    registry.register(_spec_with_fk("cat.sch.orders", "cat.sch.customers"))
    registry.register(_spec("cat.sch.customers"))

    synced_order: list[str] = []

    class _OrderCapturingExecutor:
        def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
            synced_order.append(str(qualified_name))
            return ExecutionSummary((_ok_exec(),))

    engine = Engine(_FakeReader({}), _OrderCapturingExecutor())

    # When
    engine.sync(registry)

    # Then customers syncs before orders regardless of registration order
    assert synced_order.index("cat.sch.customers") < synced_order.index("cat.sch.orders")


def test_sync_fails_all_tables_in_a_detected_cycle():
    # Given A -> B and B -> A (cycle)
    constraint_a_to_b = ForeignKeyConstraint(
        local_columns=("b_id",), references="cat.sch.b", referenced_columns=("id",)
    )
    constraint_b_to_a = ForeignKeyConstraint(
        local_columns=("a_id",), references="cat.sch.a", referenced_columns=("id",)
    )
    table_a = DeltaTable(
        "cat",
        "sch",
        "a",
        columns=(
            Column("id", String(), nullable=False, primary_key=True),
            Column("b_id", String()),
        ),
        foreign_keys=[constraint_a_to_b],
    )
    table_b = DeltaTable(
        "cat",
        "sch",
        "b",
        columns=(
            Column("id", String(), nullable=False, primary_key=True),
            Column("a_id", String()),
        ),
        foreign_keys=[constraint_b_to_a],
    )
    registry = Registry()
    registry.register(table_a, table_b)
    engine = Engine(_FakeReader({}), _FakeExecutor((_ok_exec(),)))

    # When syncing
    # Then the job fails and both tables are FOREIGN_KEY_FAILED with CYCLE
    with pytest.raises(SyncFailedError) as err:
        engine.sync(registry)
    statuses = {tr.status for tr in err.value.report}
    assert statuses == {TableRunStatus.FOREIGN_KEY_FAILED}
    reasons = {
        failure.reason
        for tr in err.value.report
        for failure in tr.pre_execution_failures
        if isinstance(failure, ForeignKeyFailure)
    }
    assert reasons == {ForeignKeyFailureReason.CYCLE}
    assert all(tr.execution is None for tr in err.value.report)


def test_sync_blocks_table_whose_dependency_has_an_unresolvable_fk():
    # Given orders -> customers (registered) and customers -> archive (NOT registered)
    registry = Registry()
    registry.register(_spec_with_fk("cat.sch.orders", "cat.sch.customers"))
    registry.register(_spec_with_fk("cat.sch.customers", "cat.sch.archive"))

    executed: list[str] = []

    class _TrackingExecutor:
        def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
            executed.append(str(qualified_name))
            return ExecutionSummary((_ok_exec(0),))

    engine = Engine(_FakeReader({}), _TrackingExecutor())

    # When syncing
    # Then the job fails: customers is UNRESOLVABLE_REFERENCE, orders is blocked by it,
    # and neither table executes (all-or-nothing)
    with pytest.raises(SyncFailedError) as err:
        engine.sync(registry)
    reports = {str(tr.qualified_name): tr for tr in err.value.report}
    assert reports["cat.sch.customers"].status is TableRunStatus.FOREIGN_KEY_FAILED
    customers_fk_failures = [
        f
        for f in reports["cat.sch.customers"].pre_execution_failures
        if isinstance(f, ForeignKeyFailure)
    ]
    assert customers_fk_failures[0].reason == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    assert reports["cat.sch.orders"].status is TableRunStatus.FOREIGN_KEY_FAILED
    orders_fk_failures = [
        f
        for f in reports["cat.sch.orders"].pre_execution_failures
        if isinstance(f, ForeignKeyFailure)
    ]
    assert orders_fk_failures[0].reason == ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
    assert executed == []


def test_fk_failed_table_executes_no_actions():
    # Given orders has an unresolvable FK and would otherwise be created
    registry = Registry()
    registry.register(_spec_with_fk("cat.sch.orders", "cat.sch.missing"))

    executed: list[str] = []

    class _TrackingExecutor:
        def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
            executed.append(str(qualified_name))
            return ExecutionSummary((_ok_exec(0),))

    engine = Engine(_FakeReader({}), _TrackingExecutor())

    # When syncing
    # Then the executor is never called for the FK-failed table (all-or-nothing)
    with pytest.raises(SyncFailedError):
        engine.sync(registry)
    assert executed == []


def _spec_with_fk_and_not_null_col(fqn: str, references: str) -> DeltaTable:
    """Build a spec with a NOT NULL FK column — adding it to an existing table trips validation."""
    catalog, schema, name = fqn.split(".")
    return DeltaTable(
        catalog,
        schema,
        name,
        columns=(Column("id", String()), Column("ref_id", String(), nullable=False)),
        foreign_keys=[
            ForeignKeyConstraint(
                local_columns=("ref_id",),
                references=references,
                referenced_columns=("id",),
            )
        ],
    )


def _spec_without_pk(fqn: str) -> DeltaTable:
    """Build a table with a plain 'id' column and no primary key — an invalid FK target."""
    catalog, schema, name = fqn.split(".")
    return DeltaTable(catalog, schema, name, columns=(Column("id", String()),))


def test_sync_surfaces_both_fk_and_validation_failures_for_a_blocked_table():
    # Given orders has an unresolvable FK AND would fail validation (adds NOT NULL to existing)
    registry = Registry()
    registry.register(_spec_with_fk_and_not_null_col("cat.sch.orders", "cat.sch.missing"))
    # Make the existing table have only 'id' so that adding 'ref_id' as NOT NULL triggers validation
    reader = _FakeReader(
        {
            "cat.sch.orders": TablePresent(
                table=ObservedTable(
                    qualified_name=QualifiedName("cat", "sch", "orders"),
                    columns=(Column("id", String()),),
                )
            )
        }
    )
    executor = _FakeExecutor(results=())

    # When syncing
    # Then the job fails with both FK and validation failures surfaced together
    with pytest.raises(SyncFailedError) as err:
        Engine(reader=reader, executor=executor).sync(registry)
    [tr] = list(err.value.report)
    assert tr.status is TableRunStatus.FOREIGN_KEY_FAILED
    fk_failures = [f for f in tr.pre_execution_failures if isinstance(f, ForeignKeyFailure)]
    val_failures = [f for f in tr.pre_execution_failures if isinstance(f, ValidationFailure)]
    assert len(fk_failures) == 1
    assert fk_failures[0].reason == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    assert len(val_failures) == 1  # NonNullableColumnAdd fires on the NOT NULL ref_id


def test_validation_failure_in_upstream_blocks_fk_dependent():
    # Given customers fails validation (adds NOT NULL to existing table),
    # and orders has a FK on customers
    registry = Registry()
    registry.register(_spec_adding_not_null("cat.sch.customers"))
    registry.register(_spec_with_fk("cat.sch.orders", "cat.sch.customers"))
    reader = _FakeReader(
        {
            "cat.sch.customers": _existing_id_table("cat.sch.customers"),
            "cat.sch.orders": TableAbsent(),
        }
    )
    executor = _FakeExecutor(results=())

    # When syncing
    # Then customers is VALIDATION_FAILED and orders is FOREIGN_KEY_FAILED
    # with BLOCKED_BY_FAILED_DEPENDENCY — because customers won't reach desired state
    with pytest.raises(SyncFailedError) as err:
        Engine(reader=reader, executor=executor).sync(registry)

    reports = {str(tr.qualified_name): tr for tr in err.value.report}
    assert reports["cat.sch.customers"].status is TableRunStatus.VALIDATION_FAILED
    assert reports["cat.sch.orders"].status is TableRunStatus.FOREIGN_KEY_FAILED
    orders_fk_failures = [
        f
        for f in reports["cat.sch.orders"].pre_execution_failures
        if isinstance(f, ForeignKeyFailure)
    ]
    assert len(orders_fk_failures) == 1
    assert orders_fk_failures[0].reason == ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
    # Neither table executes
    assert reports["cat.sch.customers"].execution is None
    assert reports["cat.sch.orders"].execution is None


def test_read_failure_in_upstream_blocks_fk_dependent():
    # Given table A fails to read, and table B has a FK referencing A
    registry = Registry()
    registry.register(_spec("cat.sch.a"))
    registry.register(_spec_with_fk("cat.sch.b", "cat.sch.a"))
    reader = _FakeReader({"cat.sch.a": ReadFailed(ReadFailure("IOError", "cannot read"))})

    executed: list[str] = []

    class _TrackingExecutor:
        def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
            executed.append(str(qualified_name))
            return ExecutionSummary((_ok_exec(0),))

    engine = Engine(reader=reader, executor=_TrackingExecutor())

    # When syncing
    # Then the job fails; B is FOREIGN_KEY_FAILED with BLOCKED_BY_FAILED_DEPENDENCY
    # and the executor is never called for either table
    with pytest.raises(SyncFailedError) as err:
        engine.sync(registry)

    reports = {str(tr.qualified_name): tr for tr in err.value.report}
    assert reports["cat.sch.a"].status is TableRunStatus.READ_FAILED
    assert reports["cat.sch.b"].status is TableRunStatus.FOREIGN_KEY_FAILED
    b_fk_failures = [
        f for f in reports["cat.sch.b"].pre_execution_failures if isinstance(f, ForeignKeyFailure)
    ]
    assert len(b_fk_failures) == 1
    assert b_fk_failures[0].reason == ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
    assert executed == []


def test_dry_run_does_not_execute_and_reports_no_execution():
    # Given two tables that would otherwise be created (absent -> real plans)
    reg = Registry()
    reg.register(_spec("c.s.a"), _spec("c.s.b"))
    reader = _FakeReader({"c.s.a": TableAbsent(), "c.s.b": TableAbsent()})

    executed: list[str] = []

    class _TrackingExecutor:
        def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
            executed.append(str(qualified_name))
            return ExecutionSummary((_ok_exec(0),))

    engine = Engine(reader, _TrackingExecutor())

    # When syncing in dry-run mode
    report = engine.sync(reg, dry_run=True)

    # Then the executor was never called and no table carries an execution result
    assert executed == []
    assert isinstance(report, SyncReport)
    assert [tr.execution for tr in report] == [None, None]
    # And the tables still report SUCCESS, since read/validate/resolve all passed
    assert [tr.status for tr in report] == [
        TableRunStatus.SUCCESS,
        TableRunStatus.SUCCESS,
    ]


def test_unchanged_table_is_not_executed():
    # Given a table whose observed state already matches desired (empty plan)
    reg = Registry()
    reg.register(_spec("c.s.same"))
    reader = _FakeReader({"c.s.same": _existing_id_table_synced("c.s.same")})

    executed: list[str] = []

    class _TrackingExecutor:
        def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
            executed.append(str(qualified_name))
            return ExecutionSummary((_ok_exec(0),))

    engine = Engine(reader, _TrackingExecutor())

    # When syncing
    report = engine.sync(reg)

    # Then the no-op table is reported SUCCESS but the executor was never called
    assert executed == []
    [tr] = list(report)
    assert tr.status is TableRunStatus.SUCCESS
    assert tr.execution is None


def test_dry_run_returns_report_instead_of_raising_when_a_table_would_fail():
    # Given an existing table whose desired spec adds a NOT NULL column
    # (a real validation failure that a normal sync would raise on)
    reg = Registry()
    reg.register(_spec_adding_not_null("c.s.val_fail"))
    reader = _FakeReader({"c.s.val_fail": _existing_id_table("c.s.val_fail")})
    # The executor must never be reached on a dry run; an empty result set makes
    # an accidental call fail loudly rather than pass silently.
    executor = _FakeExecutor(results=())
    engine = Engine(reader=reader, executor=executor)

    # When syncing in dry-run mode
    report = engine.sync(reg, dry_run=True)

    # Then no SyncFailedError is raised; the report is returned for inspection
    # and surfaces the failure that a real run would have hit
    assert isinstance(report, SyncReport)
    assert report.any_failures is True
    [tr] = list(report)
    assert tr.status is TableRunStatus.VALIDATION_FAILED
    assert tr.execution is None


def test_dry_run_exposes_the_planned_actions_on_the_report():
    # Given a table that would be created (absent -> a CreateTable plan)
    reg = Registry()
    reg.register(_spec("c.s.new_table"))
    reader = _FakeReader({"c.s.new_table": TableAbsent()})
    # The executor must never be reached on a dry run; an empty result set makes
    # an accidental call fail loudly rather than pass silently.
    executor = _FakeExecutor(results=())
    engine = Engine(reader=reader, executor=executor)

    # When syncing in dry-run mode
    report = engine.sync(reg, dry_run=True)

    # Then the table's report carries the plan that would have been applied
    [tr] = list(report)
    assert [type(action) for action in tr.plan] == [CreateTable]


def test_real_run_records_the_planned_actions_on_the_report():
    # Given a table that is created on a normal sync (absent -> CreateTable plan)
    reg = Registry()
    reg.register(_spec("c.s.new_table"))
    reader = _FakeReader({"c.s.new_table": TableAbsent()})
    executor = _FakeExecutor(results=(_ok_exec(0),))
    engine = Engine(reader=reader, executor=executor)

    # When syncing for real
    report = engine.sync(reg)

    # Then the report still records the plan that was applied
    [tr] = list(report)
    assert [type(action) for action in tr.plan] == [CreateTable]
    assert tr.status is TableRunStatus.SUCCESS


def test_sync_fails_fk_that_does_not_reference_a_primary_key():
    # Given orders references customers, customers is registered but has NO PK
    registry = Registry()
    registry.register(_spec_with_fk("cat.sch.orders", "cat.sch.customers"))
    registry.register(_spec_without_pk("cat.sch.customers"))
    engine = Engine(_FakeReader({}), _FakeExecutor((_ok_exec(),)))

    # When / Then the run fails and orders is FOREIGN_KEY_FAILED, unexecuted
    with pytest.raises(SyncFailedError) as err:
        engine.sync(registry)
    reports = {str(tr.qualified_name): tr for tr in err.value.report}
    orders = reports["cat.sch.orders"]
    assert orders.status is TableRunStatus.FOREIGN_KEY_FAILED
    fk_failures = [f for f in orders.pre_execution_failures if isinstance(f, ForeignKeyFailure)]
    assert fk_failures[0].reason == ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY
    assert orders.execution is None
    # customers is a clean create with no FK — it is not itself foreign-key-failed
    assert reports["cat.sch.customers"].status is TableRunStatus.SUCCESS
