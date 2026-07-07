from delta_engine.application.failures import (
    ExecutionFailure,
    FailurePhase,
    ForeignKeyFailure,
    ForeignKeyFailureReason,
    ReadFailure,
    ValidationFailure,
)
from delta_engine.domain.model import QualifiedName


def test_read_failure_formats_itself_as_a_display_line():
    # Given a read failure
    failure = ReadFailure(exception_type="AnalysisException", message="table not found")

    # Then it renders its own one-line description
    assert failure.format_lines() == ("Read error: AnalysisException - table not found",)


def test_validation_failure_formats_itself_as_a_display_line():
    # Given a validation failure
    failure = ValidationFailure(
        rule_name="DisallowPartitioningChange", message="cannot repartition"
    )

    # Then it renders its own one-line description
    assert failure.format_lines() == (
        "Validation failed: DisallowPartitioningChange - cannot repartition",
    )


def test_execution_failure_formats_itself_as_two_lines_including_sql_preview():
    # Given an execution failure with a SQL preview
    failure = ExecutionFailure(
        action_index=2,
        exception_type="SparkException",
        message="boom",
        statement_preview="ALTER TABLE t ADD COLUMN x INT",
    )

    # Then it renders the error line and the SQL preview together
    lines = failure.format_lines()
    assert lines[0] == "Execution failed at action 2: SparkException - boom"
    assert "ALTER TABLE t ADD COLUMN x INT" in lines[1]


def test_foreign_key_failure_renders_a_descriptive_line():
    # Given a foreign-key failure described by its content
    failure = ForeignKeyFailure(
        table=QualifiedName("cat", "sch", "orders"),
        local_columns=("customer_id",),
        references=QualifiedName("cat", "sch", "customers"),
        reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
    )

    # When rendering its lines
    lines = failure.format_lines()

    # Then the message identifies the FK by its local columns and referenced table
    assert lines == (
        "Foreign key (customer_id) → cat.sch.customers on cat.sch.orders was not applied:"
        " it references a table that is not registered.",
    )


def test_foreign_key_failure_renders_not_a_key_reason():
    # Given a FK failure because the referenced columns are not the parent's PK
    failure = ForeignKeyFailure(
        table=QualifiedName("cat", "sch", "orders"),
        local_columns=("customer_id",),
        references=QualifiedName("cat", "sch", "customers"),
        reason=ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY,
    )

    # When rendered
    [line] = failure.format_lines()

    # Then the message names the local columns, referenced table, owning table, and reason
    assert "(customer_id)" in line
    assert "cat.sch.customers" in line
    assert "cat.sch.orders" in line
    assert "not the primary key" in line


def test_failure_headlines_summarize_without_the_detail_message():
    # Given one failure of each kind
    # Then headline() drops the detail so the grid can show a compact summary
    assert (
        ReadFailure("AnalysisException", "table not found").headline()
        == "Read error: AnalysisException"
    )
    assert (
        ValidationFailure("NonNullableColumnAdd", "cannot add NOT NULL").headline()
        == "Validation failed: NonNullableColumnAdd"
    )
    assert (
        ExecutionFailure(
            action_index=2, exception_type="SparkException", message="boom", statement_preview="SQL"
        ).headline()
        == "Execution failed at action 2: SparkException"
    )
    assert (
        ForeignKeyFailure(
            table=QualifiedName("cat", "sch", "orders"),
            local_columns=("customer_id",),
            references=QualifiedName("cat", "sch", "customers"),
            reason=ForeignKeyFailureReason.CYCLE,
        ).headline()
        == "Foreign key (customer_id) → cat.sch.customers not applied"
    )


def test_each_failure_kind_declares_its_producing_phase():
    # Given the four failure kinds
    # Then each declares the phase that produced it, ordered read < validation < fk < execution
    assert ReadFailure("E", "m").phase is FailurePhase.READ
    assert ValidationFailure("R", "m").phase is FailurePhase.VALIDATION
    assert (
        ForeignKeyFailure(
            table=QualifiedName("c", "s", "t"),
            local_columns=("x",),
            references=QualifiedName("c", "s", "o"),
            reason=ForeignKeyFailureReason.CYCLE,
        ).phase
        is FailurePhase.FOREIGN_KEY
    )
    assert (
        ExecutionFailure(
            action_index=0, exception_type="E", message="m", statement_preview="SQL"
        ).phase
        is FailurePhase.EXECUTION
    )
    assert (
        FailurePhase.READ
        < FailurePhase.VALIDATION
        < FailurePhase.FOREIGN_KEY
        < FailurePhase.EXECUTION
    )


def test_foreign_key_reason_detail_is_defined_for_every_member():
    # Given every FK failure reason
    # Then each renders a non-empty human-readable detail string from the enum itself
    for reason in ForeignKeyFailureReason:
        assert reason.detail
        assert isinstance(reason.detail, str)
