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


def test_execution_failure_formats_itself_as_two_lines_including_the_sql():
    # Given an execution failure carrying its SQL
    failure = ExecutionFailure(
        statement_index=2,
        exception_type="SparkException",
        message="boom",
        statement="ALTER TABLE t ADD COLUMN x INT",
    )

    # Then it renders the error line and the SQL together
    lines = failure.format_lines()
    # The stored index is zero-based; readers see a one-based statement number.
    assert lines[0] == "Execution failed at statement 3: SparkException - boom"
    assert "ALTER TABLE t ADD COLUMN x INT" in lines[1]


def test_a_validation_headline_names_its_subject_when_it_has_one():
    # Given a validation failure about one column
    failure = ValidationFailure(
        rule_name="NonNullableColumnAdd",
        message="Operation not allowed: cannot add non-nullable column 'email'.",
        subject="email",
    )

    # Then the compact headline names the subject beside the rule
    assert failure.headline() == "Validation failed: NonNullableColumnAdd (email)"


def test_a_validation_headline_omits_an_absent_subject():
    # Given a failure judging the table as a whole
    failure = ValidationFailure(rule_name="ColumnMappingRequiredForDrop", message="nope")

    # Then the headline carries no empty subject parentheses
    assert failure.headline() == "Validation failed: ColumnMappingRequiredForDrop"


def test_adding_a_subject_preserves_positional_details_compatibility():
    # Given the pre-subject positional call shape (rule, message, details)
    failure = ValidationFailure("UnmanagedAspectDrift", "nope", ("+ email String",))

    # Then details still lands positionally and subject stays keyword-only
    assert failure.subject == ""
    assert failure.details == ("+ email String",)


def test_failure_display_shows_only_the_head_of_a_long_message():
    # Given failures whose message carries a full backend stack trace
    message = "\n".join(f"line {i}" for i in range(1, 10))

    # Then display renders the head while the field keeps the full text
    [read_line] = ReadFailure(exception_type="E", message=message).format_lines()
    assert "line 5" in read_line
    assert "line 6" not in read_line

    execution_failure = ExecutionFailure(
        statement_index=0, exception_type="E", message=message, statement="SQL"
    )
    execution_line, _ = execution_failure.format_lines()
    assert "line 5" in execution_line
    assert "line 6" not in execution_line
    assert execution_failure.message == message


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
            statement_index=2,
            exception_type="SparkException",
            message="boom",
            statement="SQL",
        ).headline()
        == "Execution failed at statement 3: SparkException"
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
    # Then each declares the phase that produced it, ordered as the pipeline
    # runs them: fk < read < planning < execution
    assert ReadFailure("E", "m").phase is FailurePhase.READ
    assert ValidationFailure("R", "m").phase is FailurePhase.PLANNING
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
        ExecutionFailure(statement_index=0, exception_type="E", message="m", statement="SQL").phase
        is FailurePhase.EXECUTION
    )
    assert (
        FailurePhase.FOREIGN_KEY
        < FailurePhase.READ
        < FailurePhase.PLANNING
        < FailurePhase.EXECUTION
    )


def test_foreign_key_reason_detail_is_defined_for_every_member():
    # Given every FK failure reason
    # Then each renders a non-empty human-readable detail string from the enum itself
    for reason in ForeignKeyFailureReason:
        assert reason.detail
        assert isinstance(reason.detail, str)
