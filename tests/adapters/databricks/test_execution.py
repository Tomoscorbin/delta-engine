"""Shared statement-execution loop used by both Databricks backends."""

from types import SimpleNamespace

from hypothesis import given, strategies as st

from delta_engine.adapters.databricks.execution import execute_statements
from delta_engine.application.ports import ExecutionFailed, ExecutionSucceeded


class RecordingRunner:
    """Runs statements by recording them; fails on a marker."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def __call__(self, statement: str) -> None:
        self.executed.append(statement)
        if "__nope__" in statement:
            raise Exception("boom: table not found")


def test_execute_maps_success_and_failure_without_leaking_backend_exception():
    # Given statements where the second one fails
    statements = ("SELECT 1", "SELECT * FROM __nope__")

    # When executing them
    summary = execute_statements(RecordingRunner(), statements)

    # Then success and failure are mapped to execution results
    results = summary.results
    assert isinstance(results[0], ExecutionSucceeded)
    assert isinstance(results[1], ExecutionFailed)
    assert results[0].statement_index == 0
    assert results[1].failure.statement_index == 1


def test_execute_failure_records_exception_details_and_the_statement():
    summary = execute_statements(RecordingRunner(), ("SELECT * FROM __nope__",))

    [result] = summary.results
    assert isinstance(result, ExecutionFailed)
    assert result.failure.statement_index == 0
    assert result.failure.exception_type == "Exception"
    assert "boom: table not found" in result.failure.message
    assert result.failure.statement == "SELECT * FROM __nope__"


def test_execute_contains_an_exception_whose_message_cannot_be_rendered():
    class UnrenderableError(Exception):
        def __str__(self) -> str:
            raise RuntimeError("rendering failed")

    def fail(_statement: str) -> None:
        raise UnrenderableError()

    summary = execute_statements(fail, ("SELECT 1",))

    [result] = summary.results
    assert isinstance(result, ExecutionFailed)
    assert result.failure.exception_type == "UnrenderableError"
    assert result.failure.message == "<exception message unavailable>"


def test_execute_failure_names_wrapped_jvm_exceptions_by_java_class():
    # Given a backend failure that wraps a JVM exception, py4j-style
    class WrappingRunner:
        def __call__(self, statement: str) -> None:
            error = Exception("boom")
            error.java_exception = SimpleNamespace(  # type: ignore[attr-defined]
                getClass=lambda: SimpleNamespace(
                    getName=lambda: "org.apache.spark.sql.AnalysisException"
                )
            )
            raise error

    summary = execute_statements(WrappingRunner(), ("SELECT 1",))

    # Then the failure carries the Java class, not the wrapper's Python class
    [result] = summary.results
    assert isinstance(result, ExecutionFailed)
    assert result.failure.exception_type == "org.apache.spark.sql.AnalysisException"


def test_execute_records_statements_verbatim_on_results():
    # Given a statement spanning multiple lines
    statement = "ALTER TABLE t\n  ADD COLUMN x INT"

    # When executing it
    summary = execute_statements(RecordingRunner(), (statement,))

    # Then the result carries the SQL exactly as executed
    [result] = summary.results
    assert isinstance(result, ExecutionSucceeded)
    assert result.statement == statement


def test_execute_stops_at_first_failure_to_avoid_half_migrating():
    # Given three statements whose middle one fails
    runner = RecordingRunner()
    statements = ("SELECT 1", "SELECT * FROM __nope__", "SELECT 2")

    # When executing them
    summary = execute_statements(runner, statements)

    # Then the third statement is never attempted
    assert runner.executed == ["SELECT 1", "SELECT * FROM __nope__"]
    assert [type(result) for result in summary.results] == [
        ExecutionSucceeded,
        ExecutionFailed,
    ]


def test_execute_returns_empty_summary_for_no_statements():
    summary = execute_statements(RecordingRunner(), ())
    assert summary.results == ()
    assert summary.failed is False


@st.composite
def _execution_scenarios(draw: st.DrawFn):
    statements = draw(st.lists(st.text(max_size=30), max_size=8))
    if not statements:
        return statements, None
    failure_index = draw(
        st.one_of(st.none(), st.integers(min_value=0, max_value=len(statements) - 1))
    )
    return statements, failure_index


@given(_execution_scenarios())
def test_execution_summary_is_exactly_the_prefix_through_the_first_failure(case) -> None:
    statements, failure_index = case
    attempted: list[str] = []

    def run(statement: str) -> None:
        attempted.append(statement)
        if failure_index is not None and len(attempted) - 1 == failure_index:
            raise RuntimeError("generated failure")

    summary = execute_statements(run, statements)
    attempted_count = len(statements) if failure_index is None else failure_index + 1

    assert attempted == statements[:attempted_count]
    assert len(summary.results) == attempted_count
    for index, result in enumerate(summary.results):
        if failure_index is not None and index == failure_index:
            assert isinstance(result, ExecutionFailed)
            assert result.failure.statement_index == index
            assert result.failure.statement == statements[index]
        else:
            assert isinstance(result, ExecutionSucceeded)
            assert result.statement_index == index
            assert result.statement == statements[index]
