"""Warehouse SQL invocation and cursor lifecycle policy."""

import logging

import pytest

from delta_engine.adapters.databricks.warehouse._runner import WarehouseSqlRunner


class RecordingCursor:
    def __init__(
        self,
        *,
        execute_failure: Exception | None = None,
        close_failure: Exception | None = None,
    ) -> None:
        self.execute_failure = execute_failure
        self.close_failure = close_failure
        self.executed: list[str] = []
        self.closed = False

    def execute(self, statement: str) -> None:
        self.executed.append(statement)
        if self.execute_failure is not None:
            raise self.execute_failure

    def fetchall(self) -> list[tuple[str]]:
        return [(self.executed[-1],)]

    def close(self) -> None:
        self.closed = True
        if self.close_failure is not None:
            raise self.close_failure


class RecordingConnection:
    def __init__(
        self,
        *,
        execute_failure: Exception | None = None,
        close_failure: Exception | None = None,
    ) -> None:
        self.execute_failure = execute_failure
        self.close_failure = close_failure
        self.cursors: list[RecordingCursor] = []

    def cursor(self) -> RecordingCursor:
        cursor = RecordingCursor(
            execute_failure=self.execute_failure,
            close_failure=self.close_failure,
        )
        self.cursors.append(cursor)
        return cursor


def test_query_batch_acquires_lazily_and_reuses_one_cursor():
    # Given a batch of two queries on one connection
    connection = RecordingConnection()
    runner = WarehouseSqlRunner(connection)

    with runner.query_batch() as query:
        assert connection.cursors == []
        assert query("SELECT 1") == [("SELECT 1",)]
        assert query("SELECT 2") == [("SELECT 2",)]

    # Then no cursor exists before the first query, both queries share one,
    # and it is closed with the batch
    assert len(connection.cursors) == 1
    assert connection.cursors[0].executed == ["SELECT 1", "SELECT 2"]
    assert connection.cursors[0].closed is True


def test_each_run_uses_a_fresh_cursor():
    # Given two standalone runs on one connection
    connection = RecordingConnection()
    runner = WarehouseSqlRunner(connection)

    runner.run("SELECT 1")
    runner.run("SELECT 2")

    # Then each run acquires and closes its own cursor
    assert [cursor.executed for cursor in connection.cursors] == [
        ["SELECT 1"],
        ["SELECT 2"],
    ]
    assert all(cursor.closed for cursor in connection.cursors)


def test_close_failure_does_not_replace_statement_failure(caplog):
    # Given a statement failure followed by a cursor-close failure
    statement_failure = RuntimeError("statement failed")
    connection = RecordingConnection(
        execute_failure=statement_failure,
        close_failure=RuntimeError("close failed"),
    )

    with (
        caplog.at_level(
            logging.DEBUG,
            logger="delta_engine.adapters.databricks.warehouse._runner",
        ),
        pytest.raises(RuntimeError) as exc_info,
    ):
        WarehouseSqlRunner(connection).run("SELECT 1")

    # Then the statement failure is the one that propagates, and the close
    # failure is only logged
    assert exc_info.value is statement_failure
    assert "Failed to close warehouse cursor" in caplog.text


def test_close_failure_after_success_is_logged_and_suppressed(caplog):
    # Given a successful run whose cursor fails to close
    connection = RecordingConnection(close_failure=RuntimeError("network dropped while closing"))

    with caplog.at_level(
        logging.DEBUG,
        logger="delta_engine.adapters.databricks.warehouse._runner",
    ):
        WarehouseSqlRunner(connection).run("SELECT 1")

    # Then the run succeeds and the close failure is only logged
    assert connection.cursors[0].closed is True
    assert "Failed to close warehouse cursor" in caplog.text
