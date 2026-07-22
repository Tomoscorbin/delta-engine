"""Warehouse SQL invocation and cursor lifecycle policy."""

import logging

import pytest

from delta_engine.adapters.databricks.warehouse._runner import WarehouseSqlRunner


class RecordingCursor:
    def __init__(self, *, execute_raises: bool = False, close_raises: bool = False) -> None:
        self.execute_raises = execute_raises
        self.close_raises = close_raises
        self.executed: list[str] = []
        self.closed = False

    def execute(self, statement: str) -> None:
        self.executed.append(statement)
        if self.execute_raises:
            raise RuntimeError("statement failed")

    def fetchall(self) -> list[tuple[str]]:
        return [(self.executed[-1],)]

    def close(self) -> None:
        self.closed = True
        if self.close_raises:
            raise RuntimeError("close failed")


class RecordingConnection:
    def __init__(
        self,
        *,
        execute_raises: bool = False,
        close_raises: bool = False,
    ) -> None:
        self.execute_raises = execute_raises
        self.close_raises = close_raises
        self.cursors: list[RecordingCursor] = []

    def cursor(self) -> RecordingCursor:
        cursor = RecordingCursor(
            execute_raises=self.execute_raises,
            close_raises=self.close_raises,
        )
        self.cursors.append(cursor)
        return cursor


def test_query_batch_acquires_lazily_and_reuses_one_cursor():
    connection = RecordingConnection()
    runner = WarehouseSqlRunner(connection)

    with runner.query_batch() as query:
        assert connection.cursors == []
        assert query("SELECT 1") == [("SELECT 1",)]
        assert query("SELECT 2") == [("SELECT 2",)]

    assert len(connection.cursors) == 1
    assert connection.cursors[0].executed == ["SELECT 1", "SELECT 2"]
    assert connection.cursors[0].closed is True


def test_each_run_uses_a_fresh_cursor():
    connection = RecordingConnection()
    runner = WarehouseSqlRunner(connection)

    runner.run("SELECT 1")
    runner.run("SELECT 2")

    assert [cursor.executed for cursor in connection.cursors] == [
        ["SELECT 1"],
        ["SELECT 2"],
    ]
    assert all(cursor.closed for cursor in connection.cursors)


def test_close_failure_does_not_replace_statement_failure(caplog):
    connection = RecordingConnection(execute_raises=True, close_raises=True)

    with (
        caplog.at_level(
            logging.DEBUG,
            logger="delta_engine.adapters.databricks.warehouse._runner",
        ),
        pytest.raises(RuntimeError, match="statement failed"),
    ):
        WarehouseSqlRunner(connection).run("SELECT 1")

    assert "Failed to close warehouse cursor" in caplog.text
