"""Factory for constructing an `Engine` wired to a Databricks SQL warehouse."""

from __future__ import annotations

from typing import TYPE_CHECKING

from delta_engine.adapters.databricks.warehouse._runner import WarehouseSqlRunner
from delta_engine.adapters.databricks.warehouse.executor import WarehouseExecutor
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.application.engine import Engine

if TYPE_CHECKING:
    from databricks.sql.client import Connection


def build_engine(connection: Connection) -> Engine:
    """
    Create an engine configured for a Databricks SQL warehouse.

    The caller opens and owns the connection (`databricks.sql.connect(...)`) —
    auth and lifecycle never touch the engine. Like the Spark factory, this
    has no logging side effect; call :func:`configure_logging` separately if
    the coloured handler is wanted.
    """
    runner = WarehouseSqlRunner(connection)
    reader = WarehouseReader(runner)
    executor = WarehouseExecutor(runner)
    return Engine(reader=reader, executor=executor)
