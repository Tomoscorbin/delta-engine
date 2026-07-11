"""The warehouse factory wires an Engine without importing the connector."""

from delta_engine.adapters.databricks.warehouse.executor import WarehouseExecutor
from delta_engine.adapters.databricks.warehouse.factory import build_engine
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.application import Engine


class _DummyConnection:
    """Stand-in for a databricks.sql Connection; the factory only stores it."""


def test_build_engine_wires_warehouse_reader_and_executor():
    engine = build_engine(_DummyConnection())
    assert isinstance(engine, Engine)
    assert isinstance(engine.reader, WarehouseReader)
    assert isinstance(engine.executor, WarehouseExecutor)
