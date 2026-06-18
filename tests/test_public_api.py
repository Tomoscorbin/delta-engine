"""
The top-level `delta_engine` namespace is the curated one-import entry point.

The pyspark-free surface (define a table, build a registry, the engine and its
result types) is eagerly available. The Databricks factory and logging helper
are exposed too, but imported lazily so `import delta_engine` never requires
pyspark — keeping the "define tables without Spark" capability intact.
"""

import subprocess
import sys

import pytest

import delta_engine

_EAGER = {
    # schema: define a table
    "DeltaTable",
    "Column",
    "Array",
    "Boolean",
    "Date",
    "Decimal",
    "Double",
    "Float",
    "Integer",
    "Long",
    "Map",
    "String",
    "Timestamp",
    # application: run a sync and read the outcome
    "Engine",
    "Registry",
    "SyncReport",
    "SyncFailedError",
    "Failure",
}
_LAZY = {"build_databricks_engine", "configure_logging"}


def test_eager_names_are_importable_and_identical_to_their_source():
    # Given the curated root namespace
    # Then every pyspark-free name resolves to the same object as its source module
    from delta_engine import Column, DeltaTable, Engine, Registry
    from delta_engine.application import Engine as EngineImpl, Registry as RegistryImpl
    from delta_engine.schema import Column as ColumnImpl, DeltaTable as DeltaTableImpl

    assert DeltaTable is DeltaTableImpl
    assert Column is ColumnImpl
    assert Engine is EngineImpl
    assert Registry is RegistryImpl


def test_lazy_factory_names_resolve_to_their_source():
    # Given the lazily-exposed Databricks entry points
    from delta_engine import build_databricks_engine, configure_logging
    from delta_engine.adapters.databricks import (
        build_databricks_engine as factory_impl,
        configure_logging as configure_impl,
    )

    # Then they resolve to the same objects as their source module
    assert build_databricks_engine is factory_impl
    assert configure_logging is configure_impl


def test_all_advertises_eager_and_lazy_names():
    # Then __all__ lists the full curated surface, eager and lazy alike
    assert set(delta_engine.__all__) == _EAGER | _LAZY


def test_unknown_attribute_raises_attribute_error():
    # Given an attribute the package does not expose
    # When accessing it
    # Then a normal AttributeError is raised (the lazy hook does not mask typos)
    with pytest.raises(AttributeError):
        _ = delta_engine.does_not_exist


def test_eager_surface_imports_without_pyspark_installed():
    # Given an interpreter where pyspark cannot be imported (the default install:
    # pyspark is a dev-only dependency)
    program = (
        "import sys; sys.modules['pyspark'] = None\n"
        "from delta_engine import DeltaTable, Column, Integer, Registry, Engine\n"
        "print('ok')\n"
    )

    # When importing the eager root surface
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True)

    # Then it succeeds without ever importing pyspark
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
