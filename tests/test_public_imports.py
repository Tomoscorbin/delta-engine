"""
The public import paths defer optional backends until they are used.

import-linter proves the static layering, but it cannot tell a lazy import
from an eager one — the facade's exempted edges to pyspark look identical
either way. These tests are the runtime half of that contract: each runs a
subprocess where the optional packages cannot be imported and proves the
public surface still imports, wires, and runs.
"""

import subprocess
import sys

import pytest


def test_preferred_pure_imports_and_databricks_module_import_do_not_require_pyspark():
    # Given an interpreter where pyspark cannot be imported
    program = (
        "import sys; sys.modules['pyspark'] = None; sys.modules['databricks'] = None\n"
        "from delta_engine.schema import Column, DeltaTable, Integer\n"
        "from delta_engine import (\n"
        "    Engine, Failure, SyncFailedError, SyncReport, TableChangeState, TableRunStatus,\n"
        ")\n"
        "from delta_engine.databricks import (\n"
        "    build_spark_engine, build_sql_engine, configure_logging, to_spark_schema,\n"
        ")\n"
        "print('ok')\n"
    )

    # When importing the preferred schema path, root runtime types, and the
    # Databricks module itself
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True)

    # Then no PySpark import is required until a Databricks factory is called
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


@pytest.mark.parametrize("factory", ["build_sql_engine", "build_reader"])
def test_warehouse_factories_do_not_require_the_connector_or_pyspark(factory: str):
    # Given an interpreter where neither pyspark nor databricks-sql can be imported
    program = (
        "import sys; sys.modules['pyspark'] = None; sys.modules['databricks'] = None\n"
        f"from delta_engine.databricks import {factory}\n"
        "class DummyConnection: pass\n"
        f"{factory}(DummyConnection())\n"
        "print('ok')\n"
    )

    # When building the warehouse component around a duck-typed connection
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True)

    # Then the whole warehouse backend imports and wires without either dependency
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_logging_configuration_imports_and_runs_without_pyspark_installed():
    # Given an interpreter where pyspark cannot be imported
    program = (
        "import sys; sys.modules['pyspark'] = None\n"
        "from delta_engine.databricks import configure_logging as configure_databricks_logging\n"
        "configure_databricks_logging()\n"
        "print('ok')\n"
    )

    # When resolving and calling the logging helpers
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True)

    # Then logging setup does not require the Spark-bound engine factory
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
