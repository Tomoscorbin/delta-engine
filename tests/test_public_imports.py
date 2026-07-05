"""Preferred public import paths for library users."""

import subprocess
import sys

import delta_engine.databricks as databricks
import delta_engine.schema as schema

_SCHEMA_EXPORTS = {
    "Array",
    "Boolean",
    "Column",
    "Date",
    "Decimal",
    "DeltaTable",
    "Double",
    "Float",
    "ForeignKey",
    "Integer",
    "Long",
    "Map",
    "Property",
    "Self",
    "String",
    "Timestamp",
}


class _DummySpark:
    """Stand-in for a SparkSession; the factory only stores it on the adapters."""


def test_schema_import_path_matches_the_existing_api_surface():
    # Given the preferred user-facing schema import path
    import delta_engine.api as api

    # Then it exposes exactly the same declaration names as the existing API
    assert set(schema.__all__) == _SCHEMA_EXPORTS
    for name in _SCHEMA_EXPORTS:
        assert getattr(schema, name) is getattr(api, name)


def test_databricks_import_path_exposes_backend_entry_points():
    # Given the preferred user-facing Databricks import path
    from delta_engine.application import Engine

    # Then the shorter factory name builds the same wired Engine
    engine = databricks.build_engine(_DummySpark())
    assert isinstance(engine, Engine)
    assert set(databricks.__all__) == {
        "build_engine",
        "build_databricks_engine",
        "configure_logging",
    }


def test_compat_databricks_factory_alias_builds_an_engine():
    # Given callers who still use the backend-specific factory name
    from delta_engine.application import Engine

    # Then the new Databricks import path still supports it
    engine = databricks.build_databricks_engine(_DummySpark())
    assert isinstance(engine, Engine)


def test_preferred_pure_imports_and_databricks_module_import_do_not_require_pyspark():
    # Given an interpreter where pyspark cannot be imported
    program = (
        "import sys; sys.modules['pyspark'] = None\n"
        "from delta_engine.schema import Column, DeltaTable, Integer\n"
        "from delta_engine import Engine, SyncFailedError, SyncReport, TableRunStatus\n"
        "from delta_engine.databricks import (\n"
        "    build_engine, build_databricks_engine, configure_logging,\n"
        ")\n"
        "print('ok')\n"
    )

    # When importing the preferred schema path, root runtime types, and the
    # Databricks module itself
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True)

    # Then no PySpark import is required until a Databricks factory is called
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_logging_configuration_imports_and_runs_without_pyspark_installed():
    # Given an interpreter where pyspark cannot be imported
    program = (
        "import sys; sys.modules['pyspark'] = None\n"
        "from delta_engine import configure_logging as configure_root_logging\n"
        "from delta_engine.databricks import configure_logging as configure_databricks_logging\n"
        "configure_root_logging()\n"
        "configure_databricks_logging()\n"
        "print('ok')\n"
    )

    # When resolving and calling the logging helpers
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True)

    # Then logging setup does not require the Spark-bound engine factory
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
