"""Preferred public import paths for library users."""

import subprocess
import sys

from delta_engine.api.delta_table import (
    DeltaTable as DeltaTableImpl,
    ForeignKey as ForeignKeyImpl,
    Self as SelfImpl,
)
from delta_engine.application.properties import Property as PropertyImpl
import delta_engine.databricks as databricks
from delta_engine.domain.model import (
    Array,
    Binary,
    Boolean,
    Byte,
    Date,
    Decimal,
    DesiredColumn,
    Double,
    Float,
    Integer,
    Long,
    Map,
    Short,
    String,
    Struct,
    StructField,
    Timestamp,
    TimestampNtz,
    Variant,
)
import delta_engine.schema as schema

_SCHEMA_EXPORTS = {
    "Array",
    "Binary",
    "Boolean",
    "Byte",
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
    "Short",
    "String",
    "Struct",
    "StructField",
    "Timestamp",
    "TimestampNtz",
    "Variant",
}


def test_schema_import_path_matches_implementation_objects():
    # Given the preferred user-facing schema import path
    implementations = {
        "Array": Array,
        "Binary": Binary,
        "Boolean": Boolean,
        "Byte": Byte,
        "Column": DesiredColumn,
        "Date": Date,
        "Decimal": Decimal,
        "DeltaTable": DeltaTableImpl,
        "Double": Double,
        "Float": Float,
        "ForeignKey": ForeignKeyImpl,
        "Integer": Integer,
        "Long": Long,
        "Map": Map,
        "Property": PropertyImpl,
        "Self": SelfImpl,
        "Short": Short,
        "String": String,
        "Struct": Struct,
        "StructField": StructField,
        "Timestamp": Timestamp,
        "TimestampNtz": TimestampNtz,
        "Variant": Variant,
    }

    # Then it exposes exactly the supported declaration names
    assert set(schema.__all__) == _SCHEMA_EXPORTS
    for name, implementation in implementations.items():
        assert getattr(schema, name) is implementation


def test_api_package_does_not_export_the_user_schema_surface():
    # Given the implementation package behind the schema facade
    import delta_engine.api as api

    # Then declaration names are not re-exported from the package root
    assert api.__all__ == []
    for name in _SCHEMA_EXPORTS:
        assert not hasattr(api, name)


def test_databricks_import_path_exposes_backend_entry_points():
    # Given the preferred user-facing Databricks import path

    # Then it exposes exactly the supported backend entry points
    assert set(databricks.__all__) == {
        "build_spark_engine",
        "build_sql_engine",
        "configure_logging",
    }


def test_preferred_pure_imports_and_databricks_module_import_do_not_require_pyspark():
    # Given an interpreter where pyspark cannot be imported
    program = (
        "import sys; sys.modules['pyspark'] = None; sys.modules['databricks'] = None\n"
        "from delta_engine.schema import Column, DeltaTable, Integer\n"
        "from delta_engine import (\n"
        "    Engine, Failure, SyncFailedError, SyncReport, TableChangeState, TableRunStatus,\n"
        ")\n"
        "from delta_engine.databricks import (\n"
        "    build_spark_engine, build_sql_engine, configure_logging,\n"
        ")\n"
        "print('ok')\n"
    )

    # When importing the preferred schema path, root runtime types, and the
    # Databricks module itself
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True)

    # Then no PySpark import is required until a Databricks factory is called
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_build_sql_engine_does_not_require_the_connector_or_pyspark():
    # Given an interpreter where neither pyspark nor databricks-sql can be imported
    program = (
        "import sys; sys.modules['pyspark'] = None; sys.modules['databricks'] = None\n"
        "from delta_engine.databricks import build_sql_engine\n"
        "class DummyConnection: pass\n"
        "build_sql_engine(DummyConnection())\n"
        "print('ok')\n"
    )

    # When building a warehouse engine around a duck-typed connection
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
