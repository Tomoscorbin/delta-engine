"""
The top-level `delta_engine` namespace is the curated runtime entry point.

The engine and its result types are eagerly available and pyspark-free. Schema
declarations live in `delta_engine.schema`; Databricks helpers live in
`delta_engine.databricks`.
"""

import subprocess
import sys

import pytest

import delta_engine

_EAGER = {
    "Engine",
    "SyncReport",
    "SyncFailedError",
    "Failure",
    "TableRunStatus",
}


def test_eager_names_are_importable_and_identical_to_their_source():
    # Given the curated root namespace
    # Then every pyspark-free name resolves to the same object as its source module
    from delta_engine import Engine, Failure, SyncFailedError, SyncReport, TableRunStatus
    from delta_engine.application import (
        Engine as EngineImpl,
        Failure as FailureImpl,
        SyncFailedError as SyncFailedErrorImpl,
        SyncReport as SyncReportImpl,
        TableRunStatus as TableRunStatusImpl,
    )

    assert Engine is EngineImpl
    assert Failure is FailureImpl
    assert SyncFailedError is SyncFailedErrorImpl
    assert SyncReport is SyncReportImpl
    assert TableRunStatus is TableRunStatusImpl


def test_all_advertises_eager_and_lazy_names():
    # Then __all__ lists the root runtime surface exactly
    assert set(delta_engine.__all__) == _EAGER


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
        "from delta_engine import Engine, SyncReport, SyncFailedError, Failure\n"
        "print('ok')\n"
    )

    # When importing the eager root surface
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True)

    # Then it succeeds without ever importing pyspark
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
