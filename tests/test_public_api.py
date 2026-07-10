"""
The top-level `delta_engine` namespace is the curated runtime entry point.

The engine and its result types are eagerly available and pyspark-free. Schema
declarations live in `delta_engine.schema`; Databricks helpers live in
`delta_engine.databricks`.
"""

import pytest

import delta_engine

_EAGER = {
    "Engine",
    "SyncReport",
    "TableRunReport",
    "SyncFailedError",
    "Failure",
    "FailurePhase",
    "ReadFailure",
    "ValidationFailure",
    "ExecutionFailure",
    "ForeignKeyFailure",
    "TableRunStatus",
    "render_diff",
    "render_report",
}


def test_eager_names_are_importable_and_identical_to_their_source():
    # Given the curated root namespace
    # Then every pyspark-free name resolves to the same object as its source module
    from delta_engine import (
        Engine,
        ExecutionFailure,
        Failure,
        FailurePhase,
        ForeignKeyFailure,
        ReadFailure,
        SyncFailedError,
        SyncReport,
        TableRunReport,
        TableRunStatus,
        ValidationFailure,
        render_diff,
        render_report,
    )
    from delta_engine.application import (
        Engine as EngineImpl,
        ExecutionFailure as ExecutionFailureImpl,
        Failure as FailureImpl,
        FailurePhase as FailurePhaseImpl,
        ForeignKeyFailure as ForeignKeyFailureImpl,
        ReadFailure as ReadFailureImpl,
        SyncFailedError as SyncFailedErrorImpl,
        SyncReport as SyncReportImpl,
        TableRunReport as TableRunReportImpl,
        TableRunStatus as TableRunStatusImpl,
        ValidationFailure as ValidationFailureImpl,
        render_diff as render_diff_impl,
        render_report as render_report_impl,
    )

    assert Engine is EngineImpl
    assert Failure is FailureImpl
    assert FailurePhase is FailurePhaseImpl
    assert ReadFailure is ReadFailureImpl
    assert ValidationFailure is ValidationFailureImpl
    assert ExecutionFailure is ExecutionFailureImpl
    assert ForeignKeyFailure is ForeignKeyFailureImpl
    assert SyncFailedError is SyncFailedErrorImpl
    assert SyncReport is SyncReportImpl
    assert TableRunReport is TableRunReportImpl
    assert TableRunStatus is TableRunStatusImpl
    assert render_diff is render_diff_impl
    assert render_report is render_report_impl


def test_all_advertises_eager_and_lazy_names():
    # Then __all__ lists the root runtime surface exactly
    assert set(delta_engine.__all__) == _EAGER


def test_version_is_exposed_as_a_string():
    # Then the package advertises its version, even though it is metadata
    # rather than part of the __all__ runtime surface
    assert isinstance(delta_engine.__version__, str)
    assert delta_engine.__version__


def test_unknown_attribute_raises_attribute_error():
    # Given an attribute the package does not expose
    # When accessing it
    # Then a normal AttributeError is raised (the lazy hook does not mask typos)
    with pytest.raises(AttributeError):
        _ = delta_engine.does_not_exist
