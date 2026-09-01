"""
The top-level `delta_engine` namespace is the curated runtime entry point.

The engine and its result types are eagerly available and pyspark-free. Schema
declarations live in `delta_engine.schema`; Databricks helpers live in
`delta_engine.databricks`.
"""

import delta_engine
import delta_engine.application as application

_RUNTIME_SURFACE = {
    "DuplicateTableDefinitionError",
    "Engine",
    "SyncReport",
    "TableRun",
    "SyncFailedError",
    "Failure",
    "FailurePhase",
    "ReadFailure",
    "ValidationFailure",
    "ExecutionFailure",
    "ForeignKeyFailure",
    "TableChangeState",
    "TableRunStatus",
    "render_diff",
    "render_report",
}


def test_all_advertises_the_curated_runtime_surface():
    # Then __all__ lists the root runtime surface exactly
    assert set(delta_engine.__all__) == _RUNTIME_SURFACE


def test_every_advertised_name_is_the_application_layer_object():
    # Given the curated root namespace
    # Then each advertised name resolves to the same object the application
    # layer exports, so isinstance checks and exception handling compose
    # across both import paths
    for name in delta_engine.__all__:
        assert getattr(delta_engine, name) is getattr(application, name)


def test_version_is_exposed_as_a_string():
    # Then the package advertises its version, even though it is metadata
    # rather than part of the __all__ runtime surface
    assert isinstance(delta_engine.__version__, str)
    assert delta_engine.__version__
