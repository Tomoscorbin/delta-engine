"""The application package declares the root runtime surface."""

import delta_engine.application as application


def test_public_api_exposes_the_intended_names():
    # Given the application package's declared public surface
    # When importing the runtime names re-exported by delta_engine
    from delta_engine.application import (  # noqa: F401
        DuplicateTableDefinitionError,
        Engine,
        ExecutionFailure,
        Failure,
        FailurePhase,
        ForeignKeyFailure,
        ReadFailure,
        SyncFailedError,
        SyncReport,
        TableChangeState,
        TableRun,
        TableRunStatus,
        ValidationFailure,
        render_diff,
        render_report,
    )

    # Then the package advertises exactly those names — growing or shrinking
    # this surface is a deliberate decision, not a side effect
    assert set(application.__all__) == {
        "DuplicateTableDefinitionError",
        "Engine",
        "ExecutionFailure",
        "Failure",
        "FailurePhase",
        "ForeignKeyFailure",
        "ReadFailure",
        "SyncFailedError",
        "SyncReport",
        "TableChangeState",
        "TableRun",
        "TableRunStatus",
        "ValidationFailure",
        "render_diff",
        "render_report",
    }
