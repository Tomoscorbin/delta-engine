"""The application package declares the root runtime surface."""

import delta_engine.application as application


def test_public_api_exposes_the_intended_names():
    # Given the application package's declared public surface
    # When importing the runtime names re-exported by delta_engine
    from delta_engine.application import (
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
    from delta_engine.application.engine import Engine as EngineImpl
    from delta_engine.application.errors import (
        DuplicateTableDefinitionError as DuplicateTableDefinitionErrorImpl,
        SyncFailedError as SyncFailedErrorImpl,
    )
    from delta_engine.application.failures import (
        ExecutionFailure as ExecutionFailureImpl,
        Failure as FailureImpl,
        FailurePhase as FailurePhaseImpl,
        ForeignKeyFailure as ForeignKeyFailureImpl,
        ReadFailure as ReadFailureImpl,
        ValidationFailure as ValidationFailureImpl,
    )
    from delta_engine.application.rendering import (
        render_diff as render_diff_impl,
        render_report as render_report_impl,
    )
    from delta_engine.application.report import (
        SyncReport as SyncReportImpl,
        TableChangeState as TableChangeStateImpl,
        TableRun as TableRunImpl,
        TableRunStatus as TableRunStatusImpl,
    )

    # Then it advertises exactly those names and each resolves to its implementation
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
    assert DuplicateTableDefinitionError is DuplicateTableDefinitionErrorImpl
    assert Engine is EngineImpl
    assert SyncFailedError is SyncFailedErrorImpl
    assert SyncReport is SyncReportImpl
    assert TableChangeState is TableChangeStateImpl
    assert TableRun is TableRunImpl
    assert Failure is FailureImpl
    assert FailurePhase is FailurePhaseImpl
    assert ReadFailure is ReadFailureImpl
    assert ValidationFailure is ValidationFailureImpl
    assert ExecutionFailure is ExecutionFailureImpl
    assert ForeignKeyFailure is ForeignKeyFailureImpl
    assert TableRunStatus is TableRunStatusImpl
    assert render_diff is render_diff_impl
    assert render_report is render_report_impl

    assert not hasattr(application, "render_planned_sql")
