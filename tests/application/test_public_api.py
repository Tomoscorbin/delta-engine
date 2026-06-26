"""The application package declares its public surface for library consumers."""

import delta_engine.application as application


def test_public_api_exposes_the_intended_names():
    # Given the application package's declared public surface
    # Then the entry points a consumer needs are importable from the package root
    from delta_engine.application import (
        Engine,
        Failure,
        Registry,
        SyncFailedError,
        SyncReport,
        TableRunStatus,
    )
    from delta_engine.application.engine import Engine as EngineImpl
    from delta_engine.application.errors import SyncFailedError as SyncFailedErrorImpl
    from delta_engine.application.registry import Registry as RegistryImpl
    from delta_engine.application.results import (
        Failure as FailureImpl,
        SyncReport as SyncReportImpl,
        TableRunStatus as TableRunStatusImpl,
    )

    assert set(application.__all__) == {
        "Engine",
        "Registry",
        "SyncFailedError",
        "SyncReport",
        "Failure",
        "TableRunStatus",
    }
    # And each name resolves to the real type (single identity, not a shadow copy)
    assert Engine is EngineImpl
    assert Registry is RegistryImpl
    assert SyncFailedError is SyncFailedErrorImpl
    assert SyncReport is SyncReportImpl
    assert Failure is FailureImpl
    assert TableRunStatus is TableRunStatusImpl
