"""Unified-auth connection configuration and lifecycle behaviour."""

import logging
import sys

from databricks.sql.exc import OperationalError
import pytest

from delta_engine.cli.connection import open_connection
from delta_engine.cli.errors import ConfigError

_HTTP_PATH = "/sql/1.0/warehouses/test"


class _FakeConnection:
    def __init__(self, close_error: Exception | None = None) -> None:
        self.close_error = close_error
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


@pytest.fixture
def fake_dependencies(monkeypatch):
    from databricks import sql as databricks_sql
    from databricks.sdk import core as sdk_core

    captured: dict[str, object] = {"config_calls": [], "connect_calls": []}
    fake_connection = _FakeConnection()

    class FakeConfig:
        def __init__(self, **kwargs) -> None:
            captured["config_calls"].append(kwargs)
            self.host = kwargs.get("host", "https://resolved.cloud.databricks.com")

        def authenticate(self) -> dict[str, str]:
            return {"Authorization": "Bearer resolved"}

    def fake_connect(**kwargs):
        captured["connect_calls"].append(kwargs)
        return fake_connection

    monkeypatch.setattr(sdk_core, "Config", FakeConfig)
    monkeypatch.setattr(databricks_sql, "connect", fake_connect)
    captured["connection"] = fake_connection
    return captured


def test_sdk_resolves_environment_and_default_profile_when_flags_are_absent(fake_dependencies):
    with open_connection(None, None, None, environ={"DATABRICKS_HTTP_PATH": _HTTP_PATH}):
        pass

    assert fake_dependencies["config_calls"] == [{}]
    [connect_call] = fake_dependencies["connect_calls"]
    assert connect_call["server_hostname"] == "https://resolved.cloud.databricks.com"
    assert connect_call["http_path"] == _HTTP_PATH


def test_host_and_profile_overrides_are_passed_to_the_sdk(fake_dependencies):
    with open_connection(
        "https://flag.cloud.databricks.com",
        _HTTP_PATH,
        "production",
        environ={},
    ):
        pass

    assert fake_dependencies["config_calls"] == [
        {"host": "https://flag.cloud.databricks.com", "profile": "production"}
    ]


def test_http_path_flag_overrides_the_environment(fake_dependencies):
    with open_connection(
        None,
        "/sql/1.0/warehouses/flag",
        None,
        environ={"DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/env"},
    ):
        pass

    [connect_call] = fake_dependencies["connect_calls"]
    assert connect_call["http_path"] == "/sql/1.0/warehouses/flag"


@pytest.mark.parametrize("auth_type", ["pat", "oauth-m2m", "github-oidc"])
def test_all_unified_auth_types_use_the_sdk_credential_provider(
    auth_type, fake_dependencies, monkeypatch
):
    monkeypatch.setenv("DATABRICKS_AUTH_TYPE", auth_type)

    with open_connection(None, _HTTP_PATH, None, environ={}):
        pass

    [connect_call] = fake_dependencies["connect_calls"]
    authenticate = connect_call["credentials_provider"]()
    assert authenticate() == {"Authorization": "Bearer resolved"}


def test_missing_http_path_is_a_config_error(fake_dependencies):
    with pytest.raises(ConfigError, match="DATABRICKS_HTTP_PATH"):
        with open_connection(None, None, None, environ={}):
            pass


def test_missing_auth_and_http_path_are_reported_in_one_error(monkeypatch):
    # Given neither authentication nor an HTTP path is configured
    from databricks.sdk import core as sdk_core

    class BrokenConfig:
        def __init__(self, **kwargs) -> None:
            raise ValueError("cannot configure default credentials")

    monkeypatch.setattr(sdk_core, "Config", BrokenConfig)

    # When opening a connection
    with pytest.raises(ConfigError) as excinfo:
        with open_connection(None, None, None, environ={}):
            pass

    # Then one error names both gaps, so a misconfigured CI job is fixed in one round trip
    message = str(excinfo.value)
    assert "authentication" in message
    assert "DATABRICKS_HTTP_PATH" in message


def test_sdk_configuration_error_is_a_config_error(monkeypatch):
    from databricks.sdk import core as sdk_core

    class BrokenConfig:
        def __init__(self, **kwargs) -> None:
            raise ValueError("profile is not configured")

    monkeypatch.setattr(sdk_core, "Config", BrokenConfig)

    with pytest.raises(ConfigError, match="profile is not configured"):
        with open_connection(None, _HTTP_PATH, "missing", environ={}):
            pass


@pytest.mark.parametrize(
    ("missing_module", "distribution"),
    [
        ("databricks.sdk.core", "databricks-sdk"),
        ("databricks.sql", "databricks-sql-connector"),
    ],
)
def test_missing_optional_package_has_the_cli_extra_hint(missing_module, distribution, monkeypatch):
    # Given the module cannot be imported: poison the exact module the code
    # imports (a None sys.modules entry raises ImportError with .name set,
    # like a genuinely absent package) and drop the package attribute so the
    # from-import cannot short-circuit through an earlier test's cache.
    import databricks

    monkeypatch.delattr(databricks, "sql", raising=False)
    monkeypatch.setitem(sys.modules, missing_module, None)

    with pytest.raises(ConfigError) as excinfo:
        with open_connection(None, _HTTP_PATH, None, environ={}):
            pass

    message = str(excinfo.value)
    assert distribution in message
    assert 'pip install "delta-engine[cli]"' in message


def test_a_local_databricks_module_is_reported_as_shadowing(tmp_path, monkeypatch):
    # Given a project file named databricks.py ahead of the installed packages
    (tmp_path / "databricks.py").write_text("x = 1\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    # Evict the cached packages so the import re-runs, as in a fresh process.
    monkeypatch.delitem(sys.modules, "databricks", raising=False)
    monkeypatch.delitem(sys.modules, "databricks.sql", raising=False)

    # When opening a connection
    with pytest.raises(ConfigError) as excinfo:
        with open_connection(None, _HTTP_PATH, None, environ={}):
            pass

    # Then the error names the shadowing file instead of a misleading install hint
    message = str(excinfo.value)
    assert "shadows" in message
    assert "databricks.py" in message


@pytest.mark.parametrize(
    "connect_error",
    [
        OperationalError("warehouse unavailable"),
        RuntimeError("Connection reset by peer"),
        ValueError("github-oidc auth: token exchange failed"),
    ],
)
def test_any_connect_failure_is_reported_as_a_config_error(
    fake_dependencies, monkeypatch, connect_error
):
    # The connector re-raises transport and auth errors of many types
    # unchanged, so the CLI boundary translates them all.
    from databricks import sql as databricks_sql

    def fail_connect(**kwargs):
        raise connect_error

    monkeypatch.setattr(databricks_sql, "connect", fail_connect)

    with pytest.raises(ConfigError) as excinfo:
        with open_connection(None, _HTTP_PATH, None, environ={}):
            pass

    message = str(excinfo.value)
    assert type(connect_error).__name__ in message
    assert str(connect_error) in message


def test_close_failure_is_logged_without_replacing_success(fake_dependencies, monkeypatch, caplog):
    from databricks import sql as databricks_sql

    failing_connection = _FakeConnection(RuntimeError("close failed"))
    monkeypatch.setattr(databricks_sql, "connect", lambda **kwargs: failing_connection)

    with caplog.at_level(logging.WARNING):
        with open_connection(None, _HTTP_PATH, None, environ={}):
            completed = True

    assert completed is True
    assert failing_connection.close_calls == 1
    assert "Failed to close" in caplog.text


def test_close_failure_does_not_replace_the_primary_sync_exception(fake_dependencies, monkeypatch):
    from databricks import sql as databricks_sql

    failing_connection = _FakeConnection(RuntimeError("close failed"))
    monkeypatch.setattr(databricks_sql, "connect", lambda **kwargs: failing_connection)

    with pytest.raises(LookupError, match="primary failure"):
        with open_connection(None, _HTTP_PATH, None, environ={}):
            raise LookupError("primary failure")
