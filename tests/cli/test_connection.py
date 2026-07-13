"""Unified-auth target configuration and connection ownership."""

import logging
import sys

from databricks.sql.exc import OperationalError
import pytest

from delta_engine.cli.connection import Target, open_connection
from delta_engine.cli.errors import ConfigError

_WAREHOUSE_ID = "test-warehouse-id"


class _FakeConnection:
    def __init__(self, close_error: Exception | None = None) -> None:
        self.close_error = close_error
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


@pytest.fixture
def warehouse_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_SQL_WAREHOUSE_ID", _WAREHOUSE_ID)


@pytest.fixture
def fake_dependencies(monkeypatch, warehouse_env):
    from databricks import sql as databricks_sql
    from databricks.sdk import core as sdk_core

    captured: dict[str, object] = {"config_calls": [], "connect_calls": []}
    fake_connection = _FakeConnection()

    class FakeConfig:
        def __init__(self, **kwargs) -> None:
            captured["config_calls"].append(kwargs)
            self.host = "https://test.cloud.databricks.com"

        def authenticate(self) -> dict[str, str]:
            return {"Authorization": "Bearer resolved"}

    def fake_connect(**kwargs):
        captured["connect_calls"].append(kwargs)
        return fake_connection

    monkeypatch.setattr(sdk_core, "Config", FakeConfig)
    monkeypatch.setattr(databricks_sql, "connect", fake_connect)
    captured["connection"] = fake_connection
    return captured


def test_target_is_immutable_and_derives_connector_values():
    target = Target(" https://test.cloud.databricks.com/ ", " warehouse ")

    assert target.host == "https://test.cloud.databricks.com"
    assert target.warehouse_id == "warehouse"
    assert target.server_hostname == "test.cloud.databricks.com"
    assert target.http_path == "/sql/1.0/warehouses/warehouse"
    with pytest.raises(AttributeError):
        target.host = "other"  # type: ignore[misc]


def test_warehouse_environment_variable_is_required(monkeypatch):
    monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)

    with pytest.raises(ConfigError, match="DATABRICKS_SQL_WAREHOUSE_ID"):
        with open_connection():
            pass


def test_target_is_normalized_and_authentication_is_delegated_to_the_sdk(fake_dependencies):
    with open_connection() as (target, connection):
        assert target == Target(
            "https://test.cloud.databricks.com",
            "test-warehouse-id",
        )
        assert connection is fake_dependencies["connection"]

    assert fake_dependencies["config_calls"] == [{}]
    [connect_call] = fake_dependencies["connect_calls"]
    assert connect_call["server_hostname"] == "test.cloud.databricks.com"
    assert connect_call["http_path"] == "/sql/1.0/warehouses/test-warehouse-id"
    authenticate = connect_call["credentials_provider"]()
    assert authenticate() == {"Authorization": "Bearer resolved"}


def test_unified_auth_environment_is_left_for_the_sdk(fake_dependencies, monkeypatch):
    monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "pat")
    monkeypatch.setenv("DATABRICKS_TOKEN", "legacy-token")

    with open_connection():
        pass

    assert fake_dependencies["config_calls"] == [{}]


def test_warehouse_http_path_is_rejected_instead_of_being_double_prefixed(monkeypatch):
    monkeypatch.setenv("DATABRICKS_SQL_WAREHOUSE_ID", "/sql/1.0/warehouses/test")

    with pytest.raises(ConfigError, match="warehouse ID, not an HTTP path"):
        with open_connection():
            pass


def test_sdk_configuration_error_is_sanitized_and_translated(monkeypatch, warehouse_env):
    from databricks.sdk import core as sdk_core

    class BrokenConfig:
        def __init__(self, **kwargs) -> None:
            raise ValueError("bad token legacy-token and secret client-secret")

    monkeypatch.setattr(sdk_core, "Config", BrokenConfig)
    monkeypatch.setenv("DATABRICKS_TOKEN", "legacy-token")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "client-secret")

    with pytest.raises(ConfigError) as excinfo:
        with open_connection():
            pass

    message = str(excinfo.value)
    assert "authentication configuration failed" in message
    assert "legacy-token" not in message
    assert "client-secret" not in message
    assert "<redacted>" in message


def test_sdk_configuration_must_resolve_a_workspace_host(monkeypatch, warehouse_env):
    from databricks.sdk import core as sdk_core

    class HostlessConfig:
        host = None

    monkeypatch.setattr(sdk_core, "Config", HostlessConfig)

    with pytest.raises(ConfigError, match="resolved no workspace host"):
        with open_connection():
            pass


@pytest.mark.parametrize(
    "connect_error",
    [
        OperationalError("warehouse unavailable"),
        RuntimeError("Connection reset by peer"),
        ValueError("authentication failed"),
    ],
)
def test_auth_and_connect_failures_are_one_line_configuration_errors(
    fake_dependencies, monkeypatch, connect_error
):
    from databricks import sql as databricks_sql

    def fail_connect(**kwargs):
        raise connect_error

    monkeypatch.setattr(databricks_sql, "connect", fail_connect)

    with pytest.raises(ConfigError) as excinfo:
        with open_connection():
            pass

    message = str(excinfo.value)
    assert "cannot connect to Databricks" in message
    assert type(connect_error).__name__ in message
    assert str(connect_error) in message
    assert "\n" not in message


@pytest.mark.parametrize(
    ("missing_module", "distribution"),
    [
        ("databricks.sdk.core", "databricks-sdk"),
        ("databricks.sql", "databricks-sql-connector"),
    ],
)
def test_missing_optional_package_has_the_cli_extra_hint(
    missing_module, distribution, monkeypatch, warehouse_env
):
    import databricks

    monkeypatch.delattr(databricks, "sql", raising=False)
    monkeypatch.setitem(sys.modules, missing_module, None)

    with pytest.raises(ConfigError) as excinfo:
        with open_connection():
            pass

    message = str(excinfo.value)
    assert distribution in message
    assert 'pip install "delta-engine[cli]"' in message


def test_local_databricks_module_is_reported_as_shadowing(tmp_path, monkeypatch, warehouse_env):
    (tmp_path / "databricks.py").write_text("x = 1\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "databricks", raising=False)
    monkeypatch.delitem(sys.modules, "databricks.sql", raising=False)

    with pytest.raises(ConfigError) as excinfo:
        with open_connection():
            pass

    message = str(excinfo.value)
    assert "shadows" in message
    assert "databricks.py" in message


def test_close_failure_is_logged_without_replacing_success(fake_dependencies, monkeypatch, caplog):
    from databricks import sql as databricks_sql

    failing_connection = _FakeConnection(RuntimeError("close failed"))
    monkeypatch.setattr(databricks_sql, "connect", lambda **kwargs: failing_connection)

    with caplog.at_level(logging.WARNING):
        with open_connection():
            completed = True

    assert completed is True
    assert failing_connection.close_calls == 1
    assert "Failed to close" in caplog.text


def test_close_failure_does_not_replace_primary_plan_exception(fake_dependencies, monkeypatch):
    from databricks import sql as databricks_sql

    failing_connection = _FakeConnection(RuntimeError("close failed"))
    monkeypatch.setattr(databricks_sql, "connect", lambda **kwargs: failing_connection)

    with pytest.raises(LookupError, match="primary failure"):
        with open_connection():
            raise LookupError("primary failure")
