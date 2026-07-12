"""Behaviour of env/flag connection resolution."""

import pytest

from delta_engine.cli.connection import (
    ConnectionSettings,
    open_connection,
    resolve_connection_settings,
)
from delta_engine.cli.errors import ConfigError

_FULL_ENV = {
    "DATABRICKS_SERVER_HOSTNAME": "env.cloud.databricks.com",
    "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/env",
    "DATABRICKS_TOKEN": "env-token",
}


def test_env_vars_alone_resolve_settings():
    settings = resolve_connection_settings(None, None, _FULL_ENV)

    assert settings == ConnectionSettings(
        server_hostname="env.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/env",
        access_token="env-token",
    )


def test_flags_override_their_env_vars_but_token_stays_env_only():
    settings = resolve_connection_settings(
        "flag.cloud.databricks.com", "/sql/1.0/warehouses/flag", _FULL_ENV
    )

    assert settings.server_hostname == "flag.cloud.databricks.com"
    assert settings.http_path == "/sql/1.0/warehouses/flag"
    assert settings.access_token == "env-token"


def test_every_missing_value_is_reported_in_one_error():
    with pytest.raises(ConfigError) as excinfo:
        resolve_connection_settings(None, None, {})

    message = str(excinfo.value)
    assert "DATABRICKS_SERVER_HOSTNAME" in message
    assert "DATABRICKS_HTTP_PATH" in message
    assert "DATABRICKS_TOKEN" in message


def test_a_flag_satisfies_its_missing_env_var():
    environ = {"DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/env", "DATABRICKS_TOKEN": "t"}

    settings = resolve_connection_settings("flag.cloud.databricks.com", None, environ)

    assert settings.server_hostname == "flag.cloud.databricks.com"


def test_open_connection_passes_settings_to_the_connector(monkeypatch):
    # Given the connector boundary is stubbed out
    import databricks.sql

    captured: dict[str, str] = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return "sentinel-connection"

    monkeypatch.setattr(databricks.sql, "connect", fake_connect)
    settings = ConnectionSettings(server_hostname="host", http_path="/path", access_token="token")

    # When opening
    connection = open_connection(settings)

    # Then the settings map onto the connector's keyword arguments
    assert connection == "sentinel-connection"
    assert captured == {
        "server_hostname": "host",
        "http_path": "/path",
        "access_token": "token",
    }
