"""
Connection factory for credentialed Databricks end-to-end tests.

Local runs can authenticate with a personal access token. GitHub Actions can
instead use workload identity federation through the Databricks SDK.

The caller owns the returned connection and is responsible for closing it.
"""

from __future__ import annotations

from collections.abc import Mapping
import os
from typing import TYPE_CHECKING

from databricks import sql as databricks_sql

if TYPE_CHECKING:
    from databricks.sql.client import Connection


class DatabricksE2EConfigurationError(RuntimeError):
    """Raised when the live-test connection is not fully configured."""


def open_sql_warehouse_connection(
    environ: Mapping[str, str] | None = None,
) -> Connection:
    """
    Open the SQL warehouse connection configured for live tests.

    Authentication is resolved in this order:

    1. ``DATABRICKS_TOKEN`` for local PAT-based runs.
    2. Databricks SDK authentication, normally ``github-oidc`` in CI.

    Required connection variables:

    - ``DATABRICKS_SERVER_HOSTNAME`` or ``DATABRICKS_HOST``
    - ``DATABRICKS_HTTP_PATH``

    SDK authentication additionally requires:

    - ``DATABRICKS_CLIENT_ID``
    - ``DATABRICKS_AUTH_TYPE``; defaults to ``github-oidc``
    """
    environment = os.environ if environ is None else environ

    server_hostname = _resolve_server_hostname(environment)
    http_path = _require(environment, "DATABRICKS_HTTP_PATH")

    access_token = environment.get("DATABRICKS_TOKEN")
    if access_token:
        return databricks_sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
        )

    return _open_with_sdk_authentication(
        environment=environment,
        server_hostname=server_hostname,
        http_path=http_path,
    )


def _open_with_sdk_authentication(
    environment: Mapping[str, str],
    server_hostname: str,
    http_path: str,
) -> Connection:
    """Open a connection using Databricks unified authentication."""
    try:
        from databricks.sdk.core import Config
    except ImportError as error:
        raise DatabricksE2EConfigurationError(
            "SDK authentication requires databricks-sdk to be installed"
        ) from error

    profile = environment.get("DATABRICKS_CONFIG_PROFILE")

    if profile:
        sdk_config = Config(profile=profile)
    else:
        sdk_config = Config(
            host=_resolve_sdk_host(environment, server_hostname),
            client_id=_require(environment, "DATABRICKS_CLIENT_ID"),
            auth_type=environment.get(
                "DATABRICKS_AUTH_TYPE",
                "github-oidc",
            ),
        )

    def credentials_provider():
        return sdk_config.authenticate

    return databricks_sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=credentials_provider,
    )


def _resolve_server_hostname(environment: Mapping[str, str]) -> str:
    """Resolve and normalise the hostname expected by the SQL connector."""
    configured_host = environment.get("DATABRICKS_SERVER_HOSTNAME") or environment.get(
        "DATABRICKS_HOST"
    )
    if not configured_host:
        raise DatabricksE2EConfigurationError(
            "missing Databricks hostname: set DATABRICKS_SERVER_HOSTNAME or DATABRICKS_HOST"
        )

    return configured_host.removeprefix("https://").removeprefix("http://").rstrip("/")


def _resolve_sdk_host(
    environment: Mapping[str, str],
    server_hostname: str,
) -> str:
    """Resolve the URL-form host expected by the Databricks SDK."""
    configured_host = environment.get("DATABRICKS_HOST")
    if configured_host:
        if configured_host.startswith(("https://", "http://")):
            return configured_host.rstrip("/")
        return f"https://{configured_host.rstrip('/')}"

    return f"https://{server_hostname}"


def _require(environment: Mapping[str, str], variable_name: str) -> str:
    """Return a configured value or raise a useful configuration error."""
    value = environment.get(variable_name)
    if value:
        return value

    raise DatabricksE2EConfigurationError(f"missing required environment variable: {variable_name}")
