"""
Connection factory for credentialed live tests.

Authentication is OAuth through Databricks unified authentication: a single
``databricks.sdk.core.Config()`` resolves credentials the same way in every
context, so this module has no fallback chain of its own.

- Locally: run ``databricks auth login --host https://<workspace>`` once
  (OAuth, cached), or point ``DATABRICKS_CONFIG_PROFILE`` at a
  ``~/.databrickscfg`` profile.
- CI: the standard SDK variables (``DATABRICKS_HOST``,
  ``DATABRICKS_CLIENT_ID``, ``DATABRICKS_AUTH_TYPE=github-oidc``) select
  OIDC workload identity federation for the service principal.

The workspace host comes from the same resolution (``DATABRICKS_HOST`` or
the profile); the only extra variable this module reads is
``DATABRICKS_HTTP_PATH``, the SQL warehouse endpoint.

The caller owns the returned connection and is responsible for closing it.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from databricks import sql as databricks_sql
from databricks.sdk.core import Config

if TYPE_CHECKING:
    from databricks.sql.client import Connection


class DatabricksE2EConfigurationError(RuntimeError):
    """Raised when the live-test connection is not fully configured."""


def open_sql_warehouse_connection() -> Connection:
    """Open the SQL warehouse connection configured for live tests."""
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    if not http_path:
        raise DatabricksE2EConfigurationError(
            "missing required environment variable: DATABRICKS_HTTP_PATH"
        )

    try:
        sdk_config = Config()
    except ValueError as error:
        raise DatabricksE2EConfigurationError(
            "Databricks authentication is not configured: locally, run"
            " `databricks auth login --host https://<workspace>` once or set"
            " DATABRICKS_CONFIG_PROFILE; CI sets DATABRICKS_HOST,"
            " DATABRICKS_CLIENT_ID, and DATABRICKS_AUTH_TYPE=github-oidc"
        ) from error

    def credentials_provider():
        return sdk_config.authenticate

    return databricks_sql.connect(
        server_hostname=_server_hostname(sdk_config),
        http_path=http_path,
        credentials_provider=credentials_provider,
    )


def _server_hostname(sdk_config: Config) -> str:
    """Reduce the SDK's URL-form host to the hostname the connector expects."""
    if not sdk_config.host:
        raise DatabricksE2EConfigurationError(
            "no Databricks workspace host configured: set DATABRICKS_HOST or"
            " use a profile that defines one"
        )
    return sdk_config.host.removeprefix("https://").removeprefix("http://").rstrip("/")
