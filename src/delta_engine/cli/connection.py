"""
Resolve Databricks SQL warehouse connection settings and open connections.

Settings come from ``DATABRICKS_SERVER_HOSTNAME`` / ``DATABRICKS_HTTP_PATH`` /
``DATABRICKS_TOKEN``, with ``--server-hostname`` / ``--http-path`` flags
overriding their variables. The token is env-only so secrets stay out of
shell history and process listings. The connector import is function-local,
mirroring the lazy-import pattern in ``delta_engine.databricks``.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from delta_engine.cli.errors import ConfigError

if TYPE_CHECKING:
    from databricks.sql.client import Connection

_SERVER_HOSTNAME_VAR = "DATABRICKS_SERVER_HOSTNAME"
_HTTP_PATH_VAR = "DATABRICKS_HTTP_PATH"
_TOKEN_VAR = "DATABRICKS_TOKEN"


@dataclass(frozen=True)
class ConnectionSettings:
    """Everything needed to open a SQL warehouse connection."""

    server_hostname: str
    http_path: str
    access_token: str


def resolve_connection_settings(
    server_hostname: str | None,
    http_path: str | None,
    environ: Mapping[str, str],
) -> ConnectionSettings:
    """
    Combine flags and environment into settings; flags win over env vars.

    Raises:
        ConfigError: Naming every missing value at once, so a misconfigured
            CI job is fixed in one round trip.

    """
    resolved_hostname = server_hostname or environ.get(_SERVER_HOSTNAME_VAR) or ""
    resolved_http_path = http_path or environ.get(_HTTP_PATH_VAR) or ""
    resolved_token = environ.get(_TOKEN_VAR) or ""

    missing: list[str] = []
    if not resolved_hostname:
        missing.append(f"{_SERVER_HOSTNAME_VAR} (or --server-hostname)")
    if not resolved_http_path:
        missing.append(f"{_HTTP_PATH_VAR} (or --http-path)")
    if not resolved_token:
        missing.append(_TOKEN_VAR)
    if missing:
        raise ConfigError("missing connection settings: " + ", ".join(missing))

    return ConnectionSettings(
        server_hostname=resolved_hostname,
        http_path=resolved_http_path,
        access_token=resolved_token,
    )


def open_connection(settings: ConnectionSettings) -> "Connection":
    """
    Open a SQL warehouse connection from ``settings``.

    Raises:
        ConfigError: When databricks-sql-connector is not installed.

    """
    try:
        from databricks import sql as databricks_sql
    except ImportError as error:
        raise ConfigError(
            'the CLI needs databricks-sql-connector: pip install "delta-engine[cli]"'
        ) from error
    return databricks_sql.connect(
        server_hostname=settings.server_hostname,
        http_path=settings.http_path,
        access_token=settings.access_token,
    )
