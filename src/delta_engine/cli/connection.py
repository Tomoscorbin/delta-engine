"""Open a Databricks SQL connection through Databricks unified auth."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import logging
import os
import sys
from types import ModuleType
from typing import TYPE_CHECKING

from delta_engine.adapters.databricks.exception_inspection import exception_message
from delta_engine.cli.errors import ConfigError

if TYPE_CHECKING:
    from databricks.sdk.core import Config
    from databricks.sql.client import Connection

_WAREHOUSE_ID_VAR = "DATABRICKS_SQL_WAREHOUSE_ID"
_INSTALL_HINT = 'pip install "delta-engine[cli]"'
_CANNOT_CONNECT = "cannot connect to Databricks"
_SECRET_MARKERS = ("TOKEN", "SECRET", "PASSWORD", "PRIVATE_KEY")
_SQL_CONNECTOR_LOGGER = "databricks.sql.client"
_OPTIONAL_PYARROW_WARNING = "pyarrow is not installed by default"

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Target:
    """One normalized Databricks SQL warehouse target."""

    host: str
    warehouse_id: str

    def __post_init__(self) -> None:
        """Normalize the resolved identity at its owning boundary."""
        object.__setattr__(self, "host", self.host.strip().rstrip("/"))
        object.__setattr__(self, "warehouse_id", self.warehouse_id.strip())

    @property
    def server_hostname(self) -> str:
        """Return the hostname form expected by the SQL connector."""
        return self.host.removeprefix("https://").removeprefix("http://")

    @property
    def http_path(self) -> str:
        """Derive the connector path without exposing it as CLI configuration."""
        return f"/sql/1.0/warehouses/{self.warehouse_id}"


@contextmanager
def open_connection() -> Iterator[tuple[Target, Connection]]:
    """
    Resolve unified authentication, connect, and own the connection.

    Authentication policy belongs to the environment that invokes the CLI. The
    Databricks SDK resolves its standard environment variables and profiles;
    this adapter adds only warehouse selection and connection ownership.
    Connection-close errors are logged and suppressed so they cannot replace a
    completed plan or the primary exception raised while planning.

    Raises:
        ConfigError: If the warehouse, authentication, or optional dependencies
            are not configured, or the connection cannot open.

    """
    warehouse_id = _warehouse_id_from_environment()
    databricks_sql, config_class = _import_backends()
    config = _build_config(config_class)
    target = _target_from_config(config, warehouse_id)
    connection = _connect(databricks_sql, config, target)

    try:
        yield target, connection
    finally:
        try:
            connection.close()
        except Exception as error:
            logger.warning(
                "Failed to close Databricks SQL connection (%s)",
                type(error).__name__,
            )


def _warehouse_id_from_environment() -> str:
    """Read the one connector setting not owned by Databricks unified auth."""
    warehouse_id = os.environ.get(_WAREHOUSE_ID_VAR, "").strip()
    if not warehouse_id:
        raise ConfigError(f"missing required environment: {_WAREHOUSE_ID_VAR}")
    if "/" in warehouse_id:
        raise ConfigError(f"{_WAREHOUSE_ID_VAR} must be a warehouse ID, not an HTTP path")
    return warehouse_id


def _target_from_config(config: Config, warehouse_id: str) -> Target:
    """Freeze the non-credential identity resolved by the SDK."""
    host = (config.host or "").strip()
    if not host:
        raise ConfigError(f"{_CANNOT_CONNECT}: authentication resolved no workspace host")
    return Target(host=host, warehouse_id=warehouse_id)


def _import_backends() -> tuple[ModuleType, type[Config]]:
    """Import the connector and SDK, translating optional-dependency failures."""
    try:
        from databricks import sql as databricks_sql
        from databricks.sdk.core import Config
    except ImportError as error:
        shadow = _shadowing_module_file()
        if shadow is not None:
            raise ConfigError(
                f"'{shadow}' shadows the installed databricks packages; "
                "rename that file or run the CLI from a different directory"
            ) from error
        raise ConfigError(f"the CLI needs {_distribution_for(error)}: {_INSTALL_HINT}") from error
    return databricks_sql, Config


def _keep_relevant_connector_logs(record: logging.LogRecord) -> bool:
    """Hide the connector's irrelevant import warning while retaining other logs."""
    return _OPTIONAL_PYARROW_WARNING not in record.getMessage()


@contextmanager
def _suppress_optional_pyarrow_warning() -> Iterator[None]:
    """Filter the connector's lazy PyArrow warning within one call boundary."""
    connector_logger = logging.getLogger(_SQL_CONNECTOR_LOGGER)
    connector_logger.addFilter(_keep_relevant_connector_logs)
    try:
        yield
    finally:
        connector_logger.removeFilter(_keep_relevant_connector_logs)


def _shadowing_module_file() -> str | None:
    """Return a plain module file shadowing the ``databricks`` namespace."""
    # Declaration loading fronts the working directory on sys.path
    # (declarations._ensure_working_directory_on_path), which is what makes
    # this shadowing likely enough to deserve its own diagnosis.
    module = sys.modules.get("databricks")
    if module is not None and not hasattr(module, "__path__"):
        return getattr(module, "__file__", None) or repr(module)
    return None


def _distribution_for(error: ImportError) -> str:
    """Name the missing optional distribution represented by ``error``."""
    name = error.name or ""
    if name.startswith("databricks.sdk"):
        return "databricks-sdk"
    if name.startswith("databricks"):
        return "databricks-sql-connector"
    return "databricks-sdk and databricks-sql-connector"


def _build_config(config_class: type[Config]) -> Config:
    """Delegate credential and workspace resolution to Databricks unified auth."""
    try:
        return config_class()
    except ValueError as error:
        detail = _safe_error_detail(error)
        raise ConfigError(
            f"{_CANNOT_CONNECT}: authentication configuration failed{detail}"
        ) from error


def _connect(
    databricks_sql: ModuleType,
    config: Config,
    target: Target,
) -> Connection:
    """Open the connector transport and translate its broad failure surface."""
    try:
        with _suppress_optional_pyarrow_warning():
            return databricks_sql.connect(
                server_hostname=target.server_hostname,
                http_path=target.http_path,
                credentials_provider=lambda: config.authenticate,
            )
    except Exception as error:
        detail = _safe_error_detail(error)
        raise ConfigError(f"{_CANNOT_CONNECT} ({type(error).__name__}){detail}") from error


def _safe_error_detail(error: Exception) -> str:
    """Return one-line detail with secret-looking environment values redacted."""
    message = " ".join(exception_message(error).split())
    sensitive_values = {
        value
        for name, value in os.environ.items()
        if value and any(marker in name.upper() for marker in _SECRET_MARKERS)
    }
    for value in sorted(sensitive_values, key=len, reverse=True):
        if value:
            message = message.replace(value, "<redacted>")
    return f": {message}" if message else ""
