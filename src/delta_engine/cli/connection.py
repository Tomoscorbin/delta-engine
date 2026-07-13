"""Open a Databricks SQL connection using GitHub Actions OIDC only."""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
import logging
import os
import sys
from types import ModuleType
from typing import TYPE_CHECKING

from delta_engine.cli.errors import ConfigError

if TYPE_CHECKING:
    from databricks.sdk.core import Config
    from databricks.sql.client import Connection

_HOST_VAR = "DATABRICKS_HOST"
_CLIENT_ID_VAR = "DATABRICKS_CLIENT_ID"
_WAREHOUSE_ID_VAR = "DATABRICKS_SQL_WAREHOUSE_ID"
_OIDC_URL_VAR = "ACTIONS_ID_TOKEN_REQUEST_URL"
_OIDC_TOKEN_VAR = "ACTIONS_ID_TOKEN_REQUEST_TOKEN"
_INSTALL_HINT = 'pip install "delta-engine[cli]"'
_CANNOT_CONNECT = "cannot connect to Databricks using GitHub OIDC"

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Target:
    """One normalized Databricks SQL warehouse target and its plan identity."""

    host: str
    client_id: str = field(repr=False)
    warehouse_id: str

    def __post_init__(self) -> None:
        """Normalize the environment-derived identity at its owning boundary."""
        object.__setattr__(self, "host", self.host.strip().rstrip("/"))
        object.__setattr__(self, "client_id", self.client_id.strip())
        object.__setattr__(self, "warehouse_id", self.warehouse_id.strip())

    @property
    def http_path(self) -> str:
        """Derive the connector path without exposing it as CLI configuration."""
        return f"/sql/1.0/warehouses/{self.warehouse_id}"


@contextmanager
def open_connection(
    *,
    environ: Mapping[str, str] | None = None,
) -> Iterator[tuple[Target, "Connection"]]:
    """
    Validate GitHub OIDC configuration, connect, and own the connection.

    This is the CLI's single deep authentication boundary. It deliberately
    constructs the SDK with ``auth_type='github-oidc'`` and offers no path to
    profiles, PATs, OAuth client secrets, or local user authentication.
    Connection-close errors are logged and suppressed so they cannot replace a
    completed plan or the primary exception raised while planning.

    Raises:
        ConfigError: If required environment or optional dependencies are
            missing, SDK configuration fails, or the connection cannot open.

    """
    environment = os.environ if environ is None else environ
    target = _target_from_environment(environment)
    _validate_oidc_environment(environment)
    databricks_sql, config_class = _import_backends()
    config = _build_config(config_class, target, environment)
    connection = _connect(databricks_sql, config, target, environment)

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


def _target_from_environment(environment: Mapping[str, str]) -> Target:
    """Read, validate, and normalize the three public target settings."""
    values = {
        name: environment.get(name, "").strip()
        for name in (_HOST_VAR, _CLIENT_ID_VAR, _WAREHOUSE_ID_VAR)
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise ConfigError(f"missing required environment: {', '.join(missing)}")

    warehouse_id = values[_WAREHOUSE_ID_VAR]
    if "/" in warehouse_id:
        raise ConfigError(f"{_WAREHOUSE_ID_VAR} must be a warehouse ID, not an HTTP path")

    return Target(
        host=values[_HOST_VAR],
        client_id=values[_CLIENT_ID_VAR],
        warehouse_id=warehouse_id,
    )


def _validate_oidc_environment(environment: Mapping[str, str]) -> None:
    """Require the token endpoint variables granted by ``id-token: write``."""
    missing = [
        name for name in (_OIDC_URL_VAR, _OIDC_TOKEN_VAR) if not environment.get(name, "").strip()
    ]
    if missing:
        raise ConfigError(
            "GitHub Actions OIDC is unavailable: "
            f"missing {', '.join(missing)}; grant the job id-token: write"
        )


def _import_backends() -> tuple[ModuleType, type["Config"]]:
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


def _shadowing_module_file() -> str | None:
    """Return a plain module file shadowing the ``databricks`` namespace."""
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


def _build_config(
    config_class: type["Config"],
    target: Target,
    environment: Mapping[str, str],
) -> "Config":
    """Construct the one supported SDK authentication strategy explicitly."""
    try:
        return config_class(
            host=target.host,
            client_id=target.client_id,
            auth_type="github-oidc",
        )
    except ValueError as error:
        detail = _safe_error_detail(error, target, environment)
        raise ConfigError(f"{_CANNOT_CONNECT}: SDK configuration failed{detail}") from error


def _connect(
    databricks_sql: ModuleType,
    config: "Config",
    target: Target,
    environment: Mapping[str, str],
) -> "Connection":
    """Open the connector transport and translate its broad failure surface."""
    try:
        return databricks_sql.connect(
            server_hostname=config.host,
            http_path=target.http_path,
            credentials_provider=lambda: config.authenticate,
        )
    except Exception as error:
        detail = _safe_error_detail(error, target, environment)
        raise ConfigError(f"{_CANNOT_CONNECT} ({type(error).__name__}){detail}") from error


def _safe_error_detail(
    error: Exception,
    target: Target,
    environment: Mapping[str, str],
) -> str:
    """Return a useful one-line detail with identity and OIDC values redacted."""
    message = " ".join(str(error).split())
    sensitive_values = (
        target.client_id,
        environment.get(_OIDC_URL_VAR, ""),
        environment.get(_OIDC_TOKEN_VAR, ""),
    )
    for value in sensitive_values:
        if value:
            message = message.replace(value, "<redacted>")
    return f": {message}" if message else ""
