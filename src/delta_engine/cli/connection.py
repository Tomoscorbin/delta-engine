"""Open Databricks SQL connections through unified authentication."""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
import logging
import os
import sys
from types import ModuleType
from typing import TYPE_CHECKING

from delta_engine.cli.errors import ConfigError

if TYPE_CHECKING:
    from databricks.sdk.core import Config
    from databricks.sql.client import Connection

_HTTP_PATH_VAR = "DATABRICKS_HTTP_PATH"
_INSTALL_HINT = 'pip install "delta-engine[cli]"'
_CANNOT_CONNECT = "cannot connect to Databricks"

logger = logging.getLogger(__name__)


@contextmanager
def open_connection(
    host: str | None,
    http_path: str | None,
    profile: str | None,
    *,
    environ: Mapping[str, str] | None = None,
) -> Iterator["Connection"]:
    """
    Open and own one SQL connection using Databricks unified authentication.

    Explicit ``host`` and ``profile`` values are passed to the SDK
    :class:`Config`; otherwise it resolves environment variables and Databricks
    configuration profiles itself. ``http_path`` falls back to
    ``DATABRICKS_HTTP_PATH`` because the warehouse path is a connector setting,
    not an SDK authentication field. A missing HTTP path is reported before
    importing the connector or asking the SDK to resolve authentication.

    Connection-close errors are logged and suppressed so they never replace a
    completed report or the primary exception raised by a sync.

    Raises:
        ConfigError: If a dependency is missing or shadowed, authentication or
            the HTTP path is not configured, or the connection cannot be
            established.

    """
    environment = os.environ if environ is None else environ
    resolved_http_path = http_path if http_path is not None else environment.get(_HTTP_PATH_VAR)
    if not resolved_http_path:
        raise ConfigError(f"{_CANNOT_CONNECT}: missing {_HTTP_PATH_VAR} (or --http-path)")

    databricks_sql, config_class = _import_backends()
    config = _build_config(config_class, host, profile)

    # The connector re-raises connect-time failures unchanged and of many
    # types (urllib3 transport errors, SDK auth ValueErrors), so this boundary
    # translates everything rather than enumerating exception classes.
    try:
        connection = databricks_sql.connect(
            server_hostname=config.host,
            http_path=resolved_http_path,
            credentials_provider=lambda: config.authenticate,
        )
    except Exception as error:
        raise ConfigError(
            f"failed to connect to Databricks ({type(error).__name__}): {error}"
        ) from error

    try:
        yield connection
    finally:
        try:
            connection.close()
        except Exception:
            logger.warning("Failed to close Databricks SQL connection", exc_info=True)


def _import_backends() -> tuple[ModuleType, type["Config"]]:
    """Import the connector and SDK, translating failures into actionable errors."""
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
    """
    Return the file shadowing the ``databricks`` namespace package, if any.

    The CLI prepends the working directory to ``sys.path`` to load
    declarations, so a project file named ``databricks.py`` resolves ahead of
    the installed namespace package. A real ``databricks`` package always has
    ``__path__``; a plain module does not.
    """
    module = sys.modules.get("databricks")
    if module is not None and not hasattr(module, "__path__"):
        return getattr(module, "__file__", None) or repr(module)
    return None


def _distribution_for(error: ImportError) -> str:
    name = error.name or ""
    if name.startswith("databricks.sdk"):
        return "databricks-sdk"
    if name.startswith("databricks"):
        return "databricks-sql-connector"
    return "databricks-sdk and databricks-sql-connector"


def _build_config(
    config_class: type["Config"],
    host: str | None,
    profile: str | None,
) -> "Config":
    """Build the SDK config, translating authentication failures."""
    try:
        if host is not None and profile is not None:
            return config_class(host=host, profile=profile)
        if host is not None:
            return config_class(host=host)
        if profile is not None:
            return config_class(profile=profile)
        return config_class()
    except ValueError as error:
        raise ConfigError(f"{_CANNOT_CONNECT}: authentication: {error}") from error
