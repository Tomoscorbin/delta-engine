from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from delta_engine.adapters.databricks.factory import build_engine
    from delta_engine.adapters.databricks.log_config import configure_logging

__all__ = ["build_engine", "configure_logging"]


def __getattr__(name: str) -> object:
    """Resolve Databricks helpers without importing PySpark until needed."""
    if name == "build_engine":
        from delta_engine.adapters.databricks.factory import build_engine

        return build_engine
    if name == "configure_logging":
        from delta_engine.adapters.databricks.log_config import configure_logging

        return configure_logging
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
