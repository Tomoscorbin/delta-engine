"""Error codes surfaced by Databricks adapter implementations."""

from enum import StrEnum


class DatabricksError(StrEnum):
    """Selected Databricks error identifiers used in adapters."""
    TABLE_OR_VIEW_NOT_FOUND = "TABLE_OR_VIEW_NOT_FOUND"
