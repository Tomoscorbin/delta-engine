"""
Delta Lake table properties.

Reference: https://docs.delta.io/latest/table-properties.html
"""

from enum import StrEnum

from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY


class Property(StrEnum):
    """
    Supported Delta Lake table properties.

    Members must stay in sync with the application property catalogue
    (``DELTA_PROPERTY_REGISTRY``). Deliberately absent:
    ``delta.enableDeletionVectors`` — Databricks manages it.
    """

    COLUMN_MAPPING_MODE = "delta.columnMapping.mode"
    CHANGE_DATA_FEED = "delta.enableChangeDataFeed"
    DELETED_FILE_RETENTION_DURATION = "delta.deletedFileRetentionDuration"
    LOG_RETENTION_DURATION = "delta.logRetentionDuration"
    DATA_SKIPPING_NUM_INDEXED_COLS = "delta.dataSkippingNumIndexedCols"


# Keys a user may declare on a DeltaTable, sourced from the application
# catalogue. Used only for construction-time validation (fast-fail on typo'd
# keys), never to filter properties read back from the catalog.
MANAGED_PROPERTY_KEYS: frozenset[str] = frozenset(DELTA_PROPERTY_REGISTRY)
