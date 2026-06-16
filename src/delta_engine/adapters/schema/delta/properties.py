"""
Delta Lake table properties.

Reference: https://docs.delta.io/latest/table-properties.html
"""

from enum import StrEnum


class Property(StrEnum):
    """Supported Delta Lake table properties."""

    ENABLE_DELETION_VECTORS = "delta.enableDeletionVectors"
    COLUMN_MAPPING_MODE = "delta.columnMapping.mode"
    # MIN_READER_VERSION              =    "delta.minReaderVersion"
    # MIN_WRITER_VERSION              =    "delta.minWriterVersion"
    CHANGE_DATA_FEED = "delta.enableChangeDataFeed"
    DELETED_FILE_RETENTION_DURATION = "delta.deletedFileRetentionDuration"
    LOG_RETENTION_DURATION = "delta.logRetentionDuration"
    DATA_SKIPPING_NUM_INDEXED_COLS = "delta.dataSkippingNumIndexedCols"


# Keys a user may declare on a DeltaTable. Used only for construction-time
# validation (fast-fail on typo'd keys), never to filter properties read back
# from the catalog — the engine reconciles only the properties a user declares.
MANAGED_PROPERTY_KEYS: frozenset[str] = frozenset(p.value for p in Property)
