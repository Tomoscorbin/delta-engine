"""
Delta Lake table properties.

Reference: https://docs.delta.io/latest/table-properties.html
"""

from enum import StrEnum


class Property(StrEnum):
    ENABLE_DELETION_VECTORS = "delta.enableDeletionVectors"
    COLUMN_MAPPING_MODE = "delta.columnMapping.mode"
    # MIN_READER_VERSION              =    "delta.minReaderVersion"
    # MIN_WRITER_VERSION              =    "delta.minWriterVersion"
    CHANGE_DATA_FEED = "delta.enableChangeDataFeed"
    DELETED_FILE_RETENTION_DURATION = "delta.deletedFileRetentionDuration"
    LOG_RETENTION_DURATION = "delta.logRetentionDuration"
    DATA_SKIPPING_NUM_INDEXED_COLS = "delta.dataSkippingNumIndexedCols"
