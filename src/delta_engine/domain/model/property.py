"""
The Delta table properties this engine manages, and what their values mean.

Reference: https://docs.delta.io/latest/table-properties.html

Properties share their namespace with the platform: Databricks writes keys
like ``delta.minReaderVersion`` and ``delta.enableRowTracking`` into table
metadata autonomously. The engine manages properties by exact declaration
over the managed keys below; everything else is invisible — the reader
adapter filters unmanaged keys out of the observed state before the
domain ever sees them.

``TableProperty`` is the single source of the managed key names: the
application property policy's definitions reference its members, and the api
layer re-exports it as the user-facing declaration vocabulary. There is no
second list to keep in sync.

Deliberately absent: ``delta.enableDeletionVectors``. Databricks manages it
(workspaces auto-enable it on new tables), so the engine leaves the key
entirely to the platform.
"""

from collections.abc import Iterator, Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Final

_NO_PROPERTIES: Final[Mapping[str, str | None]] = MappingProxyType({})


class TableProperty(StrEnum):
    """The Delta table properties a user may declare on a table."""

    COLUMN_MAPPING_MODE = "delta.columnMapping.mode"
    CHANGE_DATA_FEED = "delta.enableChangeDataFeed"
    DELETED_FILE_RETENTION_DURATION = "delta.deletedFileRetentionDuration"
    LOG_RETENTION_DURATION = "delta.logRetentionDuration"
    DATA_SKIPPING_NUM_INDEXED_COLS = "delta.dataSkippingNumIndexedCols"
    TYPE_WIDENING = "delta.enableTypeWidening"


class DeclaredProperties(Mapping[str, str | None]):
    """
    The properties a table declaration carries, and what they mean.

    A read-only mapping of property keys to declared values: a string value
    sets the key, and ``None`` asserts the key must be absent from the table.
    What a declared value means lives here, next to the vocabulary — rules
    ask the ``enables_*`` questions instead of comparing raw property strings.
    """

    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, str | None] = _NO_PROPERTIES) -> None:
        self._values: Mapping[str, str | None] = MappingProxyType(dict(values))

    def __getitem__(self, key: str) -> str | None:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({dict(self._values)!r})"

    def enables_column_mapping(self) -> bool:
        """Whether the declared properties turn on name-based column mapping."""
        return self.get(TableProperty.COLUMN_MAPPING_MODE) == "name"

    def enables_change_data_feed(self) -> bool:
        """Whether the declared properties turn on change data feed."""
        return self.get(TableProperty.CHANGE_DATA_FEED) == "true"

    def enables_type_widening(self) -> bool:
        """Whether the declared properties turn on type widening."""
        return self.get(TableProperty.TYPE_WIDENING) == "true"
