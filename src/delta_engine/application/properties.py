"""
The Delta table properties this engine manages, and their restrictions.

Reference: https://docs.delta.io/latest/table-properties.html

Properties share their namespace with the platform: Databricks writes keys
like ``delta.minReaderVersion`` and ``delta.enableRowTracking`` into table
metadata autonomously. The engine manages properties by exact declaration
over the registered keys below; everything else is invisible — the reader
adapter filters unregistered keys out of the observed state before the
domain ever sees them.

``Property`` is the single source of the managed key names: the catalogue
below references its members, and the api layer re-exports it as the
user-facing declaration vocabulary. There is no second list to keep in sync.

Deliberately absent: ``delta.enableDeletionVectors``. Databricks manages it
(workspaces auto-enable it on new tables), so the engine leaves the key
entirely to the platform.

Admission policy — adding a key is a breaking change: tables carrying it
undeclared start failing validation on upgrade. Before registering a key,
create a fresh table on a current Databricks Runtime and inspect DESCRIBE
DETAIL's properties: if the platform auto-writes the key, do not register
it. Additions are called out in release notes.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
import re
from types import MappingProxyType
from typing import Final


class Property(StrEnum):
    """The Delta table properties a user may declare on a table."""

    COLUMN_MAPPING_MODE = "delta.columnMapping.mode"
    CHANGE_DATA_FEED = "delta.enableChangeDataFeed"
    DELETED_FILE_RETENTION_DURATION = "delta.deletedFileRetentionDuration"
    LOG_RETENTION_DURATION = "delta.logRetentionDuration"
    DATA_SKIPPING_NUM_INDEXED_COLS = "delta.dataSkippingNumIndexedCols"


COLUMN_MAPPING_MODE_KEY: Final[str] = Property.COLUMN_MAPPING_MODE.value

_INTERVAL_FORMAT = re.compile(
    r"interval\s+\d+\s+(nanosecond|microsecond|millisecond|second|minute|hour|day|week)s?",
    re.IGNORECASE,
)


def _is_lowercase_boolean(value: str) -> bool:
    # The catalog stores 'true'/'false'; any other casing would re-diff
    # as drift on every sync.
    return value in {"true", "false"}


def _is_interval(value: str) -> bool:
    return _INTERVAL_FORMAT.fullmatch(value.strip()) is not None


def _is_integer_at_least_minus_one(value: str) -> bool:
    try:
        return int(value) >= -1
    except ValueError:
        return False


def _is_column_mapping_mode(value: str) -> bool:
    return value in {"none", "name"}


@dataclass(frozen=True, slots=True)
class PropertyDefinition:
    """
    One manageable property key, its value constraints, and its restrictions.

    ``value_description``: a human phrase describing the expected value
    format, used in error messages when a declared value fails
    ``is_valid_value``.

    ``is_valid_value``: the predicate a declared value must satisfy. Declared
    ``None`` asserts absence rather than a value, so it is exempt from this
    check — the caller must not invoke it for ``None``.

    ``permitted_transitions``: the ``(observed_value, desired_value)`` pairs
    that are legal in-place changes, where a ``desired_value`` of ``None``
    means removal (the key declared absent). Empty means unrestricted; a
    non-empty set blocks any pair not in it. A first write (key absent from
    the catalog) is always legal and is never looked up here.
    """

    key: str
    value_description: str
    is_valid_value: Callable[[str], bool]
    permitted_transitions: frozenset[tuple[str, str | None]] = field(default_factory=frozenset)


type PropertyRegistry = Mapping[str, PropertyDefinition]

_DEFINITIONS: Final[tuple[PropertyDefinition, ...]] = (
    PropertyDefinition(
        key=Property.CHANGE_DATA_FEED,
        value_description="'true' or 'false' (lowercase, as the catalog stores it)",
        is_valid_value=_is_lowercase_boolean,
    ),
    PropertyDefinition(
        key=Property.DELETED_FILE_RETENTION_DURATION,
        value_description="an interval string such as 'interval 7 days'",
        is_valid_value=_is_interval,
    ),
    PropertyDefinition(
        key=Property.LOG_RETENTION_DURATION,
        value_description="an interval string such as 'interval 30 days'",
        is_valid_value=_is_interval,
    ),
    PropertyDefinition(
        key=Property.DATA_SKIPPING_NUM_INDEXED_COLS,
        value_description="an integer >= -1 (-1 indexes all columns)",
        is_valid_value=_is_integer_at_least_minus_one,
    ),
    PropertyDefinition(
        key=Property.COLUMN_MAPPING_MODE,
        value_description="'none' or 'name'",
        is_valid_value=_is_column_mapping_mode,
        # The protocol upgrade (minReader 2 / minWriter 5, physical column
        # names) is permanent: only none -> name is a legal change. The
        # absence of any (value, None) pair blocks removal by the same
        # mechanism — declaring the key absent is a transition to None.
        permitted_transitions=frozenset({("none", "name")}),
    ),
)

DELTA_PROPERTY_REGISTRY: Final[PropertyRegistry] = MappingProxyType(
    {definition.key: definition for definition in _DEFINITIONS}
)
