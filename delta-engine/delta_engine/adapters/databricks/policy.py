"""Policies for filtering and enforcing supported Delta table properties."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from delta_engine.adapters.schema.delta.properties import Property

# TODO: create a generic policy enforcement framework
# TODO: try without .value
_PROPERTY_ALLOWLIST: Final[frozenset[str]] = frozenset(
    {
        Property.CHANGE_DATA_FEED.value,
        Property.DELETED_FILE_RETENTION_DURATION.value,
        Property.LOG_RETENTION_DURATION.value,
        Property.DATA_SKIPPING_NUM_INDEXED_COLS.value,
        Property.COLUMN_MAPPING_MODE.value,
        Property.ENABLE_DELETION_VECTORS.value,
        Property.ENABLE_DELETION_VECTORS.value,
    }
)


def enforce_property_policy(properties: Mapping[str, str]) -> MappingProxyType[str, str]:
    """
    Return a read-only mapping of allowed Delta table properties.

    Filters the input mapping to the subset of keys permitted by the policy
    and wraps the result in a ``MappingProxyType`` to prevent modification.

    Args:
        properties: Arbitrary property mapping as read from the catalog.

    Returns:
        Read-only mapping containing only allowed properties.

    """
    return MappingProxyType({k: v for k, v in properties.items() if k in _PROPERTY_ALLOWLIST})
