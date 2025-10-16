"""Policies for filtering and enforcing supported Delta table properties."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from delta_engine.adapters.schema.delta.properties import Property

# TODO: create a generic policy enforcement framework
_PROPERTY_ALLOWLIST: Final[frozenset[str]] = frozenset(
    {
        Property.CHANGE_DATA_FEED,
        Property.DELETED_FILE_RETENTION_DURATION,
        Property.LOG_RETENTION_DURATION,
        Property.DATA_SKIPPING_NUM_INDEXED_COLS,
        Property.COLUMN_MAPPING_MODE,
        Property.ENABLE_DELETION_VECTORS,
        Property.ENABLE_DELETION_VECTORS,
    }
)


class PropertyPolicy:
    """Policy that keeps only keys in `allowed_keys` and returns a read-only map."""

    def __init__(self, allowed_keys: frozenset[str]) -> None:
        self._allowed_keys = allowed_keys

    def enforce(self, properties: Mapping[str, str]) -> MappingProxyType[str, str]:
        """
        Return a read-only mapping of allowed Delta table properties.

        Filters the input mapping to the subset of keys permitted by the policy
        and wraps the result in a ``MappingProxyType`` to prevent modification.

        Args:
            properties: Arbitrary property mapping as read from the catalog.

        Returns:
            Read-only mapping containing only allowed properties.

        """
        filtered = {k: v for k, v in properties.items() if k in self._allowed_keys}
        return MappingProxyType(filtered)


DEFAULT_PROPERTY_POLICY = PropertyPolicy(_PROPERTY_ALLOWLIST)
