"""Policies for filtering and enforcing supported Delta table properties."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from delta_engine.adapters.schema.delta.properties import SUPPORTED_PROPERTIES

_PROPERTY_ALLOWLIST: Final[frozenset[str]] = SUPPORTED_PROPERTIES


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
