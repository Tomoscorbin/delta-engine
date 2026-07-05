"""
Delta Lake table properties — the user-facing declaration vocabulary.

``Property`` and the managed-key set are owned by ``application.properties``
(the single source: the same enum drives the property catalogue the engine
validates against). This module re-exports them so user code imports
property names from the public API surface.

Reference: https://docs.delta.io/latest/table-properties.html
"""

from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY, Property

# Keys a user may declare on a DeltaTable. Used only for construction-time
# validation (fast-fail on typo'd keys), never to filter properties read back
# from the catalog.
MANAGED_PROPERTY_KEYS: frozenset[str] = frozenset(DELTA_PROPERTY_REGISTRY)

__all__ = ["MANAGED_PROPERTY_KEYS", "Property"]
