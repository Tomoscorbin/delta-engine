"""
The Delta table properties this engine manages, and their restrictions.

The registry's shape is domain (`PropertyDefinition`); the concrete keys are
Databricks knowledge and live here, beside the other Databricks-flavoured
policy (the validation rules that exist because Databricks rejects an
operation).

Deliberately absent: ``delta.enableDeletionVectors``. Databricks manages it
(workspaces auto-enable it on new tables), so the engine leaves it entirely
to the platform — it is unregistered and therefore invisible to the diff.

Admission policy — adding a key is a breaking change: tables carrying it
undeclared start failing validation on upgrade. Before registering a key,
create a fresh table on a current Databricks Runtime and inspect DESCRIBE
DETAIL's properties: if the platform auto-writes the key, do not register
it. Additions are called out in release notes.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Final

from delta_engine.domain.model.property import PropertyDefinition, PropertyRegistry

_DEFINITIONS: Final[tuple[PropertyDefinition, ...]] = (
    PropertyDefinition(key="delta.enableChangeDataFeed"),
    PropertyDefinition(key="delta.deletedFileRetentionDuration"),
    PropertyDefinition(key="delta.logRetentionDuration"),
    PropertyDefinition(key="delta.dataSkippingNumIndexedCols"),
    PropertyDefinition(
        key="delta.columnMapping.mode",
        # The protocol upgrade (minReader 2 / minWriter 5, physical column
        # names) is permanent: only none -> name is a legal value change,
        # and the key cannot be unset once present.
        permitted_transitions=frozenset({("none", "name")}),
        unset_permitted=False,
    ),
)

DELTA_PROPERTY_REGISTRY: Final[PropertyRegistry] = MappingProxyType(
    {definition.key: definition for definition in _DEFINITIONS}
)
