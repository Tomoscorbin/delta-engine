from typing import FrozenSet, Final, Mapping
from types import MappingProxyType

from delta_engine.adapters.schema.delta.properties import Property

#TODO: create a generic policy enforcment framework
#TODO: try without .value
_PROPERTY_ALLOWLIST: Final[FrozenSet[str]] = frozenset({
    Property.CHANGE_DATA_FEED.value,
    Property.DELETED_FILE_RETENTION_DURATION.value,
    Property.LOG_RETENTION_DURATION.value,
    Property.DATA_SKIPPING_NUM_INDEXED_COLS.value,
    Property.COLUMN_MAPPING_MODE.value,
    Property.ENABLE_DELETION_VECTORS.value,
    Property.ENABLE_DELETION_VECTORS.value
})

def enforce_property_policy(properties: Mapping[str, str]) -> MappingProxyType[str, str]:
        return MappingProxyType({k: v for k, v in properties.items() if k in _PROPERTY_ALLOWLIST})
