"""Databricks spellings for the Delta table features managed by the engine."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final

from delta_engine.domain.model import TableFeature

_FEATURE_PROPERTY_PREFIX: Final = "delta.feature."
_SUPPORTED_VALUE: Final = "supported"


@dataclass(frozen=True, slots=True)
class TableProperty:
    """One Databricks table-property assignment."""

    key: str
    value: str


@dataclass(frozen=True, slots=True)
class _FeatureDefinition:
    """Relate one canonical feature identity to its Databricks enablement spelling."""

    feature: TableFeature
    enable_name: str | None = None

    @property
    def enablement(self) -> TableProperty:
        name = self.enable_name or self.feature.value
        return TableProperty(
            key=f"{_FEATURE_PROPERTY_PREFIX}{name}",
            value=_SUPPORTED_VALUE,
        )

    @property
    def recognized_names(self) -> frozenset[str]:
        enable_name = self.enablement.key.removeprefix(_FEATURE_PROPERTY_PREFIX)
        return frozenset({self.feature.value, enable_name})


_DEFINITIONS: Final[Mapping[TableFeature, _FeatureDefinition]] = MappingProxyType(
    {
        TableFeature.TIMESTAMP_NTZ: _FeatureDefinition(TableFeature.TIMESTAMP_NTZ),
        # Databricks still uses the preview property key when explicitly
        # enabling VARIANT, while catalog state may expose its canonical name.
        TableFeature.VARIANT: _FeatureDefinition(
            TableFeature.VARIANT,
            enable_name="variantType-preview",
        ),
    }
)

if frozenset(_DEFINITIONS) != frozenset(TableFeature):
    raise RuntimeError("Databricks table feature definitions do not match TableFeature")

_FEATURES_BY_RECOGNIZED_NAME: Final[Mapping[str, TableFeature]] = MappingProxyType(
    {
        name: definition.feature
        for definition in _DEFINITIONS.values()
        for name in definition.recognized_names
    }
)


def supported_features_from_properties(
    properties: Mapping[str, str],
) -> frozenset[TableFeature]:
    """Extract managed table features from the AS JSON property projection."""
    names = (
        key.removeprefix(_FEATURE_PROPERTY_PREFIX)
        for key, value in properties.items()
        if key.startswith(_FEATURE_PROPERTY_PREFIX) and value == _SUPPORTED_VALUE
    )
    return recognized_table_features(names)


def recognized_table_features(names: Iterable[str]) -> frozenset[TableFeature]:
    """Map Databricks catalog spellings onto canonical feature identities."""
    return frozenset(
        feature for name in names if (feature := _FEATURES_BY_RECOGNIZED_NAME.get(name)) is not None
    )


def enable_property(feature: TableFeature) -> TableProperty:
    """Return the Databricks table property used to enable a feature."""
    return _DEFINITIONS[feature].enablement
