"""
The Databricks encoding of Delta table features.

Enabled features surface as ``delta.feature.<name> = 'supported'`` entries in
the describe document's table properties; enabling one is a SET TBLPROPERTIES
of the same shape. This module owns the name tables for both directions so
the read side and the compiler cannot diverge. It pivots on ``TableFeature``,
the engine's vocabulary, and exchanges canonical feature names with the plan
either way.
"""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from delta_engine.application.features import TableFeature

_FEATURE_PROPERTY_PREFIX: Final = "delta.feature."
_SUPPORTED: Final = "supported"

# Observation accepts every name a modeled feature has ever been recorded
# under. Unknown names are ignored: they are not modeled, the only consumer
# is the required-subset check, and the engine never plans a disable.
_FEATURES_BY_OBSERVED_NAME: Final[Mapping[str, TableFeature]] = MappingProxyType(
    {
        "timestampNtz": TableFeature.TIMESTAMP_NTZ,
        "variantType": TableFeature.VARIANT,
        "variantType-preview": TableFeature.VARIANT,
    }
)

# Enablement uses the documented key per feature. 'variantType-preview' is
# what Databricks documents for VARIANT; if the live suite shows the platform
# accepting/recording the GA name instead, flip it here (observation already
# accepts both, so round-trips stay idempotent either way).
_ENABLE_NAMES: Final[Mapping[TableFeature, str]] = MappingProxyType(
    {
        TableFeature.TIMESTAMP_NTZ: "timestampNtz",
        TableFeature.VARIANT: "variantType-preview",
    }
)


def enable_feature_property_key(feature: str) -> str:
    """Return the ``delta.feature.*`` property key that enables ``feature``."""
    return f"{_FEATURE_PROPERTY_PREFIX}{_ENABLE_NAMES[TableFeature(feature)]}"


def enabled_features_from_properties(
    properties: Mapping[str, str],
) -> frozenset[str]:
    """
    Return the modeled features an observed property mapping records as enabled.

    Raises:
        ValueError: A modeled feature key carries a value other than
            ``'supported'`` — state the engine cannot interpret must fail the
            read rather than shrink it.

    """
    enabled: set[TableFeature] = set()
    for key, value in properties.items():
        if not key.startswith(_FEATURE_PROPERTY_PREFIX):
            continue
        feature = _FEATURES_BY_OBSERVED_NAME.get(key.removeprefix(_FEATURE_PROPERTY_PREFIX))
        if feature is None:
            continue
        if value != _SUPPORTED:
            raise ValueError(
                f"table feature property {key} has unrecognized value {value!r};"
                f" expected {_SUPPORTED!r}"
            )
        enabled.add(feature)
    return frozenset(feature.value for feature in enabled)
