"""
The Delta table features this engine enables, and their restrictions.

Reference: https://docs.delta.io/latest/versioning.html

Features share their namespace with the platform, as properties do: Databricks
enables features like ``deletionVectors`` and ``rowTracking`` on its own, and
enables a declared type's feature as part of creating a table. The engine
enables the features below, on tables that already exist, and no others; the
rest of a table's features are invisible to it.

``TableFeature`` is the single source of the managed feature names: the policy
definitions below reference its members, and the values — plain strings — are
what a desired table records as required and an observed table records as
enabled. The domain compares those two sets and plans the difference without
holding the vocabulary itself.

Everything that governs a feature lives in one definition here: the declared
type that requires it, the name the engine writes to enable it, and every name
the catalog may record it under. Databricks spells features as ordinary table
properties (``delta.feature.<name> = 'supported'``), so the adapters read and
compile them through this policy and hold no feature knowledge of their own.

Admission policy: a feature joins ``TableFeature`` only when it is a
prerequisite of an engine-planned action that the platform does not enable
automatically as part of executing that action. Property-gated features
(appendOnly, changeDataFeed, columnMapping, typeWidening) ride along with
their enabling property or DDL, and leave a permanent but inert support marker
whose behaviour follows a toggleable property; platform-managed features
(deletionVectors, rowTracking, invariants) are not the engine's to manage. The
features below are the other kind: no property, no off switch — support *is*
activation, which is why they alone need a planned enable step and why the
plan flags them as permanent.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Final

from delta_engine.domain.model.column import DesiredColumn
from delta_engine.domain.model.data_type import (
    Array,
    DataType,
    Map,
    Struct,
    TimestampNtz,
    Variant,
)

# Databricks records and accepts feature enablement as table properties under
# this prefix; 'supported' is the only value the protocol assigns meaning to.
_FEATURE_PROPERTY_PREFIX: Final = "delta.feature."
_SUPPORTED: Final = "supported"


class TableFeature(StrEnum):
    """A Delta table feature the engine enables when a declared type requires it."""

    TIMESTAMP_NTZ = "timestampNtz"
    VARIANT = "variantType"


@dataclass(frozen=True, slots=True)
class FeatureDefinition:
    """
    One manageable feature, and the judgments the engine needs about it.

    ``required_by`` is the declared type whose storage needs the feature, found
    at any depth of a column's type tree. ``enable_name`` is the name the
    engine writes to enable it, and ``observed_names`` every name the catalog
    may record it under — a feature that outlives a preview keeps its old
    spelling on existing tables, so the two differ.
    """

    feature: TableFeature
    required_by: type[DataType]
    enable_name: str
    observed_names: frozenset[str]


@dataclass(frozen=True, slots=True)
class FeaturePolicy:
    """Resolve which features a declaration requires, a table has, and how to enable one."""

    definitions: tuple[FeatureDefinition, ...]
    _definitions_by_feature: Mapping[TableFeature, FeatureDefinition] = field(
        init=False, repr=False, compare=False
    )
    _features_by_type: Mapping[type[DataType], TableFeature] = field(
        init=False, repr=False, compare=False
    )
    _features_by_observed_name: Mapping[str, TableFeature] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        definitions_by_feature = {
            definition.feature: definition for definition in self.definitions
        }

        if len(definitions_by_feature) != len(self.definitions):
            raise ValueError("Feature policy contains duplicate feature definitions")

        # TableFeature is the vocabulary; every member needs an encoding, or
        # planning its enablement would fail at compile time.
        undefined = sorted(
            feature.value for feature in TableFeature if feature not in definitions_by_feature
        )
        if undefined:
            raise ValueError(f"Feature policy defines no encoding for: {', '.join(undefined)}")

        # A feature must be observable under the name the engine writes, or
        # every later sync would re-plan an enablement already applied.
        unobservable = sorted(
            definition.feature.value
            for definition in self.definitions
            if definition.enable_name not in definition.observed_names
        )
        if unobservable:
            raise ValueError(
                "Feature policy enables these features under a name it does not"
                f" observe: {', '.join(unobservable)}"
            )

        object.__setattr__(
            self, "_definitions_by_feature", MappingProxyType(definitions_by_feature)
        )
        object.__setattr__(
            self,
            "_features_by_type",
            MappingProxyType(
                {definition.required_by: definition.feature for definition in self.definitions}
            ),
        )
        object.__setattr__(
            self,
            "_features_by_observed_name",
            MappingProxyType(
                {
                    name: definition.feature
                    for definition in self.definitions
                    for name in definition.observed_names
                }
            ),
        )

    def required_features(self, columns: Iterable[DesiredColumn]) -> frozenset[str]:
        """Return the names of every managed feature these columns' types require."""
        required: set[TableFeature] = set()
        for column in columns:
            required |= self._features_required_by(column.data_type)
        return frozenset(feature.value for feature in required)

    def enabled_features(self, properties: Mapping[str, str]) -> frozenset[str]:
        """
        Return the names of the managed features these table properties record as enabled.

        Feature keys outside the managed vocabulary are ignored: the engine
        neither requires nor disables them, so they are not its state.

        Raises:
            ValueError: A managed feature key carries a value other than
                ``'supported'`` — state the engine cannot interpret must fail
                the read rather than shrink it.

        """
        enabled: set[TableFeature] = set()
        for key, value in properties.items():
            if not key.startswith(_FEATURE_PROPERTY_PREFIX):
                continue
            name = key.removeprefix(_FEATURE_PROPERTY_PREFIX)
            feature = self._features_by_observed_name.get(name)
            if feature is None:
                continue
            if value != _SUPPORTED:
                raise ValueError(
                    f"table feature property {key} has unrecognized value {value!r};"
                    f" expected {_SUPPORTED!r}"
                )
            enabled.add(feature)
        return frozenset(feature.value for feature in enabled)

    def enable_property(self, feature: str) -> tuple[str, str]:
        """Return the ``(key, value)`` table property that enables ``feature``."""
        definition = self._definitions_by_feature[TableFeature(feature)]
        return f"{_FEATURE_PROPERTY_PREFIX}{definition.enable_name}", _SUPPORTED

    def _features_required_by(self, data_type: DataType) -> frozenset[TableFeature]:
        """Walk a type tree, collecting the features its leaves require."""
        match data_type:
            case Array(element=element):
                return self._features_required_by(element)
            case Map(key=key, value=value):
                return self._features_required_by(key) | self._features_required_by(value)
            case Struct(fields=fields):
                empty: frozenset[TableFeature] = frozenset()
                return empty.union(
                    *(self._features_required_by(field.data_type) for field in fields)
                )
            case _:
                feature = self._features_by_type.get(type(data_type))
                return frozenset() if feature is None else frozenset({feature})


_DEFINITIONS: Final[tuple[FeatureDefinition, ...]] = (
    FeatureDefinition(
        feature=TableFeature.TIMESTAMP_NTZ,
        required_by=TimestampNtz,
        enable_name="timestampNtz",
        observed_names=frozenset({"timestampNtz"}),
    ),
    FeatureDefinition(
        feature=TableFeature.VARIANT,
        required_by=Variant,
        # Databricks documents 'variantType-preview' as the enable key. If a
        # live run shows the platform rejecting it, or recording the GA name
        # instead, move that name here — both stay observable either way, so
        # round-trips remain idempotent.
        enable_name="variantType-preview",
        observed_names=frozenset({"variantType", "variantType-preview"}),
    ),
)

DELTA_FEATURE_POLICY: Final = FeaturePolicy(_DEFINITIONS)
