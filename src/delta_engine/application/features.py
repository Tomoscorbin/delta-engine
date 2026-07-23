"""
The Delta table features a declared schema implies, and how the engine enables them.

Reference: https://docs.delta.io/latest/versioning.html

Two kinds of feature requirement run through this engine, and only one of them
lives here.

A feature is *implied* when the desired shape cannot exist without it: a column
of type TIMESTAMP_NTZ needs ``timestampNtz``, and a table carrying such a column
always has it. Nothing is declared and nothing is chosen. Databricks enables an
implied feature as part of creating a table — this module exists because it does
not do the same when altering one, so the engine closes the gap itself and shows
the upgrade in the plan.

A feature is *operation-permitted* when the table exists happily without it and
a particular change needs it: ``columnMapping`` to drop or rename a column,
``typeWidening`` to widen a type in place. Those are the user's decision, they
carry real cost, and each is reached through a managed property — so they are
declared, and validation refuses the change when they are not
(``ColumnMappingRequiredForDrop``, ``TypeWideningRequiredForTypeChange`` in
``application/validation.py``). They never appear in ``ImpliedFeature``.

Platform-managed features (``deletionVectors``, ``rowTracking``, ``invariants``)
are neither kind: the engine leaves them alone entirely.

Admission therefore reduces to one question — can the declared shape exist
without this feature? If yes, it is not ours. Everything a feature needs lives
in its definition below: what implies it, the name the engine writes to enable
it, and every name the catalog may record it under. Databricks spells features
as ordinary table properties (``delta.feature.<name> = 'supported'``), so the
adapters read and compile through this policy and hold no feature knowledge of
their own.

``ImpliedFeature`` is the single source of the managed feature names, and its
values — plain strings — are what a desired table records as implied and an
observed table records as supported. The domain compares those two sets and
plans the difference without holding the vocabulary itself.
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

# Databricks records and accepts feature support as table properties under this
# prefix; 'supported' is the only value the protocol assigns meaning to.
_FEATURE_PROPERTY_PREFIX: Final = "delta.feature."
_SUPPORTED: Final = "supported"


class ImpliedFeature(StrEnum):
    """A Delta table feature a declared schema cannot exist without."""

    TIMESTAMP_NTZ = "timestampNtz"
    VARIANT = "variantType"


@dataclass(frozen=True, slots=True)
class FeatureDefinition:
    """
    One implied feature, and the judgments the engine needs about it.

    ``implied_by`` is the declared type whose storage needs the feature, found
    at any depth of a column's type tree. ``enable_name`` is the name the engine
    writes to enable it, and ``observed_names`` every name the catalog may
    record it under — a feature that outlives a preview keeps its old spelling
    on existing tables, so the two differ.
    """

    feature: ImpliedFeature
    implied_by: type[DataType]
    enable_name: str
    observed_names: frozenset[str]


@dataclass(frozen=True, slots=True)
class FeaturePolicy:
    """Resolve which features a declaration implies, a table supports, and how to enable one."""

    definitions: tuple[FeatureDefinition, ...]
    _definitions_by_feature: Mapping[ImpliedFeature, FeatureDefinition] = field(
        init=False, repr=False, compare=False
    )
    _features_by_type: Mapping[type[DataType], ImpliedFeature] = field(
        init=False, repr=False, compare=False
    )
    _features_by_observed_name: Mapping[str, ImpliedFeature] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        definitions_by_feature = {
            definition.feature: definition for definition in self.definitions
        }

        if len(definitions_by_feature) != len(self.definitions):
            raise ValueError("Feature policy contains duplicate feature definitions")

        # ImpliedFeature is the vocabulary; every member needs an encoding, or
        # planning its enablement would fail at compile time.
        undefined = sorted(
            feature.value for feature in ImpliedFeature if feature not in definitions_by_feature
        )
        if undefined:
            raise ValueError(f"Feature policy defines no encoding for: {', '.join(undefined)}")

        # Both lookups below are built by hand rather than by comprehension:
        # a duplicate key would otherwise let the last definition win in
        # silence, resolving a type or a catalog name to the wrong feature.
        features_by_type: dict[type[DataType], ImpliedFeature] = {}
        for definition in self.definitions:
            claimed = features_by_type.setdefault(definition.implied_by, definition.feature)
            if claimed is not definition.feature:
                raise ValueError(
                    f"Feature policy implies both {claimed.value} and"
                    f" {definition.feature.value} from {definition.implied_by.__name__};"
                    " a declared type resolves to one feature"
                )

        features_by_observed_name: dict[str, ImpliedFeature] = {}
        for definition in self.definitions:
            for name in sorted(definition.observed_names):
                claimed = features_by_observed_name.setdefault(name, definition.feature)
                if claimed is not definition.feature:
                    raise ValueError(
                        f"Feature policy observes {name!r} as both {claimed.value} and"
                        f" {definition.feature.value}; a catalog name resolves to one feature"
                    )

        # Enabling a feature must observe back as that same feature, or every
        # later sync would re-plan an enablement already applied. Checking the
        # resolved name rather than mere membership also catches an enable
        # name that another definition claims.
        misrouted = sorted(
            definition.feature.value
            for definition in self.definitions
            if features_by_observed_name.get(definition.enable_name) is not definition.feature
        )
        if misrouted:
            raise ValueError(
                "Feature policy enables these features under a name that does not"
                f" observe back as the same feature: {', '.join(misrouted)}"
            )

        object.__setattr__(
            self, "_definitions_by_feature", MappingProxyType(definitions_by_feature)
        )
        object.__setattr__(self, "_features_by_type", MappingProxyType(features_by_type))
        object.__setattr__(
            self, "_features_by_observed_name", MappingProxyType(features_by_observed_name)
        )

    def implied_features(self, columns: Iterable[DesiredColumn]) -> frozenset[str]:
        """Return the names of every managed feature these columns' types imply."""
        implied: set[ImpliedFeature] = set()
        for column in columns:
            implied |= self._features_implied_by(column.data_type)
        return frozenset(feature.value for feature in implied)

    def supported_features(self, properties: Mapping[str, str]) -> frozenset[str]:
        """
        Return the names of the managed features these table properties record as supported.

        Feature keys outside the managed vocabulary are ignored: the engine
        neither implies nor disables them, so they are not its state.

        Raises:
            ValueError: A managed feature key carries a value other than
                ``'supported'`` — state the engine cannot interpret must fail
                the read rather than shrink it.

        """
        supported: set[ImpliedFeature] = set()
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
            supported.add(feature)
        return frozenset(feature.value for feature in supported)

    def enable_property(self, feature: str) -> tuple[str, str]:
        """Return the ``(key, value)`` table property that enables ``feature``."""
        definition = self._definitions_by_feature[ImpliedFeature(feature)]
        return f"{_FEATURE_PROPERTY_PREFIX}{definition.enable_name}", _SUPPORTED

    def _features_implied_by(self, data_type: DataType) -> frozenset[ImpliedFeature]:
        """Walk a type tree, collecting the features its leaves imply."""
        match data_type:
            case Array(element=element):
                return self._features_implied_by(element)
            case Map(key=key, value=value):
                return self._features_implied_by(key) | self._features_implied_by(value)
            case Struct(fields=fields):
                empty: frozenset[ImpliedFeature] = frozenset()
                return empty.union(
                    *(self._features_implied_by(field.data_type) for field in fields)
                )
            case _:
                feature = self._features_by_type.get(type(data_type))
                return frozenset() if feature is None else frozenset({feature})


_DEFINITIONS: Final[tuple[FeatureDefinition, ...]] = (
    FeatureDefinition(
        feature=ImpliedFeature.TIMESTAMP_NTZ,
        implied_by=TimestampNtz,
        enable_name="timestampNtz",
        observed_names=frozenset({"timestampNtz"}),
    ),
    FeatureDefinition(
        feature=ImpliedFeature.VARIANT,
        implied_by=Variant,
        # Databricks documents 'variantType-preview' as the enable key. If a
        # live run shows the platform rejecting it, or recording the GA name
        # instead, move that name here — both stay observable either way, so
        # round-trips remain idempotent.
        enable_name="variantType-preview",
        observed_names=frozenset({"variantType", "variantType-preview"}),
    ),
)

DELTA_FEATURE_POLICY: Final = FeaturePolicy(_DEFINITIONS)
