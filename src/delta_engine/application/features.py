"""
The Delta table features a declared schema implies, and how the engine enables them.

Delta protocol reference: https://docs.delta.io/latest/versioning.html
Databricks feature compatibility:
https://docs.databricks.com/aws/en/tables/features/feature-compatibility

Two kinds of feature requirement run through this engine, and only one of them
lives here.

A feature is *implied* when the desired shape cannot exist without it: a column
of type TIMESTAMP_NTZ needs ``timestampNtz``, and a table carrying such a column
always has it. Nothing is declared and nothing is chosen. Databricks enables
most implied features from the syntax that needs them, on ALTER as readily as on
CREATE — ``CLUSTER BY`` enables liquid clustering on an existing table, and
``GENERATED ALWAYS AS`` enables generated columns. The features below are the
exception: established by a CREATE, never by an ALTER, which is the whole reason
this module exists. It is also why liquid clustering is none of the engine's
business despite being implied by ``clustered_by`` and just as permanent — the
``ALTER TABLE ... CLUSTER BY`` the engine already plans upgrades the protocol on
its own.

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
from dataclasses import dataclass
from enum import StrEnum
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

    def __post_init__(self) -> None:
        """
        Reject a definition set that could resolve a name or a type ambiguously.

        The lookups built here are validation scaffolding, not state: with a
        handful of definitions the accessors below scan them directly, so
        nothing is cached and the policy stays an ordinary frozen value.
        """
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
        # a duplicate key would otherwise be dropped in silence, leaving a
        # type or a catalog name resolving to the wrong feature.
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
        neither implies nor disables them, so they are not its state. A managed
        key carrying anything other than ``'supported'`` reads as unsupported
        rather than failing the read. Being wrong that way costs one enablement
        statement, which is idempotent and normalizes the value; refusing the
        read would leave the whole table unmanageable over one unrecognized
        string, and misreading a feature fabricates neither drift nor a blocked
        change.
        """
        supported: set[ImpliedFeature] = set()
        for key, value in properties.items():
            if value != _SUPPORTED or not key.startswith(_FEATURE_PROPERTY_PREFIX):
                continue
            feature = self._feature_observed_as(key.removeprefix(_FEATURE_PROPERTY_PREFIX))
            if feature is not None:
                supported.add(feature)
        return frozenset(feature.value for feature in supported)

    def enable_property(self, feature: str) -> tuple[str, str]:
        """Return the ``(key, value)`` table property that enables ``feature``."""
        for definition in self.definitions:
            if definition.feature == feature:
                return f"{_FEATURE_PROPERTY_PREFIX}{definition.enable_name}", _SUPPORTED
        raise ValueError(f"No managed table feature named {feature!r}")

    def _feature_observed_as(self, name: str) -> ImpliedFeature | None:
        """Return the feature the catalog records under ``name``, if it is managed."""
        for definition in self.definitions:
            if name in definition.observed_names:
                return definition.feature
        return None

    def _feature_implied_by(self, data_type: DataType) -> ImpliedFeature | None:
        """Return the feature this exact declared type implies, if any."""
        for definition in self.definitions:
            if type(data_type) is definition.implied_by:
                return definition.feature
        return None

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
                feature = self._feature_implied_by(data_type)
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
