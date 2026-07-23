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
``application/validation.py``). They never appear below.

Platform-managed features (``deletionVectors``, ``rowTracking``, ``invariants``)
are neither kind: the engine leaves them alone entirely.

Admission therefore reduces to one question — can the declared shape exist
without this feature? If yes, it is not ours. The definitions below are the
whole vocabulary: each names a feature, the declared type that implies it, the
name the engine writes to enable it, and every name the catalog may record it
under. Databricks spells features as ordinary table properties
(``delta.feature.<name> = 'supported'``), so the adapters read and compile
through this policy and hold no feature knowledge of their own. A feature's
name — a plain string — is what a desired table records as implied and an
observed table records as supported; the domain compares those two sets and
plans the difference without holding the vocabulary itself.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class FeatureDefinition:
    """
    One managed feature, and the judgments the engine needs about it.

    ``name`` is the Delta protocol name, and the identity the rest of the
    engine carries: a desired table records it as implied, an observed table
    as supported, and an enablement action names it. ``implied_by`` is the
    declared type whose storage needs the feature, found at any depth of a
    column's type tree. ``enable_name`` is the name the engine writes to
    enable it, and ``observed_names`` every name the catalog may record it
    under — a feature that outlives a preview keeps its old spelling on
    existing tables, so the two can differ.
    """

    name: str
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
        nothing is cached and the policy stays an ordinary frozen value. They
        are also built by hand rather than by comprehension, because a
        duplicate key would otherwise be dropped in silence, leaving a type or
        a catalog name resolving to the wrong feature.
        """
        names = [definition.name for definition in self.definitions]
        if len(set(names)) != len(names):
            raise ValueError("Feature policy contains duplicate feature definitions")

        features_by_type: dict[type[DataType], str] = {}
        features_by_observed_name: dict[str, str] = {}
        for definition in self.definitions:
            claimed = features_by_type.setdefault(definition.implied_by, definition.name)
            if claimed != definition.name:
                raise ValueError(
                    f"Feature policy implies both {claimed} and {definition.name} from"
                    f" {definition.implied_by.__name__}; a declared type resolves to"
                    " one feature"
                )
            for observed in sorted(definition.observed_names):
                claimed = features_by_observed_name.setdefault(observed, definition.name)
                if claimed != definition.name:
                    raise ValueError(
                        f"Feature policy observes {observed!r} as both {claimed} and"
                        f" {definition.name}; a catalog name resolves to one feature"
                    )

        # Enabling a feature must observe back as that same feature, or every
        # later sync would re-plan an enablement already applied. Checking the
        # resolved name rather than mere membership also catches an enable
        # name that another definition claims.
        misrouted = sorted(
            definition.name
            for definition in self.definitions
            if features_by_observed_name.get(definition.enable_name) != definition.name
        )
        if misrouted:
            raise ValueError(
                "Feature policy enables these features under a name that does not"
                f" observe back as the same feature: {', '.join(misrouted)}"
            )

    def implied_features(self, columns: Iterable[DesiredColumn]) -> frozenset[str]:
        """Return the names of every managed feature these columns' types imply."""
        implied: set[str] = set()
        for column in columns:
            implied |= self._features_implied_by(column.data_type)
        return frozenset(implied)

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
        supported: set[str] = set()
        for key, value in properties.items():
            if value != _SUPPORTED or not key.startswith(_FEATURE_PROPERTY_PREFIX):
                continue
            name = self._feature_observed_as(key.removeprefix(_FEATURE_PROPERTY_PREFIX))
            if name is not None:
                supported.add(name)
        return frozenset(supported)

    def enable_property(self, feature: str) -> tuple[str, str]:
        """Return the ``(key, value)`` table property that enables ``feature``."""
        for definition in self.definitions:
            if definition.name == feature:
                return f"{_FEATURE_PROPERTY_PREFIX}{definition.enable_name}", _SUPPORTED
        raise ValueError(f"No managed table feature named {feature!r}")

    def _feature_observed_as(self, name: str) -> str | None:
        """Return the feature the catalog records under ``name``, if it is managed."""
        for definition in self.definitions:
            if name in definition.observed_names:
                return definition.name
        return None

    def _feature_implied_by(self, data_type: DataType) -> str | None:
        """Return the feature this exact declared type implies, if any."""
        for definition in self.definitions:
            if type(data_type) is definition.implied_by:
                return definition.name
        return None

    def _features_implied_by(self, data_type: DataType) -> frozenset[str]:
        """Walk a type tree, collecting the features its leaves imply."""
        match data_type:
            case Array(element=element):
                return self._features_implied_by(element)
            case Map(key=key, value=value):
                return self._features_implied_by(key) | self._features_implied_by(value)
            case Struct(fields=fields):
                empty: frozenset[str] = frozenset()
                return empty.union(
                    *(self._features_implied_by(field.data_type) for field in fields)
                )
            case _:
                name = self._feature_implied_by(data_type)
                return frozenset() if name is None else frozenset({name})


_DEFINITIONS: Final[tuple[FeatureDefinition, ...]] = (
    FeatureDefinition(
        name="timestampNtz",
        implied_by=TimestampNtz,
        enable_name="timestampNtz",
        observed_names=frozenset({"timestampNtz"}),
    ),
    FeatureDefinition(
        name="variantType",
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
