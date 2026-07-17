"""
Schema declaration implementation for Delta tables and foreign keys.

Validation here is declaration-time deployability: rules Delta or Unity
Catalog enforce when an author creates a table (layout key types, quotas,
property-dependent naming rules). Structural coherence — columns exist,
names are unique, at least one column — is the domain ``DesiredTable``'s
job, enforced the moment a declaration is lowered; this module skips names
it cannot resolve rather than re-checking existence.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, Literal, NamedTuple

from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY, Property
from delta_engine.domain.model import (
    ALL_ASPECTS,
    Array,
    Binary,
    Boolean,
    DataType,
    DesiredColumn as Column,
    DesiredTable,
    ForeignKeyConstraint,
    Map,
    PrimaryKeyConstraint,
    QualifiedName,
    Struct,
    TableAspect,
    Variant,
)

METADATA_ASPECTS: Final[frozenset[TableAspect]] = frozenset(
    {
        TableAspect.TABLE_COMMENT,
        TableAspect.COLUMN_COMMENTS,
        TableAspect.COLUMN_TAGS,
        TableAspect.TABLE_TAGS,
        TableAspect.PRIMARY_KEY,
        TableAspect.FOREIGN_KEYS,
    }
)

TAG_ASPECTS: Final[frozenset[TableAspect]] = frozenset(
    {
        TableAspect.COLUMN_TAGS,
        TableAspect.TABLE_TAGS,
    }
)

# The public API exposes named scopes only, each mapping to an aspect set.
# The TableAspect enum stays internal so a declaration can only manage
# combinations the engine's safety rules are written against.
_ASPECTS_BY_SCOPE: Final[Mapping[str, frozenset[TableAspect]]] = {
    "full": ALL_ASPECTS,
    "metadata": METADATA_ASPECTS,
    "tags": TAG_ASPECTS,
}

# Delta permits these characters in column names only under column mapping.
_CHARACTERS_REQUIRING_COLUMN_MAPPING: Final[frozenset[str]] = frozenset(" ,;{}()\n\t=")

# Column names change data feed reserves for its own output columns.
_CDF_RESERVED_COLUMN_NAMES: Final[frozenset[str]] = frozenset(
    {"_change_type", "_commit_version", "_commit_timestamp"}
)

# Partitioning and clustering are distinct backend rules with distinct type
# lists. Delta refuses complex types as partition columns
# (INVALID_PARTITION_COLUMN_DATA_TYPE).
_TYPES_UNUSABLE_AS_PARTITION_KEYS: Final[tuple[type[DataType], ...]] = (Array, Map, Struct, Variant)

# Liquid clustering's supported-type list is narrower: on top of the complex
# types, it also excludes Boolean and Binary, which partitioning accepts.
_TYPES_UNUSABLE_AS_CLUSTERING_KEYS: Final[tuple[type[DataType], ...]] = (
    *_TYPES_UNUSABLE_AS_PARTITION_KEYS,
    Boolean,
    Binary,
)

# Tag limits enforced per securable object (a table and each of its columns
# are separate securables). Databricks caps both tag keys and values at 256
# characters; the platform's rejection of a 300-character key and value is
# pinned live by test_platform_rejects_an_over_long_column_tag_key_or_value.
# (A separate 1,000-column-tag-per-table total is not enforced here.)
_MAX_TAGS_PER_SECURABLE: Final[int] = 50
_MAX_TAG_KEY_LENGTH: Final[int] = 256
_MAX_TAG_VALUE_LENGTH: Final[int] = 256


class _SelfReference:
    """Sentinel marking a foreign key that references its own table."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "Self"


Self: Final = _SelfReference()


def _validate_tags(subject: str, tags: Mapping[str, str]) -> None:
    if len(tags) > _MAX_TAGS_PER_SECURABLE:
        raise ValueError(
            f"{subject} declares {len(tags)} tags; Unity Catalog allows at"
            f" most {_MAX_TAGS_PER_SECURABLE} per securable"
        )
    for key, value in tags.items():
        if len(key) > _MAX_TAG_KEY_LENGTH:
            raise ValueError(
                f"Tag {key!r} on {subject} has a {len(key)}-character"
                f" key; delta-engine accepts at most {_MAX_TAG_KEY_LENGTH}"
            )
        if len(value) > _MAX_TAG_VALUE_LENGTH:
            raise ValueError(
                f"Tag {key!r} on {subject} has a {len(value)}-character"
                f" value; delta-engine accepts at most {_MAX_TAG_VALUE_LENGTH}"
            )


def _column_declared_names(column: Column) -> tuple[str, ...]:
    """
    Return every name a column declares under Delta's naming rules.

    This is the column's own name, plus every struct field name reachable
    through nested ``Struct``/``Array``/``Map`` types (struct-in-array,
    struct-in-struct, and so on). The column's own name is returned bare;
    nested field names are returned as dotted paths from the column, e.g.
    ``"payload.order id"`` for a field named ``"order id"`` inside a struct
    column named ``"payload"``.
    """
    return (column.name, *_nested_field_paths(column.name, column.data_type))


def _nested_field_paths(path: str, data_type: DataType) -> tuple[str, ...]:
    """Recursively collect dotted struct-field paths reachable from `data_type`."""
    match data_type:
        case Struct(fields):
            paths: list[str] = []
            for field in fields:
                field_path = f"{path}.{field.name}"
                paths.append(field_path)
                paths.extend(_nested_field_paths(field_path, field.data_type))
            return tuple(paths)
        case Array(element):
            return _nested_field_paths(path, element)
        case Map(key, value):
            return _nested_field_paths(path, key) + _nested_field_paths(path, value)
        case _:
            return ()


def _validate_layout(
    columns: tuple[Column, ...],
    partitioned_by: tuple[str, ...],
    clustered_by: tuple[str, ...],
) -> None:
    """
    Reject physical layouts Delta cannot deploy.

    Checks declaration-level deployability only: the partition/cluster
    relationship, the clustering-key budget, and key data types. Whether the
    named columns exist and are unique is a ``DesiredTable`` invariant, so
    names that do not resolve are skipped here and the domain's error fires.
    """
    if partitioned_by and clustered_by:
        raise ValueError(
            "A table cannot both partition and cluster: declare partitioned_by"
            " or clustered_by, not both."
        )
    if len(clustered_by) > 4:
        raise ValueError(
            f"A table may declare at most four clustering keys; got {len(clustered_by)}."
        )

    columns_by_name = {column.name: column for column in columns}
    for name in partitioned_by:
        column = columns_by_name.get(name)
        if column is not None and isinstance(column.data_type, _TYPES_UNUSABLE_AS_PARTITION_KEYS):
            raise ValueError(
                f"Partition column {name!r} has type"
                f" {type(column.data_type).__name__}, which Delta cannot partition by"
            )
    for name in clustered_by:
        column = columns_by_name.get(name)
        if column is not None and isinstance(column.data_type, _TYPES_UNUSABLE_AS_CLUSTERING_KEYS):
            raise ValueError(
                f"Clustering column {name!r} has type"
                f" {type(column.data_type).__name__}, which cannot be a clustering key"
            )

    if (
        partitioned_by
        and set(partitioned_by) <= columns_by_name.keys()
        and len(set(partitioned_by)) == len(columns)
    ):
        raise ValueError(
            "Cannot partition by every column: at least one non-partition column is required"
        )


def _validate_renames(columns: tuple[Column, ...], properties: Mapping[str, str | None]) -> None:
    """
    Reject rename hints without name-based column mapping.

    Hints are visible in the declaration, so reject them at construction.
    """
    hinted = [column.name for column in columns if column.renamed_from is not None]
    if not hinted:
        return
    if properties.get(Property.COLUMN_MAPPING_MODE) != "name":
        raise ValueError(
            f"Columns {hinted} declare renamed_from, which requires"
            f" {Property.COLUMN_MAPPING_MODE}='name'. Declare"
            f" properties={{'{Property.COLUMN_MAPPING_MODE}': 'name'}} on this table."
        )


def _validate_column_names(
    columns: tuple[Column, ...],
    properties: Mapping[str, str | None],
    managed_aspects: frozenset[TableAspect],
) -> None:
    """
    Reject column names the declared properties make invalid on Delta.

    Two naming rules depend on properties: characters such as spaces are only
    permitted under column mapping, and change data feed reserves its output
    column names. Both bind only when the declaration manages column
    structure — a restricted scope mirrors columns the live table already
    accepted, so it must be able to declare names this engine would refuse
    to create.
    """
    if TableAspect.COLUMN_STRUCTURE not in managed_aspects:
        return

    if properties.get(Property.COLUMN_MAPPING_MODE) != "name":
        offending = [
            declared_name
            for column in columns
            for declared_name in _column_declared_names(column)
            if set(declared_name) & _CHARACTERS_REQUIRING_COLUMN_MAPPING
        ]
        if offending:
            raise ValueError(
                f"Column or struct field names {offending} contain characters Delta only"
                " permits with column mapping. Declare"
                f" properties={{'{Property.COLUMN_MAPPING_MODE}': 'name'}}"
                " or rename the columns."
            )

    if properties.get(Property.CHANGE_DATA_FEED) == "true":
        reserved = [column.name for column in columns if column.name in _CDF_RESERVED_COLUMN_NAMES]
        if reserved:
            raise ValueError(
                f"Column names {reserved} are reserved by change data feed."
                " Rename them or do not enable"
                f" {Property.CHANGE_DATA_FEED}."
            )


class _ReferencedSide(NamedTuple):
    """The referenced side of a foreign key, resolved at lowering time."""

    table: QualifiedName
    key_columns: tuple[str, ...]
    column_types: dict[str, DataType]


@dataclass(frozen=True, slots=True)
class ForeignKey:
    """
    Public declaration of a foreign key relationship.

    ``columns`` maps each local column to the referenced-table primary key
    column it references — the mapping is the pairing, stated outright; its
    insertion order is irrelevant. The values must cover the referenced
    table's primary key exactly. The physical constraint name is generated by
    the engine and is not part of this declaration.

    ``references`` is another :class:`DeltaTable`, or the :data:`Self` sentinel
    for a self-referential key. See the architecture explanation doc for why
    the reference is an object rather than a name. The referenced table must
    live in the same catalog as the declaring table — information_schema is
    per-catalog, so a cross-catalog constraint could be created but never
    observed afterwards.
    """

    columns: Mapping[str, str]
    references: DeltaTable | _SelfReference

    def __post_init__(self) -> None:
        if not isinstance(self.columns, Mapping):
            raise TypeError(
                "foreign key columns must be a mapping of"
                " {local column: referenced column};"
                f" got {type(self.columns).__name__}"
            )
        object.__setattr__(self, "columns", MappingProxyType(dict(self.columns)))

    def _to_constraint(
        self,
        owner_name: QualifiedName,
        owner_columns: tuple[Column, ...],
        owner_primary_key: tuple[str, ...],
    ) -> ForeignKeyConstraint:
        """
        Lower this declaration into a domain constraint.

        Applies the lowering rules in order: the referenced table must live in
        the owner's catalog, must declare a primary key, the mapping's values
        must equal that key exactly, and each local column's data type must
        match its referenced column's. Local column existence is not checked
        here — the ``DesiredTable`` built right after enforces it.
        """
        referenced = self._resolve_reference(owner_name, owner_columns, owner_primary_key)

        if referenced.table.catalog != owner_name.catalog:
            raise ValueError(
                f"cross-catalog foreign key not supported: {owner_name} cannot"
                f" reference {referenced.table}. information_schema is"
                " per-catalog, so the engine could create the constraint but"
                " never observe it afterwards; declare both tables in the same"
                " catalog."
            )

        if not referenced.key_columns:
            raise ValueError(
                f"foreign key references {referenced.table}, which declares no primary key"
            )

        local_columns = tuple(self.columns.keys())
        referenced_columns = tuple(self.columns.values())

        declared = set(referenced_columns)
        key = set(referenced.key_columns)
        if declared != key:
            missing = sorted(key - declared)
            extra = sorted(declared - key)
            details = []
            if missing:
                details.append(f"missing from the mapping: {', '.join(missing)}")
            if extra:
                details.append(f"not in the key: {', '.join(extra)}")
            raise ValueError(
                f"foreign key columns must reference {referenced.table}'s"
                f" primary key ({', '.join(referenced.key_columns)}) exactly;"
                f" {'; '.join(details)}"
            )

        local_types = {column.name: column.data_type for column in owner_columns}
        for local_name, referenced_name in self.columns.items():
            local_type = local_types.get(local_name)
            if local_type is None:
                continue  # local column existence is enforced when the DesiredTable is built
            referenced_type = referenced.column_types[referenced_name]
            if local_type != referenced_type:
                raise ValueError(
                    f"foreign key column type mismatch: {owner_name}.{local_name}"
                    f" is {local_type} but {referenced.table}.{referenced_name}"
                    f" is {referenced_type}"
                )

        return ForeignKeyConstraint.generate(
            owner_table_name=owner_name.name,
            local_columns=local_columns,
            referenced_table=referenced.table,
            referenced_columns=referenced_columns,
        )

    def _resolve_reference(
        self,
        owner_name: QualifiedName,
        owner_columns: tuple[Column, ...],
        owner_primary_key: tuple[str, ...],
    ) -> _ReferencedSide:
        """
        Resolve ``references`` to the referenced side of the constraint.

        For :data:`Self` the enclosing table supplies every field — its own
        name, ``owner_primary_key``, and ``owner_columns`` — because the
        owner's ``DesiredTable`` does not exist yet while its foreign keys
        are being lowered. ``column_types`` always covers every referenced
        column: primary-key columns are validated to exist on whichever
        table declares them.
        """
        match self.references:
            case _SelfReference():
                types = {column.name: column.data_type for column in owner_columns}
                return _ReferencedSide(owner_name, owner_primary_key, types)
            case DeltaTable() as target:
                desired = target.to_desired_table()
                types = {column.name: column.data_type for column in desired.columns}
                return _ReferencedSide(desired.qualified_name, desired.primary_key_columns, types)
            case _:
                raise TypeError(
                    f"foreign key references must be a DeltaTable or Self; got {self.references!r}"
                )


class DeltaTable:
    """
    Defines a Delta table schema.

    ``scope`` selects how much of the table the declaration manages: the whole
    table (default), catalog metadata only, or tags only.

    Note on dropping columns: Delta only permits ``ALTER TABLE ... DROP COLUMN``
    when ``delta.columnMapping.mode`` is ``name``. Declare it in ``properties``
    on any table whose columns may be dropped; a sync that drops a column
    without it fails at validation with a message naming the property.
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        name: str,
        columns: Iterable[Column],
        comment: str = "",
        properties: Mapping[str, str | None] | None = None,
        tags: Mapping[str, str] | None = None,
        partitioned_by: Iterable[str] = (),
        clustered_by: Iterable[str] = (),
        primary_key: Sequence[str] | None = None,
        foreign_keys: Iterable[ForeignKey] | None = None,
        scope: Literal["full", "metadata", "tags"] = "full",
    ) -> None:
        """
        Initialise a DeltaTable definition.

        Args:
            catalog: Unity Catalog catalog name.
            schema: Schema (database) name within the catalog.
            name: Table name.
            columns: Ordered column declarations.
            comment: Table-level comment.
            properties: Delta/Spark table properties to manage.
            tags: Key/value tags to apply to the table.
            partitioned_by: Column names to partition by. Mutually exclusive
                with ``clustered_by``.
            clustered_by: Column names to use as Delta liquid-clustering keys
                (at most four). Key order is immaterial. Mutually exclusive with
                ``partitioned_by``.
            primary_key: Column names forming the table's primary key, in the
                order the constraint is rendered; None means no key.
            foreign_keys: Foreign key relationships declared on this table.
            scope: What this declaration manages. ``"full"`` (the default)
                manages the whole table. ``"metadata"`` restricts the sync to
                catalog metadata: comments, tags, and primary/foreign key
                constraints. ``"tags"`` restricts it to table and column tags
                — for tables owned elsewhere whose Unity Catalog tags this
                engine should still govern. Streaming tables are supported
                under this scope and only this scope: their definition
                belongs to the owning pipeline, so any wider scope against
                one fails validation. A
                restricted scope still declares the full table shape; aspects
                outside the scope are never changed, and drift on them fails
                validation. Properties are the exception: a declaration that
                does not manage properties never compares them at all.

        """
        # The Literal type catches bad scopes at type-check time; this guard
        # covers untyped callers.
        if scope not in _ASPECTS_BY_SCOPE:
            expected = ", ".join(repr(known_scope) for known_scope in _ASPECTS_BY_SCOPE)
            raise ValueError(f"Unknown scope {scope!r}; expected one of: {expected}")
        managed_aspects = _ASPECTS_BY_SCOPE[scope]

        user_properties = dict(properties or {})

        # Fast-fail on property keys this engine does not manage (e.g. typos).
        # None-valued assertions are validated too: asserting absence of an
        # unmanaged key is as meaningless as declaring it.
        unmanaged = [key for key in user_properties if key not in DELTA_PROPERTY_REGISTRY]
        if unmanaged:
            raise ValueError(
                f"Properties not managed by this engine: {', '.join(sorted(unmanaged))}"
            )

        # Fast-fail on malformed values; the definition owns the judgment
        # (including the exemption for None, which asserts absence).
        for key, declared_value in user_properties.items():
            rejection = DELTA_PROPERTY_REGISTRY[key].reject_declared_value(declared_value)
            if rejection is not None:
                raise ValueError(rejection)

        columns = tuple(columns)
        partitioned_by = tuple(partitioned_by)
        clustered_by = tuple(clustered_by)
        _validate_layout(columns, partitioned_by, clustered_by)
        _validate_column_names(columns, user_properties, managed_aspects)
        _validate_renames(columns, user_properties)

        table_tags = dict(tags or {})
        _validate_tags(f"table '{name}'", table_tags)
        for column in columns:
            _validate_tags(f"column '{column.name}'", column.tags)

        # A bare string is itself a Sequence[str], so the type checker cannot
        # reject it; refuse the shape before it silently means per-character
        # columns.
        if isinstance(primary_key, str):
            raise TypeError(
                "primary_key must be a sequence of column names, not a string;"
                f" write primary_key=[{primary_key!r}] for a single-column key"
            )

        primary_key_constraint = (
            PrimaryKeyConstraint.generate(table_name=name, columns=tuple(primary_key))
            if primary_key is not None
            else None
        )
        primary_key_columns = (
            primary_key_constraint.columns if primary_key_constraint is not None else ()
        )

        qualified_name = QualifiedName(catalog, schema, name)
        lowered_foreign_keys = tuple(
            declaration._to_constraint(qualified_name, columns, primary_key_columns)
            for declaration in (foreign_keys or ())
        )

        # Building DesiredTable here enforces all domain invariants (non-empty
        # columns, unique names, partition columns must exist, FK local columns
        # must exist) at construction time rather than deferring them to
        # to_desired_table().
        self._desired_table = DesiredTable(
            qualified_name=qualified_name,
            columns=columns,
            comment=comment,
            properties=user_properties,
            tags=table_tags,
            partitioned_by=partitioned_by,
            clustered_by=clustered_by,
            primary_key=primary_key_constraint,
            foreign_keys=lowered_foreign_keys,
            managed_aspects=managed_aspects,
        )

    @property
    def catalog(self) -> str:
        """Unity Catalog catalog name."""
        return self._desired_table.qualified_name.catalog

    @property
    def schema(self) -> str:
        """Schema (database) name within the catalog."""
        return self._desired_table.qualified_name.schema

    @property
    def name(self) -> str:
        """Table name."""
        return self._desired_table.qualified_name.name

    @property
    def columns(self) -> tuple[Column, ...]:
        """Declared columns, in declaration order."""
        return self._desired_table.columns

    @property
    def comment(self) -> str:
        """Table-level comment (empty string when unset)."""
        return self._desired_table.comment

    @property
    def properties(self) -> Mapping[str, str | None]:
        """
        Declared table properties.

        A ``None`` value asserts the property must be absent from the table.
        """
        return self._desired_table.properties

    @property
    def tags(self) -> Mapping[str, str]:
        """Declared table tags."""
        return self._desired_table.tags

    @property
    def partitioned_by(self) -> tuple[str, ...]:
        """Partition column names, in declaration order."""
        return self._desired_table.partitioned_by

    @property
    def clustered_by(self) -> tuple[str, ...]:
        """Clustering key column names, in declaration order."""
        return self._desired_table.clustered_by

    @property
    def primary_key(self) -> tuple[str, ...]:
        """Column names declared as the primary key, in declaration order."""
        return self._desired_table.primary_key_columns

    @property
    def foreign_keys(self) -> tuple[ForeignKeyConstraint, ...]:
        """Foreign key constraints declared on this table."""
        return self._desired_table.foreign_keys

    def to_desired_table(self) -> DesiredTable:
        """Return the domain :class:`DesiredTable` for this table definition."""
        return self._desired_table
