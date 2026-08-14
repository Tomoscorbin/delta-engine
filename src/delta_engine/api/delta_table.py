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

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, NamedTuple

from delta_engine.application.properties import DELTA_PROPERTY_POLICY, Property
from delta_engine.application.scopes import ScopeName, table_scope_for
from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model import (
    Array,
    Binary,
    Boolean,
    DataType,
    DesiredColumn as Column,
    DesiredForeignKey,
    DesiredPrimaryKey,
    DesiredTable,
    Identifier,
    Map,
    QualifiedName,
    Struct,
    TableAspect,
    TableScope,
    Variant,
)

# Delta permits these characters in column names only under column mapping.
_CHARACTERS_REQUIRING_COLUMN_MAPPING: Final[frozenset[str]] = frozenset(" ,;{}()\n\t=")

# Unity Catalog rules for securable object names (catalogs, schemas, tables,
# uniformly): at most 255 characters; no period, space, forward slash, ASCII
# control character (00-1F hex), or DEL (7F hex). Column names are exempt —
# their special characters are governed by column mapping instead.
_OBJECT_NAME_MAX_LENGTH: Final[int] = 255
_OBJECT_NAME_FORBIDDEN_CHARACTERS: Final[frozenset[str]] = frozenset(
    {".", " ", "/", chr(0x7F), *(chr(code) for code in range(0x20))}
)

# Column names change data feed reserves for its own output columns.
_CDF_RESERVED_COLUMN_NAMES: Final[frozenset[Identifier]] = frozenset(
    Identifier(name) for name in ("_change_type", "_commit_version", "_commit_timestamp")
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


def _foreign_key_constraint_name(
    *,
    owner_table_name: str,
    local_columns: tuple[str, ...],
) -> str:
    """
    Return the physical name used for a generated foreign key.

    Joins the sorted identity keys of the local columns so the generated
    name is identical across declaration casing and column order.
    """
    columns = "_".join(sorted(column.lower() for column in local_columns))
    return f"{owner_table_name}_{columns}_fk"


def _validate_object_name_parts(qualified_name: QualifiedName) -> None:
    """Reject catalog, schema, or table name parts Unity Catalog cannot store."""
    for label, part in zip(("catalog", "schema", "name"), qualified_name.parts, strict=True):
        if len(part) > _OBJECT_NAME_MAX_LENGTH:
            raise ValueError(
                f"Table {label} is {len(part)} characters long; Unity Catalog"
                f" limits object names to {_OBJECT_NAME_MAX_LENGTH} characters"
            )
        forbidden = sorted(set(part) & _OBJECT_NAME_FORBIDDEN_CHARACTERS)
        if forbidden:
            raise ValueError(
                f"Table {label} {part!r} contains characters Unity Catalog"
                f" forbids in object names: {forbidden}. Periods, spaces,"
                " forward slashes, control characters, and DEL are not allowed."
            )


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


class _NestedStructFieldContext(NamedTuple):
    path: str
    nullable: bool
    not_null_allowed: bool


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
    nested_names = (
        field.path
        for field in _nested_struct_fields(
            column.name,
            column.data_type,
            not_null_allowed=not column.nullable,
        )
    )
    return (column.name, *nested_names)


def _nested_struct_fields(
    path: str,
    data_type: DataType,
    not_null_allowed: bool,
) -> Iterator[_NestedStructFieldContext]:
    """Yield struct fields with the container context needed at the API boundary."""
    match data_type:
        case Struct(fields):
            for field in fields:
                field_path = f"{path}.{field.name}"
                yield _NestedStructFieldContext(
                    path=field_path,
                    nullable=field.nullable,
                    not_null_allowed=not_null_allowed,
                )
                yield from _nested_struct_fields(
                    field_path,
                    field.data_type,
                    not_null_allowed=not_null_allowed and not field.nullable,
                )
        case Array(element):
            yield from _nested_struct_fields(path, element, not_null_allowed=False)
        case Map(key, value):
            yield from _nested_struct_fields(path, key, not_null_allowed=False)
            yield from _nested_struct_fields(path, value, not_null_allowed=False)
        case _:
            return


def _validate_nested_not_null(
    column: Column,
) -> None:
    """Reject nested NOT NULL declarations Databricks cannot deploy."""
    for field in _nested_struct_fields(
        column.name,
        column.data_type,
        not_null_allowed=not column.nullable,
    ):
        if not field.nullable and not field.not_null_allowed:
            raise ValueError(
                f"NOT NULL struct field '{field.path}' is not deployable: every containing"
                " column and struct field must be NOT NULL, and ARRAY or MAP paths do not"
                " support nested NOT NULL"
            )


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

    partition_names = set(partitioned_by)
    if (
        partitioned_by
        and partition_names <= columns_by_name.keys()
        and len(partition_names) == len(columns)
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
    scope: TableScope,
) -> None:
    """
    Reject column names the declared properties make invalid on Delta.

    Two naming rules depend on properties: characters such as spaces are only
    permitted under column mapping, and change data feed reserves its output
    column names. Both bind only when the declaration manages column
    structure — a restricted scope mirrors columns the live table already
    accepted, so it must be able to declare names this engine would reject
    to create.
    """
    if not scope.manages(TableAspect.COLUMN_STRUCTURE):
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
    primary_key: DesiredPrimaryKey | None
    column_types: dict[str, DataType]


@dataclass(frozen=True, slots=True)
class ForeignKey:
    """
    Public declaration of a foreign key relationship.

    ``columns`` accepts one local column name for a single-column parent
    key, a list or tuple of local names for a same-name key, or an explicit
    ``{local: referenced}`` mapping. List, tuple, and mapping orders are irrelevant;
    ambiguous composite keys require the explicit form. Identifier spelling is
    preserved; identifiers differing only in case name the same column.

    ``name`` optionally chooses the physical constraint name. When omitted,
    the owning :class:`DeltaTable` generates
    ``{table}_{local_columns}_fk`` once during construction.

    ``references`` is another :class:`DeltaTable`, or the :data:`Self` sentinel
    for a self-referential key. See the architecture explanation doc for why
    the reference is an object rather than a name. The referenced table must
    live in the same catalog as the declaring table — information_schema is
    per-catalog, so a cross-catalog constraint could be created but never
    observed afterwards.
    """

    columns: str | ListOrTuple[str] | Mapping[str, str]
    references: DeltaTable | _SelfReference
    name: str | None = None

    def __post_init__(self) -> None:
        # Freeze user input once; resolve it when the owning DeltaTable is constructed.
        frozen: tuple[str, ...] | Mapping[str, str]
        match self.columns:
            case str():
                Identifier(self.columns)
                frozen = (self.columns,)
            case Mapping():
                mapping = dict(self.columns)
                if not mapping:
                    raise ValueError("foreign key columns must not be empty")
                if not all(
                    isinstance(local, str) and isinstance(referenced, str)
                    for local, referenced in mapping.items()
                ):
                    raise TypeError("foreign key mapping keys and values must be strings")
                for local, referenced in mapping.items():
                    Identifier(local)
                    Identifier(referenced)
                frozen = MappingProxyType(mapping)
            case list() | tuple():
                sequence = tuple(self.columns)
                if not sequence:
                    raise ValueError("foreign key columns must not be empty")
                if not all(isinstance(column, str) for column in sequence):
                    raise TypeError("foreign key columns must be strings")
                for column in sequence:
                    Identifier(column)
                frozen = sequence
            case _:
                raise TypeError(
                    "foreign key columns must be a column name,"
                    " a list or tuple of same-name columns, or"
                    " a {local: referenced} mapping"
                )

        if self.name is not None:
            Identifier(self.name)

        if not isinstance(self.references, (DeltaTable, _SelfReference)):
            raise TypeError(
                f"foreign key references must be a DeltaTable or Self; got {self.references!r}"
            )

        object.__setattr__(self, "columns", frozen)

    def _to_constraint(
        self,
        owner_name: QualifiedName,
        owner_columns: tuple[Column, ...],
        owner_primary_key: DesiredPrimaryKey | None,
    ) -> DesiredForeignKey:
        """
        Lower this declaration into a domain constraint.

        Applies the lowering rules in order: the referenced table must live in
        the owner's catalog, must declare a primary key, the resolved parent
        columns must equal that key exactly, and each local column's data type must
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

        primary_key = referenced.primary_key
        if primary_key is None:
            raise ValueError(
                f"foreign key references {referenced.table}, which declares no primary key"
            )

        pairs = tuple(
            (Identifier(local), Identifier(parent))
            for local, parent in self._resolve_column_pairs(referenced.table, primary_key)
        )
        local_names = {column.name: column.name for column in owner_columns}
        referenced_names = {name: name for name in referenced.column_types}
        local_columns = tuple(local_names.get(local, local) for local, _ in pairs)
        referenced_columns = tuple(referenced_names.get(parent, parent) for _, parent in pairs)

        if not primary_key.matches_columns(referenced_columns):
            declared = set(referenced_columns)
            key = set(primary_key.columns)
            missing = sorted(key - declared)
            extra = sorted(declared - key)
            details = []
            if missing:
                details.append(f"missing from the mapping: {', '.join(missing)}")
            if extra:
                details.append(f"not in the key: {', '.join(extra)}")
            raise ValueError(
                f"foreign key columns must reference {referenced.table}'s"
                f" primary key ({', '.join(primary_key.columns)}) exactly;"
                f" {'; '.join(details)}"
            )

        local_types = {column.name: column.data_type for column in owner_columns}
        for local_name, referenced_name in pairs:
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

        return DesiredForeignKey(
            local_columns=local_columns,
            referenced_table=referenced.table,
            referenced_columns=referenced_columns,
            desired_name=(
                self.name
                if self.name is not None
                else _foreign_key_constraint_name(
                    owner_table_name=owner_name.name,
                    local_columns=local_columns,
                )
            ),
        )

    def _resolve_reference(
        self,
        owner_name: QualifiedName,
        owner_columns: tuple[Column, ...],
        owner_primary_key: DesiredPrimaryKey | None,
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
        if isinstance(self.references, _SelfReference):
            types = {column.name: column.data_type for column in owner_columns}
            return _ReferencedSide(owner_name, owner_primary_key, types)

        desired = self.references.to_desired_table()
        types = {column.name: column.data_type for column in desired.columns}
        return _ReferencedSide(desired.qualified_name, desired.primary_key, types)

    def _resolve_column_pairs(
        self,
        referenced_table: QualifiedName,
        primary_key: DesiredPrimaryKey,
    ) -> tuple[tuple[str, str], ...]:
        """Resolve the declaration into explicit local-to-parent column pairs."""
        if isinstance(self.columns, Mapping):
            return tuple(self.columns.items())

        local_columns = tuple(self.columns)
        parent_columns = tuple(primary_key.columns)

        # a single-column parent has only one possible pairing
        if len(local_columns) == 1 and len(parent_columns) == 1:
            return ((local_columns[0], parent_columns[0]),)

        if primary_key.matches_columns(local_columns):
            return tuple((column, column) for column in local_columns)

        raise ValueError(
            f"cannot infer foreign key column pairing for {referenced_table};"
            f" local columns are ({', '.join(local_columns)}) and the referenced"
            f" primary key is ({', '.join(parent_columns)}). Provide an explicit"
            " mapping of {local column: referenced column}."
        )


@dataclass(frozen=True, slots=True)
class _NormalizedDeclaration:
    """One frozen representation of public ``DeltaTable`` input."""

    qualified_name: QualifiedName
    columns: tuple[Column, ...]
    comment: str
    properties: Mapping[str, str | None]
    tags: Mapping[str, str]
    partitioned_by: tuple[Identifier, ...]
    clustered_by: tuple[Identifier, ...]
    primary_key: tuple[Identifier, ...] | None
    primary_key_name: Identifier | None
    foreign_key_declarations: tuple[ForeignKey, ...]
    scope: TableScope


def _normalize_declaration(
    *,
    catalog: str,
    schema: str,
    name: str,
    columns: Iterable[Column],
    comment: str,
    properties: Mapping[str, str | None] | None,
    tags: Mapping[str, str] | None,
    partitioned_by: ListOrTuple[str],
    clustered_by: ListOrTuple[str],
    primary_key: ListOrTuple[str] | None,
    primary_key_name: str | None,
    foreign_keys: Iterable[ForeignKey] | None,
    scope: ScopeName,
) -> _NormalizedDeclaration:
    """Freeze public inputs before judging them."""
    # Annotations are not enforced at runtime, so fail clearly rather than
    # treating a bare string as an iterable of one-character column names.
    string_collections: tuple[tuple[str, object], ...] = (
        ("partitioned_by", partitioned_by),
        ("clustered_by", clustered_by),
        ("primary_key", primary_key),
    )
    for field_name, value in string_collections:
        if isinstance(value, str):
            raise TypeError(
                f"{field_name} must be a list or tuple of column names, not a string;"
                f" write {field_name}=[{value!r}] for one column"
            )

    if primary_key_name is not None:
        if not isinstance(primary_key_name, str):
            raise TypeError("primary_key_name must be a string or None")
        if primary_key is None:
            raise ValueError("primary_key_name requires primary_key")
        if not primary_key_name.strip():
            raise ValueError("primary_key_name must not be blank")

    return _NormalizedDeclaration(
        qualified_name=QualifiedName(catalog, schema, name),
        columns=tuple(columns),
        comment=comment,
        properties=MappingProxyType(dict(properties or {})),
        tags=MappingProxyType(dict(tags or {})),
        partitioned_by=tuple(Identifier(name) for name in partitioned_by),
        clustered_by=tuple(Identifier(name) for name in clustered_by),
        primary_key=(
            tuple(Identifier(name) for name in primary_key) if primary_key is not None else None
        ),
        primary_key_name=(Identifier(primary_key_name) if primary_key_name is not None else None),
        foreign_key_declarations=tuple(foreign_keys or ()),
        scope=table_scope_for(scope),
    )


def _validate_declaration(declaration: _NormalizedDeclaration) -> None:
    """Reject invalid frozen declarations before lowering."""
    DELTA_PROPERTY_POLICY.validate_declaration(declaration.properties)
    _validate_layout(
        declaration.columns,
        declaration.partitioned_by,
        declaration.clustered_by,
    )
    _validate_column_names(
        declaration.columns,
        declaration.properties,
        declaration.scope,
    )
    _validate_renames(declaration.columns, declaration.properties)

    _validate_tags(f"table '{declaration.qualified_name.name}'", declaration.tags)
    for column in declaration.columns:
        _validate_tags(f"column '{column.name}'", column.tags)
        _validate_nested_not_null(column)

    _validate_object_name_parts(declaration.qualified_name)


def _lower_declaration(declaration: _NormalizedDeclaration) -> DesiredTable:
    """Lower a valid public declaration into the domain model."""
    column_names = {column.name: column.name for column in declaration.columns}
    primary_key_columns = (
        [column_names.get(name, name) for name in declaration.primary_key]
        if declaration.primary_key is not None
        else None
    )
    primary_key_constraint = (
        DesiredPrimaryKey(
            columns=primary_key_columns,
            desired_name=(
                declaration.primary_key_name
                if declaration.primary_key_name is not None
                else Identifier(f"{declaration.qualified_name.name}_pk")
            ),
        )
        if primary_key_columns is not None
        else None
    )
    foreign_keys = [
        foreign_key._to_constraint(
            declaration.qualified_name,
            declaration.columns,
            primary_key_constraint,
        )
        for foreign_key in declaration.foreign_key_declarations
    ]

    # DesiredTable enforces domain invariants (non-empty and unique columns,
    # existing layout/key columns, and coherent constraints) at construction.
    return DesiredTable(
        qualified_name=declaration.qualified_name,
        columns=declaration.columns,
        comment=declaration.comment,
        properties=declaration.properties,
        tags=declaration.tags,
        partitioned_by=[column_names.get(name, name) for name in declaration.partitioned_by],
        clustered_by=[column_names.get(name, name) for name in declaration.clustered_by],
        primary_key=primary_key_constraint,
        foreign_keys=foreign_keys,
        scope=declaration.scope,
    )


class DeltaTable:
    """
    Defines a Delta table schema.

    ``scope`` selects how much of the table the declaration manages: the whole
    table (default), catalog metadata, comments and tags, or tags alone.

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
        partitioned_by: ListOrTuple[str] = (),
        clustered_by: ListOrTuple[str] = (),
        primary_key: ListOrTuple[str] | None = None,
        foreign_keys: Iterable[ForeignKey] | None = None,
        scope: ScopeName = "full",
        primary_key_name: str | None = None,
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
            primary_key_name: Optional physical name to manage for the primary
                key. When omitted, ``{table}_pk`` is generated and managed.
            foreign_keys: Foreign key relationships declared on this table.
                Each may choose a physical constraint ``name``; omitted names
                use ``{table}_{local_columns}_fk``.
            scope: What this declaration manages. ``"full"`` (the default)
                manages the whole table. ``"metadata"`` restricts the sync to
                catalog metadata: comments, tags, and primary/foreign key
                constraints. ``"annotations"`` restricts it further to the
                table comment, column comments, table tags, and column tags —
                for a table whose structure and keys belong to someone else.
                ``"tags"`` restricts it to table and column tags alone. The
                scopes nest: tags ⊂ annotations ⊂ metadata ⊂ full.
                Streaming tables are supported under ``"annotations"`` and
                ``"tags"``, and no wider scope: their definition — schema,
                properties, and keys — belongs to the owning pipeline, so a
                declaration managing more than comments and tags fails
                validation. A restricted scope still declares the full table
                shape; aspects outside the scope are never changed, and drift
                on them fails validation. A key the pipeline declared must
                therefore be mirrored in ``primary_key`` — it is never
                applied, and mirroring it is what keeps it from reading as
                drift. Properties are the exception: a declaration that does
                not manage properties never compares them at all.

        """
        declaration = _normalize_declaration(
            catalog=catalog,
            schema=schema,
            name=name,
            columns=columns,
            comment=comment,
            properties=properties,
            tags=tags,
            partitioned_by=partitioned_by,
            clustered_by=clustered_by,
            primary_key=primary_key,
            primary_key_name=primary_key_name,
            foreign_keys=foreign_keys,
            scope=scope,
        )
        _validate_declaration(declaration)
        self._desired_table = _lower_declaration(declaration)
        self._foreign_key_declarations = declaration.foreign_key_declarations

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
        return tuple(self._desired_table.columns)

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
        return tuple(self._desired_table.partitioned_by)

    @property
    def clustered_by(self) -> tuple[str, ...]:
        """Clustering key column names, in declaration order."""
        return tuple(self._desired_table.clustered_by)

    @property
    def primary_key(self) -> tuple[str, ...]:
        """Column names declared as the primary key, in declaration order."""
        return self._desired_table.primary_key_columns

    @property
    def primary_key_name(self) -> str | None:
        """Generated or explicitly declared primary-key name, if a key exists."""
        primary_key = self._desired_table.primary_key
        return str(primary_key.desired_name) if primary_key is not None else None

    @property
    def foreign_keys(self) -> tuple[ForeignKey, ...]:
        """Foreign key declarations, before lowering to domain constraints."""
        return self._foreign_key_declarations

    def to_desired_table(self) -> DesiredTable:
        """Return the domain :class:`DesiredTable` for this table definition."""
        return self._desired_table
