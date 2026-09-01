"""
Schema declaration implementation for Delta tables and foreign keys.

Each rule here is judged against the smallest context that can decide it,
and the file is laid out in those sections:

- Value rules judge one value in isolation: can it exist on the platform
  at all? (Object-name length and characters, tag quotas.)
- Declaration rules judge one whole declaration: can it deploy as
  declared? (Layout, property-dependent naming, nested NOT NULL.)
  ``_validate_declaration`` owns which of them bind for a scope.
- Reference rules judge a declaration against the tables it references:
  a foreign key against its parent's declaration, at lowering time.

Normalization runs before any judgment: input is frozen and every column
reference is resolved to its declared spelling (``_NormalizedDeclaration``).
Structural coherence — columns exist, names are unique, at least one column
— is the domain ``DesiredTable``'s job, enforced the moment a declaration
is lowered; a name that resolves to no column passes through for the domain
to reject.

Most rules restate what Delta or Unity Catalog would enforce at creation.
A few are this engine's own policy (the cross-catalog foreign key rule, the
scope gate on naming rules) rather than platform limits.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field as dataclass_field
from types import MappingProxyType
from typing import Final, NamedTuple, cast

from delta_engine.application.properties import DELTA_PROPERTY_POLICY, Property
from delta_engine.application.scopes import ScopeName, table_scope_for
from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model import (
    Array,
    Binary,
    Boolean,
    DataType,
    DesiredColumn as Column,
    DesiredTable,
    ForeignKeyConstraint,
    Identifier,
    Map,
    PrimaryKeyConstraint,
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

_NAME_REFERENCE_NEEDS_MAPPING: Final[str] = (
    "foreign key with a name reference requires an explicit"
    " {local: referenced} column mapping; the string and sequence"
    " shorthand forms resolve against the referenced table's primary key,"
    " which a name does not carry"
)


# ---------- Value rules ----------


def _validate_object_name_parts(qualified_name: QualifiedName, subject: str = "Table") -> None:
    """Reject catalog, schema, or table name parts Unity Catalog cannot store."""
    for label, part in zip(("catalog", "schema", "name"), qualified_name.parts, strict=True):
        if len(part) > _OBJECT_NAME_MAX_LENGTH:
            raise ValueError(
                f"{subject} {label} is {len(part)} characters long; Unity Catalog"
                f" limits object names to {_OBJECT_NAME_MAX_LENGTH} characters"
            )
        forbidden = sorted(set(part) & _OBJECT_NAME_FORBIDDEN_CHARACTERS)
        if forbidden:
            raise ValueError(
                f"{subject} {label} {part!r} contains characters Unity Catalog"
                f" forbids in object names: {forbidden}. Periods, spaces,"
                " forward slashes, control characters, and DEL are not allowed."
            )


def _validate_tags(subject: str, tags: Mapping[str, str]) -> None:
    """Reject tag counts, keys, or values Unity Catalog cannot store."""
    if len(tags) > _MAX_TAGS_PER_SECURABLE:
        raise ValueError(
            f"{subject} declares {len(tags)} tags; Unity Catalog allows at"
            f" most {_MAX_TAGS_PER_SECURABLE} per securable"
        )
    for key, value in tags.items():
        if len(key) > _MAX_TAG_KEY_LENGTH:
            raise ValueError(
                f"Tag {key!r} on {subject} has a {len(key)}-character"
                f" key; Unity Catalog allows at most {_MAX_TAG_KEY_LENGTH}"
            )
        if len(value) > _MAX_TAG_VALUE_LENGTH:
            raise ValueError(
                f"Tag {key!r} on {subject} has a {len(value)}-character"
                f" value; Unity Catalog allows at most {_MAX_TAG_VALUE_LENGTH}"
            )


# ---------- Normalization ----------


def _declared_spelling(columns_by_name: Mapping[Identifier, Column], name: str) -> Identifier:
    """
    Return ``name`` in its declared spelling.

    A name that resolves to no declared column is returned as written, for
    the domain to reject.
    """
    column = columns_by_name.get(Identifier(name))
    return Identifier(column.name) if column is not None else Identifier(name)


@dataclass(frozen=True, slots=True)
class _NormalizedDeclaration:
    """
    One frozen representation of public ``DeltaTable`` input.

    Every column reference is already canonicalized to its declared spelling
    through ``columns_by_name``. A name that matches no declared column
    passes through as written: existence is the domain ``DesiredTable``'s
    invariant, and its error shows the user's spelling. Rules and lowering
    look columns up here and never re-derive the mapping.

    ``columns`` is the declaration itself: it carries even duplicate names
    through for the domain to reject. ``columns_by_name`` is the derived
    lookup index, which collapses them, so it can never replace the tuple.
    """

    qualified_name: QualifiedName
    columns: tuple[Column, ...]
    columns_by_name: Mapping[Identifier, Column]
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

    declared_columns = tuple(columns)
    columns_by_name: dict[Identifier, Column] = {
        Identifier(column.name): column for column in declared_columns
    }
    return _NormalizedDeclaration(
        qualified_name=QualifiedName(catalog, schema, name),
        columns=declared_columns,
        columns_by_name=MappingProxyType(columns_by_name),
        comment=comment,
        properties=MappingProxyType(dict(properties or {})),
        tags=MappingProxyType(dict(tags or {})),
        partitioned_by=tuple(
            _declared_spelling(columns_by_name, column_name) for column_name in partitioned_by
        ),
        clustered_by=tuple(
            _declared_spelling(columns_by_name, column_name) for column_name in clustered_by
        ),
        primary_key=(
            tuple(_declared_spelling(columns_by_name, column_name) for column_name in primary_key)
            if primary_key is not None
            else None
        ),
        primary_key_name=(Identifier(primary_key_name) if primary_key_name is not None else None),
        foreign_key_declarations=tuple(foreign_keys or ()),
        scope=table_scope_for(scope),
    )


# ---------- Declaration rules ----------


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


def _validate_layout(declaration: _NormalizedDeclaration) -> None:
    """
    Reject physical layouts Delta cannot deploy.

    Checks declaration-level deployability only: the partition/cluster
    relationship, the clustering-key budget, and key data types. Names that
    resolve to no column are the domain's to reject (see
    ``_NormalizedDeclaration``).
    """
    partitioned_by = declaration.partitioned_by
    clustered_by = declaration.clustered_by
    if partitioned_by and clustered_by:
        raise ValueError(
            "A table cannot both partition and cluster: declare partitioned_by"
            " or clustered_by, not both."
        )
    if len(clustered_by) > 4:
        raise ValueError(
            f"A table may declare at most four clustering keys; got {len(clustered_by)}."
        )

    for name in partitioned_by:
        column = declaration.columns_by_name.get(name)
        if column is not None and isinstance(column.data_type, _TYPES_UNUSABLE_AS_PARTITION_KEYS):
            raise ValueError(
                f"Partition column {name!r} has type"
                f" {type(column.data_type).__name__}, which Delta cannot partition by"
            )
    for name in clustered_by:
        column = declaration.columns_by_name.get(name)
        if column is not None and isinstance(column.data_type, _TYPES_UNUSABLE_AS_CLUSTERING_KEYS):
            raise ValueError(
                f"Clustering column {name!r} has type"
                f" {type(column.data_type).__name__}, which cannot be a clustering key"
            )

    partition_names = set(partitioned_by)
    if (
        partitioned_by
        and partition_names <= declaration.columns_by_name.keys()
        and len(partition_names) == len(declaration.columns)
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
) -> None:
    """
    Reject column names the declared properties make invalid on Delta.

    Two naming rules depend on properties: characters such as spaces are only
    permitted under column mapping, and change data feed reserves its output
    column names.
    """
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


def _validate_declaration(declaration: _NormalizedDeclaration) -> None:
    """
    Reject invalid frozen declarations before lowering.

    Rules judge; this orchestrator decides which of them bind for the
    declaration's scope.
    """
    DELTA_PROPERTY_POLICY.validate_declaration(declaration.properties)
    _validate_layout(declaration)
    # The column rules bind only when the declaration manages column
    # structure: a restricted scope mirrors columns the live table already
    # accepted, so it must be able to declare names this engine would
    # refuse to create, and its rename hints are the domain's scope rule
    # to reject with the accurate error.
    if declaration.scope.manages(TableAspect.COLUMN_STRUCTURE):
        _validate_column_names(declaration.columns, declaration.properties)
        _validate_renames(declaration.columns, declaration.properties)

    _validate_tags(f"table '{declaration.qualified_name.name}'", declaration.tags)
    for column in declaration.columns:
        _validate_tags(f"column '{column.name}'", column.tags)
        _validate_nested_not_null(column)

    _validate_object_name_parts(declaration.qualified_name)


# ---------- Reference rules ----------


class _SelfReference:
    """Sentinel marking a foreign key that references its own table."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "Self"


Self: Final = _SelfReference()


def _parse_name_reference(raw: str) -> QualifiedName:
    """
    Parse a ``catalog.schema.table`` string into the referenced table's name.

    Every part must be a name Unity Catalog can store.
    """
    name = QualifiedName.parse(raw)
    _validate_object_name_parts(name, subject=f"foreign key reference {raw!r}")
    return name


def _validate_same_catalog(owner_name: QualifiedName, referenced_table: QualifiedName) -> None:
    """Reject a foreign key whose referenced table lives in another catalog."""
    if referenced_table.catalog != owner_name.catalog:
        raise ValueError(
            f"cross-catalog foreign keys are not currently supported:"
            f" {owner_name} cannot reference {referenced_table}"
        )


class _ParentDeclaration(NamedTuple):
    """
    A referenced table's declaration, resolved to what judgment needs.

    Present only when the parent's declaration is in scope (a ``DeltaTable``
    reference or ``Self``); a name reference supplies no declaration. One is
    only constructed once the parent is known to declare a primary key, so
    holding one means there is a key to judge against.
    """

    primary_key: PrimaryKeyConstraint
    columns_by_name: Mapping[Identifier, Column]


class _NameReference(NamedTuple):
    """
    A referenced table resolved from a name reference at declaration time.

    Carries the parsed table name and the explicit ``{local: referenced}``
    column mapping the name form requires. A name supplies no declaration to
    judge, so this is everything lowering needs; holding the mapping here
    makes a name reference without one unrepresentable after construction.
    """

    table: QualifiedName
    columns: Mapping[Identifier, Identifier]


def _validate_reference_coherence(
    owner: _NormalizedDeclaration,
    referenced_table: QualifiedName,
    declared: _ParentDeclaration,
    pairs: tuple[tuple[Identifier, Identifier], ...],
) -> None:
    """
    Judge column pairs, already in declared spelling, against the parent.

    The referenced columns must be exactly the parent's primary key, and
    each local column's data type must equal its referenced column's.
    Columns that resolve to no declaration — local or referenced — are the
    domain's to reject.
    """
    referenced_columns = tuple(referenced for _, referenced in pairs)
    if not declared.primary_key.matches_columns(referenced_columns):
        mapped = set(referenced_columns)
        key = set(declared.primary_key.columns)
        missing = sorted(key - mapped)
        extra = sorted(mapped - key)
        details = []
        if missing:
            details.append(f"missing from the mapping: {', '.join(missing)}")
        if extra:
            details.append(f"not in the key: {', '.join(extra)}")
        raise ValueError(
            f"foreign key columns must reference {referenced_table}'s"
            f" primary key ({', '.join(declared.primary_key.columns)}) exactly;"
            f" {'; '.join(details)}"
        )

    for local_name, referenced_name in pairs:
        local_column = owner.columns_by_name.get(local_name)
        if local_column is None:
            continue  # local column existence is enforced when the DesiredTable is built
        referenced_column = declared.columns_by_name.get(referenced_name)
        if referenced_column is None:
            # Only a Self parent can name a missing key column; the
            # DesiredTable built right after rejects its primary key.
            continue
        if local_column.data_type != referenced_column.data_type:
            raise ValueError(
                f"foreign key column type mismatch: {owner.qualified_name}.{local_name}"
                f" is {local_column.data_type} but {referenced_table}.{referenced_name}"
                f" is {referenced_column.data_type}"
            )


# The domain re-enforces these column rules when the constraint is built
# (constraints._constraint_columns). The two lowering helpers below check them
# anyway — deliberately — so the error points at the ForeignKey(...)
# construction site, not at whichever DeltaTable later includes the declaration.
def _lower_column_mapping(columns: Mapping[str, str]) -> Mapping[Identifier, Identifier]:
    """Lower an explicit ``{local: referenced}`` mapping, rejecting duplicate locals."""
    if not columns:
        raise ValueError("foreign key columns must not be empty")
    if not all(
        isinstance(local, str) and isinstance(referenced, str)
        for local, referenced in columns.items()
    ):
        raise TypeError("foreign key mapping keys and values must be strings")
    lowered: dict[Identifier, Identifier] = {}
    for local, referenced in columns.items():
        local_identifier = Identifier(local)
        if local_identifier in lowered:
            raise ValueError(f"Duplicate foreign key local column: {local}")
        lowered[local_identifier] = Identifier(referenced)
    return MappingProxyType(lowered)


def _lower_column_sequence(columns: ListOrTuple[str]) -> tuple[Identifier, ...]:
    """Lower a same-name column list, rejecting duplicates by identifier."""
    if not columns:
        raise ValueError("foreign key columns must not be empty")
    if not all(isinstance(column, str) for column in columns):
        raise TypeError("foreign key columns must be strings")
    lowered = tuple(Identifier(column) for column in columns)
    seen: set[Identifier] = set()
    for column in lowered:
        if column in seen:
            raise ValueError(f"Duplicate foreign key local column: {column}")
        seen.add(column)
    return lowered


def _resolve_reference(
    references: DeltaTable | _SelfReference | str,
    lowered_columns: tuple[Identifier, ...] | Mapping[Identifier, Identifier],
) -> _NameReference | DeltaTable | _SelfReference:
    """Resolve the ``references`` argument into its internal form at construction."""
    match references:
        case str() as raw:
            if not isinstance(lowered_columns, Mapping):
                raise ValueError(_NAME_REFERENCE_NEEDS_MAPPING)
            return _NameReference(_parse_name_reference(raw), lowered_columns)
        case DeltaTable() | _SelfReference():
            return references
        case _:
            raise TypeError(
                "foreign key references must be a DeltaTable, Self, or a"
                f" 'catalog.schema.table' name; got {references!r}"
            )


@dataclass(frozen=True, slots=True)
class ForeignKey:
    """
    Public declaration of a foreign key relationship.

    ``columns`` accepts one local column name for a single-column parent
    key, a list or tuple of local names for a same-name key, or an explicit
    ``{local: referenced}`` mapping. List, tuple, and mapping orders are irrelevant;
    ambiguous composite keys require the explicit form. Identifier spelling is
    preserved; identifiers differing only in case name the same column, and
    declarations differing only in column case or mapping order are equal.

    ``name`` optionally requests the physical name when the constraint is
    created. When omitted, Databricks chooses the name. Existing constraints
    match by definition regardless of their physical name.

    ``references`` is another :class:`DeltaTable`, the :data:`Self` sentinel
    for a self-referential key, or the referenced table's full
    ``"catalog.schema.table"`` name. The referenced table must live in the
    same catalog as the declaring table — information_schema is per-catalog,
    so a cross-catalog constraint could be created but never observed
    afterwards. A name reference carries no primary key to resolve shorthands
    against, so it requires the explicit ``{local: referenced}`` mapping, and
    its primary-key and column-type checks happen when the sync judges the
    registered parent instead of at declaration time. Either way the
    referenced table must be part of the same sync.

    Raises:
        TypeError: ``columns`` or ``references`` is not one of the accepted
            forms.
        ValueError: ``columns`` is empty or repeats a local column, ``name``
            is not a valid identifier, or a name reference is not a valid
            ``catalog.schema.table`` name or lacks its explicit mapping.

    """

    columns: str | ListOrTuple[str] | Mapping[str, str]
    references: DeltaTable | _SelfReference | str
    name: str | None = None
    _reference: _NameReference | DeltaTable | _SelfReference = dataclass_field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        lowered: tuple[Identifier, ...] | Mapping[Identifier, Identifier]
        match self.columns:
            case str() as column_name:
                lowered = (Identifier(column_name),)
            case Mapping() as mapping:
                lowered = _lower_column_mapping(mapping)
            case list() | tuple() as sequence:
                lowered = _lower_column_sequence(sequence)
            case _:
                raise TypeError(
                    "foreign key columns must be a column name,"
                    " a list or tuple of same-name columns, or"
                    " a {local: referenced} mapping"
                )

        if self.name is not None:
            Identifier(self.name)  # rejects an invalid physical name

        object.__setattr__(self, "columns", lowered)
        object.__setattr__(self, "_reference", _resolve_reference(self.references, lowered))

    def __hash__(self) -> int:
        """
        Hash the declaration, consistently with equality, in every column form.

        A mapping declaration hashes by its unordered pairs, so two mappings
        equal in any insertion order hash equal.
        """
        columns = self.columns
        columns_key = frozenset(columns.items()) if isinstance(columns, Mapping) else columns
        return hash((columns_key, self.references, self.name))

    def _to_constraint(
        self,
        owner: _NormalizedDeclaration,
        owner_primary_key: PrimaryKeyConstraint | None,
    ) -> ForeignKeyConstraint:
        """
        Lower this declaration into a domain constraint.

        The match arms resolve the reference into the parent to judge
        against; column pairs are then resolved to their declared spellings,
        and the judgment itself is :func:`_validate_reference_coherence`.
        A name reference supplies no declaration to judge, so its checks are
        the sync's when the registered parent is read. Every form's
        referenced table must live in the owner's catalog. Local column
        existence is never checked here — the ``DesiredTable`` built right
        after enforces it.
        """
        owner_name = owner.qualified_name

        declared_primary_key: PrimaryKeyConstraint | None
        parent_columns: Mapping[Identifier, Column]
        match self._reference:
            case _NameReference(referenced_table, mapping):
                _validate_same_catalog(owner_name, referenced_table)
                return ForeignKeyConstraint(
                    local_columns=tuple(
                        _declared_spelling(owner.columns_by_name, local) for local in mapping
                    ),
                    referenced_table=referenced_table,
                    referenced_columns=tuple(mapping.values()),
                    name=self.name,
                )
            case _SelfReference():
                referenced_table = owner_name
                declared_primary_key = owner_primary_key
                parent_columns = owner.columns_by_name
            case DeltaTable() as parent:
                desired = parent.to_desired_table()
                referenced_table = desired.qualified_name
                declared_primary_key = desired.primary_key
                parent_columns = MappingProxyType(
                    {Identifier(column.name): column for column in desired.columns}
                )

        _validate_same_catalog(owner_name, referenced_table)
        if declared_primary_key is None:
            raise ValueError(
                f"foreign key references {referenced_table}, which declares no primary key"
            )
        declared = _ParentDeclaration(declared_primary_key, parent_columns)

        pairs = tuple(
            (
                _declared_spelling(owner.columns_by_name, local),
                _declared_spelling(declared.columns_by_name, referenced),
            )
            for local, referenced in self._resolve_column_pairs(
                referenced_table, declared.primary_key
            )
        )
        _validate_reference_coherence(owner, referenced_table, declared, pairs)

        return ForeignKeyConstraint(
            local_columns=tuple(local for local, _ in pairs),
            referenced_table=referenced_table,
            referenced_columns=tuple(referenced for _, referenced in pairs),
            name=self.name,
        )

    def _resolve_column_pairs(
        self,
        referenced_table: QualifiedName,
        primary_key: PrimaryKeyConstraint,
    ) -> tuple[tuple[Identifier, Identifier], ...]:
        """Resolve the declaration into explicit local-to-parent column pairs."""
        if isinstance(self.columns, Mapping):
            mapping = cast(Mapping[Identifier, Identifier], self.columns)
            return tuple(mapping.items())

        local_columns = cast(tuple[Identifier, ...], self.columns)
        parent_columns = tuple(Identifier(column) for column in primary_key.columns)

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


# ---------- Lowering ----------


def _lower_declaration(declaration: _NormalizedDeclaration) -> DesiredTable:
    """Lower a valid public declaration into the domain model."""
    primary_key_constraint = (
        PrimaryKeyConstraint(
            columns=declaration.primary_key,
            name=declaration.primary_key_name,
        )
        if declaration.primary_key is not None
        else None
    )
    foreign_keys = [
        foreign_key._to_constraint(declaration, primary_key_constraint)
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
        partitioned_by=declaration.partitioned_by,
        clustered_by=declaration.clustered_by,
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

    A declaration is immutable once constructed: attribute assignment and
    deletion are refused, and there are no fields to reassign. This matters
    when declarations are shared — a package of tables one team imports from
    another cannot be edited in place and then synced. The state is validated
    exactly once, at construction, so a table that exists is a table whose
    declaration was accepted.

    Copying is refused too: ``copy.copy`` and ``copy.deepcopy`` raise. Share
    the single validated instance rather than copying it.
    """

    __slots__ = ("_desired_table", "_foreign_key_declarations")

    _desired_table: DesiredTable
    _foreign_key_declarations: tuple[ForeignKey, ...]

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
            primary_key_name: Optional physical name to request when creating
                the primary key. When omitted, Databricks chooses the name.
            foreign_keys: Foreign key relationships declared on this table.
                Each may choose a physical constraint ``name``; Databricks
                chooses omitted names.
            scope: What this declaration manages. ``"full"`` (the default)
                manages the whole table. ``"metadata"`` restricts the sync to
                catalog metadata: comments, tags, and primary/foreign key
                constraints. ``"annotations"`` restricts it further to table
                and column comments and tags — for a table whose structure
                and keys belong to someone else. ``"tags"`` restricts it to
                table and column tags alone. The scopes nest:
                tags ⊂ annotations ⊂ metadata ⊂ full. Every scope declares
                the full table shape; a restricted scope never changes the
                aspects outside it.

        Raises:
            TypeError: ``partitioned_by``, ``clustered_by``, or
                ``primary_key`` is a bare string rather than a list or tuple
                of column names, or ``primary_key_name`` is not a string.
            ValueError: The declaration cannot deploy as declared: an
                invalid name, column, layout, property, tag, primary key,
                foreign key, or scope.

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
        object.__setattr__(self, "_desired_table", _lower_declaration(declaration))
        object.__setattr__(self, "_foreign_key_declarations", declaration.foreign_key_declarations)

    def __setattr__(self, name: str, _value: object) -> None:
        raise AttributeError(
            f"{type(self).__name__} is immutable; cannot set {name!r}."
            " Build a new declaration instead of editing one in place."
        )

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable; cannot delete {name!r}.")

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
        """Column names of the primary key, in canonical order (sorted, case-insensitive)."""
        return self._desired_table.primary_key_columns

    @property
    def primary_key_name(self) -> str | None:
        """Explicitly declared primary-key name, if one was supplied."""
        primary_key = self._desired_table.primary_key
        if primary_key is None or primary_key.name is None:
            return None
        return str(primary_key.name)

    @property
    def foreign_keys(self) -> tuple[ForeignKey, ...]:
        """Foreign key declarations, before lowering to domain constraints."""
        return self._foreign_key_declarations

    def to_desired_table(self) -> DesiredTable:
        """Return the domain :class:`DesiredTable` for this table definition."""
        return self._desired_table
