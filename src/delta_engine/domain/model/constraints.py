"""Domain value objects representing key constraints (primary and foreign)."""

from collections.abc import Iterable
from dataclasses import dataclass

from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model.identifier import Identifier
from delta_engine.domain.model.qualified_name import QualifiedName


def _constraint_columns(columns: object, *, kind: str) -> tuple[Identifier, ...]:
    """Normalize one non-empty, unique list or tuple of constraint columns."""
    if isinstance(columns, str) or not isinstance(columns, (list, tuple)):
        raise TypeError(f"{kind} columns must be a list or tuple of strings")
    if not columns:
        raise ValueError(f"{kind} columns must not be empty")
    if not all(isinstance(column, str) for column in columns):
        raise TypeError(f"{kind} column names must be strings")

    normalized = tuple(Identifier(column) for column in columns)
    seen: set[Identifier] = set()
    for column in normalized:
        if column in seen:
            raise ValueError(f"Duplicate {kind} column: {column}")
        seen.add(column)
    return normalized


class _PrimaryKey:
    """Shared structural identity for desired and observed primary keys."""

    __slots__ = ()

    columns: ListOrTuple[str]

    def matches_columns(self, columns: Iterable[str]) -> bool:
        """Return whether columns identify this key, ignoring order and case."""
        return frozenset(self.columns) == frozenset(Identifier(column) for column in columns)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _PrimaryKey):
            return NotImplemented
        return self.matches_columns(other.columns)

    def __hash__(self) -> int:
        return hash(frozenset(self.columns))

    def _normalize_columns(self) -> None:
        object.__setattr__(self, "columns", _constraint_columns(self.columns, kind="primary key"))


@dataclass(frozen=True, slots=True, eq=False)
class PrimaryKeyConstraint(_PrimaryKey):
    """
    A primary key constraint declaration.

    Symmetric with :class:`ForeignKeyConstraint`: a table-level key constraint
    over an ordered set of columns.

    Attributes:
        columns: Ordered tuple of column names, preserving their supplied
            spelling. Identity and duplicates are judged by identifier key.
        name: Optional physical name to request when creating the constraint.
            It is not part of structural identity; once created, Databricks
            owns the catalog name.

    """

    columns: ListOrTuple[str]
    name: str | None = None

    def __post_init__(self) -> None:
        self._normalize_columns()
        object.__setattr__(
            self,
            "name",
            Identifier(self.name) if self.name is not None else None,
        )


@dataclass(frozen=True, slots=True, eq=False)
class ObservedPrimaryKeyConstraint(_PrimaryKey):
    """A catalog-observed primary key with a concrete physical name."""

    columns: ListOrTuple[str]
    name: str

    def __post_init__(self) -> None:
        self._normalize_columns()
        object.__setattr__(self, "name", Identifier(self.name))


class _ForeignKey:
    """Shared structural identity for desired and observed foreign keys."""

    __slots__ = ()

    local_columns: ListOrTuple[str]
    referenced_table: QualifiedName
    referenced_columns: ListOrTuple[str]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _ForeignKey):
            return NotImplemented
        return (
            self.local_columns == other.local_columns
            and self.referenced_table == other.referenced_table
            and self.referenced_columns == other.referenced_columns
        )

    def __hash__(self) -> int:
        return hash((self.local_columns, self.referenced_table, self.referenced_columns))

    def _normalize_columns(self) -> None:
        local_columns = _constraint_columns(self.local_columns, kind="foreign key local")
        referenced_columns = _constraint_columns(
            self.referenced_columns,
            kind="foreign key referenced",
        )

        if len(local_columns) != len(referenced_columns):
            raise ValueError(
                "local_columns and referenced_columns must have the same number of entries;"
                f" got {len(local_columns)} local and"
                f" {len(referenced_columns)} referenced"
            )

        # Column order is not part of a foreign key's meaning. Store one
        # canonical pair order while preserving each identifier's spelling.
        pairs = sorted(
            zip(local_columns, referenced_columns, strict=True),
            key=lambda pair: pair[0].lower(),
        )
        object.__setattr__(self, "local_columns", tuple(pair[0] for pair in pairs))
        object.__setattr__(self, "referenced_columns", tuple(pair[1] for pair in pairs))


@dataclass(frozen=True, slots=True, eq=False)
class ForeignKeyConstraint(_ForeignKey):
    """
    A foreign key constraint declaration.

    Attributes:
        local_columns: Tuple of local column names in the constraint,
            preserving spelling and stored sorted by identifier key (pairing
            with ``referenced_columns`` preserved). Column order is not part of
            a foreign key's meaning, mirroring the primary key's set identity,
            so identity and rendered DDL are independent of declaration order.
        referenced_table: Fully qualified name of the referenced table.
        referenced_columns: Tuple of column names in the referenced table,
            positionally aligned with ``local_columns`` after identity-key sorting.
        name: Optional physical name to request when creating the constraint.
            It is not part of structural identity; once created, Databricks
            owns the catalog name.

    """

    local_columns: ListOrTuple[str]
    referenced_table: QualifiedName
    referenced_columns: ListOrTuple[str]
    name: str | None = None

    def __post_init__(self) -> None:
        self._normalize_columns()
        object.__setattr__(
            self,
            "name",
            Identifier(self.name) if self.name is not None else None,
        )


@dataclass(frozen=True, slots=True, eq=False)
class ObservedForeignKeyConstraint(_ForeignKey):
    """A catalog-observed foreign key with a concrete physical name."""

    local_columns: ListOrTuple[str]
    referenced_table: QualifiedName
    referenced_columns: ListOrTuple[str]
    name: str

    def __post_init__(self) -> None:
        self._normalize_columns()
        object.__setattr__(self, "name", Identifier(self.name))


@dataclass(frozen=True, slots=True)
class ForeignKeyReference:
    """
    A foreign key on some table that references *this* table's key.

    The inbound counterpart of :class:`ForeignKeyConstraint`: it identifies
    the referencing constraint and its owner without carrying column detail —
    enough for validation to name what blocks a primary-key change.

    Attributes:
        name: The referencing constraint's name, as read from the
            catalog.
        referencing_table: Fully qualified name of the table that owns the
            referencing constraint.

    """

    name: str
    referencing_table: QualifiedName

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", Identifier(self.name))
