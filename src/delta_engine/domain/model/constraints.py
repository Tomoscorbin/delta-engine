"""Desired constraint declarations and catalog-observed occurrences."""

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


def _catalog_name(name: str) -> str:
    """Validate a catalog handle while retaining exact string equality."""
    Identifier(name)
    return str(name)


class _PrimaryKeyDefinition:
    """Implement structural identity shared by primary-key lifecycle values."""

    __slots__ = ()

    columns: ListOrTuple[str]

    @property
    def definition_key(self) -> frozenset[Identifier]:
        """The name-independent relational identity of this key."""
        return frozenset(Identifier(column) for column in self.columns)

    def matches_columns(self, columns: Iterable[str]) -> bool:
        """Return whether columns identify this key, ignoring order and case."""
        return self.definition_key == frozenset(Identifier(column) for column in columns)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _PrimaryKeyDefinition):
            return NotImplemented
        return self.definition_key == other.definition_key

    def __hash__(self) -> int:
        return hash(("primary_key", self.definition_key))

    def _normalize_columns(self) -> None:
        object.__setattr__(self, "columns", _constraint_columns(self.columns, kind="primary key"))


@dataclass(frozen=True, slots=True, eq=False)
class DesiredPrimaryKey(_PrimaryKeyDefinition):
    """A declared primary-key definition and the name requested on creation."""

    columns: ListOrTuple[str]
    requested_name: str

    def __post_init__(self) -> None:
        self._normalize_columns()
        object.__setattr__(self, "requested_name", Identifier(self.requested_name))


@dataclass(frozen=True, slots=True, eq=False)
class ObservedPrimaryKey(_PrimaryKeyDefinition):
    """A primary-key occurrence read from the catalog."""

    columns: ListOrTuple[str]
    catalog_name: str

    def __post_init__(self) -> None:
        self._normalize_columns()
        object.__setattr__(self, "catalog_name", _catalog_name(self.catalog_name))


class _ForeignKeyDefinition:
    """Implement structural identity shared by foreign-key lifecycle values."""

    __slots__ = ()

    local_columns: ListOrTuple[str]
    referenced_table: QualifiedName
    referenced_columns: ListOrTuple[str]

    @property
    def definition_key(
        self,
    ) -> tuple[QualifiedName, frozenset[tuple[Identifier, Identifier]]]:
        """The name-independent relational identity of this key."""
        return (
            self.referenced_table,
            frozenset(
                (Identifier(local), Identifier(referenced))
                for local, referenced in zip(
                    self.local_columns,
                    self.referenced_columns,
                    strict=True,
                )
            ),
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _ForeignKeyDefinition):
            return NotImplemented
        return self.definition_key == other.definition_key

    def __hash__(self) -> int:
        return hash(("foreign_key", self.definition_key))

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

        # Canonical storage makes rendering deterministic while preserving each
        # local-to-referenced pairing and its supplied spelling.
        pairs = sorted(
            zip(local_columns, referenced_columns, strict=True),
            key=lambda pair: pair[0].lower(),
        )
        object.__setattr__(self, "local_columns", tuple(pair[0] for pair in pairs))
        object.__setattr__(self, "referenced_columns", tuple(pair[1] for pair in pairs))


@dataclass(frozen=True, slots=True, eq=False)
class DesiredForeignKey(_ForeignKeyDefinition):
    """A declared foreign-key definition and the name requested on creation."""

    local_columns: ListOrTuple[str]
    referenced_table: QualifiedName
    referenced_columns: ListOrTuple[str]
    requested_name: str

    def __post_init__(self) -> None:
        self._normalize_columns()
        object.__setattr__(self, "requested_name", Identifier(self.requested_name))


@dataclass(frozen=True, slots=True, eq=False)
class ObservedForeignKey(_ForeignKeyDefinition):
    """A foreign-key occurrence read from the catalog."""

    local_columns: ListOrTuple[str]
    referenced_table: QualifiedName
    referenced_columns: ListOrTuple[str]
    catalog_name: str

    def __post_init__(self) -> None:
        self._normalize_columns()
        object.__setattr__(self, "catalog_name", _catalog_name(self.catalog_name))


@dataclass(frozen=True, slots=True)
class ObservedReferencingForeignKey:
    """A catalog projection identifying an inbound foreign-key occurrence."""

    catalog_name: str
    referencing_table: QualifiedName

    def __post_init__(self) -> None:
        object.__setattr__(self, "catalog_name", _catalog_name(self.catalog_name))
