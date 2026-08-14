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


@dataclass(frozen=True, slots=True, eq=False)
class DesiredPrimaryKey:
    """A declared primary-key definition and optional name desired on creation."""

    columns: ListOrTuple[str]
    desired_name: str | None = None

    def matches_columns(self, columns: Iterable[str]) -> bool:
        """Return whether columns identify this key, ignoring order and case."""
        return _primary_key_definition(self) == frozenset(Identifier(column) for column in columns)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, (DesiredPrimaryKey, ObservedPrimaryKey)):
            return NotImplemented
        return _primary_key_definition(self) == _primary_key_definition(other)

    def __hash__(self) -> int:
        return hash(("primary_key", _primary_key_definition(self)))

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "columns",
            _constraint_columns(self.columns, kind="primary key"),
        )
        object.__setattr__(
            self,
            "desired_name",
            Identifier(self.desired_name) if self.desired_name is not None else None,
        )


@dataclass(frozen=True, slots=True, eq=False)
class ObservedPrimaryKey:
    """A primary-key occurrence read from the catalog."""

    columns: ListOrTuple[str]
    catalog_name: str

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, (DesiredPrimaryKey, ObservedPrimaryKey)):
            return NotImplemented
        return _primary_key_definition(self) == _primary_key_definition(other)

    def __hash__(self) -> int:
        return hash(("primary_key", _primary_key_definition(self)))

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "columns",
            _constraint_columns(self.columns, kind="primary key"),
        )
        object.__setattr__(self, "catalog_name", _catalog_name(self.catalog_name))


def _primary_key_definition(
    key: DesiredPrimaryKey | ObservedPrimaryKey,
) -> frozenset[Identifier]:
    """Return a primary key's name-independent relational identity."""
    return frozenset(Identifier(column) for column in key.columns)


def _foreign_key_columns(
    local: object,
    referenced: object,
) -> tuple[tuple[Identifier, ...], tuple[Identifier, ...]]:
    """Normalize and canonically pair the two sides of a foreign key."""
    local_columns = _constraint_columns(local, kind="foreign key local")
    referenced_columns = _constraint_columns(referenced, kind="foreign key referenced")

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
    return (
        tuple(pair[0] for pair in pairs),
        tuple(pair[1] for pair in pairs),
    )


@dataclass(frozen=True, slots=True, eq=False)
class DesiredForeignKey:
    """A declared foreign-key definition and optional name desired on creation."""

    local_columns: ListOrTuple[str]
    referenced_table: QualifiedName
    referenced_columns: ListOrTuple[str]
    desired_name: str | None = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, (DesiredForeignKey, ObservedForeignKey)):
            return NotImplemented
        return _foreign_key_definition(self) == _foreign_key_definition(other)

    def __hash__(self) -> int:
        return hash(("foreign_key", _foreign_key_definition(self)))

    def __post_init__(self) -> None:
        local_columns, referenced_columns = _foreign_key_columns(
            self.local_columns,
            self.referenced_columns,
        )
        object.__setattr__(self, "local_columns", local_columns)
        object.__setattr__(self, "referenced_columns", referenced_columns)
        object.__setattr__(
            self,
            "desired_name",
            Identifier(self.desired_name) if self.desired_name is not None else None,
        )


@dataclass(frozen=True, slots=True, eq=False)
class ObservedForeignKey:
    """A foreign-key occurrence read from the catalog."""

    local_columns: ListOrTuple[str]
    referenced_table: QualifiedName
    referenced_columns: ListOrTuple[str]
    catalog_name: str

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, (DesiredForeignKey, ObservedForeignKey)):
            return NotImplemented
        return _foreign_key_definition(self) == _foreign_key_definition(other)

    def __hash__(self) -> int:
        return hash(("foreign_key", _foreign_key_definition(self)))

    def __post_init__(self) -> None:
        local_columns, referenced_columns = _foreign_key_columns(
            self.local_columns,
            self.referenced_columns,
        )
        object.__setattr__(self, "local_columns", local_columns)
        object.__setattr__(self, "referenced_columns", referenced_columns)
        object.__setattr__(self, "catalog_name", _catalog_name(self.catalog_name))


def _foreign_key_definition(
    key: DesiredForeignKey | ObservedForeignKey,
) -> tuple[QualifiedName, frozenset[tuple[Identifier, Identifier]]]:
    """Return a foreign key's name-independent relational identity."""
    return (
        key.referenced_table,
        frozenset(
            (Identifier(local), Identifier(referenced))
            for local, referenced in zip(
                key.local_columns,
                key.referenced_columns,
                strict=True,
            )
        ),
    )


@dataclass(frozen=True, slots=True)
class ObservedReferencingForeignKey:
    """A catalog projection identifying an inbound foreign-key occurrence."""

    catalog_name: str
    referencing_table: QualifiedName

    def __post_init__(self) -> None:
        object.__setattr__(self, "catalog_name", _catalog_name(self.catalog_name))
