"""Domain value objects representing key constraints (primary and foreign)."""

from collections.abc import Iterable
from dataclasses import dataclass, field

from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model.identifier import Identifier
from delta_engine.domain.model.qualified_name import QualifiedName


# Outer layers may pre-check these rules for error locality; this check is the
# one every producer of a constraint — declaration lowering or a catalog
# read — must pass.
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


@dataclass(frozen=True, slots=True)
class PrimaryKeyConstraint:
    """
    A primary key constraint, declared or observed.

    Symmetric with :class:`ForeignKeyConstraint`: a table-level key constraint
    over an ordered set of columns.

    Attributes:
        columns: Tuple of column names, preserving their supplied spelling and
            stored sorted by identifier key. Column order is not part of a
            primary key's meaning, so identity and rendered DDL are independent
            of declaration order. Duplicates are judged by identifier key.
        name: Physical constraint name. On a declaration it is an optional
            creation preference — once created, Databricks owns the catalog
            name; on a catalog observation it is the concrete name the catalog
            reports. Either way it is excluded from equality: names are
            creation preferences, not structural identity.

    """

    columns: ListOrTuple[str]
    name: str | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        columns = _constraint_columns(self.columns, kind="primary key")
        # Column order is not part of a primary key's meaning. Store one
        # canonical order while preserving each identifier's spelling, so
        # identity and rendered DDL are independent of declaration order.
        canonical = tuple(sorted(columns, key=lambda column: column.lower()))
        object.__setattr__(self, "columns", canonical)
        object.__setattr__(
            self,
            "name",
            Identifier(self.name) if self.name is not None else None,
        )

    def matches_columns(self, columns: Iterable[str]) -> bool:
        """Return whether columns identify this key, ignoring order and case."""
        return frozenset(self.columns) == frozenset(Identifier(column) for column in columns)


@dataclass(frozen=True, slots=True)
class ForeignKeyConstraint:
    """
    A foreign key constraint, declared or observed.

    Attributes:
        local_columns: Tuple of local column names in the constraint,
            preserving spelling and stored sorted by identifier key (pairing
            with ``referenced_columns`` preserved). Column order is not part of
            a foreign key's meaning, mirroring the primary key, so identity and
            rendered DDL are independent of declaration order.
        referenced_table: Fully qualified name of the referenced table.
        referenced_columns: Tuple of column names in the referenced table,
            positionally aligned with ``local_columns`` after identity-key sorting.
        name: Physical constraint name. On a declaration it is an optional
            creation preference — once created, Databricks owns the catalog
            name; on a catalog observation it is the concrete name the catalog
            reports. Either way it is excluded from equality: names are
            creation preferences, not structural identity.

    """

    local_columns: ListOrTuple[str]
    referenced_table: QualifiedName
    referenced_columns: ListOrTuple[str]
    name: str | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
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
        object.__setattr__(
            self,
            "name",
            Identifier(self.name) if self.name is not None else None,
        )


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
