"""Public API declaration for foreign key relationships."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias

from delta_engine.domain.model import ForeignKeyConstraint, QualifiedName, ReferencedKey

if TYPE_CHECKING:
    from delta_engine.api.table import DeltaTable

    ForeignKeyReference: TypeAlias = str | QualifiedName | DeltaTable
else:
    ForeignKeyReference: TypeAlias = object


@dataclass(frozen=True, slots=True)
class ForeignKey:
    """
    Public declaration of a foreign key relationship.

    The declaration names the local columns and the table they reference. It does
    not expose the physical constraint name; desired-side FK names are generated
    by the engine when the surrounding :class:`DeltaTable` is built.

    ``references`` may be a fully qualified name string, a :class:`QualifiedName`,
    or another :class:`DeltaTable`. When referencing a ``DeltaTable``,
    ``referenced_columns`` may be omitted and will default to that table's primary
    key columns. String and ``QualifiedName`` references must provide
    ``referenced_columns`` explicitly because they do not carry primary-key
    metadata.
    """

    local_columns: Iterable[str]
    references: ForeignKeyReference
    referenced_columns: Iterable[str] | None = None


def lower_foreign_key(declaration: ForeignKey | ForeignKeyConstraint) -> ForeignKeyConstraint:
    """Convert a public FK declaration into the internal domain constraint."""
    if isinstance(declaration, ForeignKeyConstraint):
        return declaration
    if not isinstance(declaration, ForeignKey):
        raise TypeError(f"foreign_keys entries must be ForeignKey values; got {declaration!r}")

    referenced_table, referenced_columns = _resolve_reference(
        declaration.references, declaration.referenced_columns
    )
    return ForeignKeyConstraint(
        local_columns=tuple(declaration.local_columns),
        references=ReferencedKey(table=referenced_table, columns=referenced_columns),
    )


def _resolve_reference(
    reference: ForeignKeyReference,
    referenced_columns: Iterable[str] | None,
) -> tuple[QualifiedName, tuple[str, ...]]:
    """Resolve a public FK reference into a table name and referenced columns."""
    if isinstance(reference, str):
        table = QualifiedName.parse(reference)
        columns = _explicit_referenced_columns(referenced_columns, table)
        return table, columns

    if isinstance(reference, QualifiedName):
        columns = _explicit_referenced_columns(referenced_columns, reference)
        return reference, columns

    # Avoid importing DeltaTable at runtime; use its small public protocol.
    to_desired_table = getattr(reference, "to_desired_table", None)
    if callable(to_desired_table):
        desired_table = to_desired_table()
        table = desired_table.qualified_name
        columns = tuple(referenced_columns) if referenced_columns is not None else ()
        if not columns:
            columns = tuple(reference.primary_key)
            if not columns:
                raise ValueError(
                    "referenced_columns must be provided when referencing a DeltaTable"
                    f" with no primary key: {table}"
                )
        return table, columns

    raise TypeError(
        "foreign key references must be a fully qualified name string, QualifiedName,"
        f" or DeltaTable; got {reference!r}"
    )


def _explicit_referenced_columns(
    referenced_columns: Iterable[str] | None, table: QualifiedName
) -> tuple[str, ...]:
    """Return explicit referenced columns or raise a table-specific error."""
    columns = tuple(referenced_columns) if referenced_columns is not None else ()
    if not columns:
        raise ValueError(f"referenced_columns must be provided for foreign key reference {table}")
    return columns
