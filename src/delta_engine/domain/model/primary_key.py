"""Domain value object representing a primary key constraint."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PrimaryKeyConstraint:
    """
    A primary key constraint declaration.

    Symmetric with :class:`ForeignKeyConstraint`: a table-level key constraint
    over an ordered set of columns.

    Attributes:
        columns: Ordered tuple of column names that make up the primary key.
        constraint_name: Optional constraint name. Populated from the catalog for an
            observed constraint; ``None`` for a desired declaration. The SQL adapter
            derives the emitted name when executing against the catalog.

    """

    columns: tuple[str, ...]
    constraint_name: str | None = None

    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError("columns must not be empty")
        seen: set[str] = set()
        for column in self.columns:
            if column in seen:
                raise ValueError(f"Duplicate primary key column: {column}")
            seen.add(column)
        if self.constraint_name is not None and not self.constraint_name.strip():
            raise ValueError("constraint_name must not be blank when provided")
