"""Domain value object representing a foreign key constraint."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ForeignKeyConstraint:
    """
    A foreign key constraint declaration.

    Attributes:
        local_columns: Ordered tuple of local column names in the constraint.
        references: Fully qualified name of the referenced table (catalog.schema.name).
        referenced_columns: Ordered tuple of column names in the referenced table,
            positionally aligned with ``local_columns``.
        constraint_name: Optional explicit constraint name. When omitted, the name
            is derived as ``{table_name}_{local_cols}_fk`` via ``resolved_constraint_name``.
    """

    local_columns: tuple[str, ...]
    references: str
    referenced_columns: tuple[str, ...]
    constraint_name: str | None = None

    def __post_init__(self) -> None:
        if not self.local_columns:
            raise ValueError("local_columns must not be empty")
        if not self.referenced_columns:
            raise ValueError("referenced_columns must not be empty")
        if len(self.local_columns) != len(self.referenced_columns):
            raise ValueError(
                "local_columns and referenced_columns must have the same number of entries;"
                f" got {len(self.local_columns)} local and {len(self.referenced_columns)} referenced"
            )

    def resolved_constraint_name(self, table_name: str) -> str:
        """Return the constraint name to use in SQL, deriving it when not explicitly set."""
        if self.constraint_name is not None:
            return self.constraint_name
        cols = "_".join(self.local_columns)
        return f"{table_name}_{cols}_fk"
