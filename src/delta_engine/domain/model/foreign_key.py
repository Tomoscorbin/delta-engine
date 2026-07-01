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
        constraint_name: Optional constraint name. Populated from the catalog for an
            observed constraint; ``None`` for a desired declaration (the SQL adapter
            derives the name).

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
                f" got {len(self.local_columns)} local and"
                f" {len(self.referenced_columns)} referenced"
            )
        if self.references.count(".") != 2:
            raise ValueError(
                "references must be a fully qualified 'catalog.schema.table' name;"
                f" got: {self.references!r}"
            )
        for part in self.references.split("."):
            if not part.strip():
                raise ValueError(f"references must not have a blank part; got: {self.references!r}")
            if part != part.casefold():
                raise ValueError(f"references must be lowercase; got: {self.references!r}")
        if self.constraint_name is not None and not self.constraint_name.strip():
            raise ValueError("constraint_name must not be blank when provided")

    @property
    def signature(self) -> tuple[tuple[str, ...], str, tuple[str, ...]]:
        """
        Content identity: local columns, referenced table, referenced columns.

        Excludes ``constraint_name``, so a declared foreign key (which may be
        unnamed) and a catalog-observed one (which always carries a name)
        compare equal when they describe the same relationship. This makes the
        constraint's identity independent of how it happens to be named.
        """
        return (self.local_columns, self.references, self.referenced_columns)
