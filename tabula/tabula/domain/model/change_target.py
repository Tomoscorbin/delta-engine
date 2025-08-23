"""Domain model representing a table change target."""

from __future__ import annotations

from dataclasses import dataclass

from tabula.domain.model.table import DesiredTable, ObservedTable, QualifiedName


@dataclass(frozen=True, slots=True)
class ChangeTarget:
    """Represents a desired table alongside its current observed state.

    Attributes:
        desired: Desired table definition.
        observed: Observed table state or ``None`` if the table does not exist.
    """

    desired: DesiredTable
    observed: ObservedTable | None

    def __post_init__(self) -> None:
        if self.observed and self.observed.qualified_name != self.desired.qualified_name:
            raise ValueError("qualified_name must match between desired and observed")

    @property
    def qualified_name(self) -> QualifiedName:
        """Fully qualified name of the target table."""

        return self.desired.qualified_name

    @property
    def is_new(self) -> bool:
        """Return ``True`` if the table does not currently exist."""

        return self.observed is None

    @property
    def is_existing_and_non_empty(self) -> bool:
        """Return ``True`` if the table exists and has data."""

        return bool(self.observed and not self.observed.is_empty)
