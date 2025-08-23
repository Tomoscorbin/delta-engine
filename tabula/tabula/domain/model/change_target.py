from __future__ import annotations
from dataclasses import dataclass
from tabula.domain.model.table import DesiredTable, ObservedTable, QualifiedName

@dataclass(frozen=True, slots=True)
class ChangeTarget:
    """
    One logical table to be changed: current (observed) -> desired.

    observed may be None (table does not exist yet).
    """
    desired: DesiredTable
    observed: ObservedTable | None

    def __post_init__(self) -> None:
        if self.observed and self.observed.qualified_name != self.desired.qualified_name:
            raise ValueError("qualified_name must match between desired and observed")

    @property
    def qualified_name(self) -> QualifiedName:
        return self.desired.qualified_name

    @property
    def is_new(self) -> bool:
        return self.observed is None

    @property
    def is_existing_and_non_empty(self) -> bool:
        return bool(self.observed and not self.observed.is_empty)
