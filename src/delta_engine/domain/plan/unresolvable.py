"""Canonical domain vocabulary for differences no action can close."""

from dataclasses import dataclass
from typing import ClassVar

from delta_engine.domain.model import Identifier, TableAspect


@dataclass(frozen=True, slots=True)
class ColumnCaseDrift:
    """A reference to an existing column spelled differently from the catalog."""

    declared_name: str
    observed_name: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE

    def __post_init__(self) -> None:
        # Coerce to plain str so the guards compare exact spelling even when
        # callers pass Identifiers, whose equality is case-insensitive.
        object.__setattr__(self, "declared_name", str(self.declared_name))
        object.__setattr__(self, "observed_name", str(self.observed_name))
        if self.declared_name == self.observed_name:
            raise ValueError(f"ColumnCaseDrift carries no difference: {self.declared_name!r}")
        if Identifier(self.declared_name) != Identifier(self.observed_name):
            raise ValueError(
                "ColumnCaseDrift names two different columns:"
                f" {self.declared_name!r} vs {self.observed_name!r}"
            )


@dataclass(frozen=True, slots=True)
class ColumnRenameConflict:
    """A declared rename whose source and target are both present."""

    old_name: str
    new_name: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE

    def __post_init__(self) -> None:
        if self.old_name == self.new_name:
            raise ValueError(f"ColumnRenameConflict carries no difference: {self.old_name!r}")


@dataclass(frozen=True, slots=True)
class PropertyUndeclared:
    """A managed catalog property for which the declaration states no intent."""

    name: str
    observed_value: str

    aspect: ClassVar[TableAspect] = TableAspect.PROPERTIES


@dataclass(frozen=True, slots=True)
class PartitioningChanged:
    """Partitioning drift, which has no supported in-place action."""

    desired_partitioning: tuple[str, ...]
    observed_partitioning: tuple[str, ...]

    aspect: ClassVar[TableAspect] = TableAspect.PARTITIONING

    def __post_init__(self) -> None:
        if self.desired_partitioning == self.observed_partitioning:
            raise ValueError(
                f"PartitioningChanged carries no difference: {self.desired_partitioning!r}"
            )


type Unresolvable = (
    ColumnCaseDrift | ColumnRenameConflict | PropertyUndeclared | PartitioningChanged
)
