"""Domain representation of schema change actions."""

from __future__ import annotations

from dataclasses import dataclass

from delta_engine.domain.model import Column, QualifiedName


class Action:
    """Base class for all plan actions."""


@dataclass(frozen=True, slots=True)
class CreateTable(Action):
    """Create a new table with the specified columns."""

    columns: tuple[Column, ...]


@dataclass(frozen=True, slots=True)
class AddColumn(Action):
    """Add a column to an existing table."""

    column: Column


@dataclass(frozen=True, slots=True)
class DropColumn(Action):
    """Remove a column from a table."""

    column_name: str


@dataclass(frozen=True, slots=True)
class SetProperty(Action):
    """Set a table property."""

    name: str
    value: object


@dataclass(frozen=True, slots=True)
class UnsetProperty(Action):
    """Unset a table property."""

    name: str


@dataclass(frozen=True, slots=True)
class ActionPlan:
    """Collection of actions targeting a single qualified name."""

    target: QualifiedName
    actions: tuple[Action, ...] = ()

    def __len__(self) -> int:
        """Return the number of actions in the plan."""
        return len(self.actions)

    def __bool__(self) -> bool:
        """Return ``True`` if the plan contains any actions."""
        return bool(self.actions)

    def __iter__(self):
        """Iterate over actions in plan order."""
        return iter(self.actions)

    def __getitem__(self, index):
        """Return the action at ``index`` (supports slicing)."""
        return self.actions[index]
