"""Domain representation of schema change actions."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Self

from delta_engine.domain.model import Column, QualifiedName
from delta_engine.domain.model.identifier import Identifier


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

    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", Identifier(self.name))


@dataclass(frozen=True, slots=True)
class ActionPlan:
    """Collection of actions targeting a single qualified name."""

    target: QualifiedName
    actions: tuple[Action, ...] = ()

    def __len__(self) -> int:
        return len(self.actions)

    def __bool__(self) -> bool:
        return bool(self.actions)

    def __iter__(self):
        return iter(self.actions)

    def __add__(self, other: Self) -> Self:
        if self.target != other.target:
            raise ValueError("Cannot merge plans for different targets")
        return ActionPlan(self.target, self.actions + other.actions)

    def add(self, action: Action) -> Self:
        if not isinstance(action, Action):
            raise TypeError("action must be an Action")
        return ActionPlan(self.target, (*self.actions, action))

    def count_by_action(self) -> Counter[type[Action]]:
        """Return a counter keyed by action subclass."""
        return Counter(type(a) for a in self.actions)
