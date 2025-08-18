from __future__ import annotations
from collections import Counter
from dataclasses import dataclass
from typing import Self

from tabula.domain.model.column import Column
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain._identifiers import normalize_identifier


class Action:
    """Base class for all plan actions."""


@dataclass(frozen=True, slots=True)
class CreateTable(Action):
    columns: tuple[Column, ...]

    def __post_init__(self) -> None:
        # Strict shape only — upstream model guarantees the rest.
        if not isinstance(self.columns, tuple):
            raise TypeError("columns must be a tuple[Column, ...]")

    def __str__(self) -> str:
        return f"create table with {len(self.columns)} column(s)"


@dataclass(frozen=True, slots=True)
class AddColumn(Action):
    column: Column

    def __post_init__(self) -> None:
        if not isinstance(self.column, Column):
            raise TypeError("AddColumn.column must be a Column")
        
    def __repr__(self) -> str:
        return f"AddColumn({self.column!r})"    

    def __str__(self) -> str:
        return f"add column {self.column.name}"


@dataclass(frozen=True, slots=True)
class DropColumn(Action):
    column_name: str

    def __post_init__(self) -> None:
        normalized = normalize_identifier("column_name", self.column_name)
        object.__setattr__(self, "column_name", normalized)

    def __repr__(self) -> str:
        return f"DropColumn({self.column_name!r})"   

    def __str__(self) -> str:
        return f"drop column {self.column_name}"


@dataclass(frozen=True, slots=True)
class ActionPlan:
    target: QualifiedName
    actions: tuple[Action, ...] = ()

    def __len__(self) -> int:
        return len(self.actions)

    def __bool__(self) -> bool:
        return bool(self.actions)
    
    def __iter__(self):
        return iter(self.actions)

    def add(self, action: Action) -> Self:
        if not isinstance(action, Action):
            raise TypeError("action must be an Action")
        return ActionPlan(self.target, (*self.actions, action))

    def __add__(self, other: Self) -> Self:
        if self.target != other.target:
            raise ValueError("Cannot merge plans for different targets")
        return ActionPlan(self.target, self.actions + other.actions)

    def __str__(self) -> str:
        if not self.actions:
            return f"ActionPlan({self.target}): empty"
        return f"ActionPlan({self.target}): " + "; ".join(str(a) for a in self.actions)

    def count_by_action(self) -> Counter[type[Action]]:
        """Return a Counter keyed by Action subclass."""
        return Counter(type(a) for a in self.actions)

