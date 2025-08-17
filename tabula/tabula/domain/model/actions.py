from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple, Iterator, Self
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.column import Column

class Action:
    """Marker base for schema actions."""
    @property
    def kind(self) -> str:
        return type(self).__name__

    def __repr__(self) -> str:
        return f"{self.kind}()"

@dataclass(frozen=True, slots=True)
class CreateTable(Action):
    columns: Tuple[Column, ...]
    def __str__(self) -> str:
        return f"create table with {len(self.columns)} column(s)"

@dataclass(frozen=True, slots=True)
class AddColumn(Action):
    column: Column
    def __str__(self) -> str:
        return f"add column {self.column}"

@dataclass(frozen=True, slots=True)
class DropColumn(Action):
    column_name: str
    def __post_init__(self) -> None:
        object.__setattr__(self, "column_name", self.column_name.casefold())
    def __str__(self) -> str:
        return f"drop column {self.column_name}"

@dataclass(frozen=True, slots=True)
class ActionPlan:
    qualified_name: QualifiedName
    actions: Tuple[Action, ...] = ()

    def __iter__(self) -> Iterator[Action]:
        return iter(self.actions)
    
    def __len__(self) -> int:
        return len(self.actions)
    
    def __bool__(self) -> bool:
        return bool(self.actions)

    def __str__(self) -> str:
        if not self.actions:
            return f"ActionPlan[{self.qualified_name}] (empty)"
        return "ActionPlan[" + str(self.qualified_name) + "]: " + ", ".join(str(a) for a in self.actions)

    def __repr__(self) -> str:
        return f"ActionPlan({self.qualified_name!r}, actions={self.actions!r})"

    def add(self, action: Action) -> Self:
        return ActionPlan(self.qualified_name, self.actions + (action,))

    def extend(self, more: Self) -> Self:
        if self.qualified_name != more.qualified_name:
            raise ValueError(f"Cannot merge plans for different tables: {self.qualified_name} vs {more.qualified_name}")
        return ActionPlan(self.qualified_name, self.actions + more.actions)

    def __add__(self, other: Self) -> Self:
        
        return self.extend(other)
    def __iadd__(self, other: Self) -> Self:
        return self.extend(other)
