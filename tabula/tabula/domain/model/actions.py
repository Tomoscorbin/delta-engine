from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple, Iterator, Union, Self
from tabula.domain.model.full_name import FullName
from tabula.domain.model.column import Column

class Action():
    @property
    def kind(self) -> str:
        return type(self).__name__  # e.g. "AddColumn"

@dataclass(frozen=True, slots=True)
class CreateTable(Action):
    columns: Tuple[Column, ...]
    def __str__(self) -> str:
        return f"CreateTable(columns={len(self.columns)})"

@dataclass(frozen=True, slots=True)
class AddColumn(Action):
    column: Column
    def __str__(self) -> str:
        return f"AddColumn({self.column})"

@dataclass(frozen=True, slots=True)
class DropColumn(Action):
    column_name: str
    def __str__(self) -> str:
        return f"DropColumn(column_name={self.column_name})"

@dataclass(frozen=True, slots=True)
class ActionPlan:
    full_name: FullName                      # single source of table identity
    actions: Tuple[Action, ...] = ()

    def __len__(self) -> int: 
        return len(self.actions)
    
    def __bool__(self) -> bool: 
        return bool(self.actions)
    
    def __iter__(self) -> Iterator[Action]: 
        return iter(self.actions)

    def __str__(self) -> str:
        if not self.actions:
            return f"ActionPlan[{self.full_name}] (empty)"
        return f"ActionPlan[{self.full_name}]: " + ", ".join(str(a) for a in self.actions)

    def add_action(self, action: Action) -> Self:
        return ActionPlan(self.full_name, self.actions + (action,))

    def extend_with(self, more: Self) -> Self:
        if self.full_name != more.full_name:
            raise ValueError("Cannot merge plans for different tables.")
        return ActionPlan(self.full_name, self.actions + more.actions)

    def __add__(self, other: Self) -> Self:
        return self.extend_with(other)
