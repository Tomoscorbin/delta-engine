"""Domain representation of schema change actions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum, auto
from typing import ClassVar

from delta_engine.domain.model import Column, DesiredTable


class ActionPhase(IntEnum):
    """
    Relative execution order of plan actions.

    Members are declared in execution order (lower runs first); the order
    encodes dependencies between operations -- e.g. a table must be created
    before columns are added, and tightened nullability is applied last.
    Centralising the order here keeps the full precedence readable in one
    place while each action declares its own phase by name.
    """

    CREATE_TABLE = auto()
    SET_PROPERTY = auto()
    ADD_COLUMN = auto()
    DROP_COLUMN = auto()
    SET_COLUMN_COMMENT = auto()
    SET_TABLE_COMMENT = auto()
    SET_COLUMN_NULLABILITY = auto()


class Action(ABC):
    """
    Base class for all plan actions.

    Subclasses declare their deterministic-ordering contract:

    - ``phase``: the execution phase this action belongs to (:class:`ActionPhase`).
    - ``subject``: the identifier the action targets within its phase (a column
      or property name), used to order actions that share a phase. Actions that
      target the table as a whole return the empty string.
    """

    phase: ClassVar[ActionPhase]

    @property
    @abstractmethod
    def subject(self) -> str:
        """Identifier targeted within the phase; subclasses must override."""
        ...


@dataclass(frozen=True, slots=True)
class CreateTable(Action):
    """
    Create a new table to match a desired definition.

    Carries the full :class:`DesiredTable` (columns, properties, and comment)
    so the SQL compiler can render a complete CREATE TABLE statement.
    """

    table: DesiredTable

    phase: ClassVar[ActionPhase] = ActionPhase.CREATE_TABLE

    @property
    def subject(self) -> str:
        """Targets the table as a whole."""
        return ""


@dataclass(frozen=True, slots=True)
class AddColumn(Action):
    """Add a column to an existing table."""

    column: Column

    phase: ClassVar[ActionPhase] = ActionPhase.ADD_COLUMN

    @property
    def subject(self) -> str:
        """The added column's name."""
        return self.column.name


@dataclass(frozen=True, slots=True)
class DropColumn(Action):
    """Remove a column from a table."""

    column_name: str

    phase: ClassVar[ActionPhase] = ActionPhase.DROP_COLUMN

    @property
    def subject(self) -> str:
        """The dropped column's name."""
        return self.column_name


@dataclass(frozen=True, slots=True)
class SetProperty(Action):
    """Set a table property."""

    name: str
    value: str

    phase: ClassVar[ActionPhase] = ActionPhase.SET_PROPERTY

    @property
    def subject(self) -> str:
        """The property name being set."""
        return self.name


@dataclass(frozen=True, slots=True)
class SetColumnComment(Action):
    """Set a column's comment."""

    column_name: str
    comment: str

    phase: ClassVar[ActionPhase] = ActionPhase.SET_COLUMN_COMMENT

    @property
    def subject(self) -> str:
        """The commented column's name."""
        return self.column_name


@dataclass(frozen=True, slots=True)
class SetTableComment(Action):
    """Set a table's comment."""

    comment: str

    phase: ClassVar[ActionPhase] = ActionPhase.SET_TABLE_COMMENT

    @property
    def subject(self) -> str:
        """Targets the table as a whole."""
        return ""


@dataclass(frozen=True, slots=True)
class SetColumnNullability(Action):
    """Set a column's nullability."""

    column_name: str
    nullable: bool

    phase: ClassVar[ActionPhase] = ActionPhase.SET_COLUMN_NULLABILITY

    @property
    def subject(self) -> str:
        """The column whose nullability changes."""
        return self.column_name


def _execution_order(action: Action) -> tuple[int, str]:
    """Deterministic ordering key for an action: execution phase, then subject name."""
    return (action.phase, action.subject)


@dataclass(frozen=True, slots=True)
class ActionPlan:
    """
    The actions to apply to a table, held in execution order.

    A plan keeps its actions sorted by execution phase and then by subject name,
    regardless of the order they are supplied in: ordering is an invariant of the
    plan, not a step a caller has to remember. Both the phase and the subject are
    declared by each action type (see :class:`Action`), so a new action orders
    itself with no change here.
    """

    actions: tuple[Action, ...] = ()

    def __post_init__(self) -> None:
        """Sort the actions into execution order, preserving input order on ties."""
        object.__setattr__(self, "actions", tuple(sorted(self.actions, key=_execution_order)))

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
