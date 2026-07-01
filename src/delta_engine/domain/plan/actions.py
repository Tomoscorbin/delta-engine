"""Domain representation of schema change actions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from enum import IntEnum, auto
from typing import ClassVar

from delta_engine.domain.model import Column, DesiredTable
from delta_engine.domain.model.data_type import DataType
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint


class ActionPhase(IntEnum):
    """
    Relative execution order of plan actions.

    Members are declared in execution order (lower runs first); the order
    encodes dependencies between operations. Centralising the order here keeps
    the full precedence readable in one place while each action declares its
    own phase by name. See the "Planning and determinism" section of
    ``docs/explanation-architecture.md`` for the rationale behind each
    dependency-driven ordering.
    """

    CREATE_TABLE = auto()
    SET_PROPERTY = auto()
    SET_TABLE_TAG = auto()
    UNSET_TABLE_TAG = auto()
    DROP_FOREIGN_KEY = auto()
    DROP_PRIMARY_KEY = auto()
    ADD_COLUMN = auto()
    DROP_COLUMN = auto()
    SET_COLUMN_COMMENT = auto()
    SET_TABLE_COMMENT = auto()
    SET_COLUMN_NULLABILITY = auto()
    SET_PRIMARY_KEY = auto()
    SET_FOREIGN_KEY = auto()
    COLUMN_TYPE_CHANGE = auto()
    PARTITIONING_CHANGE = auto()


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
        return ""


@dataclass(frozen=True, slots=True)
class AddColumn(Action):
    """Add a column to an existing table."""

    column: Column

    phase: ClassVar[ActionPhase] = ActionPhase.ADD_COLUMN

    @property
    def subject(self) -> str:
        return self.column.name


@dataclass(frozen=True, slots=True)
class DropColumn(Action):
    """Remove a column from a table."""

    column_name: str

    phase: ClassVar[ActionPhase] = ActionPhase.DROP_COLUMN

    @property
    def subject(self) -> str:
        return self.column_name


@dataclass(frozen=True, slots=True)
class SetProperty(Action):
    """Set a table property."""

    name: str
    value: str

    phase: ClassVar[ActionPhase] = ActionPhase.SET_PROPERTY

    @property
    def subject(self) -> str:
        return self.name


@dataclass(frozen=True, slots=True)
class SetTableTag(Action):
    """Set a Unity Catalog tag on a table (distinct from a table property)."""

    name: str
    value: str

    phase: ClassVar[ActionPhase] = ActionPhase.SET_TABLE_TAG

    @property
    def subject(self) -> str:
        return self.name


@dataclass(frozen=True, slots=True)
class UnsetTableTag(Action):
    """Remove a Unity Catalog tag from a table."""

    name: str

    phase: ClassVar[ActionPhase] = ActionPhase.UNSET_TABLE_TAG

    @property
    def subject(self) -> str:
        return self.name


@dataclass(frozen=True, slots=True)
class SetColumnComment(Action):
    """Set a column's comment."""

    column_name: str
    comment: str

    phase: ClassVar[ActionPhase] = ActionPhase.SET_COLUMN_COMMENT

    @property
    def subject(self) -> str:
        return self.column_name


@dataclass(frozen=True, slots=True)
class SetTableComment(Action):
    """Set a table's comment."""

    comment: str

    phase: ClassVar[ActionPhase] = ActionPhase.SET_TABLE_COMMENT

    @property
    def subject(self) -> str:
        return ""


@dataclass(frozen=True, slots=True)
class SetColumnNullability(Action):
    """Set a column's nullability."""

    column_name: str
    nullable: bool

    phase: ClassVar[ActionPhase] = ActionPhase.SET_COLUMN_NULLABILITY

    @property
    def subject(self) -> str:
        return self.column_name


@dataclass(frozen=True, slots=True)
class DropPrimaryKey(Action):
    """Drop the existing primary key constraint from a table."""

    phase: ClassVar[ActionPhase] = ActionPhase.DROP_PRIMARY_KEY

    @property
    def subject(self) -> str:
        return ""


@dataclass(frozen=True, slots=True)
class SetPrimaryKey(Action):
    """
    Add a primary key constraint to a table.

    Carries the primary key column names (in declaration order) and the
    engine-generated ``constraint_name`` so the compiler renders it directly.
    That primary key columns must be NOT NULL is a well-formedness invariant of
    a desired table, enforced when the ``DesiredTable`` is built, not a plan
    concern — so this action needs only the names.
    """

    columns: tuple[str, ...]
    constraint_name: str

    phase: ClassVar[ActionPhase] = ActionPhase.SET_PRIMARY_KEY

    @property
    def subject(self) -> str:
        return ""


@dataclass(frozen=True, slots=True)
class DropForeignKey(Action):
    """Drop a named foreign key constraint from a table."""

    constraint_name: str

    phase: ClassVar[ActionPhase] = ActionPhase.DROP_FOREIGN_KEY

    @property
    def subject(self) -> str:
        return self.constraint_name


@dataclass(frozen=True, slots=True)
class SetForeignKey(Action):
    """
    Add a foreign key constraint to a table.

    Carries the full :class:`ForeignKeyConstraint`, whose engine-generated
    ``constraint_name`` the compiler renders directly.
    """

    foreign_key: ForeignKeyConstraint

    phase: ClassVar[ActionPhase] = ActionPhase.SET_FOREIGN_KEY

    @property
    def subject(self) -> str:
        return ",".join(self.foreign_key.local_columns)


@dataclass(frozen=True, slots=True)
class ColumnTypeChange(Action):
    """
    Records that a column's data type differs between desired and observed.

    A descriptive action: it states the drift, not whether the drift is
    permitted — deciding what is allowed is the validator's job. It carries the
    observed and desired types so a validation rule can reason about them and
    render a clear message.
    """

    column_name: str
    from_type: DataType
    to_type: DataType

    phase: ClassVar[ActionPhase] = ActionPhase.COLUMN_TYPE_CHANGE

    @property
    def subject(self) -> str:
        return self.column_name


@dataclass(frozen=True, slots=True)
class PartitioningChange(Action):
    """
    Records that the desired and observed partition specs differ.

    A descriptive action: it states the drift, not whether the drift is
    permitted — deciding what is allowed is the validator's job. It carries the
    observed and desired partition columns so a validation rule can reason about
    them and render a clear message.
    """

    desired_partitioning: tuple[str, ...]
    observed_partitioning: tuple[str, ...]

    phase: ClassVar[ActionPhase] = ActionPhase.PARTITIONING_CHANGE

    @property
    def subject(self) -> str:
        return ""


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
        return len(self.actions)

    def __bool__(self) -> bool:
        return bool(self.actions)

    def __iter__(self) -> Iterator[Action]:
        return iter(self.actions)
