"""Canonical domain vocabulary for executable table actions."""

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from enum import IntEnum, auto
from typing import ClassVar

from delta_engine.domain.model import (
    DataType,
    DesiredColumn,
    DesiredTable,
    ForeignKeyConstraint,
    ForeignKeyReference,
    ObservedColumn,
    PrimaryKeyConstraint,
    QualifiedName,
    TableAspect,
    TableKind,
)


class ActionPhase(IntEnum):
    """
    Relative execution order of plan actions.

    Members are declared in execution order (lower runs first); the order
    encodes dependencies between operations. Centralising the order here keeps
    the full precedence readable in one place while each action declares its
    own phase by name. See the "Deterministic action plans" section of
    ``docs/explanation-architecture.md`` for the rationale behind each
    dependency-driven ordering.
    """

    CREATE_TABLE = auto()
    SET_PROPERTY = auto()
    UNSET_PROPERTY = auto()
    SET_TABLE_TAG = auto()
    UNSET_TABLE_TAG = auto()
    DROP_FOREIGN_KEY = auto()
    DROP_PRIMARY_KEY = auto()
    RENAME_COLUMN = auto()
    ADD_COLUMN = auto()
    ALTER_COLUMN_TYPE = auto()
    SET_CLUSTERING = auto()
    SET_COLUMN_TAG = auto()
    UNSET_COLUMN_TAG = auto()
    DROP_COLUMN = auto()
    SET_COLUMN_COMMENT = auto()
    SET_TABLE_COMMENT = auto()
    SET_COLUMN_NULLABILITY = auto()
    SET_PRIMARY_KEY = auto()
    SET_FOREIGN_KEY = auto()


class Action(ABC):
    """
    Base class for executable table actions.

    Every action carries the complete semantic state needed to validate,
    report, and execute it, with each value named once (``desired_*`` /
    ``observed_*`` for transition state). Each subclass declares its managed
    ``aspect``, execution ``phase``, and stable within-phase ``subject``.
    """

    aspect: ClassVar[TableAspect]
    phase: ClassVar[ActionPhase]

    @property
    @abstractmethod
    def subject(self) -> str:
        """Identifier targeted within the phase; subclasses must override."""
        ...


@dataclass(frozen=True, slots=True)
class CreateTable(Action):
    """Create a missing table from its complete desired definition."""

    table: DesiredTable

    aspect: ClassVar[TableAspect] = TableAspect.TABLE_EXISTENCE
    phase: ClassVar[ActionPhase] = ActionPhase.CREATE_TABLE

    @property
    def subject(self) -> str:
        return ""


@dataclass(frozen=True, slots=True)
class AddColumn(Action):
    """Add a declared column to an existing table."""

    column: DesiredColumn

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE
    phase: ClassVar[ActionPhase] = ActionPhase.ADD_COLUMN

    @property
    def subject(self) -> str:
        return self.column.name


@dataclass(frozen=True, slots=True)
class DropColumn(Action):
    """Remove an observed column from a table."""

    column: ObservedColumn

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE
    phase: ClassVar[ActionPhase] = ActionPhase.DROP_COLUMN

    @property
    def subject(self) -> str:
        return self.column.name


@dataclass(frozen=True, slots=True)
class RenameColumn(Action):
    """Rename an observed column in place."""

    old_name: str
    new_name: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE
    phase: ClassVar[ActionPhase] = ActionPhase.RENAME_COLUMN

    def __post_init__(self) -> None:
        if self.old_name == self.new_name:
            raise ValueError(f"RenameColumn carries no difference: {self.old_name!r}")

    @property
    def subject(self) -> str:
        return self.old_name


@dataclass(frozen=True, slots=True)
class SetProperty(Action):
    """Set a table property, preserving its desired and observed values."""

    name: str
    desired_value: str
    observed_value: str | None

    aspect: ClassVar[TableAspect] = TableAspect.PROPERTIES
    phase: ClassVar[ActionPhase] = ActionPhase.SET_PROPERTY

    def __post_init__(self) -> None:
        if self.desired_value == self.observed_value:
            raise ValueError(f"SetProperty carries no difference: {self.desired_value!r}")

    @property
    def subject(self) -> str:
        return self.name


@dataclass(frozen=True, slots=True)
class UnsetProperty(Action):
    """Remove an observed property the declaration asserts absent."""

    name: str
    observed_value: str

    aspect: ClassVar[TableAspect] = TableAspect.PROPERTIES
    phase: ClassVar[ActionPhase] = ActionPhase.UNSET_PROPERTY

    @property
    def subject(self) -> str:
        return self.name


@dataclass(frozen=True, slots=True)
class SetTableTag(Action):
    """Set a Unity Catalog table tag, preserving both sides of the transition."""

    name: str
    desired_value: str
    observed_value: str | None

    aspect: ClassVar[TableAspect] = TableAspect.TABLE_TAGS
    phase: ClassVar[ActionPhase] = ActionPhase.SET_TABLE_TAG

    def __post_init__(self) -> None:
        if self.desired_value == self.observed_value:
            raise ValueError(f"SetTableTag carries no difference: {self.desired_value!r}")

    @property
    def subject(self) -> str:
        return self.name


@dataclass(frozen=True, slots=True)
class UnsetTableTag(Action):
    """Remove a Unity Catalog tag from a table."""

    name: str

    aspect: ClassVar[TableAspect] = TableAspect.TABLE_TAGS
    phase: ClassVar[ActionPhase] = ActionPhase.UNSET_TABLE_TAG

    @property
    def subject(self) -> str:
        return self.name


@dataclass(frozen=True, slots=True)
class SetColumnComment(Action):
    """Set a column comment, preserving its desired and observed values."""

    column_name: str
    desired_comment: str
    observed_comment: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_COMMENTS
    phase: ClassVar[ActionPhase] = ActionPhase.SET_COLUMN_COMMENT

    def __post_init__(self) -> None:
        if self.desired_comment == self.observed_comment:
            raise ValueError(f"SetColumnComment carries no difference: {self.desired_comment!r}")

    @property
    def subject(self) -> str:
        return self.column_name


@dataclass(frozen=True, slots=True)
class SetColumnTag(Action):
    """Set a Unity Catalog column tag, preserving both sides of the transition."""

    column_name: str
    name: str
    desired_value: str
    observed_value: str | None

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_TAGS
    phase: ClassVar[ActionPhase] = ActionPhase.SET_COLUMN_TAG

    def __post_init__(self) -> None:
        if self.desired_value == self.observed_value:
            raise ValueError(f"SetColumnTag carries no difference: {self.desired_value!r}")

    @property
    def subject(self) -> str:
        return f"{self.column_name}.{self.name}"


@dataclass(frozen=True, slots=True)
class UnsetColumnTag(Action):
    """Remove a Unity Catalog tag from a column."""

    column_name: str
    name: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_TAGS
    phase: ClassVar[ActionPhase] = ActionPhase.UNSET_COLUMN_TAG

    @property
    def subject(self) -> str:
        return f"{self.column_name}.{self.name}"


@dataclass(frozen=True, slots=True)
class SetTableComment(Action):
    """Set a table comment, preserving its desired and observed values."""

    desired_comment: str
    observed_comment: str

    aspect: ClassVar[TableAspect] = TableAspect.TABLE_COMMENT
    phase: ClassVar[ActionPhase] = ActionPhase.SET_TABLE_COMMENT

    def __post_init__(self) -> None:
        if self.desired_comment == self.observed_comment:
            raise ValueError(f"SetTableComment carries no difference: {self.desired_comment!r}")

    @property
    def subject(self) -> str:
        return ""


@dataclass(frozen=True, slots=True)
class SetColumnNullability(Action):
    """Set a column's nullability, preserving both sides of the transition."""

    column_name: str
    desired_nullable: bool
    observed_nullable: bool

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE
    phase: ClassVar[ActionPhase] = ActionPhase.SET_COLUMN_NULLABILITY

    def __post_init__(self) -> None:
        if self.desired_nullable == self.observed_nullable:
            raise ValueError(
                f"SetColumnNullability carries no difference: {self.desired_nullable!r}"
            )

    @property
    def subject(self) -> str:
        return self.column_name


@dataclass(frozen=True, slots=True)
class DropPrimaryKey(Action):
    """Drop a complete observed primary key constraint."""

    primary_key: PrimaryKeyConstraint
    referencing_foreign_keys: tuple[ForeignKeyReference, ...]

    aspect: ClassVar[TableAspect] = TableAspect.PRIMARY_KEY
    phase: ClassVar[ActionPhase] = ActionPhase.DROP_PRIMARY_KEY

    @property
    def subject(self) -> str:
        return ""


@dataclass(frozen=True, slots=True)
class SetPrimaryKey(Action):
    """Set a complete declared primary key constraint."""

    primary_key: PrimaryKeyConstraint

    aspect: ClassVar[TableAspect] = TableAspect.PRIMARY_KEY
    phase: ClassVar[ActionPhase] = ActionPhase.SET_PRIMARY_KEY

    @property
    def subject(self) -> str:
        return ""


@dataclass(frozen=True, slots=True)
class DropForeignKey(Action):
    """Drop a complete observed foreign key constraint."""

    constraint: ForeignKeyConstraint

    aspect: ClassVar[TableAspect] = TableAspect.FOREIGN_KEYS
    phase: ClassVar[ActionPhase] = ActionPhase.DROP_FOREIGN_KEY

    @property
    def subject(self) -> str:
        return self.constraint.constraint_name


@dataclass(frozen=True, slots=True)
class SetForeignKey(Action):
    """Set a complete declared foreign key constraint."""

    constraint: ForeignKeyConstraint

    aspect: ClassVar[TableAspect] = TableAspect.FOREIGN_KEYS
    phase: ClassVar[ActionPhase] = ActionPhase.SET_FOREIGN_KEY

    @property
    def subject(self) -> str:
        return ",".join(self.constraint.local_columns)


@dataclass(frozen=True, slots=True)
class AlterClustering(Action):
    """Set or clear liquid-clustering keys, preserving desired and observed state."""

    desired_clustering: tuple[str, ...]
    observed_clustering: tuple[str, ...]

    aspect: ClassVar[TableAspect] = TableAspect.CLUSTERING
    phase: ClassVar[ActionPhase] = ActionPhase.SET_CLUSTERING

    def __post_init__(self) -> None:
        if set(self.desired_clustering) == set(self.observed_clustering):
            raise ValueError(f"AlterClustering carries no difference: {self.desired_clustering!r}")

    @property
    def subject(self) -> str:
        return ""


@dataclass(frozen=True, slots=True)
class AlterColumnType(Action):
    """Alter a column type, preserving desired and observed data types."""

    column_name: str
    desired_type: DataType
    observed_type: DataType

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE
    phase: ClassVar[ActionPhase] = ActionPhase.ALTER_COLUMN_TYPE

    def __post_init__(self) -> None:
        if self.desired_type == self.observed_type:
            raise ValueError(f"AlterColumnType carries no difference: {self.desired_type!r}")

    @property
    def subject(self) -> str:
        return self.column_name


def _execution_order(action: Action) -> tuple[int, str]:
    """Deterministic ordering key for an action: execution phase, then subject name."""
    return (action.phase, action.subject)


@dataclass(frozen=True, slots=True)
class ActionPlan:
    """
    Validated executable actions held in deterministic execution order.

    A plan carries the qualified table target its actions apply to and keeps
    those actions sorted by execution phase and then by subject name,
    regardless of the order they are supplied in. Target identity and ordering
    are invariants of the plan, not context a caller has to supply later.

    ``kind`` is the relation kind of the table the actions lower against —
    part of what the plan means, since the same actions compile to different
    statements per kind. Defaults to the ordinary table kind.
    """

    target: QualifiedName
    actions: tuple[Action, ...] = ()
    kind: TableKind = TableKind.TABLE

    def __post_init__(self) -> None:
        """Validate the creation target and sort actions, preserving order on ties."""
        ordered_actions = tuple(sorted(self.actions, key=_execution_order))

        for action in ordered_actions:
            if isinstance(action, CreateTable) and action.table.qualified_name != self.target:
                raise ValueError(
                    "CreateTable target does not match ActionPlan target:"
                    f" {action.table.qualified_name} != {self.target}"
                )
        object.__setattr__(self, "actions", ordered_actions)

    def __len__(self) -> int:
        return len(self.actions)

    def __bool__(self) -> bool:
        return bool(self.actions)

    def __iter__(self) -> Iterator[Action]:
        return iter(self.actions)
