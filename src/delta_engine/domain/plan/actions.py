"""Canonical domain vocabulary for executable table actions."""

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from enum import IntEnum, auto
from typing import ClassVar

from delta_engine.domain.model import (
    Column,
    DataType,
    DesiredTable,
    ForeignKeyConstraint,
    ForeignKeyReference,
    ObservedColumn,
    PrimaryKeyConstraint,
    QualifiedName,
    TableAspect,
)


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
    UNSET_PROPERTY = auto()
    SET_TABLE_TAG = auto()
    UNSET_TABLE_TAG = auto()
    DROP_FOREIGN_KEY = auto()
    DROP_PRIMARY_KEY = auto()
    RENAME_COLUMN = auto()
    ADD_COLUMN = auto()
    ALTER_COLUMN_TYPE = auto()
    SET_CLUSTERING = auto()
    DROP_COLUMN = auto()
    SET_COLUMN_TAG = auto()
    UNSET_COLUMN_TAG = auto()
    SET_COLUMN_COMMENT = auto()
    SET_TABLE_COMMENT = auto()
    SET_COLUMN_NULLABILITY = auto()
    SET_PRIMARY_KEY = auto()
    SET_FOREIGN_KEY = auto()


class Action(ABC):
    """
    Base class for executable table actions.

    Every action carries the complete semantic state needed to validate and
    report it as well as the compiler-facing state needed to execute it. Each
    subclass declares its managed ``aspect``, execution ``phase``, and stable
    within-phase ``subject``.
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

    column: Column

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
    def column_name(self) -> str:
        """Observed column name consumed by action compilers."""
        return self.column.name

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
    def value(self) -> str:
        """Desired value consumed by action compilers."""
        return self.desired_value

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
    """Set a Unity Catalog tag on a table."""

    name: str
    value: str

    aspect: ClassVar[TableAspect] = TableAspect.TABLE_TAGS
    phase: ClassVar[ActionPhase] = ActionPhase.SET_TABLE_TAG

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
    def comment(self) -> str:
        """Desired comment consumed by action compilers."""
        return self.desired_comment

    @property
    def subject(self) -> str:
        return self.column_name


@dataclass(frozen=True, slots=True)
class SetColumnTag(Action):
    """Set a Unity Catalog tag on a column."""

    column_name: str
    name: str
    value: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_TAGS
    phase: ClassVar[ActionPhase] = ActionPhase.SET_COLUMN_TAG

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
    def comment(self) -> str:
        """Desired comment consumed by action compilers."""
        return self.desired_comment

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
    def nullable(self) -> bool:
        """Desired nullability consumed by action compilers."""
        return self.desired_nullable

    @property
    def subject(self) -> str:
        return self.column_name


@dataclass(frozen=True, slots=True)
class DropPrimaryKey(Action):
    """Drop an observed primary key, optionally as one half of a replacement."""

    primary_key: PrimaryKeyConstraint
    referencing_foreign_keys: tuple[ForeignKeyReference, ...]
    replacement_primary_key: PrimaryKeyConstraint | None = None

    aspect: ClassVar[TableAspect] = TableAspect.PRIMARY_KEY
    phase: ClassVar[ActionPhase] = ActionPhase.DROP_PRIMARY_KEY

    def __post_init__(self) -> None:
        object.__setattr__(self, "referencing_foreign_keys", tuple(self.referencing_foreign_keys))
        if self.replacement_primary_key is not None and set(self.primary_key.columns) == set(
            self.replacement_primary_key.columns
        ):
            raise ValueError(
                f"DropPrimaryKey replacement carries no difference: {self.primary_key!r}"
            )

    @property
    def observed_primary_key(self) -> PrimaryKeyConstraint:
        """The catalog constraint that will be dropped."""
        return self.primary_key

    @property
    def subject(self) -> str:
        return ""


@dataclass(frozen=True, slots=True)
class SetPrimaryKey(Action):
    """Set a complete primary key, optionally as one half of a replacement."""

    primary_key: PrimaryKeyConstraint
    replaced_primary_key: PrimaryKeyConstraint | None = None

    aspect: ClassVar[TableAspect] = TableAspect.PRIMARY_KEY
    phase: ClassVar[ActionPhase] = ActionPhase.SET_PRIMARY_KEY

    def __post_init__(self) -> None:
        if self.replaced_primary_key is not None and set(self.primary_key.columns) == set(
            self.replaced_primary_key.columns
        ):
            raise ValueError(
                f"SetPrimaryKey replacement carries no difference: {self.primary_key!r}"
            )

    @property
    def columns(self) -> tuple[str, ...]:
        """Primary-key columns consumed by action compilers."""
        return self.primary_key.columns

    @property
    def constraint_name(self) -> str:
        """Primary-key name consumed by action compilers."""
        return self.primary_key.constraint_name

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
    def constraint_name(self) -> str:
        """Catalog constraint name consumed by action compilers."""
        return self.constraint.constraint_name

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
    def local_columns(self) -> tuple[str, ...]:
        """Local columns consumed by action compilers."""
        return self.constraint.local_columns

    @property
    def referenced_table(self) -> QualifiedName:
        """Referenced table consumed by action compilers."""
        return self.constraint.referenced_table

    @property
    def referenced_columns(self) -> tuple[str, ...]:
        """Referenced columns consumed by action compilers."""
        return self.constraint.referenced_columns

    @property
    def constraint_name(self) -> str:
        """Constraint name consumed by action compilers."""
        return self.constraint.constraint_name

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
        object.__setattr__(self, "desired_clustering", tuple(self.desired_clustering))
        object.__setattr__(self, "observed_clustering", tuple(self.observed_clustering))
        if set(self.desired_clustering) == set(self.observed_clustering):
            raise ValueError(f"AlterClustering carries no difference: {self.desired_clustering!r}")

    @property
    def columns(self) -> tuple[str, ...]:
        """Desired clustering columns consumed by action compilers."""
        return self.desired_clustering

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
    def data_type(self) -> DataType:
        """Desired type consumed by action compilers."""
        return self.desired_type

    @property
    def subject(self) -> str:
        return self.column_name


def _execution_order(action: Action) -> tuple[int, str]:
    """Deterministic ordering key for an action: execution phase, then subject name."""
    return (action.phase, action.subject)


def _validate_primary_key_replacements(actions: tuple[Action, ...]) -> None:
    """Require both correlated actions for every primary-key replacement."""
    drops = {
        (action.primary_key, action.replacement_primary_key)
        for action in actions
        if isinstance(action, DropPrimaryKey) and action.replacement_primary_key is not None
    }
    sets = {
        (action.replaced_primary_key, action.primary_key)
        for action in actions
        if isinstance(action, SetPrimaryKey) and action.replaced_primary_key is not None
    }
    if drops != sets:
        raise ValueError("Primary-key replacement requires correlated drop and set actions")


@dataclass(frozen=True, slots=True)
class ActionPlan:
    """Validated executable actions held in deterministic execution order."""

    actions: tuple[Action, ...] = ()

    def __post_init__(self) -> None:
        """Reject non-actions, validate correlated operations, and establish order."""
        actions = tuple(self.actions)
        for action in actions:
            if not isinstance(action, Action):
                raise TypeError(
                    f"ActionPlan accepts only Action instances; received {type(action).__name__}"
                )
        _validate_primary_key_replacements(actions)
        object.__setattr__(self, "actions", tuple(sorted(actions, key=_execution_order)))

    def __len__(self) -> int:
        return len(self.actions)

    def __bool__(self) -> bool:
        return bool(self.actions)

    def __iter__(self) -> Iterator[Action]:
        return iter(self.actions)
