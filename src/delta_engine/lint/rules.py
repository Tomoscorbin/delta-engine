"""The built-in lint rules: each states facts about one desired table."""

from dataclasses import dataclass
from typing import ClassVar, Final, Protocol

from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model import DesiredTable


class LintRule(Protocol):
    """One governance policy evaluated per table."""

    @property
    def name(self) -> str:
        """The rule id used in config keys and finding output."""
        ...

    def evaluate(self, table: DesiredTable) -> tuple[str, ...]:
        """Return one message per way ``table`` breaks this rule."""
        ...


@dataclass(frozen=True, slots=True)
class TableCommentRule:
    """Every table has a non-blank comment."""

    name: ClassVar[str] = "table-comment"

    def evaluate(self, table: DesiredTable) -> tuple[str, ...]:
        """Report the table when its comment is blank."""
        if table.comment.strip():
            return ()
        return ("table has no comment",)


@dataclass(frozen=True, slots=True)
class ColumnCommentRule:
    """Every column has a non-blank comment."""

    name: ClassVar[str] = "column-comment"

    def evaluate(self, table: DesiredTable) -> tuple[str, ...]:
        """Report each column whose comment is blank, by name."""
        return tuple(
            f"column '{column.name}' has no comment"
            for column in table.columns
            if not column.comment.strip()
        )


@dataclass(frozen=True, slots=True)
class PrimaryKeyRule:
    """Every table declares a primary key."""

    name: ClassVar[str] = "primary-key"

    def evaluate(self, table: DesiredTable) -> tuple[str, ...]:
        """Report the table when it has no primary key."""
        if table.primary_key is not None:
            return ()
        return ("table has no primary key",)


@dataclass(frozen=True, slots=True)
class RequiredTagRule:
    """Every table carries each required tag key; values are not checked."""

    keys: ListOrTuple[str]
    name: ClassVar[str] = "required-tag"

    def __post_init__(self) -> None:
        if isinstance(self.keys, str):
            raise ValueError("'keys' must be a list of tag key strings, not a bare string")
        keys = tuple(self.keys)
        if not keys or not all(isinstance(key, str) and key.strip() for key in keys):
            raise ValueError("'keys' must be a non-empty list of tag key strings")
        object.__setattr__(self, "keys", keys)

    def evaluate(self, table: DesiredTable) -> tuple[str, ...]:
        """Report each required tag key missing from the table's tags."""
        return tuple(f"missing required tag '{key}'" for key in self.keys if key not in table.tags)


# Every rule the config section can name. A rule's dataclass fields are its
# config parameters: an inline TOML table is `severity` plus constructor
# keyword arguments, so no rule may have a field called `severity`. A rule
# constructible without arguments defaults to enabled at error severity; one
# with required fields stays off until configured. Registering a rule here is
# all the wiring it needs.
ALL_RULES: Final = (
    TableCommentRule,
    ColumnCommentRule,
    PrimaryKeyRule,
    RequiredTagRule,
)
