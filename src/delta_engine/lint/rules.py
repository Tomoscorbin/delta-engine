"""The built-in lint rules: each states facts about one desired table."""

from dataclasses import dataclass
import re
from typing import ClassVar, Final, Protocol

from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model import DesiredTable


class LintRule(Protocol):
    """One governance policy evaluated per table."""

    enabled_by_default: ClassVar[bool]

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
    enabled_by_default: ClassVar[bool] = True

    def evaluate(self, table: DesiredTable) -> tuple[str, ...]:
        """Report the table when its comment is blank."""
        if table.comment.strip():
            return ()
        return ("table has no comment",)


@dataclass(frozen=True, slots=True)
class ColumnCommentRule:
    """Every column has a non-blank comment."""

    name: ClassVar[str] = "column-comment"
    enabled_by_default: ClassVar[bool] = True

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
    enabled_by_default: ClassVar[bool] = True

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
    enabled_by_default: ClassVar[bool] = False

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


# Snake_case: a lowercase letter followed by lowercase letters, digits, or
# underscores. The whole name must match, so hyphens, spaces, leading digits,
# and capitals are all reported.
_DEFAULT_NAME_PATTERN: Final = r"[a-z][a-z0-9_]*"


@dataclass(frozen=True, slots=True)
class NamingConventionRule:
    """Every table and column name matches a naming convention (snake_case by default)."""

    pattern: str = _DEFAULT_NAME_PATTERN
    name: ClassVar[str] = "naming-convention"
    enabled_by_default: ClassVar[bool] = False

    def __post_init__(self) -> None:
        if not self.pattern.strip():
            raise ValueError("'pattern' must not be blank")
        try:
            re.compile(self.pattern)
        except re.error as error:
            raise ValueError(f"'pattern' is not a valid regular expression: {error}") from None

    def evaluate(self, table: DesiredTable) -> tuple[str, ...]:
        """Report the table name and each column name the pattern does not fully match."""
        named_values: list[tuple[str, str]] = [("table", table.qualified_name.name)]
        named_values += [("column", str(column.name)) for column in table.columns]

        messages: list[str] = []
        for kind, value in named_values:
            if re.fullmatch(self.pattern, value) is None:
                messages.append(
                    f"{kind} name '{value}' does not match naming convention '{self.pattern}'"
                )
        return tuple(messages)


# Every rule the config section can name. A rule's dataclass fields are its
# config parameters: an inline TOML table is `severity` plus constructor
# keyword arguments, so no rule may have a field called `severity`. A rule's
# `enabled_by_default` decides whether it runs when the config is silent.
# Registering a rule here is all the wiring it needs.
ALL_RULES: Final = (
    TableCommentRule,
    ColumnCommentRule,
    PrimaryKeyRule,
    NamingConventionRule,
    RequiredTagRule,
)
