"""Parse the ``[tool.delta-engine.lint]`` section into a lint policy."""

from collections.abc import Mapping
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import Any, Final

from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model import QualifiedName
from delta_engine.lint.findings import Severity
from delta_engine.lint.rules import ALL_RULES, LintRule

_OFF: Final = "off"
_SEVERITY_SETTING: Final = "severity"
_DECLARATIONS: Final = "declarations"
_OVERRIDES: Final = "overrides"
_TABLES_SETTING: Final = "tables"
_RULE_TYPES_BY_NAME: Final = {rule_type.name: rule_type for rule_type in ALL_RULES}
_KNOWN_SETTINGS: Final = (_DECLARATIONS, _OVERRIDES, *_RULE_TYPES_BY_NAME)


class LintConfigError(Exception):
    """An invalid lint configuration: unknown setting, bad severity, bad shape."""


@dataclass(frozen=True, slots=True)
class ConfiguredRule:
    """One enabled rule paired with the severity its findings carry."""

    rule: LintRule
    severity: Severity


@dataclass(frozen=True, slots=True)
class TablePattern:
    """
    A ``catalog.schema.table`` glob matched one segment at a time.

    Each segment is its own ``fnmatch`` pattern, so a ``*`` never crosses a
    dot: ``dev.bronze.*`` covers one schema, and every table in a catalog is
    spelled ``dev.*.*``. Segments are held lowercase, matching the canonical
    case of ``QualifiedName``.
    """

    catalog: str
    schema: str
    table: str

    def __post_init__(self) -> None:
        if not all(segment.strip() for segment in (self.catalog, self.schema, self.table)):
            raise ValueError("a pattern segment must not be blank")
        object.__setattr__(self, "catalog", self.catalog.lower())
        object.__setattr__(self, "schema", self.schema.lower())
        object.__setattr__(self, "table", self.table.lower())

    def matches(self, name: QualifiedName) -> bool:
        """Whether ``name`` matches, each segment against its own glob."""
        return (
            fnmatchcase(name.catalog, self.catalog)
            and fnmatchcase(name.schema, self.schema)
            and fnmatchcase(name.name, self.table)
        )


@dataclass(frozen=True, slots=True)
class PolicyOverride:
    """
    Rule settings applied to the tables matched by any of the patterns.

    ``settings`` maps a rule name to the configured rule that replaces the
    globally configured one, or to ``None`` to turn the rule off.
    """

    patterns: ListOrTuple[TablePattern]
    settings: Mapping[str, ConfiguredRule | None]

    def __post_init__(self) -> None:
        object.__setattr__(self, "patterns", tuple(self.patterns))
        object.__setattr__(self, "settings", dict(self.settings))

    def matches(self, name: QualifiedName) -> bool:
        """Whether any of the patterns matches ``name``."""
        return any(pattern.matches(name) for pattern in self.patterns)


@dataclass(frozen=True, slots=True)
class LintPolicy:
    """
    The globally enabled rules plus per-table overrides.

    ``rules`` applies to every table. Each override refines the rules it names
    for the tables it matches; ``resolve_rules`` answers what is in effect for
    one table. Disabled rules are simply absent.
    """

    rules: ListOrTuple[ConfiguredRule]
    overrides: ListOrTuple[PolicyOverride] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "rules", tuple(self.rules))
        object.__setattr__(self, "overrides", tuple(self.overrides))

    def resolve_rules(self, table: QualifiedName) -> tuple[ConfiguredRule, ...]:
        """
        Return the configured rules in effect for ``table``.

        Matching overrides apply in order on top of the global rules, each
        changing only the rules it names, so the last override to name a rule
        wins for that table.
        """
        effective: dict[str, ConfiguredRule | None] = {
            configured.rule.name: configured for configured in self.rules
        }
        for override in self.overrides:
            if override.matches(table):
                effective.update(override.settings)
        return tuple(configured for configured in effective.values() if configured is not None)


def parse_lint_config(section: Mapping[str, object]) -> LintPolicy:
    """
    Parse one ``[tool.delta-engine.lint]`` mapping into a ``LintPolicy``.

    A rule's value is either a bare severity string or an inline table holding
    ``severity`` plus the rule's own parameters. A rule absent from the mapping
    falls back to its own default: enabled at error severity when the rule is
    enabled by default, off otherwise, so an empty mapping yields the defaults.

    The ``overrides`` key is an array of tables. Each entry lists the tables it
    applies to as ``catalog.schema.table`` globs matched one dot-separated
    segment at a time, plus rule settings in the same shape as the top level.
    The reserved ``declarations`` key is accepted but carries no policy; the
    CLI reads it to locate the declarations.

    Raises:
        LintConfigError: On an unknown setting, an invalid severity, a
            malformed override, or rule parameters the rule itself rejects.

    """
    for key in section:
        if key not in _KNOWN_SETTINGS:
            raise LintConfigError(
                f"unknown lint setting '{key}'; expected one of: " + ", ".join(_KNOWN_SETTINGS)
            )

    rules: list[ConfiguredRule] = []
    for rule_type in ALL_RULES:
        value = section.get(rule_type.name, _default_setting_for(rule_type))
        configured = _parse_rule_setting(rule_type, value)
        if configured is not None:
            rules.append(configured)

    overrides = _parse_overrides(section.get(_OVERRIDES, ()))
    return LintPolicy(rules, overrides)


def _default_setting_for(rule_type: type[LintRule]) -> str:
    """Pick an absent rule's setting from whether the rule is enabled by default."""
    return Severity.ERROR.value if rule_type.enabled_by_default else _OFF


def _parse_rule_setting(rule_type: type[LintRule], value: object) -> ConfiguredRule | None:
    """Parse one rule's configured value; ``None`` means the rule is off."""
    # A bare severity string is sugar for the inline-table form.
    settings = value if isinstance(value, Mapping) else {_SEVERITY_SETTING: value}
    severity = _parse_rule_severity(
        rule_type.name, settings.get(_SEVERITY_SETTING, Severity.ERROR.value)
    )
    if severity is None:
        return None
    parameters: dict[str, Any] = {
        key: item for key, item in settings.items() if key != _SEVERITY_SETTING
    }
    try:
        rule = rule_type(**parameters)
    except (TypeError, ValueError) as error:
        raise LintConfigError(f"{rule_type.name}: {error}") from None
    return ConfiguredRule(rule, severity)


def _parse_overrides(value: object) -> tuple[PolicyOverride, ...]:
    """Parse the ``overrides`` array, prefixing any error with the entry's index."""
    if not isinstance(value, (list, tuple)):
        raise LintConfigError("'overrides' must be an array of override tables")
    overrides = []
    for index, entry in enumerate(value):
        try:
            overrides.append(_parse_override(entry))
        except LintConfigError as error:
            raise LintConfigError(f"overrides[{index}]: {error}") from None
    return tuple(overrides)


def _parse_override(entry: object) -> PolicyOverride:
    """Parse one overrides entry: table patterns plus the rule settings for them."""
    if not isinstance(entry, Mapping):
        raise LintConfigError("expected a table with 'tables' and rule settings")
    patterns = _parse_table_patterns(entry.get(_TABLES_SETTING))
    rule_values = {key: value for key, value in entry.items() if key != _TABLES_SETTING}
    if not rule_values:
        raise LintConfigError("an override must set at least one rule")
    settings: dict[str, ConfiguredRule | None] = {}
    for key, value in rule_values.items():
        rule_type = _RULE_TYPES_BY_NAME.get(key)
        if rule_type is None:
            raise LintConfigError(
                f"unknown rule '{key}'; expected one of: " + ", ".join(_RULE_TYPES_BY_NAME)
            )
        settings[key] = _parse_rule_setting(rule_type, value)
    return PolicyOverride(patterns, settings)


def _parse_table_patterns(value: object) -> tuple[TablePattern, ...]:
    """Parse one override's ``tables`` list into patterns."""
    if (
        not isinstance(value, (list, tuple))
        or not value
        or not all(isinstance(item, str) for item in value)
    ):
        raise LintConfigError("'tables' must be a non-empty list of qualified-name patterns")
    return tuple(_parse_table_pattern(item) for item in value)


def _parse_table_pattern(pattern: str) -> TablePattern:
    """Parse one ``catalog.schema.table`` glob string."""
    parts = pattern.split(".")
    if len(parts) != 3:
        raise LintConfigError(
            f"pattern '{pattern}' must have three dot-separated segments, like 'catalog.schema.*'"
        )
    catalog, schema, table = parts
    try:
        return TablePattern(catalog, schema, table)
    except ValueError as error:
        raise LintConfigError(f"pattern '{pattern}': {error}") from None


def _parse_rule_severity(name: str, value: object) -> Severity | None:
    """Parse one rule's configured severity; ``None`` means the rule is off."""
    if not isinstance(value, str):
        raise LintConfigError(
            f"{name}: expected a severity string ('error', 'warning', or 'off'), got {value!r}"
        )
    if value == _OFF:
        return None
    try:
        return Severity(value)
    except ValueError:
        raise LintConfigError(
            f"{name}: invalid severity {value!r}; expected 'error', 'warning', or 'off'"
        ) from None
