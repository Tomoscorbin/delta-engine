"""Parse the ``[tool.delta-engine.lint]`` section into a lint policy."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.lint.findings import Severity
from delta_engine.lint.rules import (
    ColumnCommentRule,
    LintRule,
    PrimaryKeyRule,
    RequiredTagRule,
    TableCommentRule,
)

_OFF: Final = "off"
_SEVERITIES: Final[Mapping[str, Severity]] = {severity.value: severity for severity in Severity}
_PARAMETER_FREE_RULES: Final[Mapping[str, type[LintRule]]] = {
    TableCommentRule.name: TableCommentRule,
    ColumnCommentRule.name: ColumnCommentRule,
    PrimaryKeyRule.name: PrimaryKeyRule,
}
_DECLARATIONS: Final = "declarations"
_KNOWN_SETTINGS: Final = (
    _DECLARATIONS,
    *_PARAMETER_FREE_RULES,
    RequiredTagRule.name,
)


class LintConfigError(Exception):
    """An invalid lint configuration: unknown setting, bad severity, bad shape."""


@dataclass(frozen=True, slots=True)
class ConfiguredRule:
    """One enabled rule paired with the severity its findings carry."""

    rule: LintRule
    severity: Severity


@dataclass(frozen=True, slots=True)
class LintPolicy:
    """The enabled rules for one lint run; disabled rules are simply absent."""

    rules: ListOrTuple[ConfiguredRule]

    def __post_init__(self) -> None:
        object.__setattr__(self, "rules", tuple(self.rules))


@dataclass(frozen=True, slots=True)
class LintConfig:
    """Everything the lint config section declares: the policy and the target."""

    policy: LintPolicy
    declarations: str | None = None


def parse_lint_config(section: Mapping[str, object]) -> LintConfig:
    """
    Parse one ``[tool.delta-engine.lint]`` mapping into a ``LintConfig``.

    An empty mapping yields the defaults: every parameter-free rule enabled at
    error severity and no ``required-tag`` rule (it cannot run without keys).

    Raises:
        LintConfigError: On an unknown setting, an invalid severity, or a
            malformed ``required-tag`` shape.

    """
    for key in section:
        if key not in _KNOWN_SETTINGS:
            raise LintConfigError(
                f"unknown lint setting '{key}'; expected one of: " + ", ".join(_KNOWN_SETTINGS)
            )

    rules = [
        ConfiguredRule(rule_type(), severity)
        for name, rule_type in _PARAMETER_FREE_RULES.items()
        if (severity := _parse_rule_severity(name, section.get(name, Severity.ERROR.value)))
        is not None
    ]
    required_tag = _parse_required_tag(section.get(RequiredTagRule.name))
    if required_tag is not None:
        rules.append(required_tag)

    return LintConfig(
        policy=LintPolicy(tuple(rules)),
        declarations=_parse_declarations_target(section.get(_DECLARATIONS)),
    )


def _parse_rule_severity(name: str, value: object) -> Severity | None:
    """Parse one rule's configured severity; ``None`` means the rule is off."""
    if not isinstance(value, str):
        raise LintConfigError(
            f"{name}: expected a severity string ('error', 'warning', or 'off'), got {value!r}"
        )
    if value == _OFF:
        return None
    severity = _SEVERITIES.get(value)
    if severity is None:
        raise LintConfigError(
            f"{name}: invalid severity {value!r}; expected 'error', 'warning', or 'off'"
        )
    return severity


def _parse_required_tag(value: object) -> ConfiguredRule | None:
    """Parse the ``required-tag`` setting; ``None`` means the rule is off."""
    if value is None or value == _OFF:
        return None
    if not isinstance(value, Mapping):
        raise LintConfigError(
            "required-tag requires tag keys; use"
            ' required-tag = { keys = ["owner"] } (or "off" to disable)'
        )
    unknown = [key for key in value if key not in ("keys", "severity")]
    if unknown:
        raise LintConfigError(
            f"required-tag: unknown setting '{unknown[0]}'; expected 'keys' and 'severity'"
        )
    keys = value.get("keys")
    if (
        not isinstance(keys, Sequence)
        or isinstance(keys, str)
        or not keys
        or not all(isinstance(key, str) and key.strip() for key in keys)
    ):
        raise LintConfigError("required-tag: 'keys' must be a non-empty list of tag key strings")
    severity = _parse_rule_severity(
        RequiredTagRule.name, value.get("severity", Severity.ERROR.value)
    )
    if severity is None:
        raise LintConfigError('required-tag: severity cannot be "off"; omit the rule to disable it')
    return ConfiguredRule(RequiredTagRule(keys=tuple(keys)), severity)


def _parse_declarations_target(value: object) -> str | None:
    """Parse the optional ``declarations`` MODULE:ATTRIBUTE target."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise LintConfigError(f"declarations: expected a 'module:attribute' string, got {value!r}")
    return value
