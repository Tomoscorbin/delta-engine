"""Parse the ``[tool.delta-engine.lint]`` section into a lint policy."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.lint.findings import Severity
from delta_engine.lint.rules import PARAMETER_FREE_RULES, LintRule, RequiredTagRule

_OFF: Final = "off"
_SEVERITIES: Final[Mapping[str, Severity]] = {severity.value: severity for severity in Severity}
_DECLARATIONS: Final = "declarations"
_KNOWN_SETTINGS: Final = (
    _DECLARATIONS,
    *(rule.name for rule in PARAMETER_FREE_RULES),
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


def parse_lint_config(section: Mapping[str, object]) -> LintPolicy:
    """
    Parse one ``[tool.delta-engine.lint]`` mapping into a ``LintPolicy``.

    An empty mapping yields the defaults: every parameter-free rule enabled at
    error severity and no ``required-tag`` rule (it cannot run without keys).
    The reserved ``declarations`` key is accepted but carries no policy; the
    CLI reads it to locate the declarations.

    Raises:
        LintConfigError: On an unknown setting, an invalid severity, or a
            malformed ``required-tag`` shape.

    """
    for key in section:
        if key not in _KNOWN_SETTINGS:
            raise LintConfigError(
                f"unknown lint setting '{key}'; expected one of: " + ", ".join(_KNOWN_SETTINGS)
            )

    rules: list[ConfiguredRule] = []
    for rule in PARAMETER_FREE_RULES:
        severity = _parse_rule_severity(rule.name, section.get(rule.name, Severity.ERROR.value))
        if severity is not None:
            rules.append(ConfiguredRule(rule, severity))
    required_tag = _parse_required_tag(section.get(RequiredTagRule.name))
    if required_tag is not None:
        rules.append(required_tag)

    return LintPolicy(tuple(rules))


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
