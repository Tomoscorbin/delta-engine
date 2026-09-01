"""Parse the ``[tool.delta-engine.lint]`` section into a lint policy."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.lint.findings import Severity
from delta_engine.lint.rules import ALL_RULES, LintRule

_OFF: Final = "off"
_SEVERITY_SETTING: Final = "severity"
_DECLARATIONS: Final = "declarations"
_KNOWN_SETTINGS: Final = (
    _DECLARATIONS,
    *(rule_type.name for rule_type in ALL_RULES),
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

    A rule's value is either a bare severity string or an inline table holding
    ``severity`` plus the rule's own parameters. A rule absent from the mapping
    falls back to its own default: enabled at error severity when the rule is
    enabled by default, off otherwise, so an empty mapping yields the defaults.
    The reserved ``declarations`` key is accepted but carries no policy; the CLI
    reads it to locate the declarations.

    Raises:
        LintConfigError: On an unknown setting, an invalid severity, or rule
            parameters the rule itself rejects.

    """
    for key in section:
        if key not in _KNOWN_SETTINGS:
            raise LintConfigError(
                f"unknown lint setting '{key}'; expected one of: " + ", ".join(_KNOWN_SETTINGS)
            )

    rules: list[ConfiguredRule] = []
    for rule_type in ALL_RULES:
        value = section.get(rule_type.name, _default_setting_for(rule_type))
        # A bare severity string is sugar for the inline-table form.
        settings = value if isinstance(value, Mapping) else {_SEVERITY_SETTING: value}
        severity = _parse_rule_severity(
            rule_type.name, settings.get(_SEVERITY_SETTING, Severity.ERROR.value)
        )
        if severity is None:
            continue
        parameters: dict[str, Any] = {
            key: item for key, item in settings.items() if key != _SEVERITY_SETTING
        }
        try:
            rule = rule_type(**parameters)
        except (TypeError, ValueError) as error:
            raise LintConfigError(f"{rule_type.name}: {error}") from None
        rules.append(ConfiguredRule(rule, severity))

    return LintPolicy(rules)


def _default_setting_for(rule_type: type[LintRule]) -> str:
    """Pick an absent rule's setting from whether the rule is enabled by default."""
    return Severity.ERROR.value if rule_type.enabled_by_default else _OFF


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
