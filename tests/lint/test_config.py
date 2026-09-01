"""Behavioural tests for parsing [tool.delta-engine.lint] into a lint policy."""

import pytest

from delta_engine.lint import LintConfigError, LintPolicy, Severity, parse_lint_config
from delta_engine.lint.rules import NamingConventionRule, RequiredTagRule


def severities_by_rule(policy: LintPolicy) -> dict[str, Severity]:
    return {configured.rule.name: configured.severity for configured in policy.rules}


class TestDefaults:
    def test_empty_config_enables_the_default_rules_as_errors(self) -> None:
        # Given / When
        policy = parse_lint_config({})

        # Then
        assert severities_by_rule(policy) == {
            "table-comment": Severity.ERROR,
            "column-comment": Severity.ERROR,
            "primary-key": Severity.ERROR,
        }


class TestSeverityOverrides:
    def test_a_rule_can_be_downgraded_to_a_warning(self) -> None:
        # Given / When
        policy = parse_lint_config({"column-comment": "warning"})

        # Then
        assert severities_by_rule(policy)["column-comment"] is Severity.WARNING

    def test_an_invalid_severity_is_rejected(self) -> None:
        # Given / When / Then
        with pytest.raises(LintConfigError):
            parse_lint_config({"table-comment": "fatal"})

    def test_an_unknown_rule_id_is_rejected(self) -> None:
        # Given / When / Then
        with pytest.raises(LintConfigError):
            parse_lint_config({"primary_key": "error"})


class TestRequiredTag:
    def test_required_tag_is_enabled_by_listing_keys(self) -> None:
        # Given / When
        policy = parse_lint_config({"required-tag": {"keys": ["owner", "domain"]}})

        # Then
        rules = {configured.rule.name: configured.rule for configured in policy.rules}
        rule = rules["required-tag"]
        assert isinstance(rule, RequiredTagRule)
        assert rule.keys == ("owner", "domain")
        assert severities_by_rule(policy)["required-tag"] is Severity.ERROR

    def test_required_tag_severity_can_be_downgraded(self) -> None:
        # Given / When
        policy = parse_lint_config({"required-tag": {"keys": ["owner"], "severity": "warning"}})

        # Then
        assert severities_by_rule(policy)["required-tag"] is Severity.WARNING

    def test_required_tag_set_to_off_stays_absent(self) -> None:
        # Given / When
        policy = parse_lint_config({"required-tag": "off"})

        # Then
        assert "required-tag" not in severities_by_rule(policy)

    def test_required_tag_without_keys_is_rejected(self) -> None:
        # Given / When / Then
        with pytest.raises(LintConfigError):
            parse_lint_config({"required-tag": "error"})

    def test_required_tag_with_empty_keys_is_rejected(self) -> None:
        # Given / When / Then
        with pytest.raises(LintConfigError):
            parse_lint_config({"required-tag": {"keys": []}})

    def test_required_tag_severity_off_inside_the_table_disables_the_rule(self) -> None:
        # Given / When
        policy = parse_lint_config({"required-tag": {"keys": ["owner"], "severity": "off"}})

        # Then
        assert "required-tag" not in severities_by_rule(policy)


class TestNamingConvention:
    def test_it_is_off_by_default(self) -> None:
        # Given a config that does not mention the rule
        section: dict[str, object] = {}

        # When the config is parsed
        policy = parse_lint_config(section)

        # Then the rule is absent from the policy
        assert "naming-convention" not in severities_by_rule(policy)

    def test_a_custom_pattern_is_passed_through_to_the_rule(self) -> None:
        # Given a config that sets a custom pattern
        section = {"naming-convention": {"pattern": r"[A-Za-z][A-Za-z0-9]*"}}

        # When the config is parsed
        policy = parse_lint_config(section)

        # Then the rule carries that pattern
        rules = {configured.rule.name: configured.rule for configured in policy.rules}
        rule = rules["naming-convention"]
        assert isinstance(rule, NamingConventionRule)
        assert rule.pattern == r"[A-Za-z][A-Za-z0-9]*"

    def test_an_invalid_pattern_is_rejected(self) -> None:
        # Given a config with a pattern that is not a valid regular expression
        section = {"naming-convention": {"pattern": "[unclosed"}}

        # When the config is parsed
        # Then parsing fails
        with pytest.raises(LintConfigError):
            parse_lint_config(section)

    def test_it_can_be_turned_off(self) -> None:
        # Given a config that sets the rule to off
        section = {"naming-convention": "off"}

        # When the config is parsed
        policy = parse_lint_config(section)

        # Then the rule is absent from the policy
        assert "naming-convention" not in severities_by_rule(policy)


class TestInlineTableForm:
    def test_a_parameter_free_rule_accepts_the_inline_table_form(self) -> None:
        # Given / When
        policy = parse_lint_config({"table-comment": {"severity": "warning"}})

        # Then
        assert severities_by_rule(policy)["table-comment"] is Severity.WARNING

    def test_unknown_parameters_for_a_rule_are_rejected(self) -> None:
        # Given / When / Then
        with pytest.raises(LintConfigError):
            parse_lint_config({"table-comment": {"keys": ["owner"]}})


class TestDeclarationsKey:
    def test_the_reserved_declarations_key_carries_no_policy(self) -> None:
        # Given / When
        policy = parse_lint_config({"declarations": "pkg.tables:all_tables"})

        # Then it parses exactly as if the key were absent
        assert severities_by_rule(policy) == severities_by_rule(parse_lint_config({}))
