"""Behavioural tests for parsing [tool.delta-engine.lint] into a lint policy."""

import pytest

from delta_engine.lint import (
    LintConfigError,
    LintPolicy,
    RequiredTagRule,
    Severity,
    parse_lint_config,
)


def severities_by_rule(policy: LintPolicy) -> dict[str, Severity]:
    return {configured.rule.name: configured.severity for configured in policy.rules}


class TestDefaults:
    def test_empty_config_enables_the_parameter_free_rules_as_errors(self) -> None:
        # Given / When
        config = parse_lint_config({})

        # Then
        assert severities_by_rule(config.policy) == {
            "table-comment": Severity.ERROR,
            "column-comment": Severity.ERROR,
            "primary-key": Severity.ERROR,
        }

    def test_empty_config_carries_no_declarations_target(self) -> None:
        # Given / When
        config = parse_lint_config({})

        # Then
        assert config.declarations is None


class TestSeverityOverrides:
    def test_a_rule_can_be_downgraded_to_a_warning(self) -> None:
        # Given / When
        config = parse_lint_config({"column-comment": "warning"})

        # Then
        assert severities_by_rule(config.policy)["column-comment"] is Severity.WARNING

    def test_a_rule_set_to_off_is_absent_from_the_policy(self) -> None:
        # Given / When
        config = parse_lint_config({"primary-key": "off"})

        # Then
        assert "primary-key" not in severities_by_rule(config.policy)

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
        config = parse_lint_config({"required-tag": {"keys": ["owner", "domain"]}})

        # Then
        rules = {configured.rule.name: configured.rule for configured in config.policy.rules}
        rule = rules["required-tag"]
        assert isinstance(rule, RequiredTagRule)
        assert rule.keys == ("owner", "domain")
        assert severities_by_rule(config.policy)["required-tag"] is Severity.ERROR

    def test_required_tag_severity_can_be_downgraded(self) -> None:
        # Given / When
        config = parse_lint_config({"required-tag": {"keys": ["owner"], "severity": "warning"}})

        # Then
        assert severities_by_rule(config.policy)["required-tag"] is Severity.WARNING

    def test_required_tag_set_to_off_stays_absent(self) -> None:
        # Given / When
        config = parse_lint_config({"required-tag": "off"})

        # Then
        assert "required-tag" not in severities_by_rule(config.policy)

    def test_required_tag_without_keys_is_rejected(self) -> None:
        # Given / When / Then
        with pytest.raises(LintConfigError):
            parse_lint_config({"required-tag": "error"})

    def test_required_tag_with_empty_keys_is_rejected(self) -> None:
        # Given / When / Then
        with pytest.raises(LintConfigError):
            parse_lint_config({"required-tag": {"keys": []}})

    def test_an_unknown_setting_inside_required_tag_is_rejected(self) -> None:
        # Given / When / Then
        with pytest.raises(LintConfigError):
            parse_lint_config({"required-tag": {"key": ["owner"]}})


class TestShapeErrors:
    def test_an_inline_table_for_a_parameter_free_rule_is_rejected(self) -> None:
        # Given / When / Then
        with pytest.raises(LintConfigError):
            parse_lint_config({"table-comment": {"severity": "error"}})

    def test_a_non_string_declarations_target_is_rejected(self) -> None:
        # Given / When / Then
        with pytest.raises(LintConfigError):
            parse_lint_config({"declarations": ["pkg.tables:all_tables"]})


class TestDeclarationsTarget:
    def test_the_declarations_target_is_read_from_config(self) -> None:
        # Given / When
        config = parse_lint_config({"declarations": "pkg.tables:all_tables"})

        # Then
        assert config.declarations == "pkg.tables:all_tables"
