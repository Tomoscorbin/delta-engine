"""Behavioural tests for parsing [tool.delta-engine.lint] into a lint policy."""

import pytest

from delta_engine.domain.model import QualifiedName
from delta_engine.lint import LintConfigError, LintPolicy, Severity, parse_lint_config
from delta_engine.lint.rules import NamingConventionRule, RequiredTagRule


def severities_by_rule(policy: LintPolicy) -> dict[str, Severity]:
    return {configured.rule.name: configured.severity for configured in policy.rules}


def severities_for(policy: LintPolicy, table: QualifiedName) -> dict[str, Severity]:
    return {configured.rule.name: configured.severity for configured in policy.resolve_rules(table)}


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


class TestOverrides:
    def test_a_matching_override_turns_a_rule_off(self) -> None:
        # Given an override that turns primary-key off for bronze tables
        section = {"overrides": [{"tables": ["dev.bronze.*"], "primary-key": "off"}]}

        # When the config is parsed
        policy = parse_lint_config(section)

        # Then the rule is absent for a bronze table and unchanged elsewhere
        assert "primary-key" not in severities_for(policy, QualifiedName("dev", "bronze", "raw"))
        silver = QualifiedName("dev", "silver", "orders")
        assert severities_for(policy, silver)["primary-key"] is Severity.ERROR

    def test_a_matching_override_downgrades_severity(self) -> None:
        # Given an override that downgrades column-comment for bronze tables
        section = {"overrides": [{"tables": ["dev.bronze.*"], "column-comment": "warning"}]}

        # When the config is parsed
        policy = parse_lint_config(section)

        # Then the rule carries the downgraded severity for a bronze table
        bronze = QualifiedName("dev", "bronze", "raw")
        assert severities_for(policy, bronze)["column-comment"] is Severity.WARNING

    def test_overlapping_overrides_merge_per_rule_and_the_last_match_wins(self) -> None:
        # Given a broad bronze override and a narrower one for a single table
        section = {
            "overrides": [
                {"tables": ["dev.bronze.*"], "primary-key": "off", "column-comment": "warning"},
                {"tables": ["dev.bronze.raw_events"], "column-comment": "off"},
            ]
        }

        # When the config is parsed
        policy = parse_lint_config(section)

        # Then the narrow table keeps the broad block's settings for rules the
        # narrow block does not name, and the narrow block wins where both speak
        raw_events = severities_for(policy, QualifiedName("dev", "bronze", "raw_events"))
        assert "primary-key" not in raw_events
        assert "column-comment" not in raw_events
        other_bronze = severities_for(policy, QualifiedName("dev", "bronze", "raw_clicks"))
        assert "primary-key" not in other_bronze
        assert other_bronze["column-comment"] is Severity.WARNING

    def test_an_override_can_enable_a_rule_that_is_off_globally(self) -> None:
        # Given naming-convention off globally but enabled for gold tables
        section = {"overrides": [{"tables": ["prod.gold.*"], "naming-convention": "error"}]}

        # When the config is parsed
        policy = parse_lint_config(section)

        # Then the rule is in effect only for a gold table
        gold = QualifiedName("prod", "gold", "orders")
        assert severities_for(policy, gold)["naming-convention"] is Severity.ERROR
        bronze = QualifiedName("prod", "bronze", "orders")
        assert "naming-convention" not in severities_for(policy, bronze)

    def test_a_wildcard_matches_within_a_single_segment(self) -> None:
        # Given a pattern with a wildcard schema segment
        section = {"overrides": [{"tables": ["dev.*.orders"], "primary-key": "off"}]}

        # When the config is parsed
        policy = parse_lint_config(section)

        # Then the pattern matches across schemas but only the named table
        assert "primary-key" not in severities_for(policy, QualifiedName("dev", "silver", "orders"))
        assert "primary-key" not in severities_for(policy, QualifiedName("dev", "bronze", "orders"))
        payments = QualifiedName("dev", "silver", "payments")
        assert severities_for(policy, payments)["primary-key"] is Severity.ERROR

    def test_pattern_matching_ignores_case(self) -> None:
        # Given a pattern written with capitals
        section = {"overrides": [{"tables": ["DEV.Bronze.*"], "primary-key": "off"}]}

        # When the config is parsed
        policy = parse_lint_config(section)

        # Then it matches the canonical lowercase qualified name
        assert "primary-key" not in severities_for(policy, QualifiedName("dev", "bronze", "raw"))

    def test_a_pattern_without_three_segments_is_rejected(self) -> None:
        # Given a pattern that names only a catalog and a schema
        section = {"overrides": [{"tables": ["dev.bronze"], "primary-key": "off"}]}

        # When the config is parsed
        # Then parsing fails
        with pytest.raises(LintConfigError):
            parse_lint_config(section)

    def test_an_empty_tables_list_is_rejected(self) -> None:
        # Given an override that matches no tables
        section = {"overrides": [{"tables": [], "primary-key": "off"}]}

        # When the config is parsed
        # Then parsing fails
        with pytest.raises(LintConfigError):
            parse_lint_config(section)

    def test_an_override_without_rule_settings_is_rejected(self) -> None:
        # Given an override that names tables but sets no rule
        section = {"overrides": [{"tables": ["dev.bronze.*"]}]}

        # When the config is parsed
        # Then parsing fails
        with pytest.raises(LintConfigError):
            parse_lint_config(section)

    def test_an_unknown_rule_in_an_override_is_rejected(self) -> None:
        # Given an override that names a rule that does not exist
        section = {"overrides": [{"tables": ["dev.bronze.*"], "primary_key": "off"}]}

        # When the config is parsed
        # Then parsing fails
        with pytest.raises(LintConfigError):
            parse_lint_config(section)

    def test_invalid_rule_parameters_in_an_override_are_rejected(self) -> None:
        # Given an override whose rule parameters the rule itself rejects
        section = {
            "overrides": [
                {"tables": ["dev.bronze.*"], "naming-convention": {"pattern": "[unclosed"}}
            ]
        }

        # When the config is parsed
        # Then parsing fails, even though no table has been matched yet
        with pytest.raises(LintConfigError):
            parse_lint_config(section)

    def test_overrides_that_are_not_an_array_are_rejected(self) -> None:
        # Given an overrides value that is not an array of tables
        section = {"overrides": {"tables": ["dev.bronze.*"], "primary-key": "off"}}

        # When the config is parsed
        # Then parsing fails
        with pytest.raises(LintConfigError):
            parse_lint_config(section)


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
