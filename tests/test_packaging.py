"""
Distribution policies nothing else guards.

The wheel smoke tests in ``tests/distribution/`` prove a bare install behaves;
these two pins catch a dependency slipping into ``pyproject.toml`` unreviewed —
an unconditional dependency breaking the zero-dep core, or an optional one
without an upper bound.
"""

from importlib.metadata import requires

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet


def test_base_distribution_has_no_unconditional_runtime_dependencies():
    # Given the published requirements
    requirements = requires("delta-engine") or []

    # Then every one is gated behind an extra — a bare install pulls nothing
    assert requirements
    assert all("extra ==" in requirement for requirement in requirements)


def test_each_extra_pulls_its_packages_bounded_to_a_major_line():
    # Given the published optional requirements, resolved per extra
    parsed_requirements = [
        Requirement(requirement) for requirement in requires("delta-engine") or []
    ]

    def requirements_for(extra: str) -> dict[str, SpecifierSet]:
        return {
            requirement.name: requirement.specifier
            for requirement in parsed_requirements
            if requirement.marker is not None and requirement.marker.evaluate({"extra": extra})
        }

    sql_requirements = requirements_for("sql")
    cli_requirements = requirements_for("cli")

    # Then each extra pulls exactly its supported packages
    assert set(sql_requirements) == {"databricks-sql-connector"}
    assert set(cli_requirements) == {"databricks-sdk", "databricks-sql-connector", "typer"}

    # Then every optional dependency carries a floor and a ceiling, so a
    # breaking major release cannot resolve into a fresh install
    for specifier_set in {**sql_requirements, **cli_requirements}.values():
        assert {specifier.operator for specifier in specifier_set} == {">=", "<"}
