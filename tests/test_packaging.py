"""Distribution metadata and optional-dependency import boundaries."""

from importlib.metadata import entry_points, metadata, requires
import subprocess
import sys

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet


def test_distribution_requires_a_python_floor_without_an_upper_bound():
    # Given the published Requires-Python range
    python_range = SpecifierSet(metadata("delta-engine")["Requires-Python"])

    # Then it sets a minimum only — no ceiling that a future Python would trip
    assert {specifier.operator for specifier in python_range} == {">="}


def test_base_distribution_has_no_unconditional_runtime_dependencies():
    # Given the published requirements
    requirements = requires("delta-engine") or []

    # Then every one is gated behind an extra — a bare install pulls nothing
    assert requirements
    assert all("extra ==" in requirement for requirement in requirements)


def test_distribution_exposes_only_supported_extras():
    # Given the published extras
    extras = metadata("delta-engine").get_all("Provides-Extra") or []

    # Then only the supported install profiles are offered
    assert set(extras) == {"cli", "sql"}


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


def test_console_script_points_at_the_stdlib_only_shim():
    # Given the installed console scripts
    scripts = {
        entry_point.name: entry_point.value for entry_point in entry_points(group="console_scripts")
    }

    # Then delta-engine resolves through the shim that needs no optional
    # dependency to print its install hint
    assert scripts["delta-engine"] == "delta_engine.cli:main"


def test_base_cli_shim_and_databricks_facade_load_no_optional_dependencies():
    # Given a fresh interpreter importing the root, the CLI shim, and the
    # Databricks facade
    program = """
import sys

import delta_engine
import delta_engine.cli
import delta_engine.databricks

optional_modules = ("typer", "databricks.sdk", "databricks.sql", "pyspark", "delta", "py4j")
loaded = [name for name in optional_modules if name in sys.modules]
if loaded:
    raise AssertionError(f"optional modules loaded eagerly: {loaded}")
print("ok")
"""

    # When running it in a subprocess
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True)

    # Then no optional dependency was loaded on the way
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
