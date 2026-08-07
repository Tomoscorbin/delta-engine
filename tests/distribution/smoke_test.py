"""Smoke-test an installed base distribution as an isolated consumer."""

from __future__ import annotations

import argparse
from importlib.metadata import PackageNotFoundError, distribution, version
from pathlib import Path
import subprocess
import sys


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-version")
    return parser.parse_args()


def _console_script() -> Path:
    suffix = ".exe" if sys.platform == "win32" else ""
    return Path(sys.executable).with_name(f"delta-engine{suffix}")


def _assert_distribution_absent(name: str) -> None:
    try:
        distribution(name)
    except PackageNotFoundError:
        return
    raise AssertionError(f"base distribution unexpectedly installed {name}")


def main() -> None:
    """Exercise the dependency-free public surface and lazy-import contract."""
    arguments = _parse_args()

    import delta_engine
    from delta_engine import Engine
    import delta_engine.cli
    import delta_engine.databricks
    from delta_engine.schema import Column, DeltaTable, Integer, String

    installed_version = version("delta-engine")
    assert delta_engine.__version__ == installed_version
    if arguments.expected_version is not None:
        assert installed_version == arguments.expected_version
    source_root = Path(__file__).resolve().parents[2] / "src"
    assert not Path(delta_engine.__file__).resolve().is_relative_to(source_root)

    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="customers",
        columns=[
            Column("id", Integer(), nullable=False),
            Column("name", String()),
        ],
        primary_key=["id"],
        primary_key_name="customers_business_key",
    )
    assert table.to_desired_table().qualified_name.name == "customers"
    assert table.primary_key_name == "customers_business_key"
    assert Engine is not None

    for distribution_name in (
        "databricks-sdk",
        "databricks-sql-connector",
        "delta-spark",
        "pyspark",
        "typer",
    ):
        _assert_distribution_absent(distribution_name)

    optional_modules = (
        "databricks.sdk",
        "databricks.sql",
        "delta",
        "py4j",
        "pyspark",
        "typer",
    )
    loaded = [
        name
        for name in optional_modules
        if name in sys.modules or any(module.startswith(f"{name}.") for module in sys.modules)
    ]
    assert not loaded, f"optional modules loaded eagerly: {loaded}"

    result = subprocess.run(
        [_console_script(), "--help"],
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 1
    assert 'pip install "delta-engine[cli]"' in result.stderr
    assert "Traceback" not in result.stderr

    print(f"smoke-tested delta-engine {installed_version}")


if __name__ == "__main__":
    main()
