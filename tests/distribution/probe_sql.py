"""Smoke-test the installed SQL extra without contacting Databricks."""

from __future__ import annotations

import argparse
from importlib.metadata import PackageNotFoundError, distribution, version


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-version", required=True)
    return parser.parse_args()


def _assert_distribution_absent(name: str) -> None:
    try:
        distribution(name)
    except PackageNotFoundError:
        return
    raise AssertionError(f"SQL extra unexpectedly installed {name}")


def main() -> None:
    arguments = _parse_args()

    import databricks.sql

    from delta_engine.application import Engine
    from delta_engine.databricks import build_sql_engine

    assert version("delta-engine") == arguments.expected_version
    assert databricks.sql is not None
    assert isinstance(build_sql_engine(object()), Engine)

    _assert_distribution_absent("delta-spark")
    _assert_distribution_absent("pyspark")


if __name__ == "__main__":
    main()
