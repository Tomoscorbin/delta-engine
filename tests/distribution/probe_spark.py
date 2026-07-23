"""Smoke-test the installed local Spark extra without starting a Spark session."""

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
    raise AssertionError(f"Spark extra unexpectedly installed {name}")


def main() -> None:
    arguments = _parse_args()

    import delta
    import pyspark

    from delta_engine.application import Engine
    from delta_engine.databricks import build_spark_engine

    assert version("delta-engine") == arguments.expected_version
    assert delta is not None
    assert pyspark is not None
    assert isinstance(build_spark_engine(object()), Engine)

    _assert_distribution_absent("databricks-sdk")
    _assert_distribution_absent("databricks-sql-connector")


if __name__ == "__main__":
    main()
