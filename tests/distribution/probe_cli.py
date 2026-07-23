"""Smoke-test the installed CLI extra and console-script entry point."""

from __future__ import annotations

import argparse
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path
import subprocess
import sys


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-version", required=True)
    return parser.parse_args()


def _console_script() -> Path:
    suffix = ".exe" if sys.platform == "win32" else ""
    return Path(sys.executable).with_name(f"delta-engine{suffix}")


def _run_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [_console_script(), *arguments],
        capture_output=True,
        check=False,
        text=True,
    )


def _assert_distribution_absent(name: str) -> None:
    try:
        distribution(name)
    except PackageNotFoundError:
        return
    raise AssertionError(f"CLI extra unexpectedly installed {name}")


def main() -> None:
    arguments = _parse_args()

    help_result = _run_cli("--help")
    assert help_result.returncode == 0, help_result.stderr
    assert "Usage:" in help_result.stdout

    version_result = _run_cli("--version")
    assert version_result.returncode == 0, version_result.stderr
    assert version_result.stdout.strip() == arguments.expected_version

    _assert_distribution_absent("delta-spark")
    _assert_distribution_absent("pyspark")


if __name__ == "__main__":
    main()
