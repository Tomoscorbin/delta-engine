"""Smoke-test the installed CLI from an isolated consumer environment."""

from __future__ import annotations

import argparse
from importlib.metadata import version
from pathlib import Path
import subprocess
import sys


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-typer-version")
    return parser.parse_args()


def _console_script() -> Path:
    suffix = ".exe" if sys.platform == "win32" else ""
    return Path(sys.executable).with_name(f"delta-engine{suffix}")


def _run(*arguments: str) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [_console_script(), *arguments],
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return result


def main() -> None:
    """Exercise the two dependency-independent CLI entry points."""
    arguments = _parse_args()

    typer_version = version("typer")
    if arguments.expected_typer_version is not None:
        assert typer_version == arguments.expected_typer_version

    help_result = _run("--help")
    assert "plan" in help_result.stdout

    version_result = _run("--version")
    assert version("delta-engine") in version_result.stdout

    print(f"smoke-tested CLI with Typer {typer_version}")


if __name__ == "__main__":
    main()
