"""Command-line interface for delta-engine (requires the ``cli`` extra)."""

_CLI_DEPENDENCIES = {"typer", "click", "rich", "shellingham"}

_INSTALL_HINT = 'delta-engine requires the CLI extra: pip install "delta-engine[cli]"'


def main() -> None:
    """
    Run the delta-engine CLI; degrade gracefully when the extra is missing.

    This module stays stdlib-only so the console script always starts: the
    Typer app is imported lazily, and a missing CLI dependency becomes an
    install hint instead of a traceback. Any other ImportError is a real bug
    and propagates.
    """
    try:
        from delta_engine.cli.app import app
    except ImportError as error:
        if error.name not in _CLI_DEPENDENCIES:
            raise
        import sys

        print(f"error: {_INSTALL_HINT}", file=sys.stderr)
        raise SystemExit(1) from None
    app()
