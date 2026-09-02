"""Command-line interface for delta-engine (requires the ``cli`` extra)."""

_INSTALL_HINT = 'delta-engine requires the CLI extra: pip install "delta-engine[cli]"'


def main() -> None:
    """
    Run the delta-engine CLI; degrade gracefully when the extra is missing.

    This module stays stdlib-only so the console script always starts. A
    missing third-party dependency — typer or anything it needs — becomes an
    install hint instead of a traceback. Everything else propagates: an
    import failure in delta-engine's own modules is a real bug, and an
    installed-but-incompatible dependency (module present, symbol gone —
    an ImportError that is not ModuleNotFoundError) needs its real
    traceback, not advice to install what is already installed.
    """
    try:
        from delta_engine.cli.app import app
    except ModuleNotFoundError as error:
        top_level_package = (error.name or "").partition(".")[0]
        if top_level_package in ("", "delta_engine"):
            raise
        import sys

        print(f"error: {_INSTALL_HINT}", file=sys.stderr)
        raise SystemExit(1) from None
    app()
