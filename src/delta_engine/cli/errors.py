"""Typed CLI failures: anticipated errors that print a message instead of a traceback."""


class ConfigError(Exception):
    """
    An anticipated CLI-level failure: bad spec, missing settings, failed connect.

    Rendered as a single ``error: ...`` line on stderr with exit code 1 —
    never as a traceback.
    """
