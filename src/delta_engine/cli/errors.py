"""Typed CLI failures: anticipated errors that print a message instead of a traceback."""


class ConfigError(Exception):
    """
    An anticipated CLI-level failure: bad spec, missing settings, failed connect.

    Rendered as a single ``error: ...`` line on stderr with exit code 1 —
    never as a traceback.
    """


class DeclarationImportError(Exception):
    """
    A declarations module raised while being imported.

    This is the user's bug, so the CLI prints the original traceback (carried
    on ``__cause__``) under a header naming the module.
    """

    def __init__(self, module_name: str) -> None:
        super().__init__(f"importing declarations module '{module_name}' failed")
        self.module_name = module_name
