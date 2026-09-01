"""The command-independent surface of the ``delta-engine`` application."""

import delta_engine
from delta_engine.cli.app import app


def test_help_and_version_keep_the_minimal_public_surface(runner):
    # When asking for the global help and version
    help_result = runner.invoke(app, ["--help"])
    version_result = runner.invoke(app, ["--version"])

    # Then help lists exactly the offered commands (no completion command)
    # and version prints the installed package version
    assert help_result.exit_code == 0
    assert "plan" in help_result.stdout
    assert "generate" in help_result.stdout
    assert "apply" in help_result.stdout
    assert "lint" in help_result.stdout
    assert "completion" not in help_result.stdout
    assert version_result.exit_code == 0
    assert delta_engine.__version__ in version_result.stdout
