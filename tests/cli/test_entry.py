"""The console-script shim: graceful degradation without the [cli] extra."""

import sys

import pytest

from delta_engine.cli import main


def test_missing_typer_prints_an_install_hint_instead_of_a_traceback(monkeypatch, capsys):
    # Given an environment where typer cannot be imported (a None sys.modules
    # entry makes the import raise ImportError with name="typer")
    monkeypatch.delitem(sys.modules, "delta_engine.cli.app", raising=False)
    monkeypatch.setitem(sys.modules, "typer", None)

    # When running the entry point
    with pytest.raises(SystemExit) as excinfo:
        main()

    # Then the user gets the install hint, not a stack trace
    assert excinfo.value.code == 1
    assert 'pip install "delta-engine[cli]"' in capsys.readouterr().err


def test_an_import_error_from_our_own_modules_propagates(monkeypatch):
    # A bug inside delta-engine must not be masked by the install hint
    monkeypatch.delitem(sys.modules, "delta_engine.cli.app", raising=False)
    monkeypatch.setitem(sys.modules, "delta_engine.application", None)

    with pytest.raises(ImportError):
        main()
