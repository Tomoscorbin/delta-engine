"""The console-script shim: graceful degradation without the [cli] extra."""

import sys

import pytest

from delta_engine.cli import main


def test_missing_typer_prints_an_install_hint_instead_of_a_traceback(monkeypatch, capsys):
    # Given an environment where typer cannot be imported (a None sys.modules
    # entry makes the import raise ModuleNotFoundError with name="typer")
    monkeypatch.delitem(sys.modules, "delta_engine.cli.app", raising=False)
    monkeypatch.setitem(sys.modules, "typer", None)

    # When running the entry point
    with pytest.raises(SystemExit) as excinfo:
        main()

    # Then the user gets the install hint, not a stack trace
    assert excinfo.value.code == 1
    assert 'pip install "delta-engine[cli]"' in capsys.readouterr().err


def test_an_import_error_from_our_own_modules_propagates(monkeypatch):
    # Given an environment where a delta-engine module itself fails to import
    monkeypatch.delitem(sys.modules, "delta_engine.cli.app", raising=False)
    monkeypatch.setitem(sys.modules, "delta_engine.application", None)

    # Then the bug propagates instead of being masked by the install hint
    with pytest.raises(ImportError):
        main()


def test_an_incompatible_installed_dependency_propagates_instead_of_the_install_hint(monkeypatch):
    # Given typer installed but incompatible: importing the app raises a
    # plain ImportError (missing symbol), not ModuleNotFoundError
    incompatible = ImportError("cannot import name 'Typer' from 'typer'", name="typer")

    class FailingAppImport:
        def find_spec(self, name, path=None, target=None):
            if name == "delta_engine.cli.app":
                raise incompatible
            return None

    monkeypatch.delitem(sys.modules, "delta_engine.cli.app", raising=False)
    monkeypatch.setattr(sys, "meta_path", [FailingAppImport(), *sys.meta_path])

    # Then the real failure propagates — the package is installed, so the
    # install hint would be wrong advice
    with pytest.raises(ImportError) as excinfo:
        main()
    assert excinfo.value is incompatible
