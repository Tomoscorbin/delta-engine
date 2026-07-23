"""Validate built distributions from the perspective of an isolated consumer."""

from __future__ import annotations

import argparse
from configparser import ConfigParser
from dataclasses import dataclass
from email.message import Message
from email.parser import BytesParser
from email.policy import default
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
from tempfile import TemporaryDirectory
from typing import Final
import zipfile

_DISTRIBUTION_NAME: Final = "delta-engine"
_FILENAME_STEM: Final = "delta_engine"
_EXPECTED_EXTRAS: Final = frozenset({"cli", "spark", "sql"})
_EXPECTED_ENTRY_POINT: Final = "delta_engine.cli:main"
_PROBES_DIRECTORY: Final = Path(__file__).resolve().parents[1] / "tests" / "distribution"


@dataclass(frozen=True)
class _Artifacts:
    wheel: Path
    sdist: Path
    version: str

    @property
    def paths(self) -> tuple[Path, Path]:
        return (self.wheel, self.sdist)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dist-dir",
        type=Path,
        required=True,
        help="Directory containing exactly one delta-engine wheel and one sdist.",
    )
    parser.add_argument(
        "--expected-version",
        help="Require an exact release version and reject development/fallback versions.",
    )
    parser.add_argument(
        "--checksums-file",
        type=Path,
        default=Path("build/distribution.sha256"),
        help="File in which to record the validated artifact SHA-256 digests.",
    )
    parser.add_argument(
        "--version-file",
        type=Path,
        default=Path("build/distribution.version"),
        help="File in which to record the validated distribution version.",
    )
    return parser.parse_args()


def _require_one(paths: list[Path], description: str) -> Path:
    if len(paths) != 1:
        rendered = ", ".join(path.name for path in paths) or "none"
        raise ValueError(f"expected exactly one {description}, found: {rendered}")
    return paths[0].resolve()


def _wheel_version(path: Path) -> str:
    prefix = f"{_FILENAME_STEM}-"
    suffix = "-py3-none-any.whl"
    if not path.name.startswith(prefix) or not path.name.endswith(suffix):
        raise ValueError(f"wheel must be a py3-none-any delta-engine wheel: {path.name}")
    return path.name.removeprefix(prefix).removesuffix(suffix)


def _sdist_version(path: Path) -> str:
    prefix = f"{_FILENAME_STEM}-"
    suffix = ".tar.gz"
    if not path.name.startswith(prefix) or not path.name.endswith(suffix):
        raise ValueError(f"unexpected delta-engine sdist filename: {path.name}")
    return path.name.removeprefix(prefix).removesuffix(suffix)


def _find_artifacts(dist_dir: Path) -> _Artifacts:
    resolved = dist_dir.resolve()
    if not resolved.is_dir():
        raise ValueError(f"distribution directory does not exist: {resolved}")

    wheel = _require_one(sorted(resolved.glob(f"{_FILENAME_STEM}-*.whl")), "wheel")
    sdist = _require_one(sorted(resolved.glob(f"{_FILENAME_STEM}-*.tar.gz")), "sdist")
    wheel_version = _wheel_version(wheel)
    sdist_version = _sdist_version(sdist)
    if wheel_version != sdist_version:
        raise ValueError(
            f"wheel version {wheel_version!r} does not match sdist version {sdist_version!r}"
        )
    return _Artifacts(wheel=wheel, sdist=sdist, version=wheel_version)


def _parse_metadata(data: bytes, source: str) -> Message:
    metadata = BytesParser(policy=default).parsebytes(data)
    if metadata.get("Name") != _DISTRIBUTION_NAME:
        raise ValueError(f"{source} has unexpected Name metadata: {metadata.get('Name')!r}")
    return metadata


def _parse_message(data: bytes) -> Message:
    return BytesParser(policy=default).parsebytes(data)


def _require_metadata_version(metadata: Message, expected: str, source: str) -> None:
    actual = metadata.get("Version")
    if actual != expected:
        raise ValueError(f"{source} version {actual!r} does not match filename {expected!r}")


def _single_member(names: list[str], suffix: str, source: str) -> str:
    matches = [name for name in names if name.endswith(suffix)]
    if len(matches) != 1:
        rendered = ", ".join(matches) or "none"
        raise ValueError(f"{source} must contain one {suffix!r} member, found: {rendered}")
    return matches[0]


def _validate_dependencies(metadata: Message) -> None:
    extras = set(metadata.get_all("Provides-Extra", []))
    if extras != _EXPECTED_EXTRAS:
        raise ValueError(
            f"wheel extras {sorted(extras)!r} do not match {sorted(_EXPECTED_EXTRAS)!r}"
        )

    requirements = metadata.get_all("Requires-Dist", [])
    if not requirements:
        raise ValueError("wheel must contain the optional dependency metadata")
    unconditional = [requirement for requirement in requirements if "extra ==" not in requirement]
    if unconditional:
        raise ValueError(f"base wheel has unconditional dependencies: {unconditional!r}")
    for extra in _EXPECTED_EXTRAS:
        if not any(f"extra == '{extra}'" in requirement for requirement in requirements):
            raise ValueError(f"wheel has no dependency associated with the {extra!r} extra")


def _validate_entry_point(data: bytes) -> None:
    parser = ConfigParser(interpolation=None)
    parser.read_string(data.decode())
    actual = parser.get("console_scripts", "delta-engine", fallback=None)
    if actual != _EXPECTED_ENTRY_POINT:
        raise ValueError(
            f"delta-engine console script is {actual!r}, expected {_EXPECTED_ENTRY_POINT!r}"
        )


def _inspect_wheel(artifacts: _Artifacts) -> None:
    with zipfile.ZipFile(artifacts.wheel) as archive:
        names = archive.namelist()
        metadata_name = _single_member(names, ".dist-info/METADATA", "wheel")
        wheel_name = _single_member(names, ".dist-info/WHEEL", "wheel")
        entry_points_name = _single_member(names, ".dist-info/entry_points.txt", "wheel")
        metadata = _parse_metadata(archive.read(metadata_name), "wheel")
        _require_metadata_version(metadata, artifacts.version, "wheel")
        _validate_dependencies(metadata)
        _validate_entry_point(archive.read(entry_points_name))

        wheel_metadata = _parse_message(archive.read(wheel_name))
        if "py3-none-any" not in wheel_metadata.get_all("Tag", []):
            raise ValueError("wheel metadata does not declare the py3-none-any tag")

        required_members = {
            "delta_engine/__init__.py",
            "delta_engine/py.typed",
        }
        missing = required_members.difference(names)
        if missing:
            raise ValueError(f"wheel is missing required members: {sorted(missing)!r}")
        if not any(name.endswith(".dist-info/licenses/LICENSE") for name in names):
            raise ValueError("wheel does not contain the MIT LICENSE file")


def _inspect_sdist(artifacts: _Artifacts) -> None:
    with tarfile.open(artifacts.sdist, mode="r:gz") as archive:
        names = archive.getnames()
        roots = {name.partition("/")[0] for name in names if name}
        if len(roots) != 1:
            raise ValueError(
                f"sdist must contain one top-level directory, found: {sorted(roots)!r}"
            )
        root = roots.pop()
        expected_root = f"{_FILENAME_STEM}-{artifacts.version}"
        if root != expected_root:
            raise ValueError(f"sdist root {root!r} does not match {expected_root!r}")

        required_members = {
            f"{root}/LICENSE",
            f"{root}/README.md",
            f"{root}/pyproject.toml",
            f"{root}/src/delta_engine/__init__.py",
            f"{root}/src/delta_engine/py.typed",
        }
        missing = required_members.difference(names)
        if missing:
            raise ValueError(f"sdist is missing required members: {sorted(missing)!r}")

        pkg_info_name = _single_member(names, "/PKG-INFO", "sdist")
        extracted = archive.extractfile(pkg_info_name)
        if extracted is None:
            raise ValueError("could not read sdist PKG-INFO")
        metadata = _parse_metadata(extracted.read(), "sdist")
        _require_metadata_version(metadata, artifacts.version, "sdist")
        _validate_dependencies(metadata)


def _require_release_version(actual: str, expected: str | None) -> None:
    if expected is None:
        return
    if actual != expected:
        raise ValueError(f"built version {actual!r} does not match release version {expected!r}")
    lowered = actual.lower()
    if actual == "0.0.0" or ".dev" in lowered or "+" in actual:
        raise ValueError(f"release artifact has a non-release version: {actual!r}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_checksums(paths: tuple[Path, ...], destination: Path) -> dict[Path, str]:
    checksums = {path: _sha256(path) for path in paths}
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        "".join(f"{digest}  {path}\n" for path, digest in checksums.items()),
        encoding="utf-8",
    )
    return checksums


def _write_version(version: str, destination: Path) -> None:
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(f"{version}\n", encoding="utf-8")


def _environment_python(environment: Path) -> Path:
    if os.name == "nt":
        return environment / "Scripts" / "python.exe"
    return environment / "bin" / "python"


def _run(command: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(command), flush=True)
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    environment.pop("VIRTUAL_ENV", None)
    return subprocess.run(
        command,
        check=True,
        cwd=cwd,
        env=environment,
        text=True,
    )


def _install_requirement(
    uv: str,
    environment: Path,
    requirement: str,
    *,
    no_dependencies: bool = False,
) -> Path:
    _run([uv, "venv", "--python", sys.executable, str(environment)])
    python = _environment_python(environment)
    command = [uv, "pip", "install", "--python", str(python)]
    if no_dependencies:
        command.append("--no-deps")
    command.append(requirement)
    _run(command)
    return python


def _run_probe(
    python: Path,
    probe_name: str,
    version: str,
    *,
    working_directory: Path,
) -> None:
    _run(
        [
            str(python),
            "-I",
            str(_PROBES_DIRECTORY / probe_name),
            "--expected-version",
            version,
        ],
        cwd=working_directory,
    )


def _validate_installs(artifacts: _Artifacts) -> None:
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("uv must be available to create clean validation environments")

    with TemporaryDirectory(prefix="delta-engine-dist-") as temporary:
        root = Path(temporary)
        base_python = _install_requirement(
            uv,
            root / "base",
            str(artifacts.wheel),
            no_dependencies=True,
        )
        _run_probe(base_python, "probe_base.py", artifacts.version, working_directory=root)

        for extra, probe in (
            ("sql", "probe_sql.py"),
            ("cli", "probe_cli.py"),
            ("spark", "probe_spark.py"),
        ):
            requirement = f"{_DISTRIBUTION_NAME}[{extra}] @ {artifacts.wheel.resolve().as_uri()}"
            python = _install_requirement(uv, root / extra, requirement)
            _run_probe(python, probe, artifacts.version, working_directory=root)


def main() -> None:
    """Validate the two built artifacts and all supported installation paths."""
    arguments = _parse_args()
    artifacts = _find_artifacts(arguments.dist_dir)
    _require_release_version(artifacts.version, arguments.expected_version)
    _inspect_wheel(artifacts)
    _inspect_sdist(artifacts)
    initial_checksums = _write_checksums(artifacts.paths, arguments.checksums_file)
    _write_version(artifacts.version, arguments.version_file)
    _validate_installs(artifacts)
    final_checksums = {path: _sha256(path) for path in artifacts.paths}
    if final_checksums != initial_checksums:
        raise ValueError("distribution artifacts changed during validation")
    print(f"validated delta-engine {artifacts.version}", flush=True)


if __name__ == "__main__":
    main()
