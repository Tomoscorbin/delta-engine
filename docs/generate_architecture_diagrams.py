"""Generate architecture diagrams included by the documentation."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "docs" / "_generated"


def main() -> None:
    """Regenerate checked-in architecture diagrams."""
    OUTPUT_DIR.mkdir(exist_ok=True)

    subprocess.run(
        [
            sys.executable,
            "-m",
            "pydeps",
            "src/delta_engine",
            "--only",
            "delta_engine",
            "--max-module-depth",
            "2",
            "--rankdir",
            "TB",
            "--noshow",
            "-T",
            "svg",
            "-o",
            str(OUTPUT_DIR / "imports.svg"),
        ],
        cwd=ROOT,
        check=True,
    )


if __name__ == "__main__":
    main()
