"""Sphinx configuration for delta-engine."""

import importlib.util
from pathlib import Path
import sys

ROOT = Path(__file__).parent.parent
DOCS_ROOT = ROOT / "docs"

sys.path.insert(0, str(ROOT / "src"))

project = "delta-engine"
author = "Tomos Corbin"
release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
    "sphinxcontrib.mermaid",
    "myst_parser",
]

myst_enable_extensions = ["colon_fence"]
myst_fence_as_directive = ["mermaid"]

html_theme = "furo"
html_title = "delta-engine"

autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_mock_imports = ["pyspark", "delta"]
napoleon_use_ivar = True

exclude_patterns = ["_build", "superpowers", "todo"]


def _load_diagram_generator():
    spec = importlib.util.spec_from_file_location(
        "generate_architecture_diagrams",
        DOCS_ROOT / "generate_architecture_diagrams.py",
    )
    if spec is None or spec.loader is None:
        msg = "Could not load architecture diagram generator."
        raise RuntimeError(msg)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.generate


def setup(app):
    """Register build hooks."""
    app.connect("builder-inited", lambda app: _load_diagram_generator()())
    return {"parallel_read_safe": True}
