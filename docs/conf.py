import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

project = "delta-engine"
author = "Tomos Corbin"
release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
    "myst_parser",
]

myst_enable_extensions = ["colon_fence"]

html_theme = "furo"
html_title = "delta-engine"

autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_mock_imports = ["pyspark", "delta"]
napoleon_use_ivar = True

exclude_patterns = ["_build", "superpowers"]

# Suppress mermaid lexer warnings until sphinxcontrib-mermaid is added.
# Suppress toctree and cross-reference warnings for docs not yet wired into nav
# (navigation is assembled in a later task).
suppress_warnings = [
    "misc.highlighting_failure",
    "toc.not_included",
    "myst.xref_missing",
]
