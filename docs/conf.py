from pathlib import Path

project = "delta-engine"
author = "Tomos Corbin"
release = "0.1.0"

extensions = [
    "autoapi.extension",
    "sphinx.ext.napoleon",
    "sphinx_copybutton",
    "sphinxcontrib.mermaid",
    "myst_parser",
]

myst_enable_extensions = ["colon_fence"]
myst_heading_anchors = 3
myst_fence_as_directive = ["mermaid"]

html_theme = "furo"
html_title = "delta-engine"

# sphinx-autoapi documents the package by static analysis: nothing is
# imported at build time, so pyspark/delta need no mocking.
autoapi_dirs = [str(Path(__file__).parent.parent / "src" / "delta_engine")]
autoapi_root = "autoapi"
autoapi_add_toctree_entry = False
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
]
autoapi_member_order = "bysource"
autoapi_python_class_content = "class"

napoleon_use_ivar = True

exclude_patterns = ["_build", "superpowers", "todo"]
