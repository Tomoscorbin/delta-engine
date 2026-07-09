from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

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
myst_heading_anchors = 3
myst_fence_as_directive = ["mermaid"]

html_theme = "furo"
html_title = "delta-engine"

autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_mock_imports = ["pyspark", "delta"]
napoleon_use_ivar = True

exclude_patterns = ["_build", "superpowers", "todo"]
