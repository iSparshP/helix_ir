import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

project = "helix-ir"
author = "Sparsh Prakash"
copyright = "2026, Sparsh Prakash"
release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pyarrow": ("https://arrow.apache.org/docs/python/", None),
}

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "furo"
html_static_path = ["_static"]

autodoc_member_order = "bysource"
napoleon_google_docstring = True
napoleon_numpy_docstring = False
