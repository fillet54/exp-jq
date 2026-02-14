from datetime import datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

project = "jobqueue-app"
author = "jobqueue"
copyright = f"{datetime.now().year}, {author}"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinxcontrib.mermaid",
]

templates_path = []
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

try:
    import sphinx_rtd_theme  # noqa: F401

    html_theme = "sphinx_rtd_theme"
except Exception:
    # Keep docs buildable in environments where theme deps aren't installed yet.
    html_theme = "alabaster"

# Publish local static assets for docs pages.
html_static_path = ["_static"]

# Prefer local Mermaid bundles so docs can be built/viewed offline.
# `mermaid_use_local` is used by newer sphinxcontrib-mermaid versions.
mermaid_use_local = "_static/mermaid.esm.min.mjs"
# `mermaid_version = ""` disables CDN injection in older versions.
mermaid_version = ""
d3_use_local = "d3.min.js"
# `d3_version = ""` disables CDN injection in older versions.
d3_version = ""
html_js_files = [
    "d3.min.js",
    "mermaid.min.js",
]

autodoc_member_order = "bysource"
