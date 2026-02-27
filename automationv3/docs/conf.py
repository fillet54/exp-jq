from datetime import datetime
import importlib
import pkgutil
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
DOCS_SOURCE = Path(__file__).resolve().parent

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


def _discover_plugin_doc_roots():
    """Return ``[(plugin_name, docs_dir), ...]`` for installed automationv3 plugins."""
    try:
        import automationv3.plugins as plugins_pkg
    except Exception:
        return []

    roots = []
    for _, modname, _ in pkgutil.iter_modules(
        plugins_pkg.__path__, plugins_pkg.__name__ + "."
    ):
        try:
            module = importlib.import_module(modname)
        except Exception:
            continue
        module_paths = getattr(module, "__path__", None)
        if not module_paths:
            continue
        plugin_name = modname.rsplit(".", 1)[-1]
        for module_path in module_paths:
            docs_dir = Path(module_path) / "docs"
            if docs_dir.is_dir():
                roots.append((plugin_name, docs_dir))
                break
    return roots


def _stage_plugin_docs():
    """Copy plugin-local docs into ``docs/_generated/plugins`` for Sphinx build."""
    generated_root = DOCS_SOURCE / "_generated" / "plugins"
    if generated_root.exists():
        shutil.rmtree(generated_root)
    generated_root.mkdir(parents=True, exist_ok=True)

    toc_entries = []
    for plugin_name, docs_dir in sorted(_discover_plugin_doc_roots()):
        target_dir = generated_root / plugin_name
        shutil.copytree(docs_dir, target_dir, dirs_exist_ok=True)
        index_file = target_dir / "index.rst"
        if index_file.exists():
            toc_entries.append(f"{plugin_name}/index")
            continue

        rst_files = sorted(
            file.relative_to(target_dir).with_suffix("")
            for file in target_dir.rglob("*.rst")
        )
        if not rst_files:
            continue
        fallback_lines = [
            f"{plugin_name} Plugin",
            "=" * (len(plugin_name) + len(" Plugin")),
            "",
            ".. toctree::",
            "   :maxdepth: 2",
            "",
        ]
        for rel in rst_files:
            fallback_lines.append(f"   {rel.as_posix()}")
        index_file.write_text("\n".join(fallback_lines).rstrip() + "\n", encoding="utf-8")
        toc_entries.append(f"{plugin_name}/index")

    plugin_index = generated_root / "index.rst"
    lines = [
        "Plugin Documentation",
        "====================",
        "",
        "This section is generated from plugin-local ``docs/`` folders.",
        "",
    ]
    if toc_entries:
        lines.extend(
            [
                ".. toctree::",
                "   :maxdepth: 2",
                "",
            ]
        )
        for entry in toc_entries:
            lines.append(f"   {entry}")
    else:
        lines.append("No plugin documentation was discovered.")
    plugin_index.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


_stage_plugin_docs()
