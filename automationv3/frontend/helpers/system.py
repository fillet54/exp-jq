"""System/documentation frontend helper functions."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from flask import abort, send_from_directory

from .context import FrontendHelperContext


def serve_docs_asset(ctx: FrontendHelperContext, asset_path: str) -> Any:
    docs_dir_str = ctx.app.config.get("DOCS_HTML_DIR")
    docs_status = ctx.app.config.get("DOCS_STATUS", {})
    if not docs_dir_str:
        return "Documentation is not configured.", 503

    docs_dir = Path(docs_dir_str).resolve()
    has_built_docs = docs_dir.exists() and (docs_dir / "index.html").is_file()
    if (not docs_status.get("built") and not has_built_docs) or not docs_dir.exists():
        details = docs_status.get("message") or "Sphinx docs have not been generated."
        return f"Documentation unavailable: {details}", 503

    requested = (docs_dir / asset_path).resolve()
    try:
        requested.relative_to(docs_dir)
    except ValueError:
        abort(404)

    if not requested.is_file():
        abort(404)
    return send_from_directory(str(docs_dir), asset_path)
