"""System/doc/health handlers."""

from __future__ import annotations

from typing import Any

from flask import Blueprint, redirect, url_for

from automationv3.frontend.helpers import system as system_helpers

from .state import frontend_ctx as ctx

bp = Blueprint("system", __name__)


@bp.route("/docs", methods=["GET"], endpoint="docs_index_redirect")
def docs_index_redirect() -> Any:
    return redirect(url_for("system.docs_index"), code=308)


@bp.route("/docs/", methods=["GET"], endpoint="docs_index")
def docs_index() -> Any:
    return system_helpers.serve_docs_asset(ctx, "index.html")


@bp.route("/docs/<path:asset_path>", methods=["GET"], endpoint="docs_asset")
def docs_asset(asset_path: str) -> Any:
    return system_helpers.serve_docs_asset(ctx, asset_path)


@bp.route("/health", methods=["GET"], endpoint="health")
def health() -> dict[str, str]:
    return {"status": "ok"}
