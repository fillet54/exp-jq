"""System/doc/health handlers and route registration."""

from __future__ import annotations

from typing import Any, Mapping

from flask import redirect, url_for


def register_system_routes(app, helpers: Mapping[str, Any]) -> None:
    _serve_docs_asset = helpers["_serve_docs_asset"]

    def docs_index_redirect() -> Any:
        return redirect(url_for("docs_index"), code=308)

    def docs_index() -> Any:
        return _serve_docs_asset("index.html")

    def docs_asset(asset_path: str) -> Any:
        return _serve_docs_asset(asset_path)

    def health() -> dict[str, str]:
        return {"status": "ok"}

    app.add_url_rule("/docs", endpoint="docs_index_redirect", view_func=docs_index_redirect, methods=["GET"])
    app.add_url_rule("/docs/", endpoint="docs_index", view_func=docs_index, methods=["GET"])
    app.add_url_rule("/docs/<path:asset_path>", endpoint="docs_asset", view_func=docs_asset, methods=["GET"])
    app.add_url_rule("/health", endpoint="health", view_func=health, methods=["GET"])
