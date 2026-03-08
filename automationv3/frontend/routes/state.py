"""Frontend route runtime state access."""

from __future__ import annotations

from flask import current_app
from werkzeug.local import LocalProxy

from automationv3.frontend.helpers.context import FrontendHelperContext

FRONTEND_CTX_KEY = "automationv3.frontend_ctx"


def set_frontend_ctx(app, ctx: FrontendHelperContext) -> None:
    app.extensions[FRONTEND_CTX_KEY] = ctx


def get_frontend_ctx() -> FrontendHelperContext:
    ctx = current_app.extensions.get(FRONTEND_CTX_KEY)
    if not isinstance(ctx, FrontendHelperContext):
        raise RuntimeError("Frontend context not initialized")
    return ctx


frontend_ctx = LocalProxy(get_frontend_ctx)


__all__ = ["FRONTEND_CTX_KEY", "set_frontend_ctx", "get_frontend_ctx", "frontend_ctx"]
