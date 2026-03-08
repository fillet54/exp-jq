"""Frontend route registration for the jobqueue dashboard."""

from __future__ import annotations

import logging
from pathlib import Path

from flask import Flask

from automationv3.frontend.helpers.context import FrontendHelperContext
from automationv3.frontend.helpers import reports as report_helpers
from automationv3.frontend.helpers import scripts as script_helpers
from automationv3.frontend.routes import queue_bp, reports_bp, scripts_bp, system_bp
from automationv3.frontend.routes.state import set_frontend_ctx

# Backward-compatible test imports.
# Tests monkeypatch these symbols at `automationv3.jobqueue.views.*`.
shutil = report_helpers.shutil
subprocess = report_helpers.subprocess
docutils = report_helpers.docutils
_build_raw_source_rows = script_helpers.build_raw_source_rows
_build_script_directory_index = script_helpers.build_script_directory_index
_discover_scripts = script_helpers.discover_scripts
_parent_directory = script_helpers.parent_directory


def register_frontend_routes(
    app: Flask,
    queue,
    reporting_service,
    central,
    uut_store,
    suite_manager,
    scripts_root: Path,
    scripts_cache_dir: str,
    log: logging.Logger,
) -> None:
    reporting = reporting_service or queue
    ctx = FrontendHelperContext(
        app=app,
        queue=queue,
        reporting=reporting,
        central=central,
        uut_store=uut_store,
        suite_manager=suite_manager,
        scripts_root=scripts_root,
        scripts_cache_dir=scripts_cache_dir,
        log=log,
    )
    set_frontend_ctx(app, ctx)
    app.register_blueprint(queue_bp)
    app.register_blueprint(reports_bp)
    app.register_blueprint(scripts_bp)
    app.register_blueprint(system_bp)


__all__ = [
    "register_frontend_routes",
    "_build_raw_source_rows",
    "_build_script_directory_index",
    "_discover_scripts",
    "_parent_directory",
]
