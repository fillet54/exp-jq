"""Frontend route registration for the jobqueue dashboard."""

from __future__ import annotations

import logging
from functools import partial
from pathlib import Path

from flask import Flask

from automationv3.framework.rst import collect_script_syntax_issues, render_script_rst_html
from automationv3.frontend.helpers.context import FrontendHelperContext
from automationv3.frontend.helpers import queue as queue_helpers
from automationv3.frontend.helpers import reports as report_helpers
from automationv3.frontend.helpers import scripts as script_helpers
from automationv3.frontend.helpers import system as system_helpers

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

    from automationv3.frontend.routes import (
        register_queue_routes,
        register_report_routes,
        register_script_routes,
        register_system_routes,
    )

    helper_context = {
        "UNSPECIFIED_SYSTEM": script_helpers.UNSPECIFIED_SYSTEM,
        "_build_job_output_context": partial(script_helpers.build_job_output_context, ctx),
        "_build_jobs_for_relpath": partial(script_helpers.build_jobs_for_relpath, ctx),
        "_build_queue_overview_context": partial(queue_helpers.build_queue_overview_context, ctx),
        "_build_raw_source_rows": script_helpers.build_raw_source_rows,
        "_build_report_export_context": partial(report_helpers.build_report_export_context, ctx),
        "_build_report_export_rst": report_helpers.build_report_export_rst,
        "_build_report_listing": report_helpers.build_report_listing,
        "_build_report_requirement_groups": partial(report_helpers.build_report_requirement_groups, ctx),
        "_build_script_directory_index": script_helpers.build_script_directory_index,
        "_build_script_system_index": script_helpers.build_script_system_index,
        "_coerce_positive_int": queue_helpers.coerce_positive_int,
        "_discover_scripts": script_helpers.discover_scripts,
        "_extract_rst_title": script_helpers.extract_rst_title,
        "_iter_completed_results_for_report": partial(report_helpers.iter_completed_results_for_report, ctx),
        "_parent_directory": script_helpers.parent_directory,
        "_queue_report_scripts": partial(report_helpers.queue_report_scripts, ctx),
        "_render_jobs_table": partial(queue_helpers.render_jobs_table, ctx),
        "_render_queue_overview_panel": partial(queue_helpers.render_queue_overview_panel, ctx),
        "_render_results_table": partial(queue_helpers.render_results_table, ctx),
        "_render_rst_pdf": partial(report_helpers.render_rst_pdf, ctx),
        "_render_suites_table": partial(queue_helpers.render_suites_table, ctx),
        "_render_uuts_table": partial(queue_helpers.render_uuts_table, ctx),
        "_render_workers_table": partial(queue_helpers.render_workers_table, ctx),
        "_resolve_rel_script_path": script_helpers.resolve_rel_script_path,
        "_safe_return_to": script_helpers.safe_return_to,
        "_serve_docs_asset": partial(system_helpers.serve_docs_asset, ctx),
        "_sorted_systems": script_helpers.sorted_systems,
        "central": central,
        "collect_script_syntax_issues": collect_script_syntax_issues,
        "log": log,
        "queue": queue,
        "render_script_rst_html": render_script_rst_html,
        "reporting": reporting,
        "scripts_cache_dir": scripts_cache_dir,
        "scripts_root": scripts_root,
        "suite_manager": suite_manager,
        "uut_store": uut_store,
    }

    register_report_routes(app, helper_context)
    register_queue_routes(app, helper_context)
    register_script_routes(app, helper_context)
    register_system_routes(app, helper_context)


__all__ = [
    "register_frontend_routes",
    "_build_raw_source_rows",
    "_build_script_directory_index",
    "_discover_scripts",
    "_parent_directory",
]
