"""Report handlers."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List

from flask import Blueprint, Response, redirect, render_template, request, url_for

from automationv3.framework.requirements import load_default_requirements
from automationv3.frontend.helpers import reports as report_helpers
from automationv3.frontend.helpers import scripts as script_helpers

from .state import frontend_ctx as ctx

bp = Blueprint("reports", __name__)


def _human_datetime(ts: Any) -> str:
    if ts is None:
        return "—"
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    except (TypeError, ValueError, OSError):
        return "—"


def _normalized_report_view(raw: Any, *, allow_script: bool = False) -> str:
    view = str(raw or "requirement").strip().lower()
    allowed = {"requirement"}
    if allow_script:
        allowed.add("script")
    return view if view in allowed else "requirement"


@bp.route("/reports", methods=["GET"], endpoint="reports_page")
def reports_page() -> str:
    report_records = ctx.reporting.list_reports(limit=2000)
    results = ctx.queue.list_results(limit=5000)
    pending = ctx.queue.list_jobs()
    reports = report_helpers.build_report_listing(
        results,
        report_records=report_records,
        pending_jobs=pending,
    )
    return render_template(
        "reports.html",
        page_title="AutomationV3 | Reports",
        reports=reports,
    )


@bp.route("/reports", methods=["POST"], endpoint="create_report")
def create_report() -> Any:
    title = (request.form.get("title") or "").strip()
    description = (request.form.get("description") or "").strip()
    return_to = script_helpers.safe_return_to(request.form.get("return_to") or "")
    if not title:
        return "title is required", 400
    report = ctx.reporting.create_report(title=title, description=description)
    if return_to:
        return redirect(return_to, code=303)
    return redirect(url_for("reports.report_detail_page", report_id=report["report_id"]), code=303)


@bp.route("/reports/<report_id>/delete", methods=["POST"], endpoint="delete_report")
def delete_report(report_id: str) -> Any:
    return_to = script_helpers.safe_return_to(request.form.get("return_to") or "")
    if not ctx.reporting.get_report(report_id):
        return "Unknown report", 404
    ctx.reporting.delete_report(report_id)
    if return_to:
        return redirect(return_to, code=303)
    return redirect(url_for("reports.reports_page"), code=303)


@bp.route("/reports/<report_id>", methods=["GET"], endpoint="report_detail_page")
def report_detail_page(report_id: str) -> str:
    report_view = "requirement"
    is_scratch_report = str(report_id or "").strip() == "__scratch__"
    requirement_text_map: Dict[str, str] = {}
    known_requirements: List[Dict[str, str]] = []
    try:
        loaded_requirements = load_default_requirements()
        requirement_text_map = {req.id: req.text for req in loaded_requirements}
        known_requirements = [
            {"id": req.id, "text": req.text, "system_id": req.system_id}
            for req in loaded_requirements
        ]
    except Exception:
        requirement_text_map = {}
        known_requirements = []

    report_meta = ctx.reporting.get_report(report_id)
    if not report_meta:
        return "Unknown report", 404

    if is_scratch_report:
        requirement_view = {
            "report_script_total": 0,
            "report_tracked_script_total": 0,
            "report_requirement_ids": [],
            "report_requirement_groups": [],
            "report_system_summaries": [],
            "report_requeue_script_paths": [],
            "has_report_queue_seed": False,
        }
        scratch_view = report_helpers.build_scratch_report_runs(ctx, report_id)
    else:
        requirement_view = report_helpers.build_report_requirement_groups(
            ctx,
            report_id=report_id,
            requirement_text_map=requirement_text_map,
        )
        scratch_view = {
            "scratch_runs": [],
            "scratch_run_total": 0,
            "scratch_script_total": 0,
        }
    completed = report_helpers.iter_completed_results_for_report(ctx, report_id)

    pending = [
        job
        for job in ctx.queue.list_jobs()
        if (job or {}).get("report_id") == report_id
    ]
    pending_rows = []
    for job in pending:
        row = dict(job)
        row["inserted_at_human"] = _human_datetime(job.get("inserted_at"))
        pending_rows.append(row)

    return render_template(
        "report_detail.html",
        page_title=f"AutomationV3 | {(report_meta or {}).get('title') or report_id}",
        report_id=report_id,
        report_meta=report_meta,
        report_view=report_view,
        is_scratch_report=is_scratch_report,
        report_results=completed,
        report_script_total=(
            scratch_view["scratch_script_total"]
            if is_scratch_report
            else requirement_view["report_script_total"]
        ),
        report_tracked_script_total=requirement_view["report_tracked_script_total"],
        report_requirement_ids=requirement_view["report_requirement_ids"],
        report_requirement_groups=requirement_view["report_requirement_groups"],
        report_system_summaries=requirement_view["report_system_summaries"],
        report_requeue_script_paths=requirement_view["report_requeue_script_paths"],
        can_queue_report_scripts=requirement_view["has_report_queue_seed"],
        scratch_runs=scratch_view["scratch_runs"],
        scratch_run_total=scratch_view["scratch_run_total"],
        scratch_script_total=scratch_view["scratch_script_total"],
        known_requirements=known_requirements,
        pending_jobs=pending_rows,
    )


@bp.route("/reports/<report_id>/export", methods=["GET"], endpoint="report_export_page")
def report_export_page(report_id: str) -> Any:
    report_meta = ctx.reporting.get_report(report_id)
    if not report_meta:
        return "Unknown report", 404
    export_context = report_helpers.build_report_export_context(ctx, report_id)
    auto_print = (request.args.get("print") or "").strip().lower() in {"1", "true", "yes", "on"}
    return render_template(
        "report_export.html",
        page_title=f"AutomationV3 | Report Export | {report_meta.get('title') or report_id}",
        auto_print=auto_print,
        **export_context,
    )


@bp.route("/reports/<report_id>/export.pdf", methods=["GET"], endpoint="report_export_pdf")
def report_export_pdf(report_id: str) -> Any:
    report_meta = ctx.reporting.get_report(report_id)
    if not report_meta:
        return "Unknown report", 404
    export_context = report_helpers.build_report_export_context(ctx, report_id)
    rst_export = report_helpers.build_report_export_rst(export_context)
    try:
        pdf_bytes = report_helpers.render_rst_pdf(
            ctx,
            rst_export,
            report_id=report_id,
            latest_completed_human=str(export_context.get("latest_completed_human") or "—"),
        )
    except Exception as exc:
        ctx.log.exception("PDF export failed for report %s: %s", report_id, exc)
        return f"PDF export unavailable: {exc}", 503
    safe_title = re.sub(r"[^A-Za-z0-9._-]+", "-", str(report_meta.get("title") or report_id)).strip("-")
    filename = f"{safe_title or report_id}.pdf"
    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )


@bp.route("/reports/<report_id>/requeue_all", methods=["POST"], endpoint="requeue_report_all")
def requeue_report_all(report_id: str) -> Any:
    report_view = _normalized_report_view(request.form.get("report_view"))
    return_to = script_helpers.safe_return_to(request.form.get("return_to") or "")
    if not ctx.reporting.get_report(report_id):
        return "Unknown report", 404
    requirement_view = report_helpers.build_report_requirement_groups(
        ctx,
        report_id=report_id,
        requirement_text_map={},
    )
    report_helpers.queue_report_scripts(ctx, report_id, requirement_view["report_requeue_script_paths"])
    if return_to:
        return redirect(return_to, code=303)
    return redirect(url_for("reports.report_detail_page", report_id=report_id, view=report_view), code=303)


@bp.route("/reports/<report_id>/requirements/add", methods=["POST"], endpoint="add_report_requirement")
def add_report_requirement(report_id: str) -> Any:
    report_view = _normalized_report_view(request.form.get("report_view"))
    return_to = script_helpers.safe_return_to(request.form.get("return_to") or "")
    if not ctx.reporting.get_report(report_id):
        return "Unknown report", 404
    requirement_id = str(request.form.get("requirement_id") or "").strip()
    if not requirement_id:
        return "requirement_id required", 400
    ctx.reporting.add_report_requirement(report_id, requirement_id)
    if return_to:
        return redirect(return_to, code=303)
    return redirect(url_for("reports.report_detail_page", report_id=report_id, view=report_view), code=303)


@bp.route("/reports/<report_id>/requirements/remove", methods=["POST"], endpoint="remove_report_requirement")
def remove_report_requirement(report_id: str) -> Any:
    report_view = _normalized_report_view(request.form.get("report_view"))
    return_to = script_helpers.safe_return_to(request.form.get("return_to") or "")
    if not ctx.reporting.get_report(report_id):
        return "Unknown report", 404
    requirement_id = str(request.form.get("requirement_id") or "").strip()
    if not requirement_id:
        return "requirement_id required", 400
    ctx.reporting.remove_report_requirement(report_id, requirement_id)
    if return_to:
        return redirect(return_to, code=303)
    return redirect(url_for("reports.report_detail_page", report_id=report_id, view=report_view), code=303)


@bp.route("/reports/<report_id>/requeue_script", methods=["POST"], endpoint="requeue_report_script")
def requeue_report_script(report_id: str) -> Any:
    report_view = _normalized_report_view(request.form.get("report_view"))
    return_to = script_helpers.safe_return_to(request.form.get("return_to") or "")
    if not ctx.reporting.get_report(report_id):
        return "Unknown report", 404
    script_path = str(request.form.get("script_path") or "").strip()
    if not script_path:
        return "script_path required", 400
    report_helpers.queue_report_scripts(ctx, report_id, [script_path])
    if return_to:
        return redirect(return_to, code=303)
    return redirect(url_for("reports.report_detail_page", report_id=report_id, view=report_view), code=303)


@bp.route("/reports/<report_id>/requeue_requirement", methods=["POST"], endpoint="requeue_report_requirement")
def requeue_report_requirement(report_id: str) -> Any:
    report_view = _normalized_report_view(request.form.get("report_view"))
    return_to = script_helpers.safe_return_to(request.form.get("return_to") or "")
    if not ctx.reporting.get_report(report_id):
        return "Unknown report", 404
    requirement_id = str(request.form.get("requirement_id") or "").strip()
    if not requirement_id:
        return "requirement_id required", 400
    requirement_view = report_helpers.build_report_requirement_groups(
        ctx,
        report_id=report_id,
        requirement_text_map={},
    )
    script_paths: List[str] = []
    for group in requirement_view["report_requirement_groups"]:
        if str(group.get("requirement") or "").strip() != requirement_id:
            continue
        script_paths = list(group.get("script_paths") or [])
        break
    if not script_paths:
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("reports.report_detail_page", report_id=report_id, view=report_view), code=303)
    report_helpers.queue_report_scripts(ctx, report_id, script_paths)
    if return_to:
        return redirect(return_to, code=303)
    return redirect(url_for("reports.report_detail_page", report_id=report_id, view=report_view), code=303)


@bp.route("/reports/<report_id>/clear_results", methods=["POST"], endpoint="clear_report_results")
def clear_report_results(report_id: str) -> Any:
    report_view = _normalized_report_view(request.form.get("report_view"), allow_script=True)
    return_to = script_helpers.safe_return_to(request.form.get("return_to") or "")
    if not ctx.reporting.get_report(report_id):
        return "Unknown report", 404
    ctx.queue.clear_results_for_report(report_id)
    ctx.queue.clear_pending_results_for_report(report_id)
    if return_to:
        return redirect(return_to, code=303)
    return redirect(url_for("reports.report_detail_page", report_id=report_id, view=report_view), code=303)


@bp.route("/reports/<report_id>/scripts/remove", methods=["POST"], endpoint="remove_report_script")
def remove_report_script(report_id: str) -> Any:
    report_view = _normalized_report_view(request.form.get("report_view"), allow_script=True)
    return_to = script_helpers.safe_return_to(request.form.get("return_to") or "")
    if not ctx.reporting.get_report(report_id):
        return "Unknown report", 404
    script_path = str(request.form.get("script_path") or "").strip()
    if not script_path:
        return "script_path required", 400
    ctx.reporting.remove_script_from_report(report_id, script_path)
    if return_to:
        return redirect(return_to, code=303)
    return redirect(url_for("reports.report_detail_page", report_id=report_id, view=report_view), code=303)
