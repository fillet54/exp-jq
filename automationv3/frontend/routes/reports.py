"""Report handlers and route registration."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping

from flask import Response, redirect, render_template, request, url_for

from automationv3.framework.requirements import load_default_requirements


def register_report_routes(app, helpers: Mapping[str, Any]) -> None:
    queue = helpers["queue"]
    reporting = helpers["reporting"]
    log = helpers["log"]

    _safe_return_to = helpers["_safe_return_to"]
    _build_report_listing = helpers["_build_report_listing"]
    _build_report_requirement_groups = helpers["_build_report_requirement_groups"]
    _iter_completed_results_for_report = helpers["_iter_completed_results_for_report"]
    _build_report_export_context = helpers["_build_report_export_context"]
    _build_report_export_rst = helpers["_build_report_export_rst"]
    _render_rst_pdf = helpers["_render_rst_pdf"]
    _queue_report_scripts = helpers["_queue_report_scripts"]

    def reports_page() -> str:
        report_records = reporting.list_reports(limit=2000)
        results = queue.list_results(limit=5000)
        pending = queue.list_jobs()
        reports = _build_report_listing(
            results,
            report_records=report_records,
            pending_jobs=pending,
        )
        return render_template(
            "reports.html",
            page_title="AutomationV3 | Reports",
            reports=reports,
        )

    def create_report() -> Any:
        title = (request.form.get("title") or "").strip()
        description = (request.form.get("description") or "").strip()
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not title:
            return "title is required", 400
        report = reporting.create_report(title=title, description=description)
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("report_detail_page", report_id=report["report_id"]), code=303)

    def delete_report(report_id: str) -> Any:
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not reporting.get_report(report_id):
            return "Unknown report", 404
        reporting.delete_report(report_id)
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("reports_page"), code=303)

    def report_detail_page(report_id: str) -> str:
        def _human_datetime(ts: Any) -> str:
            if ts is None:
                return "—"
            try:
                return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S UTC"
                )
            except (TypeError, ValueError, OSError):
                return "—"

        report_view = "requirement"
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

        report_meta = reporting.get_report(report_id)
        if not report_meta:
            return "Unknown report", 404

        requirement_view = _build_report_requirement_groups(
            report_id=report_id, requirement_text_map=requirement_text_map
        )
        completed = _iter_completed_results_for_report(report_id)

        pending = [
            job
            for job in queue.list_jobs()
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
            report_results=completed,
            report_script_total=requirement_view["report_script_total"],
            report_tracked_script_total=requirement_view["report_tracked_script_total"],
            report_requirement_ids=requirement_view["report_requirement_ids"],
            report_requirement_groups=requirement_view["report_requirement_groups"],
            report_requeue_script_paths=requirement_view["report_requeue_script_paths"],
            can_queue_report_scripts=requirement_view["has_report_queue_seed"],
            known_requirements=known_requirements,
            pending_jobs=pending_rows,
        )

    def report_export_page(report_id: str) -> Any:
        report_meta = reporting.get_report(report_id)
        if not report_meta:
            return "Unknown report", 404
        export_context = _build_report_export_context(report_id)
        auto_print = (request.args.get("print") or "").strip().lower() in {"1", "true", "yes", "on"}
        return render_template(
            "report_export.html",
            page_title=f"AutomationV3 | Report Export | {report_meta.get('title') or report_id}",
            auto_print=auto_print,
            **export_context,
        )

    def report_export_pdf(report_id: str) -> Any:
        report_meta = reporting.get_report(report_id)
        if not report_meta:
            return "Unknown report", 404
        export_context = _build_report_export_context(report_id)
        rst_export = _build_report_export_rst(export_context)
        try:
            pdf_bytes = _render_rst_pdf(
                rst_export,
                report_id=report_id,
                latest_completed_human=str(export_context.get("latest_completed_human") or "—"),
            )
        except Exception as exc:
            log.exception("PDF export failed for report %s: %s", report_id, exc)
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

    def requeue_report_all(report_id: str) -> Any:
        report_view = (request.form.get("report_view") or "requirement").strip().lower()
        if report_view != "requirement":
            report_view = "requirement"
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not reporting.get_report(report_id):
            return "Unknown report", 404
        requirement_view = _build_report_requirement_groups(
            report_id=report_id, requirement_text_map={}
        )
        _queue_report_scripts(report_id, requirement_view["report_requeue_script_paths"])
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("report_detail_page", report_id=report_id, view=report_view), code=303)

    def add_report_requirement(report_id: str) -> Any:
        report_view = (request.form.get("report_view") or "requirement").strip().lower()
        if report_view != "requirement":
            report_view = "requirement"
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not reporting.get_report(report_id):
            return "Unknown report", 404
        requirement_id = str(request.form.get("requirement_id") or "").strip()
        if not requirement_id:
            return "requirement_id required", 400
        reporting.add_report_requirement(report_id, requirement_id)
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("report_detail_page", report_id=report_id, view=report_view), code=303)

    def remove_report_requirement(report_id: str) -> Any:
        report_view = (request.form.get("report_view") or "requirement").strip().lower()
        if report_view != "requirement":
            report_view = "requirement"
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not reporting.get_report(report_id):
            return "Unknown report", 404
        requirement_id = str(request.form.get("requirement_id") or "").strip()
        if not requirement_id:
            return "requirement_id required", 400
        reporting.remove_report_requirement(report_id, requirement_id)
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("report_detail_page", report_id=report_id, view=report_view), code=303)

    def requeue_report_script(report_id: str) -> Any:
        report_view = (request.form.get("report_view") or "requirement").strip().lower()
        if report_view != "requirement":
            report_view = "requirement"
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not reporting.get_report(report_id):
            return "Unknown report", 404
        script_path = str(request.form.get("script_path") or "").strip()
        if not script_path:
            return "script_path required", 400
        _queue_report_scripts(report_id, [script_path])
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("report_detail_page", report_id=report_id, view=report_view), code=303)

    def requeue_report_requirement(report_id: str) -> Any:
        report_view = (request.form.get("report_view") or "requirement").strip().lower()
        if report_view != "requirement":
            report_view = "requirement"
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not reporting.get_report(report_id):
            return "Unknown report", 404
        requirement_id = str(request.form.get("requirement_id") or "").strip()
        if not requirement_id:
            return "requirement_id required", 400
        requirement_view = _build_report_requirement_groups(
            report_id=report_id, requirement_text_map={}
        )
        script_paths = []
        for group in requirement_view["report_requirement_groups"]:
            if str(group.get("requirement") or "").strip() != requirement_id:
                continue
            script_paths = list(group.get("script_paths") or [])
            break
        if not script_paths:
            if return_to:
                return redirect(return_to, code=303)
            return redirect(url_for("report_detail_page", report_id=report_id, view=report_view), code=303)
        _queue_report_scripts(report_id, script_paths)
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("report_detail_page", report_id=report_id, view=report_view), code=303)

    def clear_report_results(report_id: str) -> Any:
        report_view = (request.form.get("report_view") or "requirement").strip().lower()
        if report_view not in {"script", "requirement"}:
            report_view = "requirement"
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not reporting.get_report(report_id):
            return "Unknown report", 404
        queue.clear_results_for_report(report_id)
        queue.clear_pending_results_for_report(report_id)
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("report_detail_page", report_id=report_id, view=report_view), code=303)

    def remove_report_script(report_id: str) -> Any:
        report_view = (request.form.get("report_view") or "requirement").strip().lower()
        if report_view not in {"script", "requirement"}:
            report_view = "requirement"
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not reporting.get_report(report_id):
            return "Unknown report", 404
        script_path = str(request.form.get("script_path") or "").strip()
        if not script_path:
            return "script_path required", 400
        reporting.remove_script_from_report(report_id, script_path)
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("report_detail_page", report_id=report_id, view=report_view), code=303)

    app.add_url_rule("/reports", endpoint="reports_page", view_func=reports_page, methods=["GET"])
    app.add_url_rule("/reports", endpoint="create_report", view_func=create_report, methods=["POST"])
    app.add_url_rule("/reports/<report_id>/delete", endpoint="delete_report", view_func=delete_report, methods=["POST"])
    app.add_url_rule("/reports/<report_id>", endpoint="report_detail_page", view_func=report_detail_page, methods=["GET"])
    app.add_url_rule("/reports/<report_id>/export", endpoint="report_export_page", view_func=report_export_page, methods=["GET"])
    app.add_url_rule("/reports/<report_id>/export.pdf", endpoint="report_export_pdf", view_func=report_export_pdf, methods=["GET"])
    app.add_url_rule("/reports/<report_id>/requeue_all", endpoint="requeue_report_all", view_func=requeue_report_all, methods=["POST"])
    app.add_url_rule("/reports/<report_id>/requirements/add", endpoint="add_report_requirement", view_func=add_report_requirement, methods=["POST"])
    app.add_url_rule("/reports/<report_id>/requirements/remove", endpoint="remove_report_requirement", view_func=remove_report_requirement, methods=["POST"])
    app.add_url_rule("/reports/<report_id>/requeue_script", endpoint="requeue_report_script", view_func=requeue_report_script, methods=["POST"])
    app.add_url_rule("/reports/<report_id>/requeue_requirement", endpoint="requeue_report_requirement", view_func=requeue_report_requirement, methods=["POST"])
    app.add_url_rule("/reports/<report_id>/clear_results", endpoint="clear_report_results", view_func=clear_report_results, methods=["POST"])
    app.add_url_rule("/reports/<report_id>/scripts/remove", endpoint="remove_report_script", view_func=remove_report_script, methods=["POST"])
