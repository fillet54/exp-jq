"""Queue/worker/job/suite handlers."""

from __future__ import annotations

import time
from typing import Any

from flask import Blueprint, Response, jsonify, redirect, render_template, request, stream_with_context, url_for

from automationv3.frontend.helpers import queue as queue_helpers

from .state import frontend_ctx as ctx

bp = Blueprint("queue", __name__)


def int_clamped(lo: int, hi: int):
    """Clamp a request arg parsed as int to [lo, hi]."""

    def clamped(raw: Any) -> int:
        value = int(raw)
        return max(lo, min(hi, value))

    return clamped


@bp.route("/", methods=["GET"], endpoint="index")
def index() -> Any:
    return redirect(url_for("queue.queue_page"), code=308)


@bp.route("/queue", methods=["GET"], endpoint="queue_page")
def queue_page() -> str:
    queued_page = request.args.get("queued_page", 1, type=int)
    in_progress_page = request.args.get("in_progress_page", 1, type=int)
    completed_page = request.args.get("completed_page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int_clamped(5, 200))
    panel_context = queue_helpers.build_queue_overview_context(
        ctx,
        queued_page=queued_page,
        in_progress_page=in_progress_page,
        completed_page=completed_page,
        per_page=per_page,
    )
    return render_template(
        "queue.html",
        page_title="AutomationV3 | Queue",
        **panel_context,
    )


@bp.route("/workers", methods=["GET"], endpoint="workers_page")
def workers_page() -> str:
    workers = ctx.central.get_workers_snapshot()
    return render_template(
        "workers.html",
        page_title="AutomationV3 | Workers",
        workers=workers,
        now_ts=time.time(),
    )


@bp.route("/completed-jobs", methods=["GET"], endpoint="completed_jobs_page")
def completed_jobs_page() -> Any:
    return redirect(url_for("queue.queue_page"), code=308)


@bp.route("/queue/overview", methods=["GET"], endpoint="queue_overview_panel")
def queue_overview_panel() -> str:
    queued_page = request.args.get("queued_page", 1, type=int)
    in_progress_page = request.args.get("in_progress_page", 1, type=int)
    completed_page = request.args.get("completed_page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int_clamped(5, 200))
    return queue_helpers.render_queue_overview_panel(
        ctx,
        queued_page=queued_page,
        in_progress_page=in_progress_page,
        completed_page=completed_page,
        per_page=per_page,
    )


@bp.route("/queue/events", methods=["GET"], endpoint="queue_events_stream")
def queue_events_stream() -> Response:
    def event_stream():
        while True:
            yield f"event: queue-refresh\\ndata: {int(time.time())}\\n\\n"
            time.sleep(3.0)

    return Response(
        stream_with_context(event_stream()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@bp.route("/jobs", methods=["POST"], endpoint="add_job")
def add_job() -> str:
    form: dict[str, Any] = request.form.to_dict()
    priority = int(form.get("priority") or 0)
    job = {
        "file": form.get("file", "").strip(),
        "uut": form.get("uut", "").strip(),
        "report_id": form.get("report_id", "").strip(),
    }
    ctx.queue.add_job(job, priority=priority)
    return queue_helpers.render_jobs_table(ctx)


@bp.route("/jobs/table", methods=["GET"], endpoint="jobs_table")
def jobs_table() -> str:
    return queue_helpers.render_jobs_table(ctx)


@bp.route("/jobs/<job_id>/skip", methods=["POST"], endpoint="skip_job")
def skip_job(job_id: str) -> str:
    ctx.queue.mark_skipped(job_id)
    return queue_helpers.render_jobs_table(ctx)


@bp.route("/jobs/restore_all", methods=["POST"], endpoint="restore_all")
def restore_all() -> str:
    ctx.queue.restore_all_skipped()
    return queue_helpers.render_jobs_table(ctx)


@bp.route("/queue/restore_all", methods=["POST"], endpoint="queue_restore_all_panel")
def queue_restore_all_panel() -> str:
    ctx.queue.restore_all_skipped()
    queued_page = request.args.get("queued_page", 1, type=int)
    in_progress_page = request.args.get("in_progress_page", 1, type=int)
    completed_page = request.args.get("completed_page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int_clamped(5, 200))
    return queue_helpers.render_queue_overview_panel(
        ctx,
        queued_page=queued_page,
        in_progress_page=in_progress_page,
        completed_page=completed_page,
        per_page=per_page,
    )


@bp.route("/jobs/<job_id>/remove", methods=["POST"], endpoint="remove_job")
def remove_job(job_id: str) -> str:
    ctx.queue.remove_job(job_id)
    return queue_helpers.render_jobs_table(ctx)


@bp.route("/jobs/<job_id>/promote", methods=["POST"], endpoint="promote_job")
def promote_job(job_id: str) -> str:
    priority_param = request.form.get("priority")
    new_priority = int(priority_param) if priority_param else 100
    ctx.queue.promote_job(job_id, new_priority=new_priority)
    return queue_helpers.render_jobs_table(ctx)


@bp.route("/jobs/next", methods=["GET"], endpoint="next_job")
def next_job() -> str:
    job = ctx.queue.get_next_job()
    return render_template("partials/next_job_card.html", job=job)


@bp.route("/workers/table", methods=["GET"], endpoint="workers_table")
def workers_table() -> str:
    return queue_helpers.render_workers_table(ctx)


@bp.route("/uuts", methods=["POST"], endpoint="add_uut")
def add_uut() -> str:
    name = (request.form.get("name") or "").strip()
    path = (request.form.get("path") or "").strip()
    if not name or not path:
        return "Name and path are required", 400
    config = ctx.uut_store.add(name=name, path=path)
    try:
        ctx.uut_store.snapshot(config.uut_id)
    except Exception:
        pass
    return queue_helpers.render_uuts_table(ctx)


@bp.route("/uuts/table", methods=["GET"], endpoint="uuts_table")
def uuts_table() -> str:
    return queue_helpers.render_uuts_table(ctx)


@bp.route("/suites", methods=["POST"], endpoint="add_suite")
def add_suite() -> str:
    name = (request.form.get("name") or "").strip()
    if not name:
        return "Name required", 400
    ctx.suite_manager.create_suite(name)
    return queue_helpers.render_suites_table(ctx)


@bp.route("/suites/table", methods=["GET"], endpoint="suites_table")
def suites_table() -> str:
    return queue_helpers.render_suites_table(ctx)


@bp.route("/suites/<suite_name>/add_script", methods=["POST"], endpoint="suite_add_script")
def suite_add_script(suite_name: str) -> str:
    script_path = (request.form.get("script_path") or "").strip()
    if not script_path:
        return "script_path required", 400
    ctx.suite_manager.add_script(suite_name, script_path)
    return queue_helpers.render_suites_table(ctx)


@bp.route("/suites/<suite_name>/remove_script", methods=["POST"], endpoint="suite_remove_script")
def suite_remove_script(suite_name: str) -> str:
    script_path = (request.form.get("script_path") or "").strip()
    ctx.suite_manager.remove_script(suite_name, script_path)
    return queue_helpers.render_suites_table(ctx)


@bp.route("/suites/<suite_name>/delete", methods=["POST"], endpoint="suite_delete")
def suite_delete(suite_name: str) -> str:
    ctx.suite_manager.delete_suite(suite_name)
    return queue_helpers.render_suites_table(ctx)


@bp.route("/results/table", methods=["GET"], endpoint="results_table")
def results_table() -> str:
    return queue_helpers.render_results_table(ctx)


@bp.route("/results/suite/<suite_run_id>", methods=["GET"], endpoint="results_for_suite")
def results_for_suite(suite_run_id: str) -> Any:
    results = ctx.queue.list_results_for_suite(suite_run_id)
    return jsonify(results)
