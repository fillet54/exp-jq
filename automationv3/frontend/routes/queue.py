"""Queue/worker/job/suite handlers and route registration."""

from __future__ import annotations

import time
from typing import Any, Mapping

from flask import Response, jsonify, redirect, render_template, request, stream_with_context, url_for


def register_queue_routes(app, helpers: Mapping[str, Any]) -> None:
    queue = helpers["queue"]
    central = helpers["central"]
    uut_store = helpers["uut_store"]
    suite_manager = helpers["suite_manager"]

    _coerce_positive_int = helpers["_coerce_positive_int"]
    _build_queue_overview_context = helpers["_build_queue_overview_context"]
    _render_queue_overview_panel = helpers["_render_queue_overview_panel"]
    _render_jobs_table = helpers["_render_jobs_table"]
    _render_workers_table = helpers["_render_workers_table"]
    _render_uuts_table = helpers["_render_uuts_table"]
    _render_results_table = helpers["_render_results_table"]
    _render_suites_table = helpers["_render_suites_table"]

    def index() -> Any:
        return redirect(url_for("queue_page"), code=308)

    def queue_page() -> str:
        queued_page = _coerce_positive_int(request.args.get("queued_page"), default=1)
        in_progress_page = _coerce_positive_int(request.args.get("in_progress_page"), default=1)
        completed_page = _coerce_positive_int(request.args.get("completed_page"), default=1)
        per_page = _coerce_positive_int(request.args.get("per_page"), default=20, minimum=5, maximum=200)
        panel_context = _build_queue_overview_context(
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

    def workers_page() -> str:
        workers = central.get_workers_snapshot()
        return render_template(
            "workers.html",
            page_title="AutomationV3 | Workers",
            workers=workers,
            now_ts=time.time(),
        )

    def completed_jobs_page() -> Any:
        return redirect(url_for("queue_page"), code=308)

    def queue_overview_panel() -> str:
        queued_page = _coerce_positive_int(request.args.get("queued_page"), default=1)
        in_progress_page = _coerce_positive_int(request.args.get("in_progress_page"), default=1)
        completed_page = _coerce_positive_int(request.args.get("completed_page"), default=1)
        per_page = _coerce_positive_int(request.args.get("per_page"), default=20, minimum=5, maximum=200)
        return _render_queue_overview_panel(
            queued_page=queued_page,
            in_progress_page=in_progress_page,
            completed_page=completed_page,
            per_page=per_page,
        )

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

    def add_job() -> str:
        form: dict[str, Any] = request.form.to_dict()
        priority = int(form.get("priority") or 0)
        job = {
            "file": form.get("file", "").strip(),
            "uut": form.get("uut", "").strip(),
            "report_id": form.get("report_id", "").strip(),
        }
        queue.add_job(job, priority=priority)
        return _render_jobs_table()

    def jobs_table() -> str:
        return _render_jobs_table()

    def skip_job(job_id: str) -> str:
        queue.mark_skipped(job_id)
        return _render_jobs_table()

    def restore_all() -> str:
        queue.restore_all_skipped()
        return _render_jobs_table()

    def queue_restore_all_panel() -> str:
        queue.restore_all_skipped()
        queued_page = _coerce_positive_int(request.args.get("queued_page"), default=1)
        in_progress_page = _coerce_positive_int(request.args.get("in_progress_page"), default=1)
        completed_page = _coerce_positive_int(request.args.get("completed_page"), default=1)
        per_page = _coerce_positive_int(request.args.get("per_page"), default=20, minimum=5, maximum=200)
        return _render_queue_overview_panel(
            queued_page=queued_page,
            in_progress_page=in_progress_page,
            completed_page=completed_page,
            per_page=per_page,
        )

    def remove_job(job_id: str) -> str:
        queue.remove_job(job_id)
        return _render_jobs_table()

    def promote_job(job_id: str) -> str:
        priority_param = request.form.get("priority")
        new_priority = int(priority_param) if priority_param else 100
        queue.promote_job(job_id, new_priority=new_priority)
        return _render_jobs_table()

    def next_job() -> str:
        job = queue.get_next_job()
        return render_template("partials/next_job_card.html", job=job)

    def workers_table() -> str:
        return _render_workers_table()

    def add_uut() -> str:
        name = (request.form.get("name") or "").strip()
        path = (request.form.get("path") or "").strip()
        if not name or not path:
            return "Name and path are required", 400
        config = uut_store.add(name=name, path=path)
        try:
            uut_store.snapshot(config.uut_id)
        except Exception:
            pass
        return _render_uuts_table()

    def uuts_table() -> str:
        return _render_uuts_table()

    def add_suite() -> str:
        name = (request.form.get("name") or "").strip()
        if not name:
            return "Name required", 400
        suite_manager.create_suite(name)
        return _render_suites_table()

    def suites_table() -> str:
        return _render_suites_table()

    def suite_add_script(suite_name: str) -> str:
        script_path = (request.form.get("script_path") or "").strip()
        if not script_path:
            return "script_path required", 400
        suite_manager.add_script(suite_name, script_path)
        return _render_suites_table()

    def suite_remove_script(suite_name: str) -> str:
        script_path = (request.form.get("script_path") or "").strip()
        suite_manager.remove_script(suite_name, script_path)
        return _render_suites_table()

    def suite_delete(suite_name: str) -> str:
        suite_manager.delete_suite(suite_name)
        return _render_suites_table()

    def results_table() -> str:
        return _render_results_table()

    def results_for_suite(suite_run_id: str) -> Any:
        results = queue.list_results_for_suite(suite_run_id)
        return jsonify(results)

    app.add_url_rule("/", endpoint="index", view_func=index, methods=["GET"])
    app.add_url_rule("/queue", endpoint="queue_page", view_func=queue_page, methods=["GET"])
    app.add_url_rule("/workers", endpoint="workers_page", view_func=workers_page, methods=["GET"])
    app.add_url_rule("/completed-jobs", endpoint="completed_jobs_page", view_func=completed_jobs_page, methods=["GET"])
    app.add_url_rule("/queue/overview", endpoint="queue_overview_panel", view_func=queue_overview_panel, methods=["GET"])
    app.add_url_rule("/queue/events", endpoint="queue_events_stream", view_func=queue_events_stream, methods=["GET"])
    app.add_url_rule("/jobs", endpoint="add_job", view_func=add_job, methods=["POST"])
    app.add_url_rule("/jobs/table", endpoint="jobs_table", view_func=jobs_table, methods=["GET"])
    app.add_url_rule("/jobs/<job_id>/skip", endpoint="skip_job", view_func=skip_job, methods=["POST"])
    app.add_url_rule("/jobs/restore_all", endpoint="restore_all", view_func=restore_all, methods=["POST"])
    app.add_url_rule("/queue/restore_all", endpoint="queue_restore_all_panel", view_func=queue_restore_all_panel, methods=["POST"])
    app.add_url_rule("/jobs/<job_id>/remove", endpoint="remove_job", view_func=remove_job, methods=["POST"])
    app.add_url_rule("/jobs/<job_id>/promote", endpoint="promote_job", view_func=promote_job, methods=["POST"])
    app.add_url_rule("/jobs/next", endpoint="next_job", view_func=next_job, methods=["GET"])
    app.add_url_rule("/workers/table", endpoint="workers_table", view_func=workers_table, methods=["GET"])
    app.add_url_rule("/uuts", endpoint="add_uut", view_func=add_uut, methods=["POST"])
    app.add_url_rule("/uuts/table", endpoint="uuts_table", view_func=uuts_table, methods=["GET"])
    app.add_url_rule("/suites", endpoint="add_suite", view_func=add_suite, methods=["POST"])
    app.add_url_rule("/suites/table", endpoint="suites_table", view_func=suites_table, methods=["GET"])
    app.add_url_rule("/suites/<suite_name>/add_script", endpoint="suite_add_script", view_func=suite_add_script, methods=["POST"])
    app.add_url_rule("/suites/<suite_name>/remove_script", endpoint="suite_remove_script", view_func=suite_remove_script, methods=["POST"])
    app.add_url_rule("/suites/<suite_name>/delete", endpoint="suite_delete", view_func=suite_delete, methods=["POST"])
    app.add_url_rule("/results/table", endpoint="results_table", view_func=results_table, methods=["GET"])
    app.add_url_rule("/results/suite/<suite_run_id>", endpoint="results_for_suite", view_func=results_for_suite, methods=["GET"])
