from __future__ import annotations

import os
from typing import Any, Dict

from flask import Flask, render_template, request

from jobqueue import JobQueue


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates")

    db_path = os.getenv("JOBQUEUE_DB", "jobqueue.db")
    queue = JobQueue(db_path=db_path)

    def _render_jobs_table() -> str:
        jobs = queue.list_jobs()
        return render_template("partials/jobs_table.html", jobs=jobs)

    @app.route("/", methods=["GET"])
    def index() -> str:
        jobs = queue.list_jobs()
        next_job = queue.get_next_job()
        return render_template("index.html", jobs=jobs, next_job=next_job)

    @app.route("/jobs", methods=["POST"])
    def add_job() -> str:
        form: Dict[str, Any] = request.form.to_dict()
        priority = int(form.get("priority") or 0)
        job = {
            "file": form.get("file", "").strip(),
            "uut": form.get("uut", "").strip(),
            "report_id": form.get("report_id", "").strip(),
        }
        queue.add_job(job, priority=priority)
        return _render_jobs_table()

    @app.route("/jobs/table", methods=["GET"])
    def jobs_table() -> str:
        return _render_jobs_table()

    @app.route("/jobs/<job_id>/skip", methods=["POST"])
    def skip_job(job_id: str) -> str:
        queue.mark_skipped(job_id)
        return _render_jobs_table()

    @app.route("/jobs/restore_all", methods=["POST"])
    def restore_all() -> str:
        queue.restore_all_skipped()
        return _render_jobs_table()

    @app.route("/jobs/<job_id>/remove", methods=["POST"])
    def remove_job(job_id: str) -> str:
        queue.remove_job(job_id)
        return _render_jobs_table()

    @app.route("/jobs/<job_id>/promote", methods=["POST"])
    def promote_job(job_id: str) -> str:
        priority_param = request.form.get("priority")
        new_priority = int(priority_param) if priority_param else 100
        queue.promote_job(job_id, new_priority=new_priority)
        return _render_jobs_table()

    @app.route("/jobs/next", methods=["GET"])
    def next_job() -> str:
        job = queue.get_next_job()
        return render_template("partials/next_job_card.html", job=job)

    @app.route("/health", methods=["GET"])
    def health() -> str:
        return {"status": "ok"}

    return app


__all__ = ["create_app"]
