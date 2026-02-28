"""Queue-focused frontend helper functions."""

from __future__ import annotations

import math
import time
from typing import Any, Dict, List

from flask import render_template

from .context import FrontendHelperContext


def coerce_positive_int(raw_value: Any, default: int, minimum: int = 1, maximum: int = 10_000) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return default
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value


def paginate_items(items: List[Dict[str, Any]], page: int, per_page: int) -> Dict[str, Any]:
    total_count = len(items)
    total_pages = max(1, math.ceil(total_count / per_page))
    safe_page = min(max(1, page), total_pages)
    start = (safe_page - 1) * per_page
    end = start + per_page
    page_items = items[start:end]
    return {
        "items": page_items,
        "page": safe_page,
        "per_page": per_page,
        "total_count": total_count,
        "total_pages": total_pages,
        "has_prev": safe_page > 1,
        "has_next": safe_page < total_pages,
        "prev_page": safe_page - 1,
        "next_page": safe_page + 1,
    }


def render_jobs_table(ctx: FrontendHelperContext) -> str:
    jobs = ctx.queue.list_jobs()
    return render_template("partials/jobs_table.html", jobs=jobs)


def render_workers_table(ctx: FrontendHelperContext) -> str:
    workers = ctx.central.get_workers_snapshot()
    return render_template("partials/workers_table.html", workers=workers, now_ts=time.time())


def render_uuts_table(ctx: FrontendHelperContext) -> str:
    uuts = ctx.uut_store.list()
    return render_template("partials/uuts_table.html", uuts=uuts)


def render_results_table(ctx: FrontendHelperContext) -> str:
    results = ctx.queue.list_results()
    return render_template("partials/results_table.html", results=results)


def render_suites_table(ctx: FrontendHelperContext) -> str:
    suites = []
    for name in ctx.suite_manager.list_suites():
        suites.append({"name": name, "scripts": ctx.suite_manager.get_suite(name)})
    return render_template("partials/suites_table.html", suites=suites)


def build_queue_overview_context(
    ctx: FrontendHelperContext,
    queued_page: int,
    in_progress_page: int,
    completed_page: int,
    per_page: int,
) -> Dict[str, Any]:
    all_jobs = ctx.queue.list_jobs()
    workers = ctx.central.get_workers_snapshot()

    worker_by_job_id: Dict[str, Any] = {}
    for worker in workers:
        job_id = getattr(worker, "current_job", None)
        if not job_id:
            continue
        worker_by_job_id[job_id] = worker

    in_progress_jobs: List[Dict[str, Any]] = []
    queued_jobs: List[Dict[str, Any]] = []
    for job in all_jobs:
        job_id = job.get("job_id")
        worker = worker_by_job_id.get(job_id)
        if worker and bool(getattr(worker, "busy", False)):
            row = dict(job)
            row["worker_id"] = getattr(worker, "worker_id", None)
            row["worker_address"] = getattr(worker, "address", None)
            in_progress_jobs.append(row)
        else:
            queued_jobs.append(job)

    queued_page_data = paginate_items(queued_jobs, queued_page, per_page)
    in_progress_page_data = paginate_items(in_progress_jobs, in_progress_page, per_page)

    completed_total = ctx.queue.count_results()
    completed_total_pages = max(1, math.ceil(completed_total / per_page))
    safe_completed_page = min(max(1, completed_page), completed_total_pages)
    completed_offset = (safe_completed_page - 1) * per_page
    completed_items = ctx.queue.list_results(limit=per_page, offset=completed_offset)
    completed_page_data = {
        "items": completed_items,
        "page": safe_completed_page,
        "per_page": per_page,
        "total_count": completed_total,
        "total_pages": completed_total_pages,
        "has_prev": safe_completed_page > 1,
        "has_next": safe_completed_page < completed_total_pages,
        "prev_page": safe_completed_page - 1,
        "next_page": safe_completed_page + 1,
    }

    return {
        "queued_page_data": queued_page_data,
        "in_progress_page_data": in_progress_page_data,
        "completed_page_data": completed_page_data,
        "queued_page": queued_page_data["page"],
        "in_progress_page": in_progress_page_data["page"],
        "completed_page": completed_page_data["page"],
        "per_page": per_page,
    }


def render_queue_overview_panel(
    ctx: FrontendHelperContext,
    queued_page: int,
    in_progress_page: int,
    completed_page: int,
    per_page: int,
) -> str:
    panel_context = build_queue_overview_context(
        ctx,
        queued_page=queued_page,
        in_progress_page=in_progress_page,
        completed_page=completed_page,
        per_page=per_page,
    )
    return render_template("partials/queue_overview_panel.html", **panel_context)
