"""Report-centric frontend helper functions."""

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import docutils.core
from flask import render_template

from automationv3.framework.requirements import load_default_requirements
from automationv3.framework.rst import expand_rvt_variations

from .context import FrontendHelperContext
from .scripts import (
    UNSPECIFIED_SYSTEM,
    build_requeue_job_from_result_job_data,
    discover_scripts,
    extract_rst_title,
    normalize_requirements,
    parse_meta_from_lines,
    requirement_to_system,
    sorted_systems,
    variation_key_from_bindings,
)


def human_datetime(ts: Any) -> str:
    if ts is None:
        return "—"
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    except (TypeError, ValueError, OSError):
        return "—"


def build_report_listing(
    results: List[Dict[str, Any]],
    report_records: List[Dict[str, Any]] | None = None,
    pending_jobs: List[Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    reports: Dict[str, Dict[str, Any]] = {}
    for report in report_records or []:
        report_id = (report.get("report_id") or "").strip()
        if not report_id:
            continue
        reports[report_id] = {
            "report_id": report_id,
            "title": report.get("title") or report_id,
            "description": report.get("description") or "",
            "created_at": report.get("created_at"),
            "pending": 0,
            "completed": 0,
            "total": 0,
            "passed": 0,
            "failed": 0,
            "latest_completed_at": None,
            "suite_runs": set(),
        }

    for job in pending_jobs or []:
        report_id = (job or {}).get("report_id")
        if not report_id:
            continue
        if report_id not in reports:
            reports[report_id] = {
                "report_id": report_id,
                "title": report_id,
                "description": "",
                "created_at": None,
                "pending": 0,
                "completed": 0,
                "total": 0,
                "passed": 0,
                "failed": 0,
                "latest_completed_at": None,
                "suite_runs": set(),
            }
        reports[report_id]["pending"] += 1

    for res in results:
        job = res.get("job_data") or {}
        report_id = (res or {}).get("report_id") or job.get("report_id")
        if not report_id:
            continue

        if report_id not in reports:
            reports[report_id] = {
                "report_id": report_id,
                "title": report_id,
                "description": "",
                "created_at": None,
                "pending": 0,
                "completed": 0,
                "total": 0,
                "passed": 0,
                "failed": 0,
                "latest_completed_at": None,
                "suite_runs": set(),
            }

        row = reports[report_id]
        row["completed"] += 1
        row["passed"] += 1 if res.get("success") else 0
        row["failed"] += 0 if res.get("success") else 1
        completed_at = res.get("completed_at")
        if completed_at and (
            row["latest_completed_at"] is None
            or completed_at > row["latest_completed_at"]
        ):
            row["latest_completed_at"] = completed_at
        suite_run_id = res.get("suite_run_id")
        if suite_run_id:
            row["suite_runs"].add(suite_run_id)

    for row in reports.values():
        row["total"] = row["pending"] + row["completed"]

    return sorted(
        reports.values(),
        key=lambda row: (
            row.get("created_at") is None,
            row.get("created_at") or 0,
            row.get("latest_completed_at") or 0,
        ),
        reverse=True,
    )


def iter_completed_results_for_report(ctx: FrontendHelperContext, report_id: str) -> List[Dict[str, Any]]:
    return ctx.queue.list_results_for_report(report_id=report_id, limit=5000)


def build_scratch_report_runs(
    ctx: FrontendHelperContext,
    report_id: str,
) -> Dict[str, Any]:
    completed = iter_completed_results_for_report(ctx, report_id)
    title_cache: Dict[tuple[str, str], str] = {}

    def _resolve_title(job: Dict[str, Any]) -> str:
        script_path = str(job.get("file") or "").strip()
        if not script_path:
            return "Untitled Script"
        scripts_root_hint = str(job.get("scripts_root") or "").strip()
        cache_key = (scripts_root_hint, script_path)
        if cache_key in title_cache:
            return title_cache[cache_key]

        title = Path(script_path).stem or script_path
        candidates: List[Path] = []
        if scripts_root_hint:
            candidates.append((Path(scripts_root_hint).resolve() / script_path).resolve())
        candidates.append((ctx.scripts_root.resolve() / script_path).resolve())
        for candidate in candidates:
            try:
                if candidate.exists() and candidate.is_file():
                    lines = candidate.read_text(encoding="utf-8").splitlines()
                    title = extract_rst_title(lines, fallback=title)
                    break
            except Exception:
                continue
        title_cache[cache_key] = title
        return title

    runs: List[Dict[str, Any]] = []
    for res in completed:
        job = res.get("job_data") or {}
        script_path = str(job.get("file") or "").strip() or "—"
        completed_at = res.get("completed_at")
        success = bool(res.get("success"))
        runs.append(
            {
                "job_id": res.get("job_id"),
                "script": script_path,
                "script_title": _resolve_title(job),
                "success": success,
                "status_label": "PASS" if success else "FAIL",
                "status_badge_class": "badge-success" if success else "badge-error",
                "uut": job.get("uut") or "—",
                "worker_id": res.get("worker_id") or "n/a",
                "completed_at": completed_at,
                "completed_at_human": human_datetime(completed_at),
            }
        )
    runs.sort(key=lambda row: float(row.get("completed_at") or 0), reverse=True)

    script_total = len(
        {
            str(row.get("script") or "").strip()
            for row in runs
            if str(row.get("script") or "").strip() and str(row.get("script") or "").strip() != "—"
        }
    )
    return {
        "scratch_runs": runs,
        "scratch_run_total": len(runs),
        "scratch_script_total": script_total,
    }


def report_seed_job_template(ctx: FrontendHelperContext, report_id: str) -> Dict[str, Any] | None:
    completed = iter_completed_results_for_report(ctx, report_id)
    for row in completed:
        job_data = row.get("job_data") or {}
        if str(job_data.get("uut") or "").strip():
            return dict(job_data)

    for row in ctx.reporting.list_report_scripts(report_id):
        template = row.get("job_template")
        if not isinstance(template, dict):
            continue
        if str(template.get("uut") or "").strip():
            return dict(template)

    uuts = ctx.uut_store.list()
    if len(uuts) == 1:
        only = uuts[0]
        return {
            "uut": only.name,
            "uut_id": only.uut_id,
            "uut_tree": only.last_tree_sha,
            "scripts_root": str(ctx.scripts_root),
            "report_id": report_id,
            "suite_name": "",
            "suite_run_id": "",
        }
    return None


def queue_report_scripts(ctx: FrontendHelperContext, report_id: str, script_paths: List[str]) -> int:
    clean_script_paths: List[str] = []
    seen_paths = set()
    for script_path in script_paths:
        clean = str(script_path or "").strip()
        if not clean or clean in seen_paths:
            continue
        seen_paths.add(clean)
        clean_script_paths.append(clean)
    if not clean_script_paths:
        return 0

    completed = iter_completed_results_for_report(ctx, report_id)
    latest_job_by_script: Dict[str, Dict[str, Any]] = {}
    for row in completed:
        job_data = row.get("job_data") or {}
        script = str(job_data.get("file") or "").strip()
        if not script or script in latest_job_by_script:
            continue
        latest_job_by_script[script] = job_data

    tracked_templates: Dict[str, Dict[str, Any]] = {}
    for row in ctx.reporting.list_report_scripts(report_id):
        script = str(row.get("script_path") or "").strip()
        template = row.get("job_template")
        if not script or not isinstance(template, dict):
            continue
        tracked_templates[script] = template

    seed_job = report_seed_job_template(ctx, report_id)
    jobs_to_queue: List[Dict[str, Any]] = []
    for script in clean_script_paths:
        source_job = latest_job_by_script.get(script)
        if not source_job:
            source_job = tracked_templates.get(script)
        if not source_job:
            source_job = seed_job
        if not source_job:
            continue
        source_payload = dict(source_job)
        source_payload["file"] = script
        source_payload["report_id"] = report_id
        if not str(source_payload.get("scripts_root") or "").strip():
            source_payload["scripts_root"] = str(ctx.scripts_root)
        queued_job = build_requeue_job_from_result_job_data(
            ctx,
            source_payload,
            report_id=report_id,
        )
        if not queued_job:
            continue
        job_scripts_root = str(queued_job.get("scripts_root") or "").strip()
        candidate_paths: List[Path] = []
        if job_scripts_root:
            candidate_paths.append((Path(job_scripts_root).resolve() / script).resolve())
        candidate_paths.append((ctx.scripts_root.resolve() / script).resolve())
        script_path = next(
            (candidate for candidate in candidate_paths if candidate.exists() and candidate.is_file()),
            None,
        )
        if script_path is None:
            jobs_to_queue.append(queued_job)
            continue
        try:
            from .scripts import expand_job_variations_from_script

            jobs_to_queue.extend(expand_job_variations_from_script(queued_job, script_path))
        except ValueError:
            ctx.log.exception("Failed to expand variations for report requeue script %s", script)
            jobs_to_queue.append(queued_job)
    if jobs_to_queue:
        ctx.queue.add_job(jobs_to_queue, priority=0)
    return len(jobs_to_queue)


def build_report_requirement_groups(
    ctx: FrontendHelperContext,
    report_id: str,
    requirement_text_map: Dict[str, str],
) -> Dict[str, Any]:
    no_requirement_label = "No Requirement Declared"

    requirement_ids = sorted(
        {
            str(row.get("requirement_id") or "").strip()
            for row in ctx.reporting.list_report_requirements(report_id)
            if str(row.get("requirement_id") or "").strip()
        },
        key=lambda req: (req == no_requirement_label, req),
    )
    requirement_set = set(requirement_ids)

    completed = iter_completed_results_for_report(ctx, report_id)
    tracked_rows = ctx.reporting.list_report_scripts(report_id)
    tracked_script_set = {
        str(row.get("script_path") or "").strip()
        for row in tracked_rows
        if str(row.get("script_path") or "").strip()
    }

    script_info_cache: Dict[tuple[str, str], Dict[str, Any]] = {}

    def _resolve_script_info(job: Dict[str, Any]) -> Dict[str, Any]:
        script = str(job.get("file") or "").strip()
        if not script:
            return {"title": "Untitled Script", "requirements": []}

        job_scripts_root = str(job.get("scripts_root") or "").strip()
        cache_key = (job_scripts_root, script)
        if cache_key in script_info_cache:
            return script_info_cache[cache_key]

        title = Path(script).stem or script
        requirements = normalize_requirements(((job.get("meta") or {}).get("requirements")))

        candidates: List[Path] = []
        if job_scripts_root:
            candidates.append((Path(job_scripts_root).resolve() / script).resolve())
        candidates.append((ctx.scripts_root.resolve() / script).resolve())

        for candidate in candidates:
            try:
                if candidate.exists() and candidate.is_file():
                    lines = candidate.read_text(encoding="utf-8").splitlines()
                    title = extract_rst_title(lines, fallback=title)
                    if not requirements:
                        file_meta = parse_meta_from_lines(lines)
                        requirements = normalize_requirements((file_meta or {}).get("requirements"))
                    break
            except Exception:
                continue

        info = {"title": title, "requirements": requirements}
        script_info_cache[cache_key] = info
        return info

    script_variation_cache: Dict[tuple[str, str], List[Dict[str, Any]]] = {}

    def _variation_key(
        bindings: Dict[str, Any] | None,
        variation_name: str = "",
        variation_index: int = 0,
    ) -> str:
        key = variation_key_from_bindings(bindings or {})
        if key:
            return key
        name = str(variation_name or "").strip()
        if name:
            return f"name:{name}"
        if variation_index > 0:
            return f"index:{variation_index}"
        return "__default__"

    def _resolve_script_variations(script_path: str, scripts_root_hint: str = "") -> List[Dict[str, Any]]:
        cache_key = (str(scripts_root_hint or ""), script_path)
        if cache_key in script_variation_cache:
            return script_variation_cache[cache_key]

        candidates: List[Path] = []
        if scripts_root_hint:
            candidates.append((Path(scripts_root_hint).resolve() / script_path).resolve())
        candidates.append((ctx.scripts_root.resolve() / script_path).resolve())

        variations: List[Dict[str, Any]] = []
        for candidate in candidates:
            try:
                if not candidate.exists() or not candidate.is_file():
                    continue
                expanded = expand_rvt_variations(candidate.read_text(encoding="utf-8"))
                for index, variation in enumerate(expanded, start=1):
                    label = str(variation.get("name") or "").strip() or f"variation-{index}"
                    key = _variation_key(
                        variation.get("bindings") if isinstance(variation.get("bindings"), dict) else {},
                        variation_name=label,
                        variation_index=index,
                    )
                    variations.append({"key": key, "label": label, "index": index})
                break
            except Exception:
                continue

        script_variation_cache[cache_key] = variations
        return variations

    all_runs: List[Dict[str, Any]] = []
    latest_report_run_by_script: Dict[str, Dict[str, Any]] = {}
    latest_report_run_by_script_variation: Dict[str, Dict[str, Dict[str, Any]]] = {}
    requirement_run_history: Dict[str, List[Dict[str, Any]]] = {}
    for res in completed:
        job = res.get("job_data") or {}
        script = str(job.get("file") or "").strip()
        if not script:
            continue
        script_info = _resolve_script_info(job)
        requirements = list(script_info.get("requirements") or [])
        if not requirements and no_requirement_label in requirement_set:
            requirements = [no_requirement_label]
        variation_name = str(job.get("variation_name") or "").strip()
        variation_bindings = (
            job.get("variation_bindings")
            if isinstance(job.get("variation_bindings"), dict)
            else {}
        )
        variation_index = int(job.get("variation_index") or 0)
        variation_total = int(job.get("variation_total") or 0)
        variation_key = _variation_key(
            variation_bindings,
            variation_name=variation_name,
            variation_index=variation_index,
        )
        is_variation_job = bool(job.get("is_variation_job")) or variation_key != "__default__"
        variation_label = variation_name or (f"variation-{variation_index}" if variation_index > 0 else "default")
        run = {
            "job_id": res.get("job_id"),
            "script": script,
            "script_title": script_info.get("title") or Path(script).stem or script,
            "requirements": requirements,
            "success": bool(res.get("success")),
            "status_label": "PASS" if res.get("success") else "FAIL",
            "uut": job.get("uut") or "—",
            "worker_id": res.get("worker_id") or "n/a",
            "completed_at": res.get("completed_at"),
            "completed_at_human": human_datetime(res.get("completed_at")),
            "variation_key": variation_key,
            "variation_name": variation_label,
            "variation_index": variation_index,
            "variation_total": variation_total,
            "is_variation_job": is_variation_job,
            "scripts_root_hint": str(job.get("scripts_root") or ""),
        }
        all_runs.append(run)
        if script not in latest_report_run_by_script:
            latest_report_run_by_script[script] = run
        script_variation_map = latest_report_run_by_script_variation.setdefault(script, {})
        if variation_key not in script_variation_map:
            script_variation_map[variation_key] = run
        for requirement in requirements:
            if requirement in requirement_set:
                requirement_run_history.setdefault(requirement, []).append(run)

    requirement_script_catalog: Dict[str, Dict[str, Dict[str, Any]]] = {
        requirement: {} for requirement in requirement_ids
    }

    def _register_requirement_script(
        requirement: str,
        script_path: str,
        script_title: str,
        scripts_root_hint: str = "",
    ) -> None:
        req = str(requirement or "").strip()
        if req not in requirement_set:
            return
        clean_script = str(script_path or "").strip()
        if not clean_script:
            return
        title = str(script_title or "").strip() or Path(clean_script).stem or clean_script
        bucket = requirement_script_catalog.setdefault(req, {})
        if clean_script not in bucket:
            bucket[clean_script] = {
                "script": clean_script,
                "script_title": title,
                "scripts_root_hint": str(scripts_root_hint or ""),
            }
        elif scripts_root_hint and not str(bucket[clean_script].get("scripts_root_hint") or "").strip():
            bucket[clean_script]["scripts_root_hint"] = str(scripts_root_hint)

    try:
        discovered_scripts = discover_scripts(ctx.scripts_root)
    except Exception:
        discovered_scripts = []
    for script_row in discovered_scripts:
        script_path = str(script_row.get("relpath") or "").strip()
        if not script_path:
            continue
        script_title = str(script_row.get("title") or "").strip() or Path(script_path).stem
        meta = script_row.get("meta") or {}
        requirements = [str(req).strip() for req in (meta.get("requirements") or []) if str(req).strip()]
        if not requirements and no_requirement_label in requirement_set:
            requirements = [no_requirement_label]
        for requirement in requirements:
            _register_requirement_script(
                requirement,
                script_path,
                script_title,
                scripts_root_hint=str(ctx.scripts_root),
            )

    for tracked_row in tracked_rows:
        script_path = str(tracked_row.get("script_path") or "").strip()
        if not script_path:
            continue
        template = tracked_row.get("job_template")
        info = _resolve_script_info(template if isinstance(template, dict) else {"file": script_path})
        requirements = list(info.get("requirements") or [])
        if not requirements and no_requirement_label in requirement_set:
            requirements = [no_requirement_label]
        for requirement in requirements:
            _register_requirement_script(
                requirement,
                script_path,
                str(info.get("title") or Path(script_path).stem or script_path),
                scripts_root_hint=(str(template.get("scripts_root") or "") if isinstance(template, dict) else ""),
            )

    for run in all_runs:
        requirements = list(run.get("requirements") or [])
        for requirement in requirements:
            _register_requirement_script(
                requirement,
                str(run.get("script") or ""),
                str(run.get("script_title") or ""),
                scripts_root_hint=str(run.get("scripts_root_hint") or ""),
            )

    report_requirement_groups: List[Dict[str, Any]] = []
    report_system_summaries: Dict[str, Dict[str, Any]] = {}
    requirement_status_counts = {"pass": 0, "partial": 0, "fail": 0, "not_run": 0}
    report_requeue_script_paths: set[str] = set()

    for requirement in requirement_ids:
        script_map = requirement_script_catalog.get(requirement, {})
        script_paths = sorted(script_map.keys())
        report_requeue_script_paths.update(script_paths)

        passing_script_total = 0
        failing_script_total = 0
        not_run_script_total = 0
        partial_script_total = 0
        requirement_script_rows: List[Dict[str, Any]] = []
        for script_path in script_paths:
            latest_run = latest_report_run_by_script.get(script_path)
            latest_variations = latest_report_run_by_script_variation.get(script_path, {})
            script_info = script_map.get(script_path) or {}
            declared_variations = sorted(
                _resolve_script_variations(
                    script_path,
                    scripts_root_hint=str(script_info.get("scripts_root_hint") or ""),
                ),
                key=lambda row: (int(row.get("index") or 0), str(row.get("label") or "")),
            )
            variation_statuses: List[Dict[str, Any]] = []
            seen_variation_keys: set[str] = set()
            for variation in declared_variations:
                variation_key = str(variation.get("key") or "").strip()
                if not variation_key:
                    continue
                seen_variation_keys.add(variation_key)
                run = latest_variations.get(variation_key)
                status_label = "NOT RUN"
                color_class = "bg-base-300 border border-base-content/20"
                if run:
                    status_label = "PASS" if run.get("success") else "FAIL"
                    color_class = "bg-success" if run.get("success") else "bg-error"
                variation_statuses.append(
                    {
                        "key": variation_key,
                        "label": str(variation.get("label") or variation_key),
                        "status_label": status_label,
                        "color_class": color_class,
                        "job_id": (run or {}).get("job_id"),
                        "completed_at_human": (run or {}).get("completed_at_human") or "—",
                        "success": bool((run or {}).get("success")) if run else None,
                    }
                )

            extra_variation_runs = sorted(
                latest_variations.items(),
                key=lambda item: (
                    int((item[1] or {}).get("variation_index") or 0),
                    str((item[1] or {}).get("variation_name") or ""),
                ),
            )
            for variation_key, run in extra_variation_runs:
                if variation_key in seen_variation_keys or variation_key == "__default__":
                    continue
                variation_statuses.append(
                    {
                        "key": variation_key,
                        "label": str((run or {}).get("variation_name") or variation_key),
                        "status_label": "PASS" if (run or {}).get("success") else "FAIL",
                        "color_class": "bg-success" if (run or {}).get("success") else "bg-error",
                        "job_id": (run or {}).get("job_id"),
                        "completed_at_human": (run or {}).get("completed_at_human") or "—",
                        "success": bool((run or {}).get("success")),
                    }
                )

            has_variations = bool(variation_statuses)
            if has_variations:
                variation_pass_count = sum(
                    1 for row in variation_statuses if row.get("status_label") == "PASS"
                )
                variation_fail_count = sum(
                    1 for row in variation_statuses if row.get("status_label") == "FAIL"
                )
                if variation_fail_count > 0:
                    latest_status = "FAIL"
                    latest_status_badge_class = "badge-error"
                elif variation_pass_count == len(variation_statuses):
                    latest_status = "PASS"
                    latest_status_badge_class = "badge-success"
                elif variation_pass_count > 0:
                    latest_status = "PARTIAL"
                    latest_status_badge_class = "badge-warning"
                else:
                    latest_status = "NOT RUN"
                    latest_status_badge_class = "badge-ghost"
            else:
                latest_success = bool((latest_run or {}).get("success"))
                if latest_run:
                    latest_status = "PASS" if latest_success else "FAIL"
                    latest_status_badge_class = (
                        "badge-success" if latest_success else "badge-error"
                    )
                else:
                    latest_status = "NOT RUN"
                    latest_status_badge_class = "badge-ghost"

            if latest_status == "PASS":
                passing_script_total += 1
            elif latest_status == "FAIL":
                failing_script_total += 1
            elif latest_status == "NOT RUN":
                not_run_script_total += 1
            else:
                partial_script_total += 1

            requirement_script_rows.append(
                {
                    "test_case_id": Path(script_path).stem or script_path,
                    "script": script_path,
                    "script_title": script_map[script_path].get("script_title")
                    or Path(script_path).stem
                    or script_path,
                    "added_to_report": script_path in tracked_script_set,
                    "latest_status": latest_status,
                    "latest_status_badge_class": latest_status_badge_class,
                    "latest_job_id": (latest_run or {}).get("job_id"),
                    "latest_completed_human": (latest_run or {}).get("completed_at_human") or "—",
                    "variation_statuses": variation_statuses,
                }
            )

        script_total = len(script_paths)
        requirement_system = (
            UNSPECIFIED_SYSTEM
            if requirement == no_requirement_label
            else requirement_to_system(requirement)
        )
        progress_denominator = script_total if script_total > 0 else 1
        progress_fail_pct = (failing_script_total / progress_denominator) * 100.0
        progress_pass_pct = (passing_script_total / progress_denominator) * 100.0
        progress_not_run_pct = (
            (not_run_script_total + partial_script_total) / progress_denominator
        ) * 100.0
        progress_label = (
            f"{passing_script_total} pass / {failing_script_total} fail / "
            f"{not_run_script_total + partial_script_total} unrun"
        )
        if script_total == 0 or not_run_script_total == script_total:
            requirement_status_label = "REQ NOT RUN"
            requirement_status_badge_class = "badge-ghost"
            overall_status_label = "UNTESTED"
            overall_status_badge_class = "badge-ghost"
            requirement_status_counts["not_run"] += 1
        elif failing_script_total > 0:
            requirement_status_label = "REQ FAIL"
            requirement_status_badge_class = "badge-error"
            overall_status_label = "FAILING"
            overall_status_badge_class = "badge-error"
            requirement_status_counts["fail"] += 1
        elif passing_script_total == script_total:
            requirement_status_label = "REQ PASS"
            requirement_status_badge_class = "badge-success"
            overall_status_label = "PASSING"
            overall_status_badge_class = "badge-success"
            requirement_status_counts["pass"] += 1
        else:
            requirement_status_label = "REQ PARTIAL"
            requirement_status_badge_class = "badge-warning"
            overall_status_label = "PARTIAL"
            overall_status_badge_class = "badge-warning"
            requirement_status_counts["partial"] += 1

        runs_for_requirement = sorted(
            requirement_run_history.get(requirement, []),
            key=lambda row: row.get("completed_at") or 0,
            reverse=True,
        )
        latest_run = runs_for_requirement[0] if runs_for_requirement else None

        tracked_script_paths = [
            script_path for script_path in script_paths if script_path in tracked_script_set
        ]
        report_requirement_groups.append(
            {
                "requirement": requirement,
                "requirement_text": (
                    "Script has no declared requirement."
                    if requirement == no_requirement_label
                    else requirement_text_map.get(requirement, "")
                ),
                "latest_job_id": (latest_run or {}).get("job_id"),
                "latest_success": bool((latest_run or {}).get("success")) if latest_run else None,
                "latest_completed_human": (latest_run or {}).get("completed_at_human") or "—",
                "run_count": len(runs_for_requirement),
                "script_count": script_total,
                "tracked_script_count": len(tracked_script_paths),
                "latest_script_total": script_total,
                "latest_passing_script_total": passing_script_total,
                "passing_script_total": passing_script_total,
                "failing_script_total": failing_script_total,
                "not_run_script_total": not_run_script_total,
                "partial_script_total": partial_script_total,
                "progress_fail_pct": progress_fail_pct,
                "progress_pass_pct": progress_pass_pct,
                "progress_not_run_pct": progress_not_run_pct,
                "progress_label": progress_label,
                "requirement_status_label": requirement_status_label,
                "requirement_status_badge_class": requirement_status_badge_class,
                "overall_status_label": overall_status_label,
                "overall_status_badge_class": overall_status_badge_class,
                "requirement_system": requirement_system,
                "script_paths": script_paths,
                "history": runs_for_requirement[:8],
                "runs": runs_for_requirement,
                "scripts": requirement_script_rows,
            }
        )
        system_summary = report_system_summaries.setdefault(
            requirement_system,
            {
                "system": requirement_system,
                "requirement_total": 0,
                "pass": 0,
                "partial": 0,
                "fail": 0,
                "not_run": 0,
            },
        )
        system_summary["requirement_total"] += 1
        if overall_status_label == "PASSING":
            system_summary["pass"] += 1
        elif overall_status_label == "FAILING":
            system_summary["fail"] += 1
        elif overall_status_label == "UNTESTED":
            system_summary["not_run"] += 1
        else:
            system_summary["partial"] += 1

    system_rows: List[Dict[str, Any]] = []
    for system in sorted_systems(set(report_system_summaries.keys())):
        row = report_system_summaries[system]
        if row["fail"] > 0:
            status_label = "FAILING"
            status_badge_class = "badge-error"
        elif row["partial"] > 0:
            status_label = "PARTIAL"
            status_badge_class = "badge-warning"
        elif row["pass"] == row["requirement_total"] and row["requirement_total"] > 0:
            status_label = "PASSING"
            status_badge_class = "badge-success"
        elif row["pass"] > 0 and row["not_run"] > 0:
            status_label = "PARTIAL"
            status_badge_class = "badge-warning"
        else:
            status_label = "UNTESTED"
            status_badge_class = "badge-ghost"
        system_rows.append(
            {
                **row,
                "status_label": status_label,
                "status_badge_class": status_badge_class,
            }
        )

    return {
        "report_requirement_ids": requirement_ids,
        "report_requirement_groups": report_requirement_groups,
        "report_system_summaries": system_rows,
        "report_requeue_script_paths": sorted(report_requeue_script_paths),
        "report_script_total": len(report_requeue_script_paths),
        "report_tracked_script_total": len([path for path in tracked_script_set if path in report_requeue_script_paths]),
        "requirement_status_counts": requirement_status_counts,
        "has_report_queue_seed": report_seed_job_template(ctx, report_id) is not None,
    }


def build_report_export_context(ctx: FrontendHelperContext, report_id: str) -> Dict[str, Any]:
    def _requirement_anchor(req: str) -> str:
        slug = re.sub(r"[^a-zA-Z0-9]+", "-", str(req or "").strip().lower()).strip("-")
        return f"req-{slug or 'unspecified'}"

    requirement_text_map: Dict[str, str] = {}
    try:
        requirement_text_map = {req.id: req.text for req in load_default_requirements()}
    except Exception:
        requirement_text_map = {}

    report_meta = ctx.reporting.get_report(report_id)
    completed = iter_completed_results_for_report(ctx, report_id)
    requirement_view = build_report_requirement_groups(
        ctx,
        report_id=report_id,
        requirement_text_map=requirement_text_map,
    )
    base_groups = requirement_view["report_requirement_groups"]
    requirement_groups = [
        {**group, "anchor_id": _requirement_anchor(str(group.get("requirement") or ""))}
        for group in base_groups
    ]
    requirement_toc = [
        {
            "anchor_id": group["anchor_id"],
            "requirement": group.get("requirement"),
            "status_label": group.get("requirement_status_label"),
            "status_badge_class": group.get("requirement_status_badge_class"),
        }
        for group in requirement_groups
    ]

    latest_status_by_script: Dict[str, str] = {}
    for group in requirement_groups:
        for row in (group.get("scripts") or []):
            script_path = str(row.get("script") or "").strip()
            if not script_path:
                continue
            latest_status = str(row.get("latest_status") or "").upper()
            latest_status_by_script.setdefault(script_path, latest_status)

    latest_pass_count = sum(1 for status in latest_status_by_script.values() if status == "PASS")
    latest_fail_count = sum(1 for status in latest_status_by_script.values() if status == "FAIL")
    latest_not_run_count = sum(
        1 for status in latest_status_by_script.values() if status not in {"PASS", "FAIL"}
    )
    latest_completed_at = None
    for row in completed:
        value = row.get("completed_at")
        if value is None:
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if latest_completed_at is None or numeric > latest_completed_at:
            latest_completed_at = numeric

    return {
        "report_id": report_id,
        "report_meta": report_meta,
        "completed_count": len(completed),
        "requirement_total": len(requirement_view["report_requirement_ids"]),
        "script_total": requirement_view["report_script_total"],
        "scripts_in_report_total": requirement_view["report_tracked_script_total"],
        "latest_pass_count": latest_pass_count,
        "latest_fail_count": latest_fail_count,
        "latest_not_run_count": latest_not_run_count,
        "latest_completed_human": human_datetime(latest_completed_at),
        "requirement_status_counts": requirement_view["requirement_status_counts"],
        "requirement_toc": requirement_toc,
        "requirement_groups": requirement_groups,
    }


def build_report_export_rst(export_context: Dict[str, Any]) -> str:
    report_meta = export_context.get("report_meta") or {}
    report_id = str(export_context.get("report_id") or "")
    title = str(report_meta.get("title") or report_id or "Report Export").strip() or "Report Export"
    description = str(report_meta.get("description") or "").strip()
    requirement_status_counts = export_context.get("requirement_status_counts") or {}
    groups = export_context.get("requirement_groups") or []

    def _safe(text: Any) -> str:
        raw = str(text or "")
        return raw.replace("\\", "\\\\")

    def _latex_escape(text: Any) -> str:
        raw = str(text or "")
        replacements = {
            "\\": r"\textbackslash{}",
            "&": r"\&",
            "%": r"\%",
            "$": r"\$",
            "#": r"\#",
            "_": r"\_",
            "{": r"\{",
            "}": r"\}",
            "~": r"\textasciitilde{}",
            "^": r"\textasciicircum{}",
        }
        out = raw
        for old, new in replacements.items():
            out = out.replace(old, new)
        return out

    title_latex = _latex_escape(title)
    desc_latex = _latex_escape(description)
    report_id_latex = _latex_escape(report_id)
    title_page_latex = render_template(
        "export/report_title_page.tex.j2",
        title_latex=title_latex,
        report_id_latex=report_id_latex,
        description_latex=desc_latex,
        has_description=bool(description),
    ).strip("\n")
    lines: List[str] = [
        title,
        "=" * len(title),
        "",
        ".. raw:: latex",
        "",
    ]
    for latex_line in title_page_latex.splitlines():
        lines.append(f"   {latex_line}")
    lines.append("")
    if description:
        lines.extend([_safe(description), ""])
    lines.extend(
        [
            f"Report ID: ``{_safe(report_id)}``",
            "",
            "Summary",
            "-------",
            "",
            f"- Requirements: {int(export_context.get('requirement_total') or 0)}",
            f"- Scripts: {int(export_context.get('script_total') or 0)}",
            f"- Scripts added to report: {int(export_context.get('scripts_in_report_total') or 0)}",
            f"- Completed runs: {int(export_context.get('completed_count') or 0)}",
            f"- Latest PASS scripts: {int(export_context.get('latest_pass_count') or 0)}",
            f"- Latest FAIL scripts: {int(export_context.get('latest_fail_count') or 0)}",
            f"- Latest NOT RUN scripts: {int(export_context.get('latest_not_run_count') or 0)}",
            f"- Requirement status PASS: {int(requirement_status_counts.get('pass') or 0)}",
            f"- Requirement status PARTIAL: {int(requirement_status_counts.get('partial') or 0)}",
            f"- Requirement status FAIL: {int(requirement_status_counts.get('fail') or 0)}",
            f"- Requirement status NOT RUN: {int(requirement_status_counts.get('not_run') or 0)}",
            "",
            "System Summary",
            "--------------",
            "",
            ".. list-table:: Requirement Systems",
            "   :header-rows: 1",
            "   :widths: 20 20 20 20 20",
            "",
            "   * - System",
            "     - Passing",
            "     - Partial",
            "     - Failing",
            "     - Untested",
        ]
    )

    def _requirement_status_for_summary(group: Dict[str, Any]) -> str:
        scripts = list(group.get("scripts") or [])
        if not scripts:
            return "UNTESTED"

        has_pass = False
        has_fail = False
        has_untested = False
        for row in scripts:
            latest_status = str(row.get("latest_status") or "NOT RUN").strip().upper()
            if latest_status == "PASS":
                has_pass = True
            elif latest_status == "FAIL":
                has_fail = True
            else:
                has_untested = True
            if not bool(row.get("added_to_report")):
                has_untested = True

        if has_fail:
            return "FAIL"
        if has_pass and not has_untested:
            return "PASS"
        if has_pass and has_untested:
            return "PARTIAL"
        return "UNTESTED"

    system_requirement_counts: Dict[str, Dict[str, int]] = {}
    for group in groups:
        requirement = str(group.get("requirement") or "").strip()
        if requirement == "No Requirement Declared":
            system = UNSPECIFIED_SYSTEM
        else:
            system = requirement_to_system(requirement)
        bucket = system_requirement_counts.setdefault(
            system,
            {"pass": 0, "partial": 0, "fail": 0, "untested": 0},
        )
        requirement_status = _requirement_status_for_summary(group)
        if requirement_status == "PASS":
            bucket["pass"] += 1
        elif requirement_status == "PARTIAL":
            bucket["partial"] += 1
        elif requirement_status == "FAIL":
            bucket["fail"] += 1
        else:
            bucket["untested"] += 1

    systems = sorted_systems(set(system_requirement_counts.keys()))
    if systems:
        for system in systems:
            counts = system_requirement_counts.get(system) or {}
            lines.extend(
                [
                    f"   * - {_safe(system)}",
                    f"     - {int(counts.get('pass') or 0)}",
                    f"     - {int(counts.get('partial') or 0)}",
                    f"     - {int(counts.get('fail') or 0)}",
                    f"     - {int(counts.get('untested') or 0)}",
                ]
            )
    else:
        lines.extend(
            [
                "   * - —",
                "     - 0",
                "     - 0",
                "     - 0",
                "     - 0",
            ]
        )

    lines.extend(["", "Requirements", "------------", ""])

    if not groups:
        lines.extend(["No requirement groups available.", ""])
        return "\n".join(lines).rstrip() + "\n"

    appendix_groups: List[Dict[str, Any]] = []
    for group in groups:
        requirement = str(group.get("requirement") or "Requirement").strip() or "Requirement"
        requirement_text = str(group.get("requirement_text") or "").strip()
        heading = f"{requirement} [{group.get('requirement_status_label') or 'REQ NOT RUN'}]"
        lines.extend([_safe(heading), "~" * len(heading), ""])
        if requirement_text:
            lines.extend([_safe(requirement_text), ""])
        lines.extend(
            [
                ".. list-table:: Script Status",
                "   :header-rows: 1",
                "   :widths: 44 10 16 30",
                "",
                "   * - Script Title",
                "     - Added",
                "     - Latest Status",
                "     - Latest Completed",
            ]
        )
        appendix_scripts: List[Dict[str, str]] = []
        for row in (group.get("scripts") or []):
            script_path = str(row.get("script") or "").strip()
            script_label = str(row.get("script_title") or "").strip()
            if not script_label:
                script_label = Path(script_path).name if script_path else "Untitled Script"
            appendix_scripts.append({"title": script_label, "path": script_path})
            added = "Added" if bool(row.get("added_to_report")) else "Not Added"
            lines.extend(
                [
                    f"   * - {_safe(script_label)}",
                    f"     - {added}",
                    f"     - {_safe(row.get('latest_status') or 'NOT RUN')}",
                    f"     - {_safe(row.get('latest_completed_human') or '—')}",
                ]
            )
        lines.append("")
        appendix_groups.append({"requirement": requirement, "scripts": appendix_scripts})
    appendix_heading = "Appendix: Requirement Script Reference"
    lines.extend(
        [
            appendix_heading,
            "-" * len(appendix_heading),
            "",
            "Requirement to script mappings with script titles and paths.",
            "",
        ]
    )
    for appendix_group in appendix_groups:
        requirement = str(appendix_group.get("requirement") or "Requirement").strip() or "Requirement"
        heading = f"{requirement} Script Mapping"
        lines.extend([_safe(heading), "~" * len(heading), ""])
        scripts = appendix_group.get("scripts") or []
        if not scripts:
            lines.extend(["- No scripts.", ""])
            continue
        for script in scripts:
            title = str(script.get("title") or "Untitled Script").strip() or "Untitled Script"
            path = str(script.get("path") or "—").strip() or "—"
            lines.append(f"- {_safe(title)}: ``{_safe(path)}``")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_rst_pdf(
    ctx: FrontendHelperContext,
    rst_text: str,
    report_id: str = "",
    latest_completed_human: str = "—",
) -> bytes:
    def _latex_escape(text: Any) -> str:
        raw = str(text or "")
        replacements = {
            "\\": r"\textbackslash{}",
            "&": r"\&",
            "%": r"\%",
            "$": r"\$",
            "#": r"\#",
            "_": r"\_",
            "{": r"\{",
            "}": r"\}",
            "~": r"\textasciitilde{}",
            "^": r"\textasciicircum{}",
        }
        out = raw
        for old, new in replacements.items():
            out = out.replace(old, new)
        return out

    pdflatex_path = shutil.which("pdflatex")
    if not pdflatex_path:
        raise RuntimeError("pdflatex not found in PATH.")
    footer_report_id = _latex_escape(report_id or "unknown")
    footer_last_run = _latex_escape(latest_completed_human or "—")
    latex_preamble = render_template(
        "export/report_pdf_preamble.tex.j2",
        footer_report_id=footer_report_id,
        footer_last_run=footer_last_run,
    ).strip()

    latex_text = docutils.core.publish_string(
        source=rst_text,
        writer_name="latex",
        settings_overrides={
            "output_encoding": "unicode",
            "input_encoding": "utf-8",
            "latex_preamble": latex_preamble,
        },
    )
    if not isinstance(latex_text, str):
        latex_text = latex_text.decode("utf-8", errors="replace")

    with tempfile.TemporaryDirectory(prefix="report-export-") as tmpdir:
        workdir = Path(tmpdir)
        tex_path = workdir / "report.tex"
        pdf_path = workdir / "report.pdf"
        tex_path.write_text(latex_text, encoding="utf-8")

        cmd = [
            pdflatex_path,
            "-interaction=nonstopmode",
            "-halt-on-error",
            "-file-line-error",
            "-output-directory",
            str(workdir),
            str(tex_path),
        ]
        for _ in range(2):
            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                tail = "\n".join((proc.stdout or "").splitlines()[-30:])
                err_tail = "\n".join((proc.stderr or "").splitlines()[-10:])
                details = "\n".join(part for part in [tail, err_tail] if part).strip()
                raise RuntimeError(f"LaTeX compile failed.\n{details}".strip())
        if not pdf_path.is_file():
            raise RuntimeError("LaTeX compile did not produce a PDF.")
        return pdf_path.read_bytes()
