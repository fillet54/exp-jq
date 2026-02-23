"""Frontend route registration for the jobqueue dashboard."""

import logging
import time
import math
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List

from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    send_from_directory,
    stream_with_context,
    url_for,
)

from automationv3.framework.requirements import REQUIREMENT_ID_PATTERN, load_default_requirements
from automationv3.framework.rst import collect_script_syntax_issues, parse_rst_chunks, render_script_rst_html

from . import uuid7_str
from .fscache import snapshot_tree


def _parse_meta_from_rst(path: Path) -> Dict[str, List[str]]:
    try:
        content = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return {"requirements": [], "tags": [], "subsystem": []}
    return _parse_meta_from_lines(content)


def _parse_meta_from_lines(content: List[str]) -> Dict[str, List[str]]:
    meta: Dict[str, List[str]] = {"requirements": [], "tags": [], "subsystem": []}
    try:
        content = list(content)
    except Exception:
        return meta
    in_meta = False
    for line in content:
        if line.strip().startswith(".. meta::"):
            in_meta = True
            continue
        if in_meta:
            if not line.startswith("   "):
                break
            stripped = line.strip()
            if stripped.startswith(":") and ":" in stripped[1:]:
                key, val = stripped[1:].split(":", 1)
                key = key.strip()
                val_list = [v.strip() for v in val.split(",") if v.strip()]
                if key in meta:
                    meta[key].extend(val_list)
                else:
                    meta[key] = val_list
    return meta


def _extract_rst_title(lines: List[str], fallback: str = "") -> str:
    adornments = set("=-~^\"`*+#:.")
    for idx in range(len(lines) - 1):
        title = lines[idx].strip()
        underline = lines[idx + 1].strip()
        if (
            title
            and underline
            and len(underline) >= len(title)
            and len(set(underline)) == 1
            and underline[0] in adornments
        ):
            return title
    return fallback


def _discover_scripts(root: Path) -> List[Dict[str, Any]]:
    scripts: List[Dict[str, Any]] = []
    if not root.exists():
        return scripts
    for path in sorted(root.rglob("*.rst")):
        rel = path.relative_to(root)
        try:
            content = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            continue
        lines = content.splitlines()
        meta = _parse_meta_from_lines(lines)
        title = _extract_rst_title(lines, fallback=path.stem)
        syntax_issues = collect_script_syntax_issues(content)
        syntax_error_count = sum(1 for issue in syntax_issues if issue.get("is_error"))
        scripts.append(
            {
                "path": str(path),
                "relpath": str(rel),
                "meta": meta,
                "name": path.stem,
                "title": title,
                "syntax_issues": syntax_issues,
                "syntax_error_count": syntax_error_count,
                "has_syntax_errors": syntax_error_count > 0,
            }
        )
    return scripts


def _build_raw_source_rows(script_content: str, issues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    line_to_issues: Dict[int, List[Dict[str, Any]]] = {}
    for issue in issues:
        line = issue.get("line")
        if isinstance(line, int) and line > 0:
            line_to_issues.setdefault(line, []).append(issue)

    rows: List[Dict[str, Any]] = []
    for index, line_text in enumerate(script_content.splitlines(), start=1):
        row_issues = line_to_issues.get(index, [])
        rows.append(
            {
                "line": index,
                "text": line_text,
                "issues": row_issues,
                "has_error": any(bool(issue.get("is_error")) for issue in row_issues),
            }
        )
    return rows


def _normalize_script_directory(relpath: str) -> str:
    parent = str(PurePosixPath(relpath).parent)
    return "" if parent == "." else parent


def _parent_directory(dirpath: str) -> str | None:
    if not dirpath:
        return None
    parent = str(PurePosixPath(dirpath).parent)
    return "" if parent == "." else parent


def _build_script_directory_index(
    scripts: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], Dict[str, List[str]], Dict[str, List[Dict[str, Any]]], Dict[str, int]]:
    directory_to_scripts: Dict[str, List[Dict[str, Any]]] = {"": []}
    directory_children: Dict[str, set[str]] = {"": set()}
    directories = {""}

    for script in scripts:
        relpath = script.get("relpath") or ""
        dirpath = _normalize_script_directory(relpath)
        directory_to_scripts.setdefault(dirpath, []).append(script)
        directories.add(dirpath)

        current = ""
        for part in (PurePosixPath(dirpath).parts if dirpath else ()):
            child = part if not current else f"{current}/{part}"
            directory_children.setdefault(current, set()).add(child)
            directory_children.setdefault(child, set())
            directory_to_scripts.setdefault(child, [])
            directories.add(child)
            current = child

    for dirpath in directories:
        directory_children.setdefault(dirpath, set())
        directory_to_scripts.setdefault(dirpath, [])
        directory_to_scripts[dirpath] = sorted(
            directory_to_scripts[dirpath],
            key=lambda row: ((row.get("title") or "").lower(), row.get("relpath") or ""),
        )

    recursive_counts: Dict[str, int] = {}
    for dirpath in sorted(directories, key=lambda d: (d.count("/"), len(d)), reverse=True):
        recursive_counts[dirpath] = len(directory_to_scripts.get(dirpath, [])) + sum(
            recursive_counts.get(child, 0)
            for child in sorted(directory_children.get(dirpath, set()))
        )

    directory_nodes: List[Dict[str, Any]] = [
        {
            "path": "",
            "name": ".",
            "depth": 0,
            "script_count": len(directory_to_scripts.get("", [])),
            "total_script_count": recursive_counts.get("", 0),
        }
    ]

    def _walk(parent: str, depth: int):
        for child in sorted(directory_children.get(parent, set())):
            directory_nodes.append(
                {
                    "path": child,
                    "name": PurePosixPath(child).name,
                    "depth": depth,
                    "script_count": len(directory_to_scripts.get(child, [])),
                    "total_script_count": recursive_counts.get(child, 0),
                }
            )
            _walk(child, depth + 1)

    _walk("", 1)
    return (
        directory_nodes,
        {key: sorted(value) for key, value in directory_children.items()},
        directory_to_scripts,
        recursive_counts,
    )


def _build_report_listing(
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
        report_id = job.get("report_id")
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


def _coerce_positive_int(raw_value: Any, default: int, minimum: int = 1, maximum: int = 10_000) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return default
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value


def _paginate_items(items: List[Dict[str, Any]], page: int, per_page: int) -> Dict[str, Any]:
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


UNSPECIFIED_SYSTEM = "UNSPECIFIED"
UNKNOWN_SYSTEM = "UNKNOWN"


def _requirement_to_system(requirement_id: str) -> str:
    req = (requirement_id or "").strip().upper()
    if not req:
        return UNKNOWN_SYSTEM
    match = REQUIREMENT_ID_PATTERN.fullmatch(req)
    if not match:
        return UNKNOWN_SYSTEM
    return match.group("system")


def _build_script_system_index(
    scripts: List[Dict[str, Any]],
) -> tuple[List[str], Dict[str, int], Dict[str, Dict[str, List[Dict[str, Any]]]]]:
    system_to_requirement_to_scripts: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

    for script in scripts:
        meta = script.get("meta") or {}
        requirements = [r.strip() for r in (meta.get("requirements") or []) if r.strip()]
        script["requirements"] = requirements

        if not requirements:
            system_to_requirement_to_scripts.setdefault(UNSPECIFIED_SYSTEM, {}).setdefault(
                "No Requirement Declared", []
            ).append(script)
            continue

        for req in requirements:
            system = _requirement_to_system(req)
            system_to_requirement_to_scripts.setdefault(system, {}).setdefault(req, []).append(script)

    for system, req_map in system_to_requirement_to_scripts.items():
        for req, items in list(req_map.items()):
            # De-duplicate scripts by relpath per requirement bucket.
            seen = set()
            deduped = []
            for item in items:
                relpath = item.get("relpath")
                if relpath in seen:
                    continue
                seen.add(relpath)
                deduped.append(item)
            req_map[req] = sorted(deduped, key=lambda row: row.get("relpath", ""))

    systems = _sorted_systems(system_to_requirement_to_scripts.keys())

    system_counts: Dict[str, int] = {}
    for system, req_map in system_to_requirement_to_scripts.items():
        unique_paths = set()
        for rows in req_map.values():
            for row in rows:
                unique_paths.add(row.get("relpath"))
        system_counts[system] = len(unique_paths)

    return systems, system_counts, system_to_requirement_to_scripts


def _sorted_systems(systems: List[str] | set[str]) -> List[str]:
    ordered = sorted(systems)
    # Keep synthetic categories predictable in the sidebar.
    if UNSPECIFIED_SYSTEM in ordered:
        ordered.remove(UNSPECIFIED_SYSTEM)
        ordered.append(UNSPECIFIED_SYSTEM)
    if UNKNOWN_SYSTEM in ordered:
        ordered.remove(UNKNOWN_SYSTEM)
        ordered.append(UNKNOWN_SYSTEM)
    return ordered


def _build_legacy_result_document(job_data: Dict[str, Any], result_data: Dict[str, Any]) -> str:
    if not isinstance(result_data, dict):
        return ""
    rvt = result_data.get("rvt")
    if not isinstance(rvt, dict):
        return ""

    output = (result_data.get("output") or "").strip()
    lines = [
        "Execution Output",
        "================",
        "",
    ]
    default_timestamp_value = result_data.get("timestamp") or result_data.get("completed_at")
    if isinstance(default_timestamp_value, (int, float)):
        default_timestamp = datetime.fromtimestamp(
            float(default_timestamp_value), tz=timezone.utc
        ).isoformat()
    else:
        default_timestamp = str(default_timestamp_value or datetime.now(timezone.utc).isoformat())
    duration = float(result_data.get("duration_seconds") or result_data.get("duration") or 0.0)

    def _make_rvt_result(
        step_text: str,
        status: str,
        detail_text: str,
        step_timestamp: Any = None,
        step_duration: Any = None,
    ) -> str:
        safe_step = step_text.strip() or "(legacy-step)"
        safe_detail = detail_text.strip() or "No output captured."
        if isinstance(step_timestamp, (int, float)):
            timestamp_value = datetime.fromtimestamp(
                float(step_timestamp), tz=timezone.utc
            ).isoformat()
        else:
            timestamp_value = str(step_timestamp or default_timestamp)
        try:
            duration_value = float(step_duration) if step_duration is not None else duration
        except (TypeError, ValueError):
            duration_value = duration
        directive_lines = [
            ".. rvt-result::",
            f"   :status: {status}",
            f"   :timestamp: {timestamp_value}",
            f"   :duration: {duration_value:.6f}",
            "",
            "   .. rvt::",
            "",
            *[f"      {line}" for line in safe_step.splitlines()],
            "",
            "   .. code-block:: text",
            "",
            *[f"      {line}" for line in safe_detail.splitlines()],
            "",
        ]
        return "\n".join(directive_lines)

    directives: List[str] = []
    invocations = rvt.get("invocations") or []
    for row in invocations:
        block = str((row or {}).get("block") or "block").strip()
        args = [str(arg) for arg in ((row or {}).get("args") or [])]
        result_text = str((row or {}).get("result") or "").strip()
        status = "pass" if bool((row or {}).get("passed")) else "fail"
        call_repr = f"({block}{(' ' + ' '.join(args)) if args else ''})"
        directives.append(
            _make_rvt_result(
                call_repr,
                status,
                result_text,
                step_timestamp=(row or {}).get("timestamp"),
                step_duration=(row or {}).get("duration"),
            )
        )

    if not directives:
        results = rvt.get("results") or []
        for row in results:
            form_text = str((row or {}).get("form") or "").strip()
            result_text = str((row or {}).get("result") or "").strip()
            status = "pass" if bool((row or {}).get("passed")) else "fail"
            if not form_text and not result_text:
                continue
            directives.append(_make_rvt_result(form_text, status, result_text))

    if not directives:
        fallback_file = str(job_data.get("file") or "unknown")
        status = "pass" if bool(rvt.get("passed", True)) else "fail"
        directives.append(
            _make_rvt_result(
                "(legacy-step)",
                status,
                output or f"No detailed RVT rows captured for {fallback_file}.",
            )
        )
    elif output:
        lines.extend(
            [
                ".. note::",
                f"   {output}",
                "",
            ]
        )

    script_file = str(job_data.get("file") or "").strip()
    scripts_root = str(job_data.get("scripts_root") or "").strip()
    if script_file and scripts_root:
        base_path = Path(scripts_root).resolve()
        script_path = (base_path / script_file).resolve()
        try:
            script_path.relative_to(base_path)
            if script_path.exists() and script_path.is_file():
                script_text = script_path.read_text(encoding="utf-8")
                chunks = parse_rst_chunks(script_text)
                rvt_count = sum(1 for chunk in chunks if chunk.kind == "rvt")
                if rvt_count > 0:
                    if len(directives) <= rvt_count:
                        grouped = []
                        cursor = 0
                        for _ in range(rvt_count):
                            if cursor < len(directives):
                                grouped.append([directives[cursor]])
                                cursor += 1
                            else:
                                grouped.append([])
                    else:
                        grouped = []
                        total = len(directives)
                        for idx in range(rvt_count):
                            start = round((idx * total) / rvt_count)
                            end = round(((idx + 1) * total) / rvt_count)
                            grouped.append(directives[start:end])
                    rendered_parts: List[str] = []
                    rvt_index = 0
                    for chunk in chunks:
                        if chunk.kind == "text":
                            rendered_parts.append(chunk.content)
                        else:
                            group = grouped[rvt_index] if rvt_index < len(grouped) else []
                            if group:
                                rendered_parts.append("\n".join(group).rstrip() + "\n\n")
                            rvt_index += 1
                    return "".join(rendered_parts).rstrip() + "\n"
        except Exception:
            pass

    lines.extend(directives)
    return "\n".join(lines).rstrip() + "\n"


def register_frontend_routes(
    app: Flask,
    queue,
    central,
    uut_store,
    suite_manager,
    scripts_root: Path,
    scripts_cache_dir: str,
    log: logging.Logger,
) -> None:
    def _safe_return_to(target: str) -> str | None:
        clean = (target or "").strip()
        if clean.startswith("/") and not clean.startswith("//"):
            return clean
        return None

    def _resolve_rel_script_path(script_path: str, base_path: Path) -> str:
        clean_path = (script_path or "").strip()
        if not clean_path:
            raise ValueError("script_path required")
        resolved_base = base_path.resolve()
        candidate = Path(clean_path)
        resolved_path = (
            candidate.resolve() if candidate.is_absolute() else (resolved_base / candidate).resolve()
        )
        if not resolved_path.is_file():
            raise ValueError("script_path not found")
        try:
            relpath = resolved_path.relative_to(resolved_base)
        except ValueError as exc:
            raise ValueError("script_path must be inside base_path") from exc
        return relpath.as_posix()

    def _normalize_requirements(raw: Any) -> List[str]:
        if raw is None:
            return []
        values: List[str] = []
        if isinstance(raw, str):
            values = [part.strip() for part in raw.split(",") if part.strip()]
        elif isinstance(raw, list):
            for item in raw:
                if item is None:
                    continue
                text = str(item).strip()
                if text:
                    values.append(text)
        else:
            text = str(raw).strip()
            if text:
                values = [text]
        deduped: List[str] = []
        seen = set()
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            deduped.append(value)
        return deduped

    def _iter_completed_results_for_report(report_id: str) -> List[Dict[str, Any]]:
        rows = [
            res
            for res in queue.list_results(limit=5000)
            if (res.get("job_data") or {}).get("report_id") == report_id
        ]
        return sorted(rows, key=lambda row: row.get("completed_at") or 0, reverse=True)

    def _build_requeue_job_from_result_job_data(
        source_job: Dict[str, Any], report_id: str
    ) -> Dict[str, Any] | None:
        script = str(source_job.get("file") or "").strip()
        uut = str(source_job.get("uut") or "").strip()
        if not script or not uut:
            return None

        job: Dict[str, Any] = {
            "file": script,
            "uut": uut,
            "report_id": report_id,
            "suite_run_id": "",
            "suite_name": "",
        }
        for key in (
            "uut_tree",
            "uut_id",
            "framework_version",
            "scripts_tree",
            "scripts_root",
        ):
            value = source_job.get(key)
            if value not in (None, ""):
                job[key] = value

        source_meta = source_job.get("meta")
        meta_copy: Dict[str, List[str]] = {}
        if isinstance(source_meta, dict):
            for key, value in source_meta.items():
                if isinstance(value, list):
                    meta_copy[str(key)] = [str(item) for item in value if str(item).strip()]
                elif value not in (None, ""):
                    meta_copy[str(key)] = [str(value)]
        job["meta"] = meta_copy

        script_candidates: List[Path] = []
        source_scripts_root = str(source_job.get("scripts_root") or "").strip()
        if source_scripts_root:
            script_candidates.append((Path(source_scripts_root).resolve() / script).resolve())
        script_candidates.append((scripts_root.resolve() / script).resolve())
        for candidate in script_candidates:
            try:
                if candidate.exists() and candidate.is_file():
                    job["meta"] = _parse_meta_from_rst(candidate)
                    break
            except Exception:
                continue
        return job

    def _requeue_report_scripts(report_id: str, script_paths: List[str]) -> int:
        completed = _iter_completed_results_for_report(report_id)
        latest_job_by_script: Dict[str, Dict[str, Any]] = {}
        for row in completed:
            job_data = row.get("job_data") or {}
            script = str(job_data.get("file") or "").strip()
            if not script or script in latest_job_by_script:
                continue
            latest_job_by_script[script] = job_data

        tracked_templates: Dict[str, Dict[str, Any]] = {}
        for row in queue.list_report_scripts(report_id):
            script = str(row.get("script_path") or "").strip()
            template = row.get("job_template")
            if not script or not isinstance(template, dict):
                continue
            tracked_templates[script] = template

        queued_count = 0
        for script in script_paths:
            source_job = latest_job_by_script.get(script)
            if not source_job:
                source_job = tracked_templates.get(script)
            if not source_job:
                continue
            queued_job = _build_requeue_job_from_result_job_data(source_job, report_id=report_id)
            if not queued_job:
                continue
            queue.add_job(queued_job, priority=0)
            queued_count += 1
        return queued_count

    def _render_jobs_table() -> str:
        jobs = queue.list_jobs()
        return render_template("partials/jobs_table.html", jobs=jobs)

    def _render_workers_table() -> str:
        workers = central.get_workers_snapshot()
        return render_template(
            "partials/workers_table.html", workers=workers, now_ts=time.time()
        )

    def _render_uuts_table() -> str:
        uuts = uut_store.list()
        return render_template("partials/uuts_table.html", uuts=uuts)

    def _render_results_table() -> str:
        results = queue.list_results()
        return render_template("partials/results_table.html", results=results)

    def _render_suites_table() -> str:
        suites = []
        for name in suite_manager.list_suites():
            suites.append({"name": name, "scripts": suite_manager.get_suite(name)})
        return render_template("partials/suites_table.html", suites=suites)

    def _build_job_output_context(job_id: str) -> Dict[str, Any] | None:
        queued_job = queue.get_job(job_id)
        result_row = queue.get_result(job_id)
        live_output = central.get_live_job_output(job_id)

        if not queued_job and not result_row and not (live_output.get("events") or live_output.get("result_document")):
            return None

        workers = central.get_workers_snapshot()
        in_progress_worker = None
        for worker in workers:
            if getattr(worker, "current_job", None) == job_id and bool(getattr(worker, "busy", False)):
                in_progress_worker = worker
                break

        state = "queued"
        if result_row:
            state = "completed"
        elif in_progress_worker:
            state = "in_progress"

        job_data = {}
        if queued_job:
            job_data.update(queued_job)
        if result_row and isinstance(result_row.get("job_data"), dict):
            job_data.update(result_row.get("job_data") or {})

        result_data = result_row.get("result_data") if result_row else {}
        if not isinstance(result_data, dict):
            result_data = {}

        nested_rvt = result_data.get("rvt") if isinstance(result_data.get("rvt"), dict) else {}
        result_document = (
            result_data.get("result_document")
            or (nested_rvt or {}).get("result_document")
            or live_output.get("result_document")
            or ""
        )
        if not result_document and result_row:
            result_document = _build_legacy_result_document(job_data, result_data)
        observer_events = result_data.get("observer_events") or live_output.get("events") or []

        rendered_output_html = ""
        if result_document.strip():
            attachment_name_to_path: Dict[str, str] = {}
            nested_invocations = []
            if isinstance(nested_rvt, dict):
                nested_invocations = nested_rvt.get("invocations") or []
            if isinstance(nested_invocations, list):
                for invocation in nested_invocations:
                    if not isinstance(invocation, dict):
                        continue
                    for attachment in invocation.get("attachments") or []:
                        if not isinstance(attachment, dict):
                            continue
                        path = str(attachment.get("path") or "").strip()
                        name = str(attachment.get("name") or "").strip()
                        if not name and path:
                            name = str(PurePosixPath(path).name)
                        if path:
                            normalized_path = str(PurePosixPath(path.lstrip("/")))
                            if name and name not in attachment_name_to_path:
                                attachment_name_to_path[name] = normalized_path

            def _resolve_attachment_ref(ref: str) -> str | None:
                name = str(ref or "").strip()
                if not name:
                    return None
                artifact_rel = attachment_name_to_path.get(name, name)
                normalized = str(PurePosixPath(str(artifact_rel).lstrip("/")))
                if not normalized or normalized == ".":
                    return None
                if any(part in {"..", ""} for part in PurePosixPath(normalized).parts):
                    return None
                return url_for(
                    "job_output_artifact",
                    job_id=job_id,
                    artifact_path=normalized,
                )

            try:
                rendered_output_html = render_script_rst_html(
                    result_document,
                    artifact_href_resolver=_resolve_attachment_ref,
                )
            except Exception as exc:
                rendered_output_html = (
                    '<div class="alert alert-error">'
                    f"<span>Render failed: {exc}</span>"
                    "</div>"
                )

        return {
            "job_id": job_id,
            "job_data": job_data,
            "result_row": result_row,
            "result_data": result_data,
            "result_document": result_document,
            "rendered_output_html": rendered_output_html,
            "observer_events": observer_events,
            "state": state,
            "is_live": state in {"queued", "in_progress"},
            "worker_id": getattr(in_progress_worker, "worker_id", None) if in_progress_worker else None,
        }

    def _build_queue_overview_context(
        queued_page: int,
        in_progress_page: int,
        completed_page: int,
        per_page: int,
    ) -> Dict[str, Any]:
        all_jobs = queue.list_jobs()
        workers = central.get_workers_snapshot()

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

        queued_page_data = _paginate_items(queued_jobs, queued_page, per_page)
        in_progress_page_data = _paginate_items(in_progress_jobs, in_progress_page, per_page)

        completed_total = queue.count_results()
        completed_total_pages = max(1, math.ceil(completed_total / per_page))
        safe_completed_page = min(max(1, completed_page), completed_total_pages)
        completed_offset = (safe_completed_page - 1) * per_page
        completed_items = queue.list_results(limit=per_page, offset=completed_offset)
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

    def _render_queue_overview_panel(
        queued_page: int,
        in_progress_page: int,
        completed_page: int,
        per_page: int,
    ) -> str:
        context = _build_queue_overview_context(
            queued_page=queued_page,
            in_progress_page=in_progress_page,
            completed_page=completed_page,
            per_page=per_page,
        )
        return render_template("partials/queue_overview_panel.html", **context)

    def _serve_docs_asset(asset_path: str) -> Any:
        docs_dir_str = app.config.get("DOCS_HTML_DIR")
        docs_status = app.config.get("DOCS_STATUS", {})
        if not docs_dir_str:
            return "Documentation is not configured.", 503

        docs_dir = Path(docs_dir_str).resolve()
        has_built_docs = docs_dir.exists() and (docs_dir / "index.html").is_file()
        if (not docs_status.get("built") and not has_built_docs) or not docs_dir.exists():
            details = docs_status.get("message") or "Sphinx docs have not been generated."
            return f"Documentation unavailable: {details}", 503

        requested = (docs_dir / asset_path).resolve()
        try:
            requested.relative_to(docs_dir)
        except ValueError:
            abort(404)

        if not requested.is_file():
            abort(404)
        return send_from_directory(str(docs_dir), asset_path)

    @app.route("/", methods=["GET"])
    def index() -> Any:
        return redirect(url_for("queue_page"), code=308)

    @app.route("/queue", methods=["GET"])
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

    @app.route("/workers", methods=["GET"])
    def workers_page() -> str:
        workers = central.get_workers_snapshot()
        return render_template(
            "workers.html",
            page_title="AutomationV3 | Workers",
            workers=workers,
            now_ts=time.time(),
        )

    @app.route("/completed-jobs", methods=["GET"])
    def completed_jobs_page() -> Any:
        # Legacy route now points to the unified queue history view.
        return redirect(url_for("queue_page"), code=308)

    @app.route("/queue/overview", methods=["GET"])
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

    @app.route("/queue/events", methods=["GET"])
    def queue_events_stream() -> Response:
        def event_stream():
            while True:
                yield f"event: queue-refresh\ndata: {int(time.time())}\n\n"
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

    @app.route("/reports", methods=["GET"])
    def reports_page() -> str:
        report_records = queue.list_reports(limit=2000)
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

    @app.route("/reports", methods=["POST"])
    def create_report() -> Any:
        title = (request.form.get("title") or "").strip()
        description = (request.form.get("description") or "").strip()
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not title:
            return "title is required", 400
        report = queue.create_report(title=title, description=description)
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("report_detail_page", report_id=report["report_id"]), code=303)

    @app.route("/reports/<report_id>", methods=["GET"])
    def report_detail_page(report_id: str) -> str:
        NO_REQUIREMENT_LABEL = "No Requirement Declared"

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
        try:
            requirement_text_map = {req.id: req.text for req in load_default_requirements()}
        except Exception:
            requirement_text_map = {}

        script_info_cache: Dict[tuple[str, str], Dict[str, Any]] = {}

        def _resolve_script_info(job: Dict[str, Any]) -> Dict[str, Any]:
            script = str(job.get("file") or "").strip()
            if not script:
                return {
                    "title": "Untitled Script",
                    "requirements": [],
                }

            job_scripts_root = str(job.get("scripts_root") or "").strip()
            cache_key = (job_scripts_root, script)
            if cache_key in script_info_cache:
                return script_info_cache[cache_key]

            title = Path(script).stem or script
            requirements = _normalize_requirements(((job.get("meta") or {}).get("requirements")))

            candidates: List[Path] = []
            if job_scripts_root:
                candidates.append((Path(job_scripts_root).resolve() / script).resolve())
            candidates.append((scripts_root.resolve() / script).resolve())

            for candidate in candidates:
                try:
                    if candidate.exists() and candidate.is_file():
                        lines = candidate.read_text(encoding="utf-8").splitlines()
                        title = _extract_rst_title(lines, fallback=title)
                        if not requirements:
                            file_meta = _parse_meta_from_lines(lines)
                            requirements = _normalize_requirements(
                                (file_meta or {}).get("requirements")
                            )
                        break
                except Exception:
                    continue

            info = {
                "title": title,
                "requirements": requirements,
            }
            script_info_cache[cache_key] = info
            return info

        report_meta = queue.get_report(report_id)
        completed = _iter_completed_results_for_report(report_id)
        tracked_rows = queue.list_report_scripts(report_id)
        tracked_script_set = {
            str(row.get("script_path") or "").strip()
            for row in tracked_rows
            if str(row.get("script_path") or "").strip()
        }

        all_runs: List[Dict[str, Any]] = []
        latest_report_run_by_script: Dict[str, Dict[str, Any]] = {}
        requirement_run_history: Dict[str, List[Dict[str, Any]]] = {}
        for res in completed:
            job = res.get("job_data") or {}
            script = str(job.get("file") or "—")
            script_info = _resolve_script_info(job)
            requirements = list(script_info.get("requirements") or [])
            if not requirements:
                requirements = [NO_REQUIREMENT_LABEL]
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
                "completed_at_human": _human_datetime(res.get("completed_at")),
            }
            all_runs.append(run)
            if script and script not in latest_report_run_by_script:
                latest_report_run_by_script[script] = run
            for requirement in requirements:
                requirement_run_history.setdefault(requirement, []).append(run)

        requirement_script_catalog: Dict[str, Dict[str, Dict[str, Any]]] = {}

        def _register_requirement_script(
            requirement: str, script_path: str, script_title: str
        ) -> None:
            req = str(requirement or "").strip() or NO_REQUIREMENT_LABEL
            clean_script = str(script_path or "").strip()
            if not clean_script:
                return
            title = str(script_title or "").strip() or Path(clean_script).stem or clean_script
            bucket = requirement_script_catalog.setdefault(req, {})
            if clean_script not in bucket:
                bucket[clean_script] = {
                    "script": clean_script,
                    "script_title": title,
                }

        try:
            discovered_scripts = _discover_scripts(scripts_root)
        except Exception:
            discovered_scripts = []
        for script_row in discovered_scripts:
            script_path = str(script_row.get("relpath") or "").strip()
            if not script_path:
                continue
            script_title = str(script_row.get("title") or "").strip() or Path(script_path).stem
            meta = script_row.get("meta") or {}
            requirements = [
                str(req).strip() for req in (meta.get("requirements") or []) if str(req).strip()
            ]
            if not requirements:
                requirements = [NO_REQUIREMENT_LABEL]
            for requirement in requirements:
                _register_requirement_script(requirement, script_path, script_title)

        for tracked_row in tracked_rows:
            script_path = str(tracked_row.get("script_path") or "").strip()
            if not script_path:
                continue
            template = tracked_row.get("job_template")
            info = _resolve_script_info(
                template if isinstance(template, dict) else {"file": script_path}
            )
            requirements = list(info.get("requirements") or [])
            if not requirements:
                requirements = [NO_REQUIREMENT_LABEL]
            for requirement in requirements:
                _register_requirement_script(
                    requirement,
                    script_path,
                    str(info.get("title") or Path(script_path).stem or script_path),
                )

        for run in all_runs:
            requirements = list(run.get("requirements") or [])
            if not requirements:
                requirements = [NO_REQUIREMENT_LABEL]
            for requirement in requirements:
                _register_requirement_script(
                    requirement,
                    str(run.get("script") or ""),
                    str(run.get("script_title") or ""),
                )

        requirement_keys = sorted(
            requirement_script_catalog.keys(),
            key=lambda req: (req == NO_REQUIREMENT_LABEL, req),
        )

        report_requirement_groups: List[Dict[str, Any]] = []
        for requirement in requirement_keys:
            script_map = requirement_script_catalog.get(requirement, {})
            script_paths = sorted(script_map.keys())
            if not script_paths:
                continue

            passing_script_total = 0
            requirement_script_rows: List[Dict[str, Any]] = []
            for script_path in script_paths:
                latest_run = latest_report_run_by_script.get(script_path)
                latest_success = bool((latest_run or {}).get("success"))
                if latest_success:
                    passing_script_total += 1
                if latest_run:
                    latest_status = "PASS" if latest_success else "FAIL"
                    latest_status_badge_class = "badge-success" if latest_success else "badge-error"
                else:
                    latest_status = "NOT RUN"
                    latest_status_badge_class = "badge-ghost"
                requirement_script_rows.append(
                    {
                        "script": script_path,
                        "script_title": script_map[script_path].get("script_title")
                        or Path(script_path).stem
                        or script_path,
                        "added_to_report": script_path in tracked_script_set,
                        "latest_status": latest_status,
                        "latest_status_badge_class": latest_status_badge_class,
                        "latest_job_id": (latest_run or {}).get("job_id"),
                        "latest_completed_human": (latest_run or {}).get("completed_at_human") or "—",
                    }
                )

            script_total = len(script_paths)
            if script_total > 0 and passing_script_total == script_total:
                requirement_status_label = "REQ PASS"
                requirement_status_badge_class = "badge-success"
            elif passing_script_total == 0:
                requirement_status_label = "REQ FAIL"
                requirement_status_badge_class = "badge-error"
            else:
                requirement_status_label = "REQ PARTIAL"
                requirement_status_badge_class = "badge-warning"

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
                        if requirement == NO_REQUIREMENT_LABEL
                        else requirement_text_map.get(requirement, "")
                    ),
                    "latest_job_id": (latest_run or {}).get("job_id"),
                    "latest_success": bool((latest_run or {}).get("success"))
                    if latest_run
                    else None,
                    "latest_completed_human": (latest_run or {}).get("completed_at_human") or "—",
                    "run_count": len(runs_for_requirement),
                    "script_count": script_total,
                    "tracked_script_count": len(tracked_script_paths),
                    "latest_script_total": script_total,
                    "latest_passing_script_total": passing_script_total,
                    "requirement_status_label": requirement_status_label,
                    "requirement_status_badge_class": requirement_status_badge_class,
                    "script_paths": tracked_script_paths,
                    "history": runs_for_requirement[:8],
                    "runs": runs_for_requirement,
                    "scripts": requirement_script_rows,
                }
            )

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

        report_requeue_script_paths = sorted(tracked_script_set)
        report_script_total = len(
            {
                script_row.get("script")
                for group in report_requirement_groups
                for script_row in (group.get("scripts") or [])
                if script_row.get("script")
            }
        )

        return render_template(
            "report_detail.html",
            page_title=f"AutomationV3 | {(report_meta or {}).get('title') or report_id}",
            report_id=report_id,
            report_meta=report_meta,
            report_view=report_view,
            report_results=completed,
            report_script_total=report_script_total,
            report_tracked_script_total=len(report_requeue_script_paths),
            report_requirement_groups=report_requirement_groups,
            report_requeue_script_paths=report_requeue_script_paths,
            pending_jobs=pending_rows,
        )

    @app.route("/reports/<report_id>/requeue_all", methods=["POST"])
    def requeue_report_all(report_id: str) -> Any:
        report_view = (request.form.get("report_view") or "requirement").strip().lower()
        if report_view not in {"script", "requirement"}:
            report_view = "requirement"
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not queue.get_report(report_id):
            return "Unknown report", 404
        script_paths = [
            str(row.get("script_path") or "").strip()
            for row in queue.list_report_scripts(report_id)
            if str(row.get("script_path") or "").strip()
        ]
        _requeue_report_scripts(report_id, script_paths)
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("report_detail_page", report_id=report_id, view=report_view), code=303)

    @app.route("/reports/<report_id>/requeue_script", methods=["POST"])
    def requeue_report_script(report_id: str) -> Any:
        report_view = (request.form.get("report_view") or "requirement").strip().lower()
        if report_view not in {"script", "requirement"}:
            report_view = "requirement"
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not queue.get_report(report_id):
            return "Unknown report", 404
        script_path = str(request.form.get("script_path") or "").strip()
        if not script_path:
            return "script_path required", 400
        _requeue_report_scripts(report_id, [script_path])
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("report_detail_page", report_id=report_id, view=report_view), code=303)

    @app.route("/reports/<report_id>/requeue_requirement", methods=["POST"])
    def requeue_report_requirement(report_id: str) -> Any:
        report_view = (request.form.get("report_view") or "requirement").strip().lower()
        if report_view not in {"script", "requirement"}:
            report_view = "requirement"
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not queue.get_report(report_id):
            return "Unknown report", 404
        raw_entries = request.form.getlist("script_paths")
        script_paths: List[str] = []
        seen_paths = set()
        for raw_entry in raw_entries:
            for part in raw_entry.replace("\r", "\n").split("\n"):
                script_path = part.strip()
                if not script_path or script_path in seen_paths:
                    continue
                seen_paths.add(script_path)
                script_paths.append(script_path)
        if not script_paths:
            return "script_paths required", 400
        _requeue_report_scripts(report_id, script_paths)
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("report_detail_page", report_id=report_id, view=report_view), code=303)

    @app.route("/reports/<report_id>/clear_results", methods=["POST"])
    def clear_report_results(report_id: str) -> Any:
        report_view = (request.form.get("report_view") or "requirement").strip().lower()
        if report_view not in {"script", "requirement"}:
            report_view = "requirement"
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not queue.get_report(report_id):
            return "Unknown report", 404
        queue.clear_results_for_report(report_id)
        queue.clear_pending_results_for_report(report_id)
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("report_detail_page", report_id=report_id, view=report_view), code=303)

    @app.route("/reports/<report_id>/scripts/remove", methods=["POST"])
    def remove_report_script(report_id: str) -> Any:
        report_view = (request.form.get("report_view") or "requirement").strip().lower()
        if report_view not in {"script", "requirement"}:
            report_view = "requirement"
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not queue.get_report(report_id):
            return "Unknown report", 404
        script_path = str(request.form.get("script_path") or "").strip()
        if not script_path:
            return "script_path required", 400
        queue.remove_script_from_report(report_id, script_path)
        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("report_detail_page", report_id=report_id, view=report_view), code=303)

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

    @app.route("/queue/restore_all", methods=["POST"])
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

    @app.route("/workers/table", methods=["GET"])
    def workers_table() -> str:
        return _render_workers_table()

    @app.route("/uuts", methods=["POST"])
    def add_uut() -> str:
        name = (request.form.get("name") or "").strip()
        path = (request.form.get("path") or "").strip()
        if not name or not path:
            return "Name and path are required", 400
        config = uut_store.add(name=name, path=path)
        # snapshot immediately (best-effort)
        try:
            uut_store.snapshot(config.uut_id)
        except Exception:
            pass
        return _render_uuts_table()

    @app.route("/uuts/table", methods=["GET"])
    def uuts_table() -> str:
        return _render_uuts_table()

    @app.route("/suites", methods=["POST"])
    def add_suite() -> str:
        name = (request.form.get("name") or "").strip()
        if not name:
            return "Name required", 400
        suite_manager.create_suite(name)
        return _render_suites_table()

    @app.route("/suites/table", methods=["GET"])
    def suites_table() -> str:
        return _render_suites_table()

    @app.route("/suites/<suite_name>/add_script", methods=["POST"])
    def suite_add_script(suite_name: str) -> str:
        script_path = (request.form.get("script_path") or "").strip()
        if not script_path:
            return "script_path required", 400
        suite_manager.add_script(suite_name, script_path)
        return _render_suites_table()

    @app.route("/suites/<suite_name>/remove_script", methods=["POST"])
    def suite_remove_script(suite_name: str) -> str:
        script_path = (request.form.get("script_path") or "").strip()
        suite_manager.remove_script(suite_name, script_path)
        return _render_suites_table()

    @app.route("/suites/<suite_name>/delete", methods=["POST"])
    def suite_delete(suite_name: str) -> str:
        suite_manager.delete_suite(suite_name)
        return _render_suites_table()

    @app.route("/scripts", methods=["GET"])
    def scripts_panel() -> str:
        base_path = Path(request.args.get("base_path") or scripts_root).resolve()
        listing_view = (request.args.get("view") or "requirements").strip().lower()
        if listing_view not in {"requirements", "directory"}:
            listing_view = "requirements"
        selected_system = (request.args.get("system") or "").strip().upper()
        selected_dir = (request.args.get("dir") or "").strip().strip("/")

        scripts = _discover_scripts(base_path)
        systems, system_counts, system_index = _build_script_system_index(scripts)
        (
            directory_nodes,
            directory_children,
            directory_to_scripts,
            _directory_recursive_counts,
        ) = _build_script_directory_index(scripts)

        requirement_text_map: Dict[str, str] = {}
        system_uncovered_counts: Dict[str, int] = {}
        system_syntax_error_counts: Dict[str, int] = {}
        try:
            for req in load_default_requirements():
                requirement_text_map[req.id] = req.text
                system_index.setdefault(req.system_id, {}).setdefault(req.id, [])
        except Exception:
            # Keep UI functional even if CSV cannot be loaded.
            log.warning("Failed to load default requirements for scripts panel.", exc_info=True)
            requirement_text_map = {}

        systems = _sorted_systems(system_index.keys())
        if systems:
            if not selected_system or selected_system not in systems:
                selected_system = systems[0]
        else:
            selected_system = UNSPECIFIED_SYSTEM

        for system, req_map in system_index.items():
            uncovered = 0
            syntax_errors = 0
            seen_relpaths = set()
            for req_id, script_rows in req_map.items():
                if req_id in requirement_text_map and not script_rows:
                    uncovered += 1
                for script in script_rows:
                    relpath = script.get("relpath")
                    if not relpath or relpath in seen_relpaths:
                        continue
                    seen_relpaths.add(relpath)
                    if script.get("has_syntax_errors"):
                        syntax_errors += 1
            system_uncovered_counts[system] = uncovered
            system_syntax_error_counts[system] = syntax_errors

        requirement_groups = []
        for requirement, rows in sorted(
            system_index.get(selected_system, {}).items(),
            key=lambda kv: kv[0],
        ):
            script_paths = [row.get("relpath") for row in rows if row.get("relpath")]
            requirement_groups.append(
                {
                    "requirement": requirement,
                    "scripts": rows,
                    "script_paths": script_paths,
                    "script_count": len(rows),
                    "uncovered_requirement_count": (
                        1 if requirement in requirement_text_map and not rows else 0
                    ),
                    "syntax_error_count": sum(
                        1 for script in rows if script.get("has_syntax_errors")
                    ),
                }
            )

        if selected_dir not in directory_to_scripts:
            selected_dir = ""
        directory_node_map = {node.get("path", ""): node for node in directory_nodes}
        open_dir_paths = set([""])
        cursor = selected_dir
        while cursor is not None:
            open_dir_paths.add(cursor)
            cursor = _parent_directory(cursor)
        selected_dir_scripts = directory_to_scripts.get(selected_dir, [])
        selected_subdirs = directory_children.get(selected_dir, [])
        selected_dir_parent = _parent_directory(selected_dir)
        root_dir_label = Path(base_path).name or str(base_path)
        return_to = request.full_path.rstrip("?")
        uuts = uut_store.list()
        report_options = queue.list_reports(limit=500)

        return render_template(
            "scripts.html",
            page_title="AutomationV3 | Scripts",
            base_path=str(base_path),
            listing_view=listing_view,
            systems=systems,
            system_counts=system_counts,
            system_uncovered_counts=system_uncovered_counts,
            system_syntax_error_counts=system_syntax_error_counts,
            selected_system=selected_system,
            selected_dir=selected_dir,
            selected_dir_parent=selected_dir_parent,
            selected_subdirs=selected_subdirs,
            selected_dir_scripts=selected_dir_scripts,
            directory_nodes=directory_nodes,
            directory_children=directory_children,
            directory_node_map=directory_node_map,
            open_dir_paths=sorted(open_dir_paths),
            root_dir_label=root_dir_label,
            requirement_groups=requirement_groups,
            requirement_text_map=requirement_text_map,
            return_to=return_to,
            uuts=uuts,
            report_options=report_options,
        )

    @app.route("/scripts/<path:script_relpath>", methods=["GET"])
    def script_detail_page(script_relpath: str) -> str:
        base_path = Path(request.args.get("base_path") or scripts_root).resolve()
        script_path = (base_path / script_relpath).resolve()
        try:
            script_path.relative_to(base_path)
        except ValueError:
            abort(404)
        if not script_path.exists() or not script_path.is_file():
            abort(404)
        script_content = script_path.read_text(encoding="utf-8")
        script_lines = script_content.splitlines()
        syntax_issues = collect_script_syntax_issues(script_content)
        rst_syntax_issues = [
            issue
            for issue in syntax_issues
            if issue.get("source") == "rst" and issue.get("is_error")
        ]
        raw_source_rows = _build_raw_source_rows(script_content, syntax_issues)
        view_mode = (request.args.get("view") or "render").strip().lower()
        if view_mode not in {"render", "raw"}:
            view_mode = "render"
        rendered_html = ""
        if view_mode == "render":
            try:
                rendered_html = render_script_rst_html(script_content)
            except Exception as exc:
                rendered_html = (
                    '<div class="alert alert-error">'
                    f"<span>Render failed: {exc}</span>"
                    "</div>"
                )

        return render_template(
            "script_detail.html",
            page_title=f"AutomationV3 | Script {script_relpath}",
            base_path=str(base_path),
            script_relpath=script_relpath,
            script_title=_extract_rst_title(script_lines, fallback=Path(script_relpath).stem),
            script_content=script_content,
            rendered_html=rendered_html,
            rst_syntax_issues=rst_syntax_issues,
            raw_source_rows=raw_source_rows,
            view_mode=view_mode,
        )

    @app.route("/jobs/<job_id>/output", methods=["GET"])
    def job_output_page(job_id: str) -> str:
        context = _build_job_output_context(job_id)
        if not context:
            abort(404)
        report_id = (context.get("job_data") or {}).get("report_id")
        return render_template(
            "job_output.html",
            page_title=f"AutomationV3 | Job Output {job_id}",
            report_id=report_id,
            **context,
        )

    @app.route("/jobs/<job_id>/output/panel", methods=["GET"])
    def job_output_panel(job_id: str) -> str:
        context = _build_job_output_context(job_id)
        if not context:
            abort(404)
        return render_template("partials/job_output_panel.html", **context)

    @app.route("/jobs/<job_id>/output/raw", methods=["GET"])
    def job_output_raw(job_id: str) -> Response:
        context = _build_job_output_context(job_id)
        if not context:
            abort(404)
        text = context.get("result_document") or ""
        return Response(text, mimetype="text/plain; charset=utf-8")

    @app.route("/jobs/<job_id>/output/artifacts/<path:artifact_path>", methods=["GET"])
    def job_output_artifact(job_id: str, artifact_path: str) -> Any:
        normalized = str(PurePosixPath(artifact_path))
        if not normalized or normalized == ".":
            abort(404)
        if any(part in {"..", ""} for part in PurePosixPath(normalized).parts):
            abort(404)

        root = (Path(central.artifacts_dir).resolve() / job_id).resolve()
        candidate = (root / normalized).resolve()
        try:
            candidate.relative_to(root)
        except ValueError:
            abort(404)

        if not candidate.exists() or not candidate.is_file():
            result = queue.get_result(job_id) or {}
            worker_address = str(result.get("worker_address") or "").strip()
            if worker_address and hasattr(central, "_download_artifact"):
                try:
                    # Best-effort on-demand fetch for recently completed jobs
                    # before background artifact sync has pulled everything.
                    central._download_artifact(worker_address, job_id, normalized)
                except Exception:
                    pass
        if not candidate.exists() or not candidate.is_file():
            abort(404)
        return send_file(candidate)

    @app.route("/docs", methods=["GET"])
    def docs_index_redirect() -> Any:
        return redirect(url_for("docs_index"), code=308)

    @app.route("/docs/", methods=["GET"])
    def docs_index() -> Any:
        return _serve_docs_asset("index.html")

    @app.route("/docs/<path:asset_path>", methods=["GET"])
    def docs_asset(asset_path: str) -> Any:
        return _serve_docs_asset(asset_path)

    @app.route("/jobs/from_script", methods=["POST"])
    def queue_from_script() -> Any:
        script_path = (request.form.get("script_path") or "").strip()
        base_path = Path(request.form.get("base_path") or scripts_root).resolve()
        uut_id = (request.form.get("uut_id") or "").strip()
        report_id = (request.form.get("report_id") or "").strip()
        framework_version = (request.form.get("framework_version") or "").strip()
        suite_name = (request.form.get("suite_name") or "").strip()
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not script_path:
            return "script_path required", 400
        if not uut_id:
            return "Select a UUT configuration first", 400
        if not report_id:
            return "Select a report first", 400
        if not queue.get_report(report_id):
            return "Unknown report", 400
        try:
            rel_script_path = _resolve_rel_script_path(script_path, base_path)
        except ValueError as exc:
            return str(exc), 400
        script_abspath = str((base_path / rel_script_path).resolve())
        config = uut_store.get(uut_id)
        if not config:
            return "Unknown UUT", 400
        try:
            config = uut_store.snapshot(uut_id) or config
            log.info("Snapshot UUT %s tree=%s", config.name, config.last_tree_sha)
        except Exception as exc:
            log.exception("Failed to snapshot UUT %s: %s", uut_id, exc)
        # snapshot scripts tree to capture includes/dependencies
        try:
            scripts_tree = snapshot_tree(base_path, cache_dir=scripts_cache_dir)
            log.info("Snapshot scripts tree at %s -> %s", base_path, scripts_tree)
        except Exception:
            scripts_tree = None
            log.exception("Failed to snapshot scripts at %s", base_path)
        meta = _parse_meta_from_rst(Path(script_abspath))
        job = {
            "file": rel_script_path,
            "uut": config.name,
            "report_id": report_id,
            "uut_tree": config.last_tree_sha,
            "uut_id": config.uut_id,
            "meta": meta,
            "framework_version": framework_version,
            "scripts_tree": scripts_tree,
            "scripts_root": str(base_path),
            "suite_name": "",
            "suite_run_id": "",
        }
        if suite_name:
            suite_manager.create_suite(suite_name)
            suite_manager.add_script(suite_name, rel_script_path)
        queue.add_job(job, priority=0)
        if return_to:
            return redirect(return_to, code=303)
        return _render_jobs_table()

    def _queue_single_job_from_relpath(
        rel_script_path: str,
        base_path: Path,
        config,
        report_id: str,
        framework_version: str,
        scripts_tree: str | None,
        suite_name: str = "",
        suite_run_id: str = "",
    ):
        script_abspath = str((base_path / rel_script_path).resolve())
        meta = _parse_meta_from_rst(Path(script_abspath))
        job = {
            "file": rel_script_path,
            "uut": config.name,
            "report_id": report_id,
            "uut_tree": config.last_tree_sha,
            "uut_id": config.uut_id,
            "meta": meta,
            "framework_version": framework_version,
            "scripts_tree": scripts_tree,
            "scripts_root": str(base_path),
            "suite_name": suite_name,
            "suite_run_id": suite_run_id,
        }
        queue.add_job(job, priority=0)

    @app.route("/jobs/from_scripts", methods=["POST"])
    def queue_from_scripts() -> Any:
        base_path = Path(request.form.get("base_path") or scripts_root).resolve()
        uut_id = (request.form.get("uut_id") or "").strip()
        report_id = (request.form.get("report_id") or "").strip()
        framework_version = (request.form.get("framework_version") or "").strip()
        return_to = _safe_return_to(request.form.get("return_to") or "")
        if not uut_id:
            return "Select a UUT configuration first", 400
        if not report_id:
            return "Select a report first", 400
        if not queue.get_report(report_id):
            return "Unknown report", 400

        raw_entries = request.form.getlist("script_paths")
        if not raw_entries:
            single = (request.form.get("script_path") or "").strip()
            if single:
                raw_entries = [single]

        rel_script_paths: List[str] = []
        invalid_paths: List[str] = []
        seen_paths = set()
        for raw_entry in raw_entries:
            for part in raw_entry.replace("\r", "\n").split("\n"):
                candidate = part.strip()
                if not candidate:
                    continue
                try:
                    rel_script_path = _resolve_rel_script_path(candidate, base_path)
                except ValueError:
                    invalid_paths.append(candidate)
                    continue
                if rel_script_path in seen_paths:
                    continue
                seen_paths.add(rel_script_path)
                rel_script_paths.append(rel_script_path)

        if invalid_paths:
            return f"Invalid script path(s): {', '.join(invalid_paths[:5])}", 400
        if not rel_script_paths:
            return "At least one script path is required", 400

        config = uut_store.get(uut_id)
        if not config:
            return "Unknown UUT", 400
        try:
            config = uut_store.snapshot(uut_id) or config
            log.info("Snapshot UUT %s tree=%s", config.name, config.last_tree_sha)
        except Exception as exc:
            log.exception("Failed to snapshot UUT %s: %s", uut_id, exc)
        try:
            scripts_tree = snapshot_tree(base_path, cache_dir=scripts_cache_dir)
            log.info("Snapshot scripts tree at %s -> %s", base_path, scripts_tree)
        except Exception:
            scripts_tree = None
            log.exception("Failed to snapshot scripts at %s", base_path)

        for rel_script_path in rel_script_paths:
            _queue_single_job_from_relpath(
                rel_script_path=rel_script_path,
                base_path=base_path,
                config=config,
                report_id=report_id,
                framework_version=framework_version,
                scripts_tree=scripts_tree,
            )

        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("queue_page"), code=303)

    @app.route("/jobs/from_suite", methods=["POST"])
    def queue_from_suite() -> Any:
        suite_name = (request.form.get("suite_name") or "").strip()
        uut_id = (request.form.get("uut_id") or "").strip()
        report_id = (request.form.get("report_id") or "").strip()
        framework_version = (request.form.get("framework_version") or "").strip()
        base_path = Path(request.form.get("base_path") or scripts_root).resolve()
        if not suite_name:
            return "suite_name required", 400
        scripts = suite_manager.get_suite(suite_name)
        if not scripts:
            return "suite has no scripts", 400
        if not uut_id:
            return "Select a UUT configuration first", 400
        if not report_id:
            return "Select a report first", 400
        if not queue.get_report(report_id):
            return "Unknown report", 400
        config = uut_store.get(uut_id)
        if not config:
            return "Unknown UUT", 400
        try:
            config = uut_store.snapshot(uut_id) or config
            log.info("Snapshot UUT %s tree=%s", config.name, config.last_tree_sha)
        except Exception as exc:
            log.exception("Failed to snapshot UUT %s: %s", uut_id, exc)
        # snapshot scripts tree once
        try:
            scripts_tree = snapshot_tree(base_path, cache_dir=scripts_cache_dir)
            log.info("Snapshot scripts tree at %s -> %s", base_path, scripts_tree)
        except Exception:
            scripts_tree = None
            log.exception("Failed to snapshot scripts at %s", base_path)
        suite_run_id = uuid7_str()
        for rel_script_path in scripts:
            try:
                clean_rel_script_path = _resolve_rel_script_path(rel_script_path, base_path)
            except ValueError:
                continue
            _queue_single_job_from_relpath(
                clean_rel_script_path,
                base_path,
                config,
                report_id,
                framework_version,
                scripts_tree,
                suite_name=suite_name,
                suite_run_id=suite_run_id,
            )
        return _render_jobs_table()

    @app.route("/results/table", methods=["GET"])
    def results_table() -> str:
        return _render_results_table()

    @app.route("/results/suite/<suite_run_id>", methods=["GET"])
    def results_for_suite(suite_run_id: str) -> Any:
        results = queue.list_results_for_suite(suite_run_id)
        return jsonify(results)

    @app.route("/health", methods=["GET"])
    def health() -> str:
        return {"status": "ok"}


__all__ = ["register_frontend_routes"]
