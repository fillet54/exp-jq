"""Frontend route registration for the jobqueue dashboard."""

import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, abort, jsonify, redirect, render_template, request, send_from_directory, url_for

from automationv3.framework.requirements import REQUIREMENT_ID_PATTERN, load_default_requirements
from automationv3.framework.rst import collect_script_syntax_issues, render_script_rst_html

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


def _build_report_listing(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    reports: Dict[str, Dict[str, Any]] = {}
    for res in results:
        job = res.get("job_data") or {}
        report_id = job.get("report_id")
        if not report_id:
            continue

        if report_id not in reports:
            reports[report_id] = {
                "report_id": report_id,
                "total": 0,
                "passed": 0,
                "failed": 0,
                "latest_completed_at": None,
                "suite_runs": set(),
            }

        row = reports[report_id]
        row["total"] += 1
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

    return sorted(
        reports.values(),
        key=lambda row: row["latest_completed_at"] or 0,
        reverse=True,
    )


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

    def _serve_docs_asset(asset_path: str) -> Any:
        docs_dir_str = app.config.get("DOCS_HTML_DIR")
        docs_status = app.config.get("DOCS_STATUS", {})
        if not docs_dir_str:
            return "Documentation is not configured.", 503

        docs_dir = Path(docs_dir_str).resolve()
        if not docs_status.get("built") or not docs_dir.exists():
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
        jobs = queue.list_jobs()
        next_job = queue.get_next_job()
        return render_template(
            "queue.html",
            page_title="AutomationV3 | Queue",
            jobs=jobs,
            next_job=next_job,
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
    def completed_jobs_page() -> str:
        results = queue.list_results(limit=200)
        return render_template(
            "completed_jobs.html",
            page_title="AutomationV3 | Completed Jobs",
            results=results,
        )

    @app.route("/reports", methods=["GET"])
    def reports_page() -> str:
        results = queue.list_results(limit=1000)
        reports = _build_report_listing(results)
        return render_template(
            "reports.html",
            page_title="AutomationV3 | Reports",
            reports=reports,
        )

    @app.route("/reports/<report_id>", methods=["GET"])
    def report_detail_page(report_id: str) -> str:
        completed = [
            res
            for res in queue.list_results(limit=2000)
            if (res.get("job_data") or {}).get("report_id") == report_id
        ]
        pending = [
            job
            for job in queue.list_jobs()
            if (job or {}).get("report_id") == report_id
        ]
        return render_template(
            "report_detail.html",
            page_title=f"AutomationV3 | Report {report_id}",
            report_id=report_id,
            report_results=completed,
            pending_jobs=pending,
        )

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
        selected_system = (request.args.get("system") or "").strip().upper()

        scripts = _discover_scripts(base_path)
        systems, system_counts, system_index = _build_script_system_index(scripts)

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
            requirement_groups.append(
                {
                    "requirement": requirement,
                    "scripts": rows,
                    "script_count": len(rows),
                    "uncovered_requirement_count": (
                        1 if requirement in requirement_text_map and not rows else 0
                    ),
                    "syntax_error_count": sum(
                        1 for script in rows if script.get("has_syntax_errors")
                    ),
                }
            )

        return render_template(
            "scripts.html",
            page_title="AutomationV3 | Scripts",
            base_path=str(base_path),
            systems=systems,
            system_counts=system_counts,
            system_uncovered_counts=system_uncovered_counts,
            system_syntax_error_counts=system_syntax_error_counts,
            selected_system=selected_system,
            requirement_groups=requirement_groups,
            requirement_text_map=requirement_text_map,
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
    def queue_from_script() -> str:
        script_path = (request.form.get("script_path") or "").strip()
        base_path = Path(request.form.get("base_path") or scripts_root)
        uut_id = (request.form.get("uut_id") or "").strip()
        framework_version = (request.form.get("framework_version") or "").strip()
        suite_name = (request.form.get("suite_name") or "").strip()
        if not script_path:
            return "script_path required", 400
        if not uut_id:
            return "Select a UUT configuration first", 400
        rel_script_path = script_path
        if os.path.isabs(script_path):
            try:
                rel_script_path = str(Path(script_path).resolve().relative_to(base_path))
            except Exception:
                rel_script_path = os.path.basename(script_path)
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
        report_id = uuid7_str()
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
        return _render_jobs_table()

    def _queue_single_job_from_relpath(
        rel_script_path: str,
        base_path: Path,
        config,
        framework_version: str,
        scripts_tree: str,
        suite_name: str = "",
        suite_run_id: str = "",
    ):
        script_abspath = str((base_path / rel_script_path).resolve())
        meta = _parse_meta_from_rst(Path(script_abspath))
        report_id = uuid7_str()
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

    @app.route("/jobs/from_suite", methods=["POST"])
    def queue_from_suite() -> str:
        suite_name = (request.form.get("suite_name") or "").strip()
        uut_id = (request.form.get("uut_id") or "").strip()
        framework_version = (request.form.get("framework_version") or "").strip()
        base_path = scripts_root
        if not suite_name:
            return "suite_name required", 400
        scripts = suite_manager.get_suite(suite_name)
        if not scripts:
            return "suite has no scripts", 400
        if not uut_id:
            return "Select a UUT configuration first", 400
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
            _queue_single_job_from_relpath(
                rel_script_path,
                base_path,
                config,
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
