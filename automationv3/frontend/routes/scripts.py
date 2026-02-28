"""Script/job-output handlers and route registration."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Mapping

from flask import Response, abort, redirect, render_template, request, send_file, url_for

from automationv3.framework.requirements import load_default_requirements
from automationv3.jobqueue.fscache import snapshot_tree
from automationv3.jobqueue.ids import uuid7_str


def register_script_routes(app, helpers: Mapping[str, Any]) -> None:
    queue = helpers["queue"]
    central = helpers["central"]
    reporting = helpers["reporting"]
    uut_store = helpers["uut_store"]
    suite_manager = helpers["suite_manager"]
    scripts_root = helpers["scripts_root"]
    scripts_cache_dir = helpers["scripts_cache_dir"]
    log = helpers["log"]

    _safe_return_to = helpers["_safe_return_to"]
    _resolve_rel_script_path = helpers["_resolve_rel_script_path"]
    _build_jobs_for_relpath = helpers["_build_jobs_for_relpath"]
    _discover_scripts = helpers["_discover_scripts"]
    _build_script_system_index = helpers["_build_script_system_index"]
    _build_script_directory_index = helpers["_build_script_directory_index"]
    _sorted_systems = helpers["_sorted_systems"]
    _parent_directory = helpers["_parent_directory"]
    _extract_rst_title = helpers["_extract_rst_title"]
    _build_raw_source_rows = helpers["_build_raw_source_rows"]
    _build_job_output_context = helpers["_build_job_output_context"]
    _render_jobs_table = helpers["_render_jobs_table"]

    collect_script_syntax_issues = helpers["collect_script_syntax_issues"]
    render_script_rst_html = helpers["render_script_rst_html"]
    UNSPECIFIED_SYSTEM = helpers["UNSPECIFIED_SYSTEM"]

    def _load_queue_prereqs(
        *,
        base_path: Path,
        uut_id: str,
        report_id: str,
    ) -> tuple[Any, str | None, Any | None]:
        if not uut_id:
            return None, None, ("Select a UUT configuration first", 400)
        if not report_id:
            return None, None, ("Select a report first", 400)
        if not reporting.get_report(report_id):
            return None, None, ("Unknown report", 400)

        config = uut_store.get(uut_id)
        if not config:
            return None, None, ("Unknown UUT", 400)
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

        return config, scripts_tree, None

    def _parse_script_paths(
        raw_entries: List[str],
        *,
        base_path: Path,
    ) -> tuple[List[str], List[str]]:
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
        return rel_script_paths, invalid_paths

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
        report_options = reporting.list_reports(limit=500)

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

    def job_output_panel(job_id: str) -> str:
        context = _build_job_output_context(job_id)
        if not context:
            abort(404)
        return render_template("partials/job_output_panel.html", **context)

    def job_output_raw(job_id: str) -> Response:
        context = _build_job_output_context(job_id)
        if not context:
            abort(404)
        text = context.get("result_document") or ""
        return Response(text, mimetype="text/plain; charset=utf-8")

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
                    central._download_artifact(worker_address, job_id, normalized)
                except Exception:
                    pass
        if not candidate.exists() or not candidate.is_file():
            abort(404)
        return send_file(candidate)

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
        try:
            rel_script_path = _resolve_rel_script_path(script_path, base_path)
        except ValueError as exc:
            return str(exc), 400
        config, scripts_tree, error_response = _load_queue_prereqs(
            base_path=base_path,
            uut_id=uut_id,
            report_id=report_id,
        )
        if error_response is not None:
            return error_response
        try:
            jobs_to_queue = _build_jobs_for_relpath(
                rel_script_path=rel_script_path,
                base_path=base_path,
                config=config,
                report_id=report_id,
                framework_version=framework_version,
                scripts_tree=scripts_tree,
            )
        except ValueError as exc:
            return str(exc), 400
        if suite_name:
            suite_manager.create_suite(suite_name)
            suite_manager.add_script(suite_name, rel_script_path)
        queue.add_job(jobs_to_queue, priority=0)
        if return_to:
            return redirect(return_to, code=303)
        return _render_jobs_table()

    def queue_from_scripts() -> Any:
        base_path = Path(request.form.get("base_path") or scripts_root).resolve()
        uut_id = (request.form.get("uut_id") or "").strip()
        report_id = (request.form.get("report_id") or "").strip()
        framework_version = (request.form.get("framework_version") or "").strip()
        return_to = _safe_return_to(request.form.get("return_to") or "")

        raw_entries = request.form.getlist("script_paths")
        if not raw_entries:
            single = (request.form.get("script_path") or "").strip()
            if single:
                raw_entries = [single]

        rel_script_paths, invalid_paths = _parse_script_paths(
            raw_entries,
            base_path=base_path,
        )

        if invalid_paths:
            return f"Invalid script path(s): {', '.join(invalid_paths[:5])}", 400
        if not rel_script_paths:
            return "At least one script path is required", 400

        config, scripts_tree, error_response = _load_queue_prereqs(
            base_path=base_path,
            uut_id=uut_id,
            report_id=report_id,
        )
        if error_response is not None:
            return error_response

        jobs_to_queue: List[Dict[str, Any]] = []
        try:
            for rel_script_path in rel_script_paths:
                jobs_to_queue.extend(
                    _build_jobs_for_relpath(
                        rel_script_path=rel_script_path,
                        base_path=base_path,
                        config=config,
                        report_id=report_id,
                        framework_version=framework_version,
                        scripts_tree=scripts_tree,
                    )
                )
        except ValueError as exc:
            return str(exc), 400

        queue.add_job(jobs_to_queue, priority=0)

        if return_to:
            return redirect(return_to, code=303)
        return redirect(url_for("queue_page"), code=303)

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
        config, scripts_tree, error_response = _load_queue_prereqs(
            base_path=base_path,
            uut_id=uut_id,
            report_id=report_id,
        )
        if error_response is not None:
            return error_response
        suite_run_id = uuid7_str()
        jobs_to_queue: List[Dict[str, Any]] = []
        for rel_script_path in scripts:
            try:
                clean_rel_script_path = _resolve_rel_script_path(rel_script_path, base_path)
            except ValueError:
                continue
            try:
                jobs_to_queue.extend(
                    _build_jobs_for_relpath(
                        clean_rel_script_path,
                        base_path,
                        config,
                        report_id,
                        framework_version,
                        scripts_tree,
                        suite_name=suite_name,
                        suite_run_id=suite_run_id,
                    )
                )
            except ValueError:
                continue
        if jobs_to_queue:
            queue.add_job(jobs_to_queue, priority=0)
        return _render_jobs_table()

    app.add_url_rule("/scripts", endpoint="scripts_panel", view_func=scripts_panel, methods=["GET"])
    app.add_url_rule("/scripts/<path:script_relpath>", endpoint="script_detail_page", view_func=script_detail_page, methods=["GET"])
    app.add_url_rule("/jobs/<job_id>/output", endpoint="job_output_page", view_func=job_output_page, methods=["GET"])
    app.add_url_rule("/jobs/<job_id>/output/panel", endpoint="job_output_panel", view_func=job_output_panel, methods=["GET"])
    app.add_url_rule("/jobs/<job_id>/output/raw", endpoint="job_output_raw", view_func=job_output_raw, methods=["GET"])
    app.add_url_rule("/jobs/<job_id>/output/artifacts/<path:artifact_path>", endpoint="job_output_artifact", view_func=job_output_artifact, methods=["GET"])
    app.add_url_rule("/jobs/from_script", endpoint="queue_from_script", view_func=queue_from_script, methods=["POST"])
    app.add_url_rule("/jobs/from_scripts", endpoint="queue_from_scripts", view_func=queue_from_scripts, methods=["POST"])
    app.add_url_rule("/jobs/from_suite", endpoint="queue_from_suite", view_func=queue_from_suite, methods=["POST"])
