"""Frontend route registration for the jobqueue dashboard."""

import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, render_template, request

from . import uuid7_str
from .fscache import snapshot_tree


def _parse_meta_from_rst(path: Path) -> Dict[str, List[str]]:
    meta: Dict[str, List[str]] = {"requirements": [], "tags": [], "subsystem": []}
    try:
        content = path.read_text().splitlines()
    except FileNotFoundError:
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


def _discover_scripts(root: Path) -> List[Dict[str, Any]]:
    scripts: List[Dict[str, Any]] = []
    if not root.exists():
        return scripts
    for path in sorted(root.rglob("*.rst")):
        rel = path.relative_to(root)
        meta = _parse_meta_from_rst(path)
        scripts.append(
            {
                "path": str(path),
                "relpath": str(rel),
                "meta": meta,
                "name": path.stem,
            }
        )
    return scripts


def _filter_scripts(
    scripts: List[Dict[str, Any]],
    requirements: List[str],
    tags: List[str],
    subsystem: List[str],
) -> List[Dict[str, Any]]:
    def _matches(values: List[str], candidate: List[str]) -> bool:
        if not values:
            return True
        return bool(set(v.lower() for v in values) & set(s.lower() for s in candidate))

    filtered = []
    for s in scripts:
        meta = s.get("meta", {})
        if not _matches(requirements, meta.get("requirements", [])):
            continue
        if not _matches(tags, meta.get("tags", [])):
            continue
        if subsystem and not _matches(subsystem, meta.get("subsystem", [])):
            continue
        filtered.append(s)
    return filtered


def register_frontend_routes(
    app: Flask,
    queue,
    central,
    uut_store,
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

    def _render_scripts_panel(
        base_path: Path,
        reqs: str = "",
        tags: str = "",
        subsystem: str = "",
        uut_id: str = "",
        framework_version: str = "",
    ) -> str:
        scripts = _discover_scripts(base_path)
        req_list = [r.strip() for r in reqs.split(",") if r.strip()]
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]
        subsystem_list = [s.strip() for s in subsystem.split(",") if s.strip()]
        scripts = _filter_scripts(scripts, req_list, tag_list, subsystem_list)
        return render_template(
            "partials/scripts_panel.html",
            scripts=scripts,
            base_path=str(base_path),
            reqs=reqs,
            tags=tags,
            subsystem=subsystem,
            uut_id=uut_id,
            framework_version=framework_version,
            uuts=uut_store.list(),
        )

    def _render_results_table() -> str:
        results = queue.list_results()
        return render_template("partials/results_table.html", results=results)

    @app.route("/", methods=["GET"])
    def index() -> str:
        jobs = queue.list_jobs()
        next_job = queue.get_next_job()
        workers = central.get_workers_snapshot()
        results = queue.list_results()
        uuts = uut_store.list()
        scripts_panel = _render_scripts_panel(scripts_root)
        return render_template(
            "index.html",
            jobs=jobs,
            next_job=next_job,
            workers=workers,
            results=results,
            uuts=uuts,
            scripts_panel=scripts_panel,
            now_ts=time.time(),
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

    @app.route("/scripts", methods=["GET"])
    def scripts_panel() -> str:
        base_path = Path(request.args.get("base_path") or scripts_root)
        reqs = request.args.get("requirements", "")
        tags = request.args.get("tags", "")
        subsystem = request.args.get("subsystem", "")
        uut_id = request.args.get("uut_id", "")
        framework_version = request.args.get("framework_version", "")
        panel = _render_scripts_panel(
            base_path,
            reqs=reqs,
            tags=tags,
            subsystem=subsystem,
            uut_id=uut_id,
            framework_version=framework_version,
        )
        if request.headers.get("HX-Request") == "true":
            return panel
        return render_template("scripts.html", scripts_panel=panel)

    @app.route("/jobs/from_script", methods=["POST"])
    def queue_from_script() -> str:
        script_path = (request.form.get("script_path") or "").strip()
        base_path = Path(request.form.get("base_path") or scripts_root)
        uut_id = (request.form.get("uut_id") or "").strip()
        framework_version = (request.form.get("framework_version") or "").strip()
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
        }
        queue.add_job(job, priority=0)
        return _render_jobs_table()

    @app.route("/results/table", methods=["GET"])
    def results_table() -> str:
        return _render_results_table()

    @app.route("/health", methods=["GET"])
    def health() -> str:
        return {"status": "ok"}


__all__ = ["register_frontend_routes"]
