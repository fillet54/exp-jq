from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

from flask import Flask

from automationv3.jobqueue import CentralServer, JobQueue, UUTStore, SuiteManager
from automationv3.jobqueue.views import register_frontend_routes


def _build_sphinx_docs(log: logging.Logger, source_dir: Path, html_dir: Path) -> tuple[bool, str]:
    if not source_dir.exists():
        return False, f"Docs source directory does not exist: {source_dir}"

    html_dir.parent.mkdir(parents=True, exist_ok=True)
    doctree_dir = html_dir.parent / "doctrees"
    cmd = [
        sys.executable,
        "-m",
        "sphinx.cmd.build",
        "-b",
        "html",
        "-d",
        str(doctree_dir),
        str(source_dir),
        str(html_dir),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        details = (result.stderr or result.stdout).strip()
        return False, details or "Sphinx build failed"
    return True, ""


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static",
        static_url_path="/static",
    )
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("jobqueue.frontend")
    project_root = Path(__file__).resolve().parents[1]

    db_path = os.getenv("JOBQUEUE_DB", "jobqueue.db")
    queue = JobQueue(db_path=db_path)
    central = CentralServer(queue=queue, app=app, route_prefix="/api/central")
    uut_store = UUTStore(db_path=db_path)
    scripts_root = Path(os.getenv("SCRIPT_ROOT", "scripts")).resolve()
    scripts_cache_dir = os.getenv("SCRIPT_CACHE_DIR", ".fscache_scripts")
    suites_dir = Path(os.getenv("SUITES_DIR", scripts_root / "suites")).resolve()
    suite_manager = SuiteManager(suites_dir)
    docs_source_dir = Path(
        os.getenv("JOBQUEUE_DOCS_SOURCE", str(project_root / "automationv3" / "docs"))
    ).resolve()
    docs_html_dir = Path(
        os.getenv(
            "JOBQUEUE_DOCS_HTML_DIR",
            str(project_root / "automationv3" / "docs" / "_build" / "html"),
        )
    ).resolve()

    docs_enabled = os.getenv("JOBQUEUE_DOCS_ENABLED", "1").lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    docs_status = {"enabled": docs_enabled, "built": False, "message": ""}
    if docs_enabled:
        built, message = _build_sphinx_docs(log, docs_source_dir, docs_html_dir)
        docs_status["built"] = built
        docs_status["message"] = message
        if built:
            log.info("Built docs at %s", docs_html_dir)
        else:
            log.warning("Docs build failed: %s", message)

    app.config["DOCS_HTML_DIR"] = str(docs_html_dir)
    app.config["DOCS_STATUS"] = docs_status

    register_frontend_routes(
        app=app,
        queue=queue,
        central=central,
        uut_store=uut_store,
        suite_manager=suite_manager,
        scripts_root=scripts_root,
        scripts_cache_dir=scripts_cache_dir,
        log=log,
    )

    return app


__all__ = ["create_app"]
