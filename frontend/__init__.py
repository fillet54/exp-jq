from __future__ import annotations

import logging
import os
from pathlib import Path

from flask import Flask

from jobqueue import CentralServer, JobQueue, UUTStore, SuiteManager
from jobqueue.views import register_frontend_routes


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates")
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("jobqueue.frontend")

    db_path = os.getenv("JOBQUEUE_DB", "jobqueue.db")
    queue = JobQueue(db_path=db_path)
    central = CentralServer(queue=queue, app=app, route_prefix="/api/central")
    uut_store = UUTStore(db_path=db_path)
    scripts_root = Path(os.getenv("SCRIPT_ROOT", "scripts")).resolve()
    scripts_cache_dir = os.getenv("SCRIPT_CACHE_DIR", ".fscache_scripts")
    suites_dir = Path(os.getenv("SUITES_DIR", scripts_root / "suites")).resolve()
    suite_manager = SuiteManager(suites_dir)

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
