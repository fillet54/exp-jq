import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional

import logging
import requests
from flask import Flask, jsonify, request, send_file

from . import JobInput, JobQueue
from .ids import uuid7_str


DEFAULT_WORKER_STATE_FILE = ".worker_id"


def _http_json(
    url: str,
    payload: Optional[dict] = None,
    method: str = "GET",
    timeout: float = 2.0,
):
    """Small helper around requests for JSON requests."""
    try:
        if method.upper() == "GET":
            resp = requests.get(url, timeout=timeout)
        else:
            resp = requests.request(method.upper(), url, json=payload, timeout=timeout)
        try:
            body = resp.json() if resp.text else None
        except ValueError:
            body = None
        return resp.status_code, body
    except requests.RequestException:
        logging.exception("HTTP request failed: %s %s", method, url)
        return None, None


@dataclass
class WorkerInfo:
    worker_id: str
    address: str
    last_seen: float
    busy: bool = False
    current_job: Optional[str] = None
    online: bool = True
    meta: Dict[str, str] = field(default_factory=dict)


class CentralServer:
    """Central coordinator that keeps the job queue and talks to workers via HTTP."""

    def __init__(
        self,
        queue: JobQueue,
        poll_interval: float = 10.0,
        dispatch_interval: float = 3.0,
        app: Optional[Flask] = None,
        start_background_threads: bool = True,
        route_prefix: str = "",
        artifacts_dir: str = "artifacts",
    ) -> None:
        self.queue = queue
        self.workers: Dict[str, WorkerInfo] = {}
        self.poll_interval = poll_interval
        self.dispatch_interval = dispatch_interval
        self.lock = threading.Lock()
        prefix = route_prefix.rstrip("/")
        if prefix and not prefix.startswith("/"):
            prefix = "/" + prefix
        self.route_prefix = prefix
        self.artifacts_dir = Path(artifacts_dir)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.app = app or Flask("jobqueue.central")
        self._setup_routes()
        if start_background_threads:
            self._start_background_threads()
        self.log = logging.getLogger("jobqueue.central")
        self.log.info("CentralServer initialized with artifacts dir %s", self.artifacts_dir)

    def _setup_routes(self) -> None:
        register_path = f"{self.route_prefix}/register" if self.route_prefix else "/register"
        @self.app.post(register_path)
        def register_worker():
            payload = request.get_json(force=True, silent=True) or {}
            provided_id = payload.get("worker_id")
            address = payload.get("address")
            meta = payload.get("meta") or {}
            if not address:
                return jsonify({"error": "address is required"}), 400
            worker_id = provided_id or uuid7_str()
            info = WorkerInfo(
                worker_id=worker_id,
                address=address.rstrip("/"),
                last_seen=time.time(),
                meta=meta,
            )
            with self.lock:
                self.workers[worker_id] = info
            self.log.info(
                "Worker registered %s at %s meta=%s (provided_id=%s)",
                worker_id,
                address,
                meta,
                bool(provided_id),
            )
            return jsonify({"worker_id": worker_id}), (200 if provided_id else 201)

        workers_path = f"{self.route_prefix}/workers" if self.route_prefix else "/workers"
        @self.app.get(workers_path)
        def list_workers():
            with self.lock:
                workers = [
                    {
                        "worker_id": w.worker_id,
                        "address": w.address,
                        "last_seen": w.last_seen,
                        "busy": w.busy,
                        "current_job": w.current_job,
                        "online": w.online,
                        "meta": w.meta,
                    }
                    for w in self.workers.values()
                ]
            return jsonify(workers)

        worker_result_path = (
            f"{workers_path}/<worker_id>/result" if workers_path else "/workers/<worker_id>/result"
        )
        @self.app.post(worker_result_path)
        def receive_result(worker_id: str):
            payload = request.get_json(force=True, silent=True) or {}
            job_id = payload.get("job_id")
            result = payload.get("result")
            success = bool(payload.get("success", True))
            artifacts = payload.get("artifacts") or []
            if not job_id:
                return jsonify({"error": "job_id is required"}), 400
            job_snapshot = self.queue.get_job(job_id)
            worker_info = self.workers.get(worker_id)
            worker_address = (
                worker_info.address if worker_info else payload.get("worker_address")
            )  # type: ignore[union-attr]
            self.log.info(
                "Result received for job %s from worker %s success=%s artifacts=%s",
                job_id,
                worker_id,
                success,
                artifacts,
            )
            self.queue.record_result(
                job_id=job_id,
                result_data=result,
                success=success,
                worker_id=worker_id,
                worker_address=worker_address,
                artifacts_manifest=artifacts,
                job_data_snapshot=job_snapshot,
            )
            self.queue.remove_job(job_id)
            with self.lock:
                if worker_info:
                    worker_info.busy = False
                    worker_info.current_job = None
                    worker_info.last_seen = time.time()
            return jsonify({"ack": True, "success": success, "result": result})

        dispatch_path = f"{self.route_prefix}/dispatch" if self.route_prefix else "/dispatch"
        @self.app.post(dispatch_path)
        def dispatch_endpoint():
            dispatched = self.dispatch_once()
            return jsonify({"dispatched": dispatched}), (200 if dispatched else 202)

        next_job_path = f"{self.route_prefix}/jobs/next" if self.route_prefix else "/jobs/next"
        @self.app.get(next_job_path)
        def peek_next():
            job = self.queue.get_next_job()
            return jsonify(job or {})

    def get_workers_snapshot(self):
        with self.lock:
            return list(self.workers.values())

    def _start_background_threads(self) -> None:
        threading.Thread(target=self._poll_workers_loop, daemon=True).start()
        threading.Thread(target=self._dispatch_loop, daemon=True).start()
        threading.Thread(target=self._artifact_sync_loop, daemon=True).start()

    def _poll_workers_loop(self) -> None:
        while True:
            with self.lock:
                workers = list(self.workers.values())
            for worker in workers:
                status_code, body = _http_json(
                    f"{worker.address}/status", method="GET", timeout=1.0
                )
                now = time.time()
                if status_code == 200 and isinstance(body, dict):
                    worker.online = True
                    worker.last_seen = now
                    worker.busy = bool(body.get("busy"))
                    worker.current_job = body.get("current_job")
                    self.log.debug(
                        "Polled worker %s online busy=%s job=%s",
                        worker.worker_id,
                        worker.busy,
                        worker.current_job,
                    )
                else:
                    worker.online = False
                    self.log.warning(
                        "Worker %s unreachable at %s", worker.worker_id, worker.address
                    )
            time.sleep(self.poll_interval)

    def _dispatch_loop(self) -> None:
        while True:
            self.dispatch_once()
            time.sleep(self.dispatch_interval)

    def _can_worker_take_job(self, worker: WorkerInfo) -> bool:
        status_code, body = _http_json(
            f"{worker.address}/status", method="GET", timeout=1.0
        )
        if status_code != 200 or not isinstance(body, dict):
            worker.online = False
            return False
        worker.online = True
        worker.last_seen = time.time()
        worker.busy = bool(body.get("busy"))
        worker.current_job = body.get("current_job")
        return not worker.busy

    def _send_job(self, worker: WorkerInfo, job: JobInput) -> bool:
        status_code, _ = _http_json(
            f"{worker.address}/jobs", payload=job, method="POST", timeout=3.0
        )
        if status_code == 200:
            worker.busy = True
            worker.current_job = job.get("job_id")
            worker.last_seen = time.time()
            return True
        return False

    def dispatch_once(self) -> bool:
        """Try to hand one job to some ready worker. Returns True if dispatched."""
        job = self.queue.get_next_job()
        if not job:
            return False
        self.log.info("Dispatch attempt for job %s", job.get("job_id"))
        with self.lock:
            workers = list(self.workers.values())
        for worker in workers:
            if not worker.online:
                continue
            if not self._can_worker_take_job(worker):
                continue
            if self._send_job(worker, job):
                self.log.info(
                    "Job %s dispatched to worker %s", job.get("job_id"), worker.worker_id
                )
                return True
        # If nobody could take the job, leave it in queue (do not mark skipped)
        self.log.warning("Job %s waiting (no available worker)", job.get("job_id"))
        return False

    def _download_artifact(
        self, worker_address: str, job_id: str, artifact_path: str
    ) -> bool:
        if ".." in artifact_path:
            return False
        local_path = self.artifacts_dir / job_id / artifact_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        url = f"{worker_address}/artifacts/{job_id}/{artifact_path}"
        try:
            resp = requests.get(url, timeout=5.0)
            if resp.status_code != 200:
                self.log.warning(
                    "Artifact fetch failed job=%s path=%s status=%s",
                    job_id,
                    artifact_path,
                    resp.status_code,
                )
                return False
            local_path.write_bytes(resp.content)
            self.log.info(
                "Artifact downloaded job=%s path=%s -> %s", job_id, artifact_path, local_path
            )
            return True
        except requests.RequestException:
            self.log.exception(
                "Artifact download error job=%s path=%s from %s", job_id, artifact_path, url
            )
            return False

    def _artifact_sync_loop(self) -> None:
        while True:
            pending = self.queue.list_pending_artifacts()
            for item in pending:
                worker_address = item.get("worker_address")
                manifest: List[str] = item.get("artifacts_manifest") or []
                if not worker_address or not manifest:
                    self.queue.mark_artifacts_downloaded(item["job_id"])
                    continue
                all_ok = True
                for path in manifest:
                    ok = self._download_artifact(worker_address, item["job_id"], path)
                    if not ok:
                        all_ok = False
                        break
                if all_ok:
                    self.queue.mark_artifacts_downloaded(item["job_id"])
                    self.log.info("All artifacts downloaded for job %s", item["job_id"])
            time.sleep(5)


class WorkerServer:
    """Lightweight worker that exposes an HTTP API and calls back to the central server."""

    def __init__(
        self,
        central_url: str,
        worker_address: str,
        job_runner: Callable[[JobInput], Dict],
        meta: Optional[Dict[str, str]] = None,
        artifacts_dir: str = "worker_artifacts",
        worker_state_file: str = DEFAULT_WORKER_STATE_FILE,
    ) -> None:
        # Logger first so helper methods can log
        self.log = logging.getLogger(f"jobqueue.worker[{worker_address}]")

        self.central_url = central_url.rstrip("/")
        self.worker_address = worker_address.rstrip("/")
        self.job_runner = job_runner
        self.meta = meta or {}
        # Normalize worker_state_file; CLI provides default to avoid drift.
        self.worker_state_file = Path(worker_state_file)
        if not self.worker_state_file.is_absolute():
            # Store alongside artifacts dir by default when relative
            self.worker_state_file = Path(artifacts_dir) / self.worker_state_file
        self.worker_state_file.parent.mkdir(parents=True, exist_ok=True)
        self.worker_id: Optional[str] = self._load_worker_id()
        self.busy = False
        self.current_job: Optional[str] = None
        self.lock = threading.Lock()
        self.artifacts_dir = Path(artifacts_dir)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.app = Flask("jobqueue.worker")
        self._setup_routes()
        threading.Thread(target=self._registration_loop, daemon=True).start()

    def _load_worker_id(self) -> Optional[str]:
        try:
            if self.worker_state_file.exists():
                wid = self.worker_state_file.read_text().strip() or None
                if wid:
                    self.log.info("Loaded worker id %s from %s", wid, self.worker_state_file)
                return wid
        except Exception:
            self.log.exception("Failed to read worker state file %s", self.worker_state_file)
        return None

    def _persist_worker_id(self) -> None:
        if not self.worker_id:
            return
        try:
            self.worker_state_file.write_text(self.worker_id)
            self.log.info("Persisted worker id %s to %s", self.worker_id, self.worker_state_file)
        except Exception:
            self.log.exception("Failed to persist worker id to %s", self.worker_state_file)

    def _setup_routes(self) -> None:
        @self.app.get("/status")
        def status():
            return jsonify(
                {
                    "worker_id": self.worker_id,
                    "busy": self.busy,
                    "current_job": self.current_job,
                    "address": self.worker_address,
                }
            )

        @self.app.post("/jobs")
        def accept_job():
            if self.busy:
                return jsonify({"error": "busy"}), 409
            job = request.get_json(force=True, silent=True) or {}
            job_id = job.get("job_id")
            if not job_id:
                return jsonify({"error": "job_id is required"}), 400
            self.log.info("Accepted job %s", job_id)
            threading.Thread(target=self._execute_job, args=(job,), daemon=True).start()
            return jsonify({"accepted": True})

        @self.app.get("/artifacts/<job_id>/<path:artifact_path>")
        def serve_artifact(job_id: str, artifact_path: str):
            if ".." in artifact_path:
                return jsonify({"error": "invalid path"}), 400
            file_path = self.artifacts_dir / job_id / artifact_path
            if not file_path.exists() or not file_path.is_file():
                return jsonify({"error": "not found"}), 404
            return send_file(file_path)

    def _registration_loop(self) -> None:
        """Continuously ensure registration with the central server (handles central restarts)."""
        while True:
            payload = {"address": self.worker_address, "meta": self.meta}
            if self.worker_id:
                payload["worker_id"] = self.worker_id
            status_code, body = _http_json(
                f"{self.central_url}/register",
                payload=payload,
                method="POST",
                timeout=3.0,
            )
            if status_code in (200, 201) and isinstance(body, dict):
                returned_id = body.get("worker_id")
                if returned_id and returned_id != self.worker_id:
                    self.log.info("Central assigned new worker_id %s (old=%s)", returned_id, self.worker_id)
                self.worker_id = returned_id or self.worker_id
                self._persist_worker_id()
                self.log.debug("Registration heartbeat ok for worker %s", self.worker_id)
            else:
                self.log.warning("Registration failed status=%s body=%s", status_code, body)
            time.sleep(5)

    def _execute_job(self, job: JobInput) -> None:
        with self.lock:
            self.busy = True
            self.current_job = job.get("job_id")
        self.log.info(
            "Starting job %s uut=%s scripts_tree=%s framework=%s",
            self.current_job,
            job.get("uut_tree"),
            job.get("scripts_tree"),
            job.get("framework_version"),
        )
        try:
            try:
                result_payload = self.job_runner(job, artifacts_dir=str(self.artifacts_dir))
            except TypeError:
                result_payload = self.job_runner(job)
            result_payload = result_payload or {}
            artifacts = result_payload.get("artifacts", [])
            summary = result_payload.get("summary", result_payload)
            success = True
        except Exception as exc:  # pragma: no cover - defensive
            summary = {"error": str(exc)}
            artifacts = []
            success = False
            self.log.exception("Job %s failed during execution", self.current_job)
        payload = {
            "job_id": job.get("job_id"),
            "worker_id": self.worker_id,
            "result": summary,
            "artifacts": artifacts,
            "worker_address": self.worker_address,
            "success": success,
        }
        _http_json(
            f"{self.central_url}/workers/{self.worker_id}/result",
            payload=payload,
            method="POST",
            timeout=3.0,
        )
        self.log.info(
            "Completed job %s success=%s artifacts=%s", self.current_job, success, len(artifacts)
        )
        with self.lock:
            self.busy = False
            self.current_job = None


def create_central_app(
    queue: Optional[JobQueue] = None,
    poll_interval: float = 10.0,
    dispatch_interval: float = 3.0,
    app: Optional[Flask] = None,
    start_background_threads: bool = True,
    route_prefix: str = "",
    artifacts_dir: str = "artifacts",
) -> Flask:
    """Factory for the central server Flask app."""
    server = CentralServer(
        queue=queue or JobQueue(),
        poll_interval=poll_interval,
        dispatch_interval=dispatch_interval,
        app=app,
        start_background_threads=start_background_threads,
        route_prefix=route_prefix,
        artifacts_dir=artifacts_dir,
    )
    return server.app


def create_worker_app(
    central_url: str,
    worker_address: str,
    job_runner: Callable[[JobInput], Dict],
    meta: Optional[Dict[str, str]] = None,
    artifacts_dir: str = "worker_artifacts",
    worker_state_file: Optional[str] = DEFAULT_WORKER_STATE_FILE,
) -> Flask:
    """Factory for a worker Flask app."""
    state_path = worker_state_file or DEFAULT_WORKER_STATE_FILE
    server = WorkerServer(
        central_url=central_url,
        worker_address=worker_address,
        job_runner=job_runner,
        meta=meta,
        artifacts_dir=artifacts_dir,
        worker_state_file=state_path,
    )
    return server.app


__all__ = ["create_central_app", "create_worker_app", "CentralServer", "WorkerServer"]
