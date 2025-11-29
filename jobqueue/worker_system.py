import json
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional

from flask import Flask, jsonify, request

from . import JobInput, JobQueue


def _http_json(url: str, payload: Optional[dict] = None, timeout: float = 5.0):
    """Small helper around urllib for JSON requests."""
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return resp.getcode(), json.loads(body) if body else None
    except urllib.error.HTTPError as exc:  # type: ignore[attr-defined]
        body = exc.read().decode("utf-8")
        return exc.code, json.loads(body) if body else {"error": body}
    except urllib.error.URLError:
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
        self.app = app or Flask("jobqueue.central")
        self._setup_routes()
        if start_background_threads:
            self._start_background_threads()

    def _setup_routes(self) -> None:
        register_path = f"{self.route_prefix}/register" if self.route_prefix else "/register"
        @self.app.post(register_path)
        def register_worker():
            payload = request.get_json(force=True, silent=True) or {}
            address = payload.get("address")
            meta = payload.get("meta") or {}
            if not address:
                return jsonify({"error": "address is required"}), 400
            worker_id = str(uuid.uuid4())
            info = WorkerInfo(
                worker_id=worker_id,
                address=address.rstrip("/"),
                last_seen=time.time(),
                meta=meta,
            )
            with self.lock:
                self.workers[worker_id] = info
            return jsonify({"worker_id": worker_id}), 201

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
            if not job_id:
                return jsonify({"error": "job_id is required"}), 400
            self.queue.remove_job(job_id)
            with self.lock:
                info = self.workers.get(worker_id)
                if info:
                    info.busy = False
                    info.current_job = None
                    info.last_seen = time.time()
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

    def _poll_workers_loop(self) -> None:
        while True:
            with self.lock:
                workers = list(self.workers.values())
            for worker in workers:
                status_code, body = _http_json(f"{worker.address}/status")
                now = time.time()
                if status_code == 200 and isinstance(body, dict):
                    worker.online = True
                    worker.last_seen = now
                    worker.busy = bool(body.get("busy"))
                    worker.current_job = body.get("current_job")
                else:
                    worker.online = False
            time.sleep(self.poll_interval)

    def _dispatch_loop(self) -> None:
        while True:
            self.dispatch_once()
            time.sleep(self.dispatch_interval)

    def _can_worker_take_job(self, worker: WorkerInfo) -> bool:
        status_code, body = _http_json(f"{worker.address}/status")
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
            f"{worker.address}/jobs", payload=job, timeout=10.0
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
        with self.lock:
            workers = list(self.workers.values())
        for worker in workers:
            if not worker.online:
                continue
            if not self._can_worker_take_job(worker):
                continue
            if self._send_job(worker, job):
                return True
        # If nobody could take the job, skip it and move on
        self.queue.mark_skipped(job["job_id"])
        return False


class WorkerServer:
    """Lightweight worker that exposes an HTTP API and calls back to the central server."""

    def __init__(
        self,
        central_url: str,
        worker_address: str,
        job_runner: Callable[[JobInput], Dict],
        meta: Optional[Dict[str, str]] = None,
    ) -> None:
        self.central_url = central_url.rstrip("/")
        self.worker_address = worker_address.rstrip("/")
        self.job_runner = job_runner
        self.meta = meta or {}
        self.worker_id: Optional[str] = None
        self.busy = False
        self.current_job: Optional[str] = None
        self.lock = threading.Lock()
        self.app = Flask("jobqueue.worker")
        self._setup_routes()
        threading.Thread(target=self._register_with_central, daemon=True).start()

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
            threading.Thread(target=self._execute_job, args=(job,), daemon=True).start()
            return jsonify({"accepted": True})

    def _register_with_central(self) -> None:
        # Retry loop until the central server is reachable.
        while self.worker_id is None:
            status_code, body = _http_json(
                f"{self.central_url}/register",
                payload={"address": self.worker_address, "meta": self.meta},
            )
            if status_code == 201 and isinstance(body, dict):
                self.worker_id = body.get("worker_id")
                break
            time.sleep(2)

    def _execute_job(self, job: JobInput) -> None:
        with self.lock:
            self.busy = True
            self.current_job = job.get("job_id")
        try:
            result = self.job_runner(job)
            success = True
        except Exception as exc:  # pragma: no cover - defensive
            result = {"error": str(exc)}
            success = False
        payload = {
            "job_id": job.get("job_id"),
            "worker_id": self.worker_id,
            "result": result,
            "success": success,
        }
        _http_json(f"{self.central_url}/workers/{self.worker_id}/result", payload=payload)
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
) -> Flask:
    """Factory for the central server Flask app."""
    server = CentralServer(
        queue=queue or JobQueue(),
        poll_interval=poll_interval,
        dispatch_interval=dispatch_interval,
        app=app,
        start_background_threads=start_background_threads,
        route_prefix=route_prefix,
    )
    return server.app


def create_worker_app(
    central_url: str,
    worker_address: str,
    job_runner: Callable[[JobInput], Dict],
    meta: Optional[Dict[str, str]] = None,
) -> Flask:
    """Factory for a worker Flask app."""
    server = WorkerServer(
        central_url=central_url,
        worker_address=worker_address,
        job_runner=job_runner,
        meta=meta,
    )
    return server.app


__all__ = ["create_central_app", "create_worker_app", "CentralServer", "WorkerServer"]
