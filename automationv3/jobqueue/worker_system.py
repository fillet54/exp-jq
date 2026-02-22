import threading
import time
import os
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import logging
import requests
from flask import Flask, jsonify, request, send_file

from . import JobInput, JobQueue
from .ids import uuid7_str
from .fscache import calculate_sha1, snapshot_tree as fscache_snapshot_tree


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
        self.live_job_events: Dict[str, List[Dict[str, Any]]] = {}
        self.live_job_documents: Dict[str, str] = {}
        self.live_job_last_seq: Dict[str, int] = {}
        self.poll_interval = poll_interval
        self.dispatch_interval = dispatch_interval
        self.lock = threading.Lock()
        prefix = route_prefix.rstrip("/")
        if prefix and not prefix.startswith("/"):
            prefix = "/" + prefix
        self.route_prefix = prefix
        self.artifacts_dir = Path(artifacts_dir)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.app = app or Flask(__name__)
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
            if self.queue.get_result(job_id):
                self.log.info(
                    "Duplicate result delivery ignored for finalized job %s from worker %s",
                    job_id,
                    worker_id,
                )
                with self.lock:
                    if worker_info:
                        worker_info.busy = False
                        worker_info.current_job = None
                        worker_info.last_seen = time.time()
                return jsonify({"ack": True, "duplicate": True, "finalized": True})
            self.log.info(
                "Result received for job %s from worker %s success=%s artifacts=%s",
                job_id,
                worker_id,
                success,
                artifacts,
            )
            live_output = self._pop_live_job_output(job_id)
            if isinstance(result, dict):
                if live_output.get("events") and not result.get("observer_events"):
                    result["observer_events"] = live_output["events"]
                if live_output.get("result_document") and not result.get("result_document"):
                    result["result_document"] = live_output["result_document"]
            self.queue.store_pending_result(
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
            return jsonify({"ack": True, "success": success, "pending_sync": True})

        worker_event_path = (
            f"{workers_path}/<worker_id>/events" if workers_path else "/workers/<worker_id>/events"
        )
        @self.app.post(worker_event_path)
        def receive_worker_event(worker_id: str):
            payload = request.get_json(force=True, silent=True) or {}
            job_id = payload.get("job_id")
            event = payload.get("event")
            if not job_id:
                return jsonify({"error": "job_id is required"}), 400
            if not isinstance(event, dict):
                return jsonify({"error": "event object is required"}), 400
            self._append_live_event(job_id, event)
            with self.lock:
                worker = self.workers.get(worker_id)
                if worker:
                    worker.last_seen = time.time()
            return jsonify({"ack": True})

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

    def _append_live_event(self, job_id: str, event: Dict[str, Any]) -> None:
        with self.lock:
            seq = event.get("seq")
            last_seq = self.live_job_last_seq.get(job_id, -1)
            if isinstance(seq, int):
                if seq <= last_seq:
                    return
                self.live_job_last_seq[job_id] = seq
            else:
                self.live_job_last_seq[job_id] = last_seq + 1
            self.live_job_events.setdefault(job_id, []).append(event)
            fragment = event.get("rst_fragment")
            if isinstance(fragment, str) and fragment:
                self.live_job_documents[job_id] = (
                    self.live_job_documents.get(job_id, "") + fragment
                )

    def get_live_job_output(self, job_id: str) -> Dict[str, Any]:
        with self.lock:
            return {
                "events": list(self.live_job_events.get(job_id, [])),
                "result_document": self.live_job_documents.get(job_id, ""),
                "last_seq": self.live_job_last_seq.get(job_id, -1),
            }

    def _pop_live_job_output(self, job_id: str) -> Dict[str, Any]:
        with self.lock:
            events = list(self.live_job_events.pop(job_id, []))
            result_document = self.live_job_documents.pop(job_id, "")
            last_seq = self.live_job_last_seq.pop(job_id, -1)
        return {
            "events": events,
            "result_document": result_document,
            "last_seq": last_seq,
        }

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

    def _fetch_worker_artifact_manifest(
        self, worker_address: str, job_id: str
    ) -> Dict[str, Any] | None:
        status_code, body = _http_json(
            f"{worker_address}/artifacts/{job_id}/manifest",
            method="GET",
            timeout=5.0,
        )
        if status_code != 200 or not isinstance(body, dict):
            return None
        return body

    def _compute_local_artifact_tree_sha(self, job_id: str) -> str:
        job_dir = (self.artifacts_dir / job_id).resolve()
        if not job_dir.exists() or not job_dir.is_dir():
            return ""
        try:
            return str(
                fscache_snapshot_tree(
                    rootdir=str(job_dir),
                    cache_dir=str(self.artifacts_dir / ".fscache_results" / job_id),
                )
            )
        except Exception:
            self.log.exception("Failed to compute local artifact tree sha job=%s", job_id)
            return ""

    def _local_file_needs_update(self, job_id: str, path: str, entry: Dict[str, Any]) -> bool:
        local_path = (self.artifacts_dir / job_id / path).resolve()
        if not local_path.exists() or not local_path.is_file():
            return True
        try:
            expected_size = int(entry.get("size")) if entry.get("size") is not None else None
        except (TypeError, ValueError):
            expected_size = None
        if expected_size is not None and local_path.stat().st_size != expected_size:
            return True
        expected_sha1 = str(entry.get("sha1") or "").strip().lower()
        if expected_sha1:
            try:
                local_sha1 = calculate_sha1(str(local_path)).lower()
            except Exception:
                return True
            if local_sha1 != expected_sha1:
                return True
        return False

    def _artifact_sync_loop(self) -> None:
        while True:
            pending = self.queue.list_pending_results()
            for item in pending:
                ok, reason = self._sync_and_finalize_pending_result(item)
                if not ok:
                    job_id = str(item.get("job_id") or "")
                    if job_id:
                        self.queue.mark_pending_result_error(job_id, reason)
            time.sleep(5)

    def _sync_and_finalize_pending_result(self, item: Dict[str, Any]) -> tuple[bool, str]:
        job_id = str(item.get("job_id") or "")
        if not job_id:
            return False, "missing job_id"

        result_data = item.get("result_data") or {}
        if not isinstance(result_data, dict):
            result_data = {}
        worker_address = str(item.get("worker_address") or "").strip()
        manifest: List[str] = [str(p) for p in (item.get("artifacts_manifest") or [])]
        expected_tree_sha = str(result_data.get("artifact_tree_sha") or "").strip()

        remote_entries: Dict[str, Dict[str, Any]] = {}
        remote_tree_sha = ""
        if worker_address:
            remote_manifest_payload = self._fetch_worker_artifact_manifest(
                worker_address,
                job_id,
            )
            if isinstance(remote_manifest_payload, dict):
                remote_tree_sha = str(remote_manifest_payload.get("tree_sha") or "").strip()
                for entry in remote_manifest_payload.get("files") or []:
                    if not isinstance(entry, dict):
                        continue
                    rel_path = str(entry.get("path") or "").strip()
                    if not rel_path or ".." in rel_path:
                        continue
                    remote_entries[rel_path] = entry
        candidate_paths = list(remote_entries.keys()) if remote_entries else list(manifest)
        candidate_paths = list(dict.fromkeys(candidate_paths))

        if candidate_paths and not worker_address:
            reason = f"worker address missing for artifact sync job={job_id}"
            self.log.warning(reason)
            return False, reason

        for path in candidate_paths:
            entry = remote_entries.get(path, {"path": path})
            if not self._local_file_needs_update(job_id, path, entry):
                continue
            ok = self._download_artifact(worker_address, job_id, path)
            if not ok:
                reason = f"artifact download failed job={job_id} path={path}"
                self.log.warning(reason)
                return False, reason

        verify_tree_sha = expected_tree_sha or remote_tree_sha
        if verify_tree_sha:
            local_tree_sha = self._compute_local_artifact_tree_sha(job_id)
            if local_tree_sha != verify_tree_sha:
                self.log.warning(
                    "Artifact tree mismatch for job %s local=%s remote=%s; retrying full sync",
                    job_id,
                    local_tree_sha,
                    verify_tree_sha,
                )
                for path in candidate_paths:
                    ok = self._download_artifact(worker_address, job_id, path)
                    if not ok:
                        reason = f"artifact retry download failed job={job_id} path={path}"
                        self.log.warning(reason)
                        return False, reason
                local_tree_sha = self._compute_local_artifact_tree_sha(job_id)
                if local_tree_sha != verify_tree_sha:
                    reason = (
                        "artifact tree mismatch "
                        f"job={job_id} local={local_tree_sha} expected={verify_tree_sha}"
                    )
                    self.log.warning(reason)
                    return False, reason

        self.queue.record_result(
            job_id=job_id,
            result_data=result_data,
            success=bool(item.get("success", True)),
            worker_id=item.get("worker_id"),
            worker_address=item.get("worker_address"),
            artifacts_manifest=manifest,
            job_data_snapshot=item.get("job_data"),
            artifacts_downloaded=True,
        )
        self.queue.delete_pending_result(job_id)
        self.log.info("Result finalized for job %s after artifact parity", job_id)
        return True, ""


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
        self.outbox_dir = self.artifacts_dir / ".outbox"
        self.outbox_dir.mkdir(parents=True, exist_ok=True)
        self.app = Flask(__name__)
        self._setup_routes()
        threading.Thread(target=self._registration_loop, daemon=True).start()
        threading.Thread(target=self._result_delivery_loop, daemon=True).start()

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
            busy, current_job = self._status_state()
            return jsonify(
                {
                    "worker_id": self.worker_id,
                    "busy": busy,
                    "current_job": current_job,
                    "address": self.worker_address,
                }
            )

        @self.app.post("/jobs")
        def accept_job():
            if self._is_unavailable_for_new_jobs():
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

        @self.app.get("/artifacts/<job_id>/manifest")
        def artifact_manifest(job_id: str):
            job_dir = (self.artifacts_dir / job_id).resolve()
            if not job_dir.exists() or not job_dir.is_dir():
                return jsonify({"tree_sha": "", "files": []})

            files: List[Dict[str, Any]] = []
            for dirpath, _, filenames in os.walk(job_dir):
                for filename in filenames:
                    full_path = Path(dirpath) / filename
                    rel_path = full_path.relative_to(job_dir).as_posix()
                    if ".." in rel_path:
                        continue
                    try:
                        size = full_path.stat().st_size
                        sha1 = calculate_sha1(str(full_path))
                    except Exception:
                        self.log.exception(
                            "Failed to hash artifact %s for job %s", rel_path, job_id
                        )
                        continue
                    files.append(
                        {
                            "path": rel_path,
                            "size": int(size),
                            "sha1": str(sha1),
                        }
                    )

            tree_sha = ""
            try:
                tree_sha = str(
                    fscache_snapshot_tree(
                        rootdir=str(job_dir),
                        cache_dir=str(self.artifacts_dir / ".fscache_results" / job_id),
                    )
                )
            except Exception:
                self.log.exception("Failed to compute artifact tree sha for job %s", job_id)
                tree_sha = ""

            files.sort(key=lambda row: row.get("path") or "")
            return jsonify({"tree_sha": tree_sha, "files": files})

    def _status_state(self) -> tuple[bool, Optional[str]]:
        with self.lock:
            local_busy = self.busy
            local_current_job = self.current_job
        if local_busy:
            return True, local_current_job
        if self._has_pending_outbox():
            return True, "delivering-results"
        return False, None

    def _is_unavailable_for_new_jobs(self) -> bool:
        busy, _ = self._status_state()
        return busy

    def _outbox_path(self, job_id: str) -> Path:
        safe_job_id = str(job_id or "unknown").strip() or "unknown"
        return self.outbox_dir / f"{safe_job_id}.json"

    def _has_pending_outbox(self) -> bool:
        return any(self.outbox_dir.glob("*.json"))

    def _queue_result_payload(self, payload: Dict[str, Any]) -> None:
        job_id = str(payload.get("job_id") or "")
        if not job_id:
            return
        outbox_path = self._outbox_path(job_id)
        tmp_path = outbox_path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp_path.replace(outbox_path)

    def _iter_outbox_paths(self) -> List[Path]:
        return sorted(self.outbox_dir.glob("*.json"))

    def _deliver_result_payload(self, payload: Dict[str, Any]) -> bool:
        if not self.worker_id:
            return False
        payload["worker_id"] = self.worker_id
        status_code, body = _http_json(
            f"{self.central_url}/workers/{self.worker_id}/result",
            payload=payload,
            method="POST",
            timeout=3.0,
        )
        if status_code != 200:
            return False
        if isinstance(body, dict) and body.get("ack"):
            return True
        return False

    def _deliver_outbox_once(self) -> bool:
        delivered_any = False
        for payload_path in self._iter_outbox_paths():
            try:
                payload = json.loads(payload_path.read_text(encoding="utf-8"))
            except Exception:
                self.log.exception("Failed to read outbox payload %s", payload_path)
                try:
                    payload_path.unlink()
                except FileNotFoundError:
                    pass
                continue
            if not isinstance(payload, dict):
                self.log.warning("Ignoring malformed outbox payload %s", payload_path)
                try:
                    payload_path.unlink()
                except FileNotFoundError:
                    pass
                continue
            if not payload.get("worker_address"):
                payload["worker_address"] = self.worker_address
            if not self._deliver_result_payload(payload):
                break
            try:
                payload_path.unlink()
            except FileNotFoundError:
                pass
            delivered_any = True
        return delivered_any

    def _result_delivery_loop(self) -> None:
        while True:
            try:
                self._deliver_outbox_once()
            except Exception:
                self.log.exception("Unexpected error in result delivery loop")
            time.sleep(2)

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

    def _post_observer_event(self, job_id: str, event: Dict[str, Any]) -> None:
        if not self.worker_id or not job_id:
            return
        _http_json(
            f"{self.central_url}/workers/{self.worker_id}/events",
            payload={
                "job_id": job_id,
                "event": event,
                "worker_address": self.worker_address,
            },
            method="POST",
            timeout=1.5,
        )

    def _execute_job(self, job: JobInput) -> None:
        with self.lock:
            self.busy = True
            self.current_job = job.get("job_id")
        job_id = job.get("job_id")
        self.log.info(
            "Starting job %s uut=%s scripts_tree=%s framework=%s",
            self.current_job,
            job.get("uut_tree"),
            job.get("scripts_tree"),
            job.get("framework_version"),
        )
        try:
            try:
                result_payload = self.job_runner(
                    job,
                    artifacts_dir=str(self.artifacts_dir),
                    observer_callback=lambda event: self._post_observer_event(
                        str(job_id or ""),
                        event,
                    ),
                )
            except TypeError:
                try:
                    result_payload = self.job_runner(
                        job,
                        artifacts_dir=str(self.artifacts_dir),
                    )
                except TypeError:
                    result_payload = self.job_runner(job)
            result_payload = result_payload or {}
            artifacts = result_payload.get("artifacts", [])
            summary = result_payload.get("summary", result_payload)
            success = bool(result_payload.get("success", True))
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
        self._queue_result_payload(payload)
        self._deliver_outbox_once()
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
