from pathlib import Path

from automationv3.jobqueue import JobQueue
from automationv3.jobqueue.worker_system import CentralServer, WorkerServer
import automationv3.jobqueue.worker_system as worker_system


def _add_job(queue: JobQueue) -> str:
    report = queue.create_report("Reliability", "test")
    return str(
        queue.add_job(
            {
                "file": "tests/reliability.rst",
                "uut": "rig",
                "report_id": report["report_id"],
            }
        )
    )


def test_pending_result_lifecycle(tmp_path: Path) -> None:
    queue = JobQueue(db_path=str(tmp_path / "jobqueue.db"))
    job_id = _add_job(queue)

    queue.store_pending_result(
        job_id=job_id,
        result_data={"status": "completed"},
        success=True,
        worker_id="worker-1",
        worker_address="http://worker",
        artifacts_manifest=["result.json"],
        job_data_snapshot=queue.get_job(job_id),
    )

    pending = queue.get_pending_result(job_id)
    assert pending is not None
    assert pending["job_id"] == job_id
    assert pending["sync_attempts"] == 0
    assert pending["artifacts_manifest"] == ["result.json"]

    queue.mark_pending_result_error(job_id, "tree mismatch")
    pending = queue.get_pending_result(job_id)
    assert pending is not None
    assert pending["sync_attempts"] == 1
    assert pending["last_error"] == "tree mismatch"

    queue.delete_pending_result(job_id)
    assert queue.get_pending_result(job_id) is None


def test_receive_result_is_deferred_until_sync_finalization(tmp_path: Path) -> None:
    queue = JobQueue(db_path=str(tmp_path / "jobqueue.db"))
    job_id = _add_job(queue)
    central = CentralServer(
        queue=queue,
        start_background_threads=False,
        artifacts_dir=str(tmp_path / "central_artifacts"),
    )
    client = central.app.test_client()

    resp = client.post(
        "/workers/worker-1/result",
        json={
            "job_id": job_id,
            "success": True,
            "result": {"status": "completed"},
            "artifacts": [],
            "worker_address": "http://worker",
        },
    )
    assert resp.status_code == 200
    body = resp.get_json() or {}
    assert body.get("ack") is True
    assert body.get("pending_sync") is True

    assert queue.get_result(job_id) is None
    pending = queue.get_pending_result(job_id)
    assert pending is not None
    assert queue.get_job(job_id) is None

    ok, reason = central._sync_and_finalize_pending_result(pending)
    assert ok is True
    assert reason == ""
    assert queue.get_pending_result(job_id) is None

    result = queue.get_result(job_id)
    assert result is not None
    assert result["artifacts_downloaded"] is True


def test_sync_does_not_finalize_when_artifact_tree_mismatches(
    tmp_path: Path, monkeypatch
) -> None:
    queue = JobQueue(db_path=str(tmp_path / "jobqueue.db"))
    job_id = _add_job(queue)
    central = CentralServer(
        queue=queue,
        start_background_threads=False,
        artifacts_dir=str(tmp_path / "central_artifacts"),
    )

    queue.store_pending_result(
        job_id=job_id,
        result_data={"artifact_tree_sha": "expected-tree"},
        success=True,
        worker_id="worker-1",
        worker_address="http://worker",
        artifacts_manifest=["summary.txt"],
        job_data_snapshot=queue.get_job(job_id),
    )
    queue.remove_job(job_id)
    pending = queue.get_pending_result(job_id)
    assert pending is not None

    monkeypatch.setattr(
        central,
        "_fetch_worker_artifact_manifest",
        lambda worker_address, pending_job_id: {
            "tree_sha": "expected-tree",
            "files": [{"path": "summary.txt", "size": 10, "sha1": "abc"}],
        },
    )
    monkeypatch.setattr(central, "_local_file_needs_update", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(central, "_download_artifact", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        central, "_compute_local_artifact_tree_sha", lambda pending_job_id: "wrong-tree"
    )

    ok, reason = central._sync_and_finalize_pending_result(pending)
    assert ok is False
    assert "mismatch" in reason
    assert queue.get_result(job_id) is None
    assert queue.get_pending_result(job_id) is not None


def test_worker_outbox_retries_until_central_ack(tmp_path: Path, monkeypatch) -> None:
    class _NoStartThread:
        def __init__(self, *args, **kwargs):
            pass

        def start(self) -> None:
            return None

    monkeypatch.setattr(worker_system.threading, "Thread", _NoStartThread)

    worker = WorkerServer(
        central_url="http://central",
        worker_address="http://worker",
        job_runner=lambda job, **kwargs: {"summary": {"ok": True}, "artifacts": [], "success": True},
        artifacts_dir=str(tmp_path / "worker_artifacts"),
        worker_state_file=str(tmp_path / "worker_state.txt"),
    )
    worker.worker_id = "worker-1"

    payload = {
        "job_id": "job-1",
        "worker_id": worker.worker_id,
        "result": {"status": "completed"},
        "artifacts": [],
        "worker_address": worker.worker_address,
        "success": True,
    }
    worker._queue_result_payload(payload)

    busy, current_job = worker._status_state()
    assert busy is True
    assert current_job == "delivering-results"

    attempts = {"count": 0}

    def _fake_http_json(url: str, payload=None, method: str = "GET", timeout: float = 2.0):
        if method == "POST" and url.endswith("/workers/worker-1/result"):
            attempts["count"] += 1
            if attempts["count"] == 1:
                return 500, {"ack": False}
            return 200, {"ack": True}
        return 200, {"worker_id": "worker-1"}

    monkeypatch.setattr(worker_system, "_http_json", _fake_http_json)

    first = worker._deliver_outbox_once()
    assert first is False
    assert worker._has_pending_outbox() is True

    second = worker._deliver_outbox_once()
    assert second is True
    assert worker._has_pending_outbox() is False
