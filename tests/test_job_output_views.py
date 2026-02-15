from pathlib import Path

from automationv3.frontend import create_app
from automationv3.jobqueue import JobQueue


def _configure_env(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "jobqueue.db"
    scripts_root = tmp_path / "scripts"
    suites_dir = tmp_path / "suites"
    cache_dir = tmp_path / ".fscache_scripts"
    scripts_root.mkdir(parents=True, exist_ok=True)
    suites_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("JOBQUEUE_DB", str(db_path))
    monkeypatch.setenv("SCRIPT_ROOT", str(scripts_root))
    monkeypatch.setenv("SUITES_DIR", str(suites_dir))
    monkeypatch.setenv("SCRIPT_CACHE_DIR", str(cache_dir))
    monkeypatch.setenv("JOBQUEUE_DOCS_ENABLED", "0")
    return db_path


def test_job_output_page_renders_saved_result_document(tmp_path: Path, monkeypatch) -> None:
    db_path = _configure_env(tmp_path, monkeypatch)
    app = create_app()
    app.testing = True
    client = app.test_client()

    queue = JobQueue(db_path=str(db_path))
    report = queue.create_report("Output Report", "desc")
    job_id = queue.add_job(
        {
            "file": "tests/demo.rst",
            "uut": "Rig-1",
            "report_id": report["report_id"],
        }
    )
    queue.record_result(
        job_id=job_id,
        result_data={
            "result_document": (
                "Results\n=======\n\n"
                ".. rvt-result::\n"
                "   :status: pass\n\n"
                "   (always-pass)\n"
            ),
            "observer_events": [],
        },
        success=True,
        worker_id="w1",
        worker_address="http://worker",
    )
    queue.remove_job(job_id)

    resp = client.get(f"/jobs/{job_id}/output")

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Job Output" in body
    assert "always-pass" in body
    assert "PASS" in body


def test_job_output_page_missing_job_returns_404(tmp_path: Path, monkeypatch) -> None:
    _configure_env(tmp_path, monkeypatch)
    app = create_app()
    app.testing = True
    client = app.test_client()

    resp = client.get("/jobs/missing-job/output")

    assert resp.status_code == 404


def test_job_output_page_renders_legacy_result_payload(tmp_path: Path, monkeypatch) -> None:
    db_path = _configure_env(tmp_path, monkeypatch)
    app = create_app()
    app.testing = True
    client = app.test_client()

    queue = JobQueue(db_path=str(db_path))
    report = queue.create_report("Legacy Report", "")
    job_id = queue.add_job(
        {
            "file": "legacy/example.rst",
            "uut": "Rig-1",
            "report_id": report["report_id"],
        }
    )
    queue.record_result(
        job_id=job_id,
        result_data={
            "status": "completed",
            "output": "legacy payload",
            "rvt": {
                "passed": False,
                "results": [
                    {
                        "form": "(always-fail)",
                        "passed": False,
                        "result": "<BlockResult: FAIL, , always-fail>",
                    }
                ],
                "invocations": [],
            },
        },
        success=False,
        worker_id="w1",
        worker_address="http://worker",
    )
    queue.remove_job(job_id)

    resp = client.get(f"/jobs/{job_id}/output")

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "always-fail" in body
    assert "FAIL" in body


def test_job_output_page_legacy_payload_emits_per_block_results(tmp_path: Path, monkeypatch) -> None:
    db_path = _configure_env(tmp_path, monkeypatch)
    app = create_app()
    app.testing = True
    client = app.test_client()

    queue = JobQueue(db_path=str(db_path))
    report = queue.create_report("Legacy Report", "")
    job_id = queue.add_job(
        {
            "file": "legacy/multi.rst",
            "uut": "Rig-1",
            "report_id": report["report_id"],
        }
    )
    queue.record_result(
        job_id=job_id,
        result_data={
            "status": "completed",
            "output": "legacy payload",
            "rvt": {
                "passed": False,
                "results": [],
                "invocations": [
                    {
                        "block": "always-pass",
                        "args": [],
                        "passed": True,
                        "result": "<BlockResult: PASS, always-pass, >",
                    },
                    {
                        "block": "always-fail",
                        "args": [],
                        "passed": False,
                        "result": "<BlockResult: FAIL, , always-fail>",
                    },
                    {
                        "block": "always-pass",
                        "args": [],
                        "passed": True,
                        "result": "<BlockResult: PASS, always-pass, >",
                    },
                ],
            },
        },
        success=False,
        worker_id="w1",
        worker_address="http://worker",
    )
    queue.remove_job(job_id)

    resp = client.get(f"/jobs/{job_id}/output")

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert body.count('class="rvt-block rvt-result-block') == 3
    assert body.count("PASS") >= 2
    assert "FAIL" in body
