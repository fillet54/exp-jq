from __future__ import annotations

from pathlib import Path

from automationv3.frontend import create_app
from automationv3.jobqueue import JobQueue, UUTStore


def _make_rst(path: Path, title: str, requirements: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    meta_block = ""
    if requirements:
        meta_block = (
            ".. meta::\n"
            f"   :requirements: {', '.join(requirements)}\n\n"
        )
    path.write_text(
        f"{title}\n{'=' * len(title)}\n\n{meta_block}Body.\n",
        encoding="utf-8",
    )


def _build_client(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "jobqueue.db"
    scripts_root = tmp_path / "scripts"
    suites_dir = tmp_path / "suites"
    cache_dir = tmp_path / ".fscache_scripts"
    uut_root = tmp_path / "uut"
    scripts_root.mkdir(parents=True, exist_ok=True)
    suites_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    uut_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("JOBQUEUE_DB", str(db_path))
    monkeypatch.setenv("SCRIPT_ROOT", str(scripts_root))
    monkeypatch.setenv("SUITES_DIR", str(suites_dir))
    monkeypatch.setenv("SCRIPT_CACHE_DIR", str(cache_dir))
    monkeypatch.setenv("JOBQUEUE_DOCS_ENABLED", "0")

    app = create_app()
    app.testing = True
    client = app.test_client()

    add_uut = client.post(
        "/uuts",
        data={
            "name": "HW Rig",
            "path": str(uut_root),
        },
    )
    assert add_uut.status_code == 200

    uut_store = UUTStore(db_path=str(db_path))
    uuts = uut_store.list()
    assert uuts
    uut_id = uuts[0].uut_id

    create_report_resp = client.post(
        "/reports",
        data={"title": "Regression Report", "description": "batch"},
        follow_redirects=False,
    )
    assert create_report_resp.status_code == 303

    queue = JobQueue(db_path=str(db_path))
    report_records = queue.list_reports(limit=10)
    assert report_records
    report_id = report_records[0]["report_id"]

    return client, queue, scripts_root, uut_id, report_id


def test_queue_from_scripts_keeps_single_selected_report(tmp_path: Path, monkeypatch) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    _make_rst(scripts_root / "alpha.rst", "Alpha")
    _make_rst(scripts_root / "beta.rst", "Beta")

    resp = client.post(
        "/jobs/from_scripts",
        data={
            "base_path": str(scripts_root),
            "uut_id": uut_id,
            "report_id": report_id,
            "script_paths": "alpha.rst\nbeta.rst",
            "return_to": "/scripts",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    jobs = queue.list_jobs()
    assert len(jobs) == 2
    assert {job["report_id"] for job in jobs} == {report_id}


def test_queue_from_suite_keeps_single_selected_report(tmp_path: Path, monkeypatch) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    _make_rst(scripts_root / "suite" / "one.rst", "One")
    _make_rst(scripts_root / "suite" / "two.rst", "Two")

    create_suite = client.post("/suites", data={"name": "Smoke"})
    assert create_suite.status_code == 200
    add_one = client.post("/suites/Smoke/add_script", data={"script_path": "suite/one.rst"})
    add_two = client.post("/suites/Smoke/add_script", data={"script_path": "suite/two.rst"})
    assert add_one.status_code == 200
    assert add_two.status_code == 200

    resp = client.post(
        "/jobs/from_suite",
        data={
            "suite_name": "Smoke",
            "uut_id": uut_id,
            "report_id": report_id,
            "base_path": str(scripts_root),
        },
        follow_redirects=False,
    )
    assert resp.status_code == 200

    jobs = queue.list_jobs()
    assert len(jobs) == 2
    assert {job["report_id"] for job in jobs} == {report_id}


def test_report_detail_can_group_completed_jobs_by_requirement(tmp_path: Path, monkeypatch) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    _make_rst(
        scripts_root / "alpha.rst",
        "Alpha",
        requirements=["ECSBOOT00001", "ECSCTRL00005"],
    )
    _make_rst(
        scripts_root / "beta.rst",
        "Beta",
        requirements=["ECSBOOT00001"],
    )
    _make_rst(scripts_root / "gamma.rst", "Gamma")

    resp = client.post(
        "/jobs/from_scripts",
        data={
            "base_path": str(scripts_root),
            "uut_id": uut_id,
            "report_id": report_id,
            "script_paths": "alpha.rst\nbeta.rst\ngamma.rst",
            "return_to": "/scripts",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    jobs = queue.list_jobs()
    assert len(jobs) == 3
    for idx, job in enumerate(jobs):
        queue.record_result(
            job_id=job["job_id"],
            result_data={"status": "ok"},
            success=(idx % 2 == 0),
            worker_id="worker-1",
            worker_address="http://worker-1",
        )
        queue.remove_job(job["job_id"])

    page = client.get(f"/reports/{report_id}?view=requirement")
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    assert "Completed Jobs by Requirement" in body
    assert "By Requirement" in body
    assert "ECSBOOT00001" in body
    assert "ECSCTRL00005" in body
    assert "No Requirement Declared" in body


def test_report_detail_can_requeue_all_tests(tmp_path: Path, monkeypatch) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    _make_rst(scripts_root / "alpha.rst", "Alpha")
    _make_rst(scripts_root / "beta.rst", "Beta")

    enqueue = client.post(
        "/jobs/from_scripts",
        data={
            "base_path": str(scripts_root),
            "uut_id": uut_id,
            "report_id": report_id,
            "script_paths": "alpha.rst\nbeta.rst",
            "return_to": "/scripts",
        },
        follow_redirects=False,
    )
    assert enqueue.status_code == 303

    original_jobs = queue.list_jobs()
    assert len(original_jobs) == 2
    for job in original_jobs:
        queue.record_result(
            job_id=job["job_id"],
            result_data={"status": "ok"},
            success=True,
            worker_id="worker-1",
            worker_address="http://worker-1",
        )
        queue.remove_job(job["job_id"])

    requeue = client.post(
        f"/reports/{report_id}/requeue_all",
        data={"report_view": "script"},
        follow_redirects=False,
    )
    assert requeue.status_code == 303

    queued = queue.list_jobs()
    assert len(queued) == 2
    assert {str(job.get("file")) for job in queued} == {"alpha.rst", "beta.rst"}


def test_report_detail_can_requeue_single_script(tmp_path: Path, monkeypatch) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    _make_rst(scripts_root / "alpha.rst", "Alpha")
    _make_rst(scripts_root / "beta.rst", "Beta")

    enqueue = client.post(
        "/jobs/from_scripts",
        data={
            "base_path": str(scripts_root),
            "uut_id": uut_id,
            "report_id": report_id,
            "script_paths": "alpha.rst\nbeta.rst",
            "return_to": "/scripts",
        },
        follow_redirects=False,
    )
    assert enqueue.status_code == 303

    original_jobs = queue.list_jobs()
    assert len(original_jobs) == 2
    for job in original_jobs:
        queue.record_result(
            job_id=job["job_id"],
            result_data={"status": "ok"},
            success=True,
            worker_id="worker-1",
            worker_address="http://worker-1",
        )
        queue.remove_job(job["job_id"])

    requeue = client.post(
        f"/reports/{report_id}/requeue_script",
        data={"report_view": "script", "script_path": "beta.rst"},
        follow_redirects=False,
    )
    assert requeue.status_code == 303

    queued = queue.list_jobs()
    assert len(queued) == 1
    assert queued[0]["file"] == "beta.rst"


def test_report_detail_can_requeue_requirement_scripts(tmp_path: Path, monkeypatch) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    _make_rst(
        scripts_root / "alpha.rst",
        "Alpha",
        requirements=["ECSBOOT00001", "ECSCTRL00005"],
    )
    _make_rst(
        scripts_root / "beta.rst",
        "Beta",
        requirements=["ECSBOOT00001"],
    )
    _make_rst(scripts_root / "gamma.rst", "Gamma", requirements=["ECSCTRL00005"])

    enqueue = client.post(
        "/jobs/from_scripts",
        data={
            "base_path": str(scripts_root),
            "uut_id": uut_id,
            "report_id": report_id,
            "script_paths": "alpha.rst\nbeta.rst\ngamma.rst",
            "return_to": "/scripts",
        },
        follow_redirects=False,
    )
    assert enqueue.status_code == 303

    original_jobs = queue.list_jobs()
    assert len(original_jobs) == 3
    for job in original_jobs:
        queue.record_result(
            job_id=job["job_id"],
            result_data={"status": "ok"},
            success=True,
            worker_id="worker-1",
            worker_address="http://worker-1",
        )
        queue.remove_job(job["job_id"])

    requeue = client.post(
        f"/reports/{report_id}/requeue_requirement",
        data={
            "report_view": "requirement",
            "script_paths": "alpha.rst\ngamma.rst",
        },
        follow_redirects=False,
    )
    assert requeue.status_code == 303

    queued = queue.list_jobs()
    assert len(queued) == 2
    assert {str(job.get("file")) for job in queued} == {"alpha.rst", "gamma.rst"}


def test_report_clear_results_keeps_tracked_scripts_for_rerun(
    tmp_path: Path, monkeypatch
) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    _make_rst(scripts_root / "alpha.rst", "Alpha")
    _make_rst(scripts_root / "beta.rst", "Beta")

    enqueue = client.post(
        "/jobs/from_scripts",
        data={
            "base_path": str(scripts_root),
            "uut_id": uut_id,
            "report_id": report_id,
            "script_paths": "alpha.rst\nbeta.rst",
            "return_to": "/scripts",
        },
        follow_redirects=False,
    )
    assert enqueue.status_code == 303

    original_jobs = queue.list_jobs()
    for job in original_jobs:
        queue.record_result(
            job_id=job["job_id"],
            result_data={"status": "ok"},
            success=True,
            worker_id="worker-1",
            worker_address="http://worker-1",
        )
        queue.remove_job(job["job_id"])

    clear_resp = client.post(
        f"/reports/{report_id}/clear_results",
        data={"report_view": "script"},
        follow_redirects=False,
    )
    assert clear_resp.status_code == 303
    assert queue.list_results(limit=100) == []

    tracked = queue.list_report_scripts(report_id)
    assert {row["script_path"] for row in tracked} == {"alpha.rst", "beta.rst"}

    requeue = client.post(
        f"/reports/{report_id}/requeue_all",
        data={"report_view": "script"},
        follow_redirects=False,
    )
    assert requeue.status_code == 303

    queued = queue.list_jobs()
    assert len(queued) == 2
    assert {job["file"] for job in queued} == {"alpha.rst", "beta.rst"}


def test_report_remove_script_wholly_removes_reference_and_runs(
    tmp_path: Path, monkeypatch
) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    _make_rst(scripts_root / "alpha.rst", "Alpha")
    _make_rst(scripts_root / "beta.rst", "Beta")

    enqueue = client.post(
        "/jobs/from_scripts",
        data={
            "base_path": str(scripts_root),
            "uut_id": uut_id,
            "report_id": report_id,
            "script_paths": "alpha.rst\nbeta.rst",
            "return_to": "/scripts",
        },
        follow_redirects=False,
    )
    assert enqueue.status_code == 303

    jobs = queue.list_jobs()
    assert len(jobs) == 2
    alpha_job = next(job for job in jobs if job["file"] == "alpha.rst")
    beta_job = next(job for job in jobs if job["file"] == "beta.rst")

    queue.record_result(
        job_id=alpha_job["job_id"],
        result_data={"status": "ok"},
        success=True,
        worker_id="worker-1",
        worker_address="http://worker-1",
    )
    queue.remove_job(alpha_job["job_id"])

    remove_resp = client.post(
        f"/reports/{report_id}/scripts/remove",
        data={"report_view": "script", "script_path": "alpha.rst"},
        follow_redirects=False,
    )
    assert remove_resp.status_code == 303

    tracked = queue.list_report_scripts(report_id)
    assert {row["script_path"] for row in tracked} == {"beta.rst"}

    remaining_jobs = queue.list_jobs()
    assert len(remaining_jobs) == 1
    assert remaining_jobs[0]["job_id"] == beta_job["job_id"]
    assert remaining_jobs[0]["file"] == "beta.rst"

    remaining_results = queue.list_results(limit=100)
    assert all((res.get("job_data") or {}).get("file") != "alpha.rst" for res in remaining_results)
