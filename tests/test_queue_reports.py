from __future__ import annotations

from pathlib import Path

import automationv3.jobqueue.views as queue_views
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


def test_report_starts_with_no_requirements(tmp_path: Path, monkeypatch) -> None:
    client, queue, _scripts_root, _uut_id, report_id = _build_client(tmp_path, monkeypatch)

    assert queue.list_report_requirements(report_id) == []

    page = client.get(f"/reports/{report_id}")
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    assert "No requirements have been added to this report." in body


def test_report_can_add_and_remove_requirement_explicitly(tmp_path: Path, monkeypatch) -> None:
    client, queue, _scripts_root, _uut_id, report_id = _build_client(tmp_path, monkeypatch)

    add_resp = client.post(
        f"/reports/{report_id}/requirements/add",
        data={"requirement_id": "ECSBOOT00001"},
        follow_redirects=False,
    )
    assert add_resp.status_code == 303
    assert [row["requirement_id"] for row in queue.list_report_requirements(report_id)] == [
        "ECSBOOT00001"
    ]

    remove_resp = client.post(
        f"/reports/{report_id}/requirements/remove",
        data={"requirement_id": "ECSBOOT00001"},
        follow_redirects=False,
    )
    assert remove_resp.status_code == 303
    assert queue.list_report_requirements(report_id) == []


def test_queueing_script_auto_adds_report_requirements(tmp_path: Path, monkeypatch) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    _make_rst(
        scripts_root / "alpha.rst",
        "Alpha",
        requirements=["ECSBOOT00001", "ECSCTRL00005"],
    )

    resp = client.post(
        "/jobs/from_scripts",
        data={
            "base_path": str(scripts_root),
            "uut_id": uut_id,
            "report_id": report_id,
            "script_paths": "alpha.rst",
            "return_to": "/scripts",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    requirement_ids = [row["requirement_id"] for row in queue.list_report_requirements(report_id)]
    assert requirement_ids == ["ECSBOOT00001", "ECSCTRL00005"]


def test_queue_from_scripts_expands_variation_jobs(tmp_path: Path, monkeypatch) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    script = scripts_root / "variation.rst"
    script.write_text(
        (
            "Variation Demo\n"
            "==============\n\n"
            ".. meta::\n"
            "   :requirements: ECSBOOT00001\n\n"
            ".. rvt::\n"
            "   :variation:\n\n"
            "   [[mode fail-prob]\n"
            "    [\"nominal\" \"nominal\" 0]\n"
            "    [\"safe\" \"safe\" 0]]\n\n"
            ".. rvt::\n\n"
            "   (random-fail fail-prob)\n"
        ),
        encoding="utf-8",
    )

    resp = client.post(
        "/jobs/from_scripts",
        data={
            "base_path": str(scripts_root),
            "uut_id": uut_id,
            "report_id": report_id,
            "script_paths": "variation.rst",
            "return_to": "/scripts",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    jobs = queue.list_jobs()
    assert len(jobs) == 2
    assert {job.get("variation_name") for job in jobs} == {"nominal", "safe"}
    assert all(bool(job.get("is_variation_job")) for job in jobs)
    assert all(int(job.get("variation_total") or 0) == 2 for job in jobs)
    assert all(isinstance(job.get("variation_bindings"), dict) for job in jobs)


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
    assert "Requirement Groups" in body
    assert "ECSBOOT00001" in body
    assert "ECSCTRL00005" in body
    assert "No Requirement Declared" not in body


def test_report_requirement_row_shows_variation_status_blocks(tmp_path: Path, monkeypatch) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    script = scripts_root / "variation.rst"
    script.write_text(
        (
            "Variation Demo\n"
            "==============\n\n"
            ".. meta::\n"
            "   :requirements: ECSBOOT00001\n\n"
            ".. rvt::\n"
            "   :variation:\n\n"
            "   [[mode]\n"
            "    [\"nominal\" \"nominal\"]\n"
            "    [\"safe\" \"safe\"]]\n\n"
            ".. rvt::\n\n"
            "   (always-pass)\n"
        ),
        encoding="utf-8",
    )

    enqueue = client.post(
        "/jobs/from_scripts",
        data={
            "base_path": str(scripts_root),
            "uut_id": uut_id,
            "report_id": report_id,
            "script_paths": "variation.rst",
            "return_to": "/scripts",
        },
        follow_redirects=False,
    )
    assert enqueue.status_code == 303

    jobs = queue.list_jobs()
    by_variation = {str(job.get("variation_name")): job for job in jobs}
    assert set(by_variation.keys()) == {"nominal", "safe"}

    queue.record_result(
        job_id=by_variation["nominal"]["job_id"],
        result_data={"status": "ok"},
        success=True,
        worker_id="worker-1",
        worker_address="http://worker-1",
    )
    queue.remove_job(by_variation["nominal"]["job_id"])

    queue.record_result(
        job_id=by_variation["safe"]["job_id"],
        result_data={"status": "ok"},
        success=False,
        worker_id="worker-1",
        worker_address="http://worker-1",
    )
    queue.remove_job(by_variation["safe"]["job_id"])

    page = client.get(f"/reports/{report_id}?view=requirement")
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    assert "ECSBOOT00001" in body
    assert "aria-label=\"nominal PASS\"" in body
    assert "aria-label=\"safe FAIL\"" in body
    assert "REQ FAIL" in body


def test_report_detail_can_requeue_all_tests(tmp_path: Path, monkeypatch) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    _make_rst(scripts_root / "alpha.rst", "Alpha", requirements=["ECSBOOT00001"])
    _make_rst(scripts_root / "beta.rst", "Beta", requirements=["ECSBOOT00001"])

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
            "requirement_id": "ECSCTRL00005",
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
    _make_rst(scripts_root / "alpha.rst", "Alpha", requirements=["ECSBOOT00001"])
    _make_rst(scripts_root / "beta.rst", "Beta", requirements=["ECSBOOT00001"])

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


def test_report_requirement_group_status_pass_fail_partial(
    tmp_path: Path, monkeypatch
) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    _make_rst(
        scripts_root / "alpha.rst",
        "Alpha",
        requirements=["ECSBOOT00001", "ECSNAVG00010"],
    )
    _make_rst(
        scripts_root / "beta.rst",
        "Beta",
        requirements=["ECSBOOT00001"],
    )
    _make_rst(
        scripts_root / "gamma.rst",
        "Gamma",
        requirements=["ECSCTRL00005", "ECSNAVG00010"],
    )

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

    jobs = queue.list_jobs()
    by_file = {job["file"]: job for job in jobs}
    queue.record_result(
        job_id=by_file["alpha.rst"]["job_id"],
        result_data={"status": "ok"},
        success=True,
        worker_id="worker-1",
        worker_address="http://worker-1",
    )
    queue.remove_job(by_file["alpha.rst"]["job_id"])
    queue.record_result(
        job_id=by_file["beta.rst"]["job_id"],
        result_data={"status": "ok"},
        success=True,
        worker_id="worker-1",
        worker_address="http://worker-1",
    )
    queue.remove_job(by_file["beta.rst"]["job_id"])
    queue.record_result(
        job_id=by_file["gamma.rst"]["job_id"],
        result_data={"status": "ok"},
        success=False,
        worker_id="worker-1",
        worker_address="http://worker-1",
    )
    queue.remove_job(by_file["gamma.rst"]["job_id"])

    page = client.get(f"/reports/{report_id}?view=requirement")
    assert page.status_code == 200
    body = page.get_data(as_text=True)

    assert "ECSBOOT00001" in body
    assert "ECSCTRL00005" in body
    assert "ECSNAVG00010" in body
    assert "REQ PASS" in body
    assert "REQ FAIL" in body
    assert "REQ PARTIAL" not in body
    assert "Latest scripts: 2/2 passing" in body
    assert "Latest scripts: 0/1 passing" in body
    assert "Latest scripts: 1/2 passing" in body


def test_report_requirement_partial_when_other_tracked_scripts_not_run(
    tmp_path: Path, monkeypatch
) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    _make_rst(
        scripts_root / "alpha.rst",
        "Alpha",
        requirements=["ECSBOOT00001"],
    )
    _make_rst(
        scripts_root / "beta.rst",
        "Beta",
        requirements=["ECSBOOT00001"],
    )
    _make_rst(
        scripts_root / "gamma.rst",
        "Gamma",
        requirements=["ECSBOOT00001"],
    )

    enqueue = client.post(
        "/jobs/from_scripts",
        data={
            "base_path": str(scripts_root),
            "uut_id": uut_id,
            "report_id": report_id,
            "script_paths": "alpha.rst",
            "return_to": "/scripts",
        },
        follow_redirects=False,
    )
    assert enqueue.status_code == 303

    jobs = queue.list_jobs()
    by_file = {job["file"]: job for job in jobs}
    assert set(by_file.keys()) == {"alpha.rst"}
    queue.record_result(
        job_id=by_file["alpha.rst"]["job_id"],
        result_data={"status": "ok"},
        success=True,
        worker_id="worker-1",
        worker_address="http://worker-1",
    )
    queue.remove_job(by_file["alpha.rst"]["job_id"])

    page = client.get(f"/reports/{report_id}?view=requirement")
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    assert "ECSBOOT00001" in body
    assert "REQ PARTIAL" in body
    assert "Latest scripts: 1/3 passing" in body


def test_report_export_page_includes_summary_toc_and_latest_script_rows(
    tmp_path: Path, monkeypatch
) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    _make_rst(
        scripts_root / "alpha.rst",
        "Alpha",
        requirements=["ECSBOOT00001"],
    )
    _make_rst(
        scripts_root / "beta.rst",
        "Beta",
        requirements=["ECSBOOT00001"],
    )

    enqueue = client.post(
        "/jobs/from_scripts",
        data={
            "base_path": str(scripts_root),
            "uut_id": uut_id,
            "report_id": report_id,
            "script_paths": "alpha.rst",
            "return_to": "/scripts",
        },
        follow_redirects=False,
    )
    assert enqueue.status_code == 303

    jobs = queue.list_jobs()
    assert len(jobs) == 1
    queue.record_result(
        job_id=jobs[0]["job_id"],
        result_data={"status": "ok"},
        success=True,
        worker_id="worker-1",
        worker_address="http://worker-1",
    )
    queue.remove_job(jobs[0]["job_id"])

    detail_page = client.get(f"/reports/{report_id}")
    assert detail_page.status_code == 200
    detail_body = detail_page.get_data(as_text=True)
    assert "Export PDF" in detail_body
    assert f"/reports/{report_id}/export.pdf" in detail_body

    export_page = client.get(f"/reports/{report_id}/export")
    assert export_page.status_code == 200
    export_body = export_page.get_data(as_text=True)
    assert "Report Export" in export_body
    assert "Table of Contents" in export_body
    assert "Latest Script Results" in export_body
    assert "ECSBOOT00001" in export_body
    assert "alpha.rst" in export_body
    assert "beta.rst" in export_body
    assert "PASS" in export_body
    assert "NOT RUN" in export_body


def test_report_export_pdf_route_returns_pdf(tmp_path: Path, monkeypatch) -> None:
    client, queue, scripts_root, uut_id, report_id = _build_client(tmp_path, monkeypatch)
    _make_rst(scripts_root / "alpha.rst", "Alpha", requirements=["ECSBOOT00001"])
    enqueue = client.post(
        "/jobs/from_scripts",
        data={
            "base_path": str(scripts_root),
            "uut_id": uut_id,
            "report_id": report_id,
            "script_paths": "alpha.rst",
            "return_to": "/scripts",
        },
        follow_redirects=False,
    )
    assert enqueue.status_code == 303

    jobs = queue.list_jobs()
    assert len(jobs) == 1
    queue.record_result(
        job_id=jobs[0]["job_id"],
        result_data={"status": "ok"},
        success=True,
        worker_id="worker-1",
        worker_address="http://worker-1",
    )
    queue.remove_job(jobs[0]["job_id"])

    publish_calls = {}
    monkeypatch.setattr(queue_views.shutil, "which", lambda cmd: "/usr/bin/pdflatex")

    def _fake_publish_string(source, writer_name, settings_overrides=None):
        publish_calls["source"] = source
        publish_calls["writer_name"] = writer_name
        publish_calls["settings_overrides"] = settings_overrides or {}
        return "\\documentclass{article}\n\\begin{document}\nMock PDF\n\\end{document}\n"

    monkeypatch.setattr(queue_views.docutils.core, "publish_string", _fake_publish_string)

    class _Proc:
        returncode = 0
        stdout = ""
        stderr = ""

    def _fake_run(cmd, stdout=None, stderr=None, text=None, check=None):
        out_dir = Path(cmd[cmd.index("-output-directory") + 1])
        (out_dir / "report.pdf").write_bytes(b"%PDF-1.4\n%mock\n")
        return _Proc()

    monkeypatch.setattr(queue_views.subprocess, "run", _fake_run)

    resp = client.get(f"/reports/{report_id}/export.pdf")
    assert resp.status_code == 200
    assert resp.mimetype == "application/pdf"
    assert resp.get_data().startswith(b"%PDF-1.4")
    assert publish_calls["writer_name"] == "latex"
    rst_source = str(publish_calls.get("source") or "")
    assert "System Summary" in rst_source
    assert "Passing" in rst_source
    assert "Partial" in rst_source
    assert "Failing" in rst_source
    assert "Untested" in rst_source
    assert "Passing Scripts" not in rst_source
    latex_preamble = str((publish_calls["settings_overrides"] or {}).get("latex_preamble") or "")
    assert "\\cfoot{\\thepage}" in latex_preamble
    assert "Report: \\texttt{" in latex_preamble
    assert report_id in latex_preamble
    assert "Last run:" in latex_preamble
    assert "UTC" in latex_preamble


def test_report_export_pdf_route_returns_503_when_pdflatex_missing(
    tmp_path: Path, monkeypatch
) -> None:
    client, _queue, _scripts_root, _uut_id, report_id = _build_client(tmp_path, monkeypatch)
    monkeypatch.setattr(queue_views.shutil, "which", lambda cmd: None)
    resp = client.get(f"/reports/{report_id}/export.pdf")
    assert resp.status_code == 503
    assert "pdflatex not found" in resp.get_data(as_text=True)


def test_delete_report_removes_report_and_associated_jobs(tmp_path: Path, monkeypatch) -> None:
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
    by_file = {job["file"]: job for job in jobs}

    queue.record_result(
        job_id=by_file["alpha.rst"]["job_id"],
        result_data={"status": "ok"},
        success=True,
        worker_id="worker-1",
        worker_address="http://worker-1",
    )
    queue.remove_job(by_file["alpha.rst"]["job_id"])

    queue.store_pending_result(
        job_id="pending-report-job",
        result_data={"status": "pending"},
        success=False,
        worker_id="worker-1",
        worker_address="http://worker-1",
        artifacts_manifest=[],
        job_data_snapshot={
            "file": "alpha.rst",
            "uut": "HW Rig",
            "report_id": report_id,
        },
    )

    detail = client.get(f"/reports/{report_id}")
    assert detail.status_code == 200
    detail_body = detail.get_data(as_text=True)
    assert "Delete Report" in detail_body

    reports_page = client.get("/reports")
    assert reports_page.status_code == 200
    reports_body = reports_page.get_data(as_text=True)
    assert "Delete" in reports_body

    delete_resp = client.post(
        f"/reports/{report_id}/delete",
        data={"return_to": "/reports"},
        follow_redirects=False,
    )
    assert delete_resp.status_code == 303

    assert queue.get_report(report_id) is None
    assert queue.list_report_scripts(report_id) == []
    assert all((job.get("report_id") != report_id) for job in queue.list_jobs())
    assert all(
        ((row.get("job_data") or {}).get("report_id") != report_id)
        for row in queue.list_results(limit=200)
    )
    assert all(
        ((row.get("job_data") or {}).get("report_id") != report_id)
        for row in queue.list_pending_results()
    )
