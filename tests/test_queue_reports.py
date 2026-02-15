from __future__ import annotations

from pathlib import Path

from automationv3.frontend import create_app
from automationv3.jobqueue import JobQueue, UUTStore


def _make_rst(path: Path, title: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{title}\n{'=' * len(title)}\n\nBody.\n", encoding="utf-8")


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
