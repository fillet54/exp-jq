from pathlib import Path
import time

from automationv3.jobqueue import JobQueue
from automationv3.jobqueue.local_tui import (
    LocalTUIConfig,
    LocalTUIConfigStore,
    LocalWorkerRuntime,
    ScriptEntry,
    build_jobs_for_script,
    ensure_scratch_report,
    ensure_uut_config,
)
from automationv3.reporting import ReportingRepository, ReportingService, UUTConfig, UUTStore


def test_local_tui_config_round_trip(tmp_path: Path) -> None:
    config_path = tmp_path / "config" / "local_tui.json"
    store = LocalTUIConfigStore(config_path)

    empty = store.load()
    assert empty.scripts_root == ""
    assert empty.uut_path == ""
    assert empty.uut_name == "Local UUT"

    expected = LocalTUIConfig(
        scripts_root=str(tmp_path / "scripts"),
        uut_path=str(tmp_path / "uut"),
        uut_name="Bench-A",
    )
    store.save(expected)
    loaded = store.load()
    assert loaded == expected


def test_build_jobs_for_script_expands_variations(tmp_path: Path) -> None:
    scripts_root = tmp_path / "scripts"
    scripts_root.mkdir(parents=True)
    script_path = scripts_root / "variation_test.rst"
    script_path.write_text(
        "\n".join(
            [
                "Variation Test",
                "==============",
                "",
                ".. meta::",
                "   :requirements: ECSBOOT00001, ECSFDIR00011",
                "   :tags: smoke",
                "",
                ".. rvt::",
                "   :variation:",
                "",
                "   [[mode level]",
                "    [SAFE \"A\" 1]",
                "    [FAST \"B\" 2]]",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    entry = ScriptEntry(
        relpath="variation_test.rst",
        title="Variation Test",
        path=str(script_path),
        meta={"requirements": ["ECSBOOT00001", "ECSFDIR00011"], "tags": ["smoke"]},
    )
    uut = UUTConfig(uut_id="uut-1", name="Bench", path=str(tmp_path / "uut"), last_tree_sha="uut-sha")
    jobs = build_jobs_for_script(
        entry,
        report_id="report-1",
        uut_config=uut,
        scripts_root=scripts_root,
        scripts_tree="scripts-sha",
    )

    assert len(jobs) == 2
    assert all(job["is_variation_job"] for job in jobs)
    assert {job["variation_name"] for job in jobs} == {"SAFE", "FAST"}
    assert {job["variation_total"] for job in jobs} == {2}
    assert all(job["report_id"] == "report-1" for job in jobs)
    assert all(job["meta"]["requirements"] == ["ECSBOOT00001", "ECSFDIR00011"] for job in jobs)


def test_local_worker_runtime_executes_job_and_records_result(tmp_path: Path) -> None:
    db_path = str(tmp_path / "jobqueue.db")
    repository = ReportingRepository(db_path=db_path)
    queue = JobQueue(db_path=db_path, reporting_repository=repository)
    reporting = ReportingService(repository=repository, queue=queue)

    report = reporting.create_report(title="Scratch", description="", report_id="scratch-report")

    scripts_root = tmp_path / "scripts"
    scripts_root.mkdir(parents=True)
    script_path = scripts_root / "run_me.rst"
    script_path.write_text(
        "\n".join(
            [
                "Run Me",
                "======",
                "",
                ".. rvt::",
                "",
                "   (always-pass)",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    job_id = str(
        queue.add_job(
            {
                "file": "run_me.rst",
                "uut": "local-uut",
                "report_id": report["report_id"],
                "scripts_root": str(scripts_root),
            }
        )
    )
    runtime = LocalWorkerRuntime(queue, artifacts_dir=tmp_path / "artifacts")
    runtime.start()
    try:
        deadline = time.time() + 5.0
        while time.time() < deadline and queue.get_result(job_id) is None:
            time.sleep(0.05)
        result = queue.get_result(job_id)
    finally:
        runtime.stop()

    assert result is not None
    assert result["success"] is True
    result_data = result.get("result_data") or {}
    assert isinstance(result_data.get("observer_events"), list)
    assert queue.get_job(job_id) is None


def test_ensure_uut_and_scratch_helpers(tmp_path: Path) -> None:
    db_path = str(tmp_path / "jobqueue.db")
    repository = ReportingRepository(db_path=db_path)
    queue = JobQueue(db_path=db_path, reporting_repository=repository)
    reporting = ReportingService(repository=repository, queue=queue)
    uut_store = UUTStore(db_path=db_path, cache_dir=str(tmp_path / ".fscache"))

    scratch = ensure_scratch_report(reporting)
    assert scratch["report_id"] == "__scratch__"
    assert reporting.get_report("__scratch__") is not None

    uut_path = tmp_path / "uut"
    uut_path.mkdir(parents=True)
    first = ensure_uut_config(uut_store, str(uut_path), uut_name="Bench-X")
    second = ensure_uut_config(uut_store, str(uut_path), uut_name="Bench-X")
    assert first.uut_id == second.uut_id
