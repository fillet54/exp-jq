from pathlib import Path
import time

from automationv3.jobqueue import JobQueue
from automationv3.jobqueue.local_tui import (
    LocalTUIConfig,
    LocalTUIConfigStore,
    LocalWorkerRuntime,
    LocalAutomationTUI,
    ScriptEntry,
    build_jobs_for_script,
    ensure_scratch_report,
    ensure_uut_config,
)
from automationv3.reporting import ReportingRepository, ReportingService, UUTConfig, UUTStore


def test_local_tui_config_round_trip(tmp_path: Path) -> None:
    config_path = tmp_path / "config" / "local_tui.toml"
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
    raw = config_path.read_text(encoding="utf-8")
    assert 'scripts_root = "' in raw
    assert 'uut_path = "' in raw
    assert 'uut_name = "' in raw
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


def test_resolve_next_pending_focus_advances_after_current(tmp_path: Path, monkeypatch) -> None:
    app = LocalAutomationTUI(
        db_path=str(tmp_path / "jobqueue.db"),
        config_path=tmp_path / "local_tui.toml",
        artifacts_dir=str(tmp_path / "artifacts"),
        cache_dir=str(tmp_path / ".fscache"),
    )
    app._current_job_ids = ["job-a", "job-b", "job-c"]

    result_map = {
        "job-a": {"job_id": "job-a"},  # completed
        "job-b": None,  # pending
        "job-c": None,  # pending
    }
    monkeypatch.setattr(app.queue, "get_result", lambda job_id: result_map.get(job_id))
    monkeypatch.setattr(app.runtime, "status", lambda: {"busy": True, "current_job_id": "job-c"})

    # Prefer currently executing pending job.
    assert app._resolve_next_pending_focus(after_job_id="job-a") == "job-c"

    # If no active runtime job, move to next pending in sequence.
    monkeypatch.setattr(app.runtime, "status", lambda: {"busy": False, "current_job_id": None})
    assert app._resolve_next_pending_focus(after_job_id="job-a") == "job-b"


def test_render_rst_fragment_meta_as_two_column_table(tmp_path: Path) -> None:
    app = LocalAutomationTUI(
        db_path=str(tmp_path / "jobqueue.db"),
        config_path=tmp_path / "local_tui.toml",
        artifacts_dir=str(tmp_path / "artifacts"),
        cache_dir=str(tmp_path / ".fscache"),
    )
    fragment = "\n".join(
        [
            ".. meta::",
            "   :requirements: ECSBOOT00001, ECSFDIR00011",
            "   :tags: smoke, regression",
            "",
            "Body text.",
        ]
    )
    lines = app._render_rst_fragment_lines(fragment)
    rendered = "\n".join(lines)
    assert ".. meta::" not in rendered
    assert "| Key" in rendered
    assert "| Value" in rendered
    assert "requirements" in rendered
    assert "ECSBOOT00001, ECSFDIR00011" in rendered
    assert "tags" in rendered
    assert "smoke, regression" in rendered


def test_render_rst_fragment_header_gets_preceding_blank_line(tmp_path: Path) -> None:
    app = LocalAutomationTUI(
        db_path=str(tmp_path / "jobqueue.db"),
        config_path=tmp_path / "local_tui.toml",
        artifacts_dir=str(tmp_path / "artifacts"),
        cache_dir=str(tmp_path / ".fscache"),
    )
    fragment = "\n".join(
        [
            "Body line before heading.",
            "",
            "Section Title",
            "=============",
        ]
    )
    lines = app._render_rst_fragment_lines(fragment)

    heading_idx = None
    for idx, line in enumerate(lines):
        if "Section Title" in line:
            heading_idx = idx
            break
    assert heading_idx is not None
    assert heading_idx > 0
    assert lines[heading_idx - 1] == ""


def test_rst_wrap_width_prefers_80_and_scales_down_for_small_terminals(tmp_path: Path, monkeypatch) -> None:
    app = LocalAutomationTUI(
        db_path=str(tmp_path / "jobqueue.db"),
        config_path=tmp_path / "local_tui.toml",
        artifacts_dir=str(tmp_path / "artifacts"),
        cache_dir=str(tmp_path / ".fscache"),
    )

    monkeypatch.setattr(app, "_terminal_width", lambda: 120)
    assert app._rst_wrap_width() == 80

    monkeypatch.setattr(app, "_terminal_width", lambda: 70)
    assert app._rst_wrap_width() == 66

    monkeypatch.setattr(app, "_terminal_width", lambda: 24)
    assert app._rst_wrap_width() == 30


def test_focus_stays_on_previous_job_until_output_drained(tmp_path: Path, monkeypatch) -> None:
    app = LocalAutomationTUI(
        db_path=str(tmp_path / "jobqueue.db"),
        config_path=tmp_path / "local_tui.toml",
        artifacts_dir=str(tmp_path / "artifacts"),
        cache_dir=str(tmp_path / ".fscache"),
    )
    app._current_job_ids = ["job-1", "job-2"]
    app._current_focus_job_id = "job-1"
    app._current_last_seq = {"job-1": 0, "job-2": -1}
    app._current_completion_announced = set()

    # Worker has already switched to job-2, but job-1 should remain focused
    # while unread output remains.
    monkeypatch.setattr(app.runtime, "status", lambda: {"busy": True, "current_job_id": "job-2"})
    monkeypatch.setattr(
        app.runtime,
        "events_since",
        lambda job_id, last_seq=-1: [{"seq": 1}] if job_id == "job-1" else [],
    )

    assert app._resolve_current_focus_job_id() == "job-1"

    # Once output is drained, focus can move immediately.
    monkeypatch.setattr(app.runtime, "events_since", lambda job_id, last_seq=-1: [])
    assert app._resolve_current_focus_job_id() == "job-2"


def test_ansi_wrap_uses_visible_length_not_escape_length(tmp_path: Path) -> None:
    app = LocalAutomationTUI(
        db_path=str(tmp_path / "jobqueue.db"),
        config_path=tmp_path / "local_tui.toml",
        artifacts_dir=str(tmp_path / "artifacts"),
        cache_dir=str(tmp_path / ".fscache"),
    )
    red = app._ansi("31")
    reset = app._ansi("0")
    text = f"{red}alpha{reset} beta gamma delta epsilon"
    lines = app._wrap_ansi_visible(text, width=20)
    assert lines
    assert all(app._visible_len(line) <= 20 for line in lines)


def test_render_rst_fragment_inserts_blank_line_between_paragraphs(tmp_path: Path) -> None:
    app = LocalAutomationTUI(
        db_path=str(tmp_path / "jobqueue.db"),
        config_path=tmp_path / "local_tui.toml",
        artifacts_dir=str(tmp_path / "artifacts"),
        cache_dir=str(tmp_path / ".fscache"),
    )
    fragment = "\n".join(
        [
            "First paragraph line one.",
            "Still first paragraph.",
            "",
            "Second paragraph begins here.",
        ]
    )
    lines = app._render_rst_fragment_lines(fragment)
    assert "First paragraph line one. Still first paragraph." in lines[0]
    assert "" in lines
    second_idx = lines.index("Second paragraph begins here.")
    assert second_idx > 0
    assert lines[second_idx - 1] == ""


def test_render_rst_fragment_admonition_box_and_color(tmp_path: Path) -> None:
    app = LocalAutomationTUI(
        db_path=str(tmp_path / "jobqueue.db"),
        config_path=tmp_path / "local_tui.toml",
        artifacts_dir=str(tmp_path / "artifacts"),
        cache_dir=str(tmp_path / ".fscache"),
    )
    warning_fragment = "\n".join(
        [
            ".. warning::",
            "",
            "   Over-temperature risk.",
        ]
    )
    note_fragment = "\n".join(
        [
            ".. note::",
            "",
            "   Operator note.",
        ]
    )
    warning_lines = app._render_rst_fragment_lines(warning_fragment)
    note_lines = app._render_rst_fragment_lines(note_fragment)

    assert any("\x1b[33m+" in line for line in warning_lines)
    assert any("\x1b[33m|" in line for line in warning_lines)
    assert any("\x1b[34m+" in line for line in note_lines)
    assert any("\x1b[34m|" in line for line in note_lines)


def test_render_rst_fragment_admonition_with_title_is_bold(tmp_path: Path) -> None:
    app = LocalAutomationTUI(
        db_path=str(tmp_path / "jobqueue.db"),
        config_path=tmp_path / "local_tui.toml",
        artifacts_dir=str(tmp_path / "artifacts"),
        cache_dir=str(tmp_path / ".fscache"),
    )
    fragment = "\n".join(
        [
            ".. admonition:: Setup Checklist",
            "",
            "   Confirm harness is disconnected.",
        ]
    )
    lines = app._render_rst_fragment_lines(fragment)
    assert any("\x1b[1mSetup Checklist\x1b[0m" in line for line in lines)


def test_render_rst_fragment_simple_admonition_argument_promoted_to_bold_title(tmp_path: Path) -> None:
    app = LocalAutomationTUI(
        db_path=str(tmp_path / "jobqueue.db"),
        config_path=tmp_path / "local_tui.toml",
        artifacts_dir=str(tmp_path / "artifacts"),
        cache_dir=str(tmp_path / ".fscache"),
    )
    fragment = "\n".join(
        [
            ".. warning:: Setup Checklist",
            "",
            "   Confirm harness is disconnected.",
        ]
    )
    lines = app._render_rst_fragment_lines(fragment)
    assert any("\x1b[1mSetup Checklist\x1b[0m" in line for line in lines)
    assert not any(line.strip() == "Setup Checklist" for line in lines)


def test_render_rst_fragment_rvt_result_preserves_multiline_rvt_source(tmp_path: Path) -> None:
    app = LocalAutomationTUI(
        db_path=str(tmp_path / "jobqueue.db"),
        config_path=tmp_path / "local_tui.toml",
        artifacts_dir=str(tmp_path / "artifacts"),
        cache_dir=str(tmp_path / ".fscache"),
    )
    fragment = "\n".join(
        [
            ".. rvt-result::",
            "   :status: pass",
            "   :timestamp: 2026-01-01T00:00:00+00:00",
            "   :duration: 0.001000",
            "",
            "   .. rvt::",
            "",
            "      (DemoBlock",
            "        {:mode \"nominal\"",
            "         :limits [1 2 3]})",
            "",
            "   .. code-block:: text",
            "",
            "      setup complete",
        ]
    )
    lines = app._render_rst_fragment_lines(fragment)
    plain = [app._strip_ansi(line) for line in lines]

    assert "(DemoBlock" in plain
    assert any(':mode "nominal"' in line for line in plain)
    assert any(":limits [1 2 3]" in line for line in plain)
    assert not any(':mode "nominal"' in line and ":limits [1 2 3]" in line for line in plain)


def test_block_source_lines_prefer_source_rst_from_live_event(tmp_path: Path) -> None:
    app = LocalAutomationTUI(
        db_path=str(tmp_path / "jobqueue.db"),
        config_path=tmp_path / "local_tui.toml",
        artifacts_dir=str(tmp_path / "artifacts"),
        cache_dir=str(tmp_path / ".fscache"),
    )
    event = {
        "kind": "block_end",
        "block": "DemoBlock",
        "args": ["legacy"],
        "source_rst": "(DemoBlock\n  {:mode \"nominal\"\n   :limits [1 2 3]})",
    }
    lines = app._block_source_lines(event)
    assert lines[0] == "(DemoBlock"
    assert any(':mode "nominal"' in line for line in lines)
    assert any(":limits [1 2 3]" in line for line in lines)
