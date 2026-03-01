from textwrap import dedent

from automationv3.framework.block import BlockResult, BuildingBlock, format_block_invocation_rst
from automationv3.framework import edn
from automationv3.framework import executor as framework_executor
from automationv3.framework.executor import build_script_env, run_script_document_text
from automationv3.framework.rst import render_script_rst_html
from automationv3.jobqueue.executor import run_job


class RecordingObserver:
    def __init__(self):
        self.events = []

    def on_text_chunk(self, chunk_index, content, line):
        self.events.append(("text", chunk_index, line, content))

    def on_block_start(self, block, args, source_rst=""):
        self.events.append(("block_start", block, list(args), source_rst))

    def on_block_end(
        self,
        block,
        args,
        result,
        passed,
        error,
        timestamp=None,
        duration=None,
        source_rst="",
    ):
        self.events.append(
            ("block_end", block, list(args), result, passed, error, timestamp, duration, source_rst)
        )

    def on_rvt_result(self, rvt_index, body, report, rst_fragment):
        self.events.append(("rvt_result", rvt_index, bool(report.get("passed")), rst_fragment))

    def on_content(self, content, mime_type, meta):
        self.events.append(("content", mime_type, meta.get("kind"), content))


def test_run_script_document_text_emits_text_and_block_observer_events():
    script = dedent(
        """\
        Demo
        ====

        Intro paragraph.

        .. rvt::

           (always-pass)
        """
    )
    observer = RecordingObserver()

    report = run_script_document_text(script, observer=observer)

    assert report["passed"] is True
    assert report["body_count"] == 1
    assert ".. rvt-result::" in report["result_document"]
    assert ":status: pass" in report["result_document"]
    assert ":timestamp:" in report["result_document"]
    assert ":duration:" in report["result_document"]
    assert ".. code-block:: text" in report["result_document"]
    assert any(evt[0] == "text" for evt in observer.events)
    assert any(evt[0] == "content" and evt[1] == "text/rst" and evt[2] == "text" for evt in observer.events)
    assert any(evt[0] == "content" and evt[1] == "text/rst" and evt[2] == "rvt_result" for evt in observer.events)
    assert any(evt[0] == "block_start" and evt[1] == "always-pass" for evt in observer.events)
    assert any(evt[0] == "block_end" and evt[1] == "always-pass" and evt[4] is True for evt in observer.events)
    assert any(evt[0] == "block_end" and isinstance(evt[6], float) for evt in observer.events)
    assert any(evt[0] == "block_end" and isinstance(evt[7], float) and evt[7] >= 0.0 for evt in observer.events)
    assert any(evt[0] == "rvt_result" and evt[2] is True for evt in observer.events)


def test_run_script_document_text_marks_failed_rvt_result():
    script = dedent(
        """\
        .. rvt::

           (always-fail)
        """
    )

    report = run_script_document_text(script)

    assert report["passed"] is False
    assert ".. rvt-result::" in report["result_document"]
    assert ":status: fail" in report["result_document"]
    assert ":timestamp:" in report["result_document"]
    assert ":duration:" in report["result_document"]
    assert "always-fail" in report["result_document"]


def test_run_script_document_text_emits_one_rvt_result_per_block_invocation():
    script = dedent(
        """\
        .. rvt::

           (always-pass)
           (always-fail)
           (always-pass)
        """
    )

    report = run_script_document_text(script)

    assert report["passed"] is False
    assert report["result_document"].count(".. rvt-result::") == 3
    assert report["result_document"].count(":status: pass") == 2
    assert report["result_document"].count(":status: fail") == 1


def test_run_script_document_text_uses_block_as_rst_for_source_lines(monkeypatch):
    class DemoBlock(BuildingBlock):
        def name(self):
            return "demo-block"

        def execute(self, *args):
            return BlockResult(True, stdout="ok")

        def as_rst(self, *args):
            return "(demo-block\n  \"custom\"\n  42)"

    monkeypatch.setattr(framework_executor, "all_blocks", [DemoBlock()])
    script = dedent(
        """\
        .. rvt::

           (demo-block)
        """
    )
    report = framework_executor.run_script_document_text(script)

    assert report["passed"] is True
    assert ".. rvt::" in report["result_document"]
    assert "      (demo-block" in report["result_document"]
    assert '      "custom"' in report["result_document"]
    assert "      42)" in report["result_document"]


def test_default_as_rst_pretty_prints_map_and_list_arguments(monkeypatch):
    class DemoBlock(BuildingBlock):
        def name(self):
            return "demo-block"

        def execute(self, *args):
            return BlockResult(True, stdout="ok")

    monkeypatch.setattr(framework_executor, "all_blocks", [DemoBlock()])
    script = dedent(
        """\
        .. rvt::

           (demo-block {:mode "nominal" :limits [1 2 3]})
        """
    )
    report = framework_executor.run_script_document_text(script)
    doc = report["result_document"]

    assert report["passed"] is True
    assert '      (demo-block {:mode "nominal" :limits [1 2 3]})' in doc


def test_format_block_invocation_rst_splits_when_line_exceeds_limit():
    formatted = format_block_invocation_rst(
        "demo-block",
        [
            {
                edn.Keyword("mode"): "nominal",
                edn.Keyword("profile"): "thermal-vacuum",
                edn.Keyword("limits"): list(range(1, 20)),
            }
        ],
    )
    assert "\n" in formatted
    assert formatted.startswith("(demo-block")
    assert ':profile "thermal-vacuum"' in formatted


def test_run_script_document_text_do_wrapper_still_emits_per_block_results():
    script = dedent(
        """\
        .. rvt::

           (do
             (always-pass)
             (always-fail)
             (always-pass))
        """
    )

    report = run_script_document_text(script)

    assert report["passed"] is False
    assert report["result_document"].count(".. rvt-result::") == 3
    assert report["result_document"].count(":status: pass") == 2
    assert report["result_document"].count(":status: fail") == 1


def test_render_script_rst_html_renders_rvt_result_blocks():
    result_rst = dedent(
        """\
        Results
        =======

        .. rvt-result::
           :status: pass
           :timestamp: 2026-01-01T00:00:00+00:00
           :duration: 0.012

           .. rvt::

              (always-pass)

           .. code-block:: text

              all checks passed
        """
    )

    html = render_script_rst_html(result_rst)

    assert "rvt-result-block" in html
    assert "PASS" in html
    assert "always-pass" in html
    assert "all checks passed" in html
    assert "2026-01-01T00:00:00+00:00" in html


def test_render_script_rst_html_allows_raw_html_directive():
    result_rst = dedent(
        """\
        .. raw:: html

           <div id="custom-artifact">artifact-link</div>
        """
    )

    html = render_script_rst_html(result_rst)

    assert "custom-artifact" in html
    assert "artifact-link" in html


def test_setup_simulation_emits_attachment_and_writes_log(tmp_path):
    script = dedent(
        """\
        .. rvt::

           (SetupSimulation "mode" "nominal" "seed" "42")
        """
    )
    env = build_script_env(
        extra_env={
            "__run_context__": {
                "job_id": "job-1",
                "artifacts_dir": str(tmp_path),
            }
        }
    )
    observer = RecordingObserver()

    report = run_script_document_text(script, env=env, observer=observer)

    assert report["passed"] is True
    assert ":attachment:`setup-simulation.log`" in report["result_document"]
    log_path = tmp_path / "attachments" / "setup-simulation.log"
    assert log_path.exists()
    content = log_path.read_text(encoding="utf-8")
    assert "SetupSimulation static log" in content
    assert "mode=nominal" in content
    assert any(
        evt[0] == "content"
        and evt[1] == "text/plain"
        and evt[2] == "attachment"
        for evt in observer.events
    )


def test_render_script_rst_html_renders_attachment_with_resolved_href():
    result_rst = dedent(
        """\
        .. rvt-result::
           :status: pass
           :timestamp: 2026-01-01T00:00:00+00:00
           :duration: 0.010000

           .. rvt::

              (SetupSimulation "mode" "nominal")

           .. code-block:: text

              setup simulation complete

           Attachments:

           - :attachment:`setup-simulation.log` (text/plain)
        """
    )

    html = render_script_rst_html(
        result_rst,
        artifact_href_resolver=lambda ref: "/jobs/job-1/output/artifacts/attachments/setup-simulation.log"
        if ref == "setup-simulation.log"
        else ref,
    )

    assert "setup-simulation.log" in html
    assert "/jobs/job-1/output/artifacts/attachments/setup-simulation.log" in html
    assert "text/plain" in html


def test_run_job_includes_generated_attachment_in_artifacts_manifest(tmp_path):
    scripts_root = tmp_path / "scripts"
    scripts_root.mkdir(parents=True, exist_ok=True)
    script_path = scripts_root / "attach_demo.rst"
    script_path.write_text(
        dedent(
            """\
            .. rvt::

               (SetupSimulation "mode" "nominal")
            """
        ),
        encoding="utf-8",
    )

    result = run_job(
        {
            "job_id": "job-attach-1",
            "file": "attach_demo.rst",
            "scripts_root": str(scripts_root),
            "uut": "Rig-1",
            "report_id": "report-1",
        },
        artifacts_dir=str(tmp_path / "worker_artifacts"),
    )

    artifacts = result.get("artifacts") or []
    assert "attachments/setup-simulation.log" in artifacts
    summary = result.get("summary") or {}
    tree_sha = str(summary.get("artifact_tree_sha") or "")
    assert len(tree_sha) == 40


def test_run_job_binds_variation_symbols_into_environment(tmp_path):
    scripts_root = tmp_path / "scripts"
    scripts_root.mkdir(parents=True, exist_ok=True)
    script_path = scripts_root / "variation_eval.rst"
    script_path.write_text(
        dedent(
            """\
            .. rvt::

               (random-fail fail-prob)
            """
        ),
        encoding="utf-8",
    )

    result = run_job(
        {
            "job_id": "job-var-1",
            "file": "variation_eval.rst",
            "scripts_root": str(scripts_root),
            "uut": "Rig-1",
            "report_id": "report-1",
            "is_variation_job": True,
            "variation_name": "nominal",
            "variation_bindings": {"fail-prob": "0"},
            "variation_index": 1,
            "variation_total": 1,
        },
        artifacts_dir=str(tmp_path / "worker_artifacts"),
    )

    assert result["success"] is True
    assert result["summary"]["rvt"]["passed"] is True
