from textwrap import dedent

from automationv3.framework.executor import run_script_document_text
from automationv3.framework.rst import render_script_rst_html


class RecordingObserver:
    def __init__(self):
        self.events = []

    def on_text_chunk(self, chunk_index, content, line):
        self.events.append(("text", chunk_index, line, content))

    def on_block_start(self, block, args):
        self.events.append(("block_start", block, list(args)))

    def on_block_end(self, block, args, result, passed, error):
        self.events.append(("block_end", block, list(args), result, passed, error))

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
    assert any(evt[0] == "text" for evt in observer.events)
    assert any(evt[0] == "content" and evt[1] == "text/rst" and evt[2] == "text" for evt in observer.events)
    assert any(evt[0] == "content" and evt[1] == "text/rst" and evt[2] == "rvt_result" for evt in observer.events)
    assert any(evt[0] == "block_start" and evt[1] == "always-pass" for evt in observer.events)
    assert any(evt[0] == "block_end" and evt[1] == "always-pass" and evt[4] is True for evt in observer.events)
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
           :output: all checks passed

           (always-pass)
        """
    )

    html = render_script_rst_html(result_rst)

    assert "rvt-result-block" in html
    assert "PASS" in html
    assert "always-pass" in html
    assert "all checks passed" in html


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
