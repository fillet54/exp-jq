"""Simple job executor used by the worker CLI."""

import json
import logging
import time
from pathlib import Path
from typing import Any, Callable, Dict, List

from . import JobInput
from automationv3.framework import edn
from automationv3.framework.executor import run_script_document
from automationv3.framework.rst import render_script_rst_html


class StreamingJobObserver:
    """Collects execution observer callbacks and emits ordered stream events."""

    def __init__(self, callback: Callable[[Dict[str, Any]], None] | None = None) -> None:
        self.callback = callback
        self.events: List[Dict[str, Any]] = []
        self._seq = 0

    def _emit(self, kind: str, **payload: Any) -> None:
        event = {
            "seq": self._seq,
            "kind": kind,
            "timestamp": time.time(),
            **payload,
        }
        self._seq += 1
        self.events.append(event)
        if self.callback:
            self.callback(event)

    def on_script_begin(self) -> None:
        self._emit("script_begin")

    def on_script_end(self, passed: bool) -> None:
        self._emit("script_end", passed=bool(passed))

    def on_text_chunk(self, chunk_index: int, content: str, line: int | None) -> None:
        self._emit(
            "text_chunk",
            chunk_index=int(chunk_index),
            line=line,
            content=content,
            rst_fragment=content,
        )

    def on_content(self, content: str, mime_type: str, meta: Dict[str, Any] | None = None) -> None:
        event: Dict[str, Any] = {
            "mime_type": mime_type,
            "content": content,
            "meta": meta or {},
        }
        if mime_type == "text/rst":
            event["rst_fragment"] = content
        self._emit("content", **event)

    def on_rvt_start(self, rvt_index: int, body: str, line: int | None) -> None:
        self._emit("rvt_start", rvt_index=int(rvt_index), line=line, body=body)

    def on_rvt_result(
        self,
        rvt_index: int,
        body: str,
        report: Dict[str, Any],
        rst_fragment: str,
    ) -> None:
        self._emit(
            "rvt_result",
            rvt_index=int(rvt_index),
            body=body,
            passed=bool(report.get("passed")),
            error=report.get("error"),
            result_count=len(report.get("results") or []),
            invocation_count=len(report.get("invocations") or []),
            rst_fragment=rst_fragment,
        )

    def on_rvt_end(self, rvt_index: int, body: str, report: Dict[str, Any]) -> None:
        self._emit(
            "rvt_end",
            rvt_index=int(rvt_index),
            body=body,
            passed=bool(report.get("passed")),
        )

    def on_step_start(self, index: int, form: Any) -> None:
        self._emit("step_start", step_index=int(index), form=edn.writes(form))

    def on_step_end(self, index: int, form: Any, value: Any) -> None:
        self._emit(
            "step_end",
            step_index=int(index),
            form=edn.writes(form),
            result=str(value),
        )

    def on_block_start(self, block: str, args: List[Any]) -> None:
        self._emit("block_start", block=block, args=[str(arg) for arg in args])

    def on_block_end(
        self,
        block: str,
        args: List[Any],
        result: str | None,
        passed: bool,
        error: str,
    ) -> None:
        self._emit(
            "block_end",
            block=block,
            args=[str(arg) for arg in args],
            result=result,
            passed=bool(passed),
            error=error,
        )


def run_job(
    job: JobInput,
    artifacts_dir: str,
    observer_callback: Callable[[Dict[str, Any]], None] | None = None,
) -> Dict:
    """Run an RST script and write artifacts + summary, streaming observer events."""
    job_id = job.get("job_id") or "unknown"
    started = time.time()

    job_folder = Path(artifacts_dir) / job_id
    job_folder.mkdir(parents=True, exist_ok=True)

    script_file = job.get("file")
    scripts_root = job.get("scripts_root")
    script_path = None
    if script_file and scripts_root:
        script_path = Path(scripts_root) / script_file

    observer = StreamingJobObserver(callback=observer_callback)
    script_report: Dict[str, Any] = {
        "passed": True,
        "results": [],
        "body_count": 0,
        "result_document": "",
        "invocations": [],
    }
    success = True
    if script_path and script_path.exists():
        script_report = run_script_document(script_path, observer=observer)
        success = bool(script_report["passed"])
    elif script_file:
        success = False
        script_report = {
            "passed": False,
            "results": [],
            "body_count": 0,
            "result_document": "",
            "invocations": [],
            "error": f"Script not found: {script_path}",
        }
        observer._emit("execution_error", message=script_report["error"])

    duration = time.time() - started
    result_document = script_report.get("result_document") or ""
    result_html = ""
    if result_document:
        try:
            result_html = render_script_rst_html(result_document)
        except Exception:
            result_html = ""

    summary_path = job_folder / "summary.txt"
    summary_content = (
        f"Job {job_id}\n"
        f"File: {job.get('file')}\n"
        f"UUT: {job.get('uut')}\n"
        f"Scripts tree: {job.get('scripts_tree')}\n"
        f"Report: {job.get('report_id')}\n"
        f"Framework: {job.get('framework_version') or 'default-env'}\n"
        f"RVT bodies: {script_report.get('body_count', 0)}\n"
        f"RVT passed: {script_report.get('passed')}\n"
        f"Observer events: {len(observer.events)}\n"
        f"Duration: {duration:.2f}s\n"
    )
    summary_path.write_text(summary_content, encoding="utf-8")

    result_doc_path = job_folder / "result_document.rst"
    result_doc_path.write_text(result_document, encoding="utf-8")

    result_html_path = job_folder / "result_document.html"
    result_html_path.write_text(result_html, encoding="utf-8")

    payload_path = job_folder / "result.json"
    payload_path.write_text(
        json.dumps(
            {
                "status": "completed" if success else "failed",
                "duration": round(duration, 2),
                "rvt": script_report,
                "observer_events": observer.events,
                "result_document": result_document,
            },
            indent=2,
        )
    )

    artifacts = [
        str(summary_path.relative_to(job_folder)),
        str(payload_path.relative_to(job_folder)),
        str(result_doc_path.relative_to(job_folder)),
        str(result_html_path.relative_to(job_folder)),
    ]

    logging.getLogger("jobqueue.executor").info(
        "Generated artifacts for job %s at %s", job_id, job_folder
    )

    return {
        "summary": {
            "status": "completed" if success else "failed",
            "duration_seconds": round(duration, 2),
            "output": (
                f"Processed {job.get('file', 'unknown')} with UUT "
                f"{job.get('uut', 'n/a')} (rvt_passed={script_report.get('passed')})"
            ),
            "rvt": script_report,
            "observer_events": observer.events,
            "result_document": result_document,
        },
        "artifacts": artifacts,
        "success": success,
    }


__all__ = ["run_job"]
