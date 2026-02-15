"""Simple job executor used by the worker CLI."""

import json
import logging
import time
from pathlib import Path
from typing import Dict

from . import JobInput
from automationv3.framework.executor import run_rvt_script


def run_job(job: JobInput, artifacts_dir: str) -> Dict:
    """Run an RST script's ``.. rvt::`` bodies and write artifacts + summary."""
    job_id = job.get("job_id") or "unknown"
    started = time.time()

    job_folder = Path(artifacts_dir) / job_id
    job_folder.mkdir(parents=True, exist_ok=True)

    script_file = job.get("file")
    scripts_root = job.get("scripts_root")
    script_path = None
    if script_file and scripts_root:
        script_path = Path(scripts_root) / script_file

    rvt_report = {"passed": True, "results": [], "body_count": 0}
    success = True
    if script_path and script_path.exists():
        rvt_report = run_rvt_script(script_path)
        success = bool(rvt_report["passed"])
    elif script_file:
        success = False
        rvt_report = {
            "passed": False,
            "results": [],
            "body_count": 0,
            "error": f"Script not found: {script_path}",
        }

    duration = time.time() - started

    summary_path = job_folder / "summary.txt"
    summary_content = (
        f"Job {job_id}\n"
        f"File: {job.get('file')}\n"
        f"UUT: {job.get('uut')}\n"
        f"Scripts tree: {job.get('scripts_tree')}\n"
        f"Report: {job.get('report_id')}\n"
        f"Framework: {job.get('framework_version') or 'default-env'}\n"
        f"RVT bodies: {rvt_report.get('body_count', 0)}\n"
        f"RVT passed: {rvt_report.get('passed')}\n"
        f"Duration: {duration:.2f}s\n"
    )
    summary_path.write_text(summary_content)

    payload_path = job_folder / "result.json"
    payload_path.write_text(
        json.dumps(
            {
                "status": "completed" if success else "failed",
                "duration": round(duration, 2),
                "rvt": rvt_report,
            },
            indent=2,
        )
    )

    artifacts = [
        str(summary_path.relative_to(job_folder)),
        str(payload_path.relative_to(job_folder)),
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
                f"{job.get('uut', 'n/a')} (rvt_passed={rvt_report.get('passed')})"
            ),
            "rvt": rvt_report,
        },
        "artifacts": artifacts,
        "success": success,
    }


__all__ = ["run_job"]
