"""Simple job executor used by the worker CLI."""

import random
import time
from pathlib import Path
from typing import Dict

from . import JobInput


def run_job(job: JobInput, artifacts_dir: str) -> Dict:
    """Sleep for a random amount of time, write artifacts, and return manifest + summary."""
    job_id = job.get("job_id") or "unknown"
    duration = random.uniform(1.0, 5.0)
    time.sleep(duration)

    job_folder = Path(artifacts_dir) / job_id
    job_folder.mkdir(parents=True, exist_ok=True)

    summary_path = job_folder / "summary.txt"
    summary_content = (
        f"Job {job_id}\n"
        f"File: {job.get('file')}\n"
        f"UUT: {job.get('uut')}\n"
        f"Report: {job.get('report_id')}\n"
        f"Duration: {duration:.2f}s\n"
    )
    summary_path.write_text(summary_content)

    payload_path = job_folder / "result.json"
    payload_path.write_text(
        f'{{"status":"completed","score":{random.randint(0,100)},"duration":{duration:.2f}}}'
    )

    artifacts = [
        str(summary_path.relative_to(job_folder)),
        str(payload_path.relative_to(job_folder)),
    ]

    return {
        "summary": {
            "status": "completed",
            "duration_seconds": round(duration, 2),
            "output": f"Processed {job.get('file', 'unknown')} with UUT {job.get('uut', 'n/a')}",
        },
        "artifacts": artifacts,
    }


__all__ = ["run_job"]
