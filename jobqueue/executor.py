"""Simple job executor used by the worker CLI."""

import random
import time
from typing import Dict

from . import JobInput


def run_job(job: JobInput) -> Dict:
    """Sleep for a random amount of time and return a simulated result payload."""
    duration = random.uniform(1.0, 5.0)
    time.sleep(duration)
    return {
        "status": "completed",
        "duration_seconds": round(duration, 2),
        "output": f"Processed {job.get('file', 'unknown')} with UUT {job.get('uut', 'n/a')}",
        "score": random.randint(0, 100),
    }


__all__ = ["run_job"]
