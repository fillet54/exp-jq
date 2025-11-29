import json
import sqlite3
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional, Union


JobInput = Dict[str, Any]
JobReturn = Dict[str, Any]


class JobQueue:
    """SQLite-backed job queue."""

    def __init__(self, db_path: str = "jobqueue.db") -> None:
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    job_data TEXT,
                    skipped INTEGER DEFAULT 0,
                    priority INTEGER DEFAULT 0,
                    inserted_at REAL
                );
                """
            )

    def _row_to_job(self, row: sqlite3.Row) -> JobReturn:
        job_data = json.loads(row["job_data"]) if row["job_data"] else {}
        job_data.update(
            {
                "job_id": row["job_id"],
                "priority": row["priority"],
                "inserted_at": row["inserted_at"],
                "skipped": bool(row["skipped"]),
            }
        )
        return job_data

    def _validate_job(self, job: JobInput) -> None:
        if not isinstance(job, dict):
            raise ValueError("Job must be a dictionary.")
        for key in ("file", "uut", "report_id"):
            if key not in job:
                raise ValueError(f"Missing required job key: {key}")

    def add_job(
        self, job_or_jobs: Union[JobInput, Iterable[JobInput]], priority: int = 0
    ) -> Union[str, List[str]]:
        """Add a job or a batch of jobs. Returns job_id or list of job_ids."""
        if isinstance(job_or_jobs, dict):
            job = job_or_jobs
            self._validate_job(job)
            job_id = str(uuid.uuid4())
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO jobs (job_id, job_data, skipped, priority, inserted_at)
                    VALUES (?, ?, 0, ?, ?)
                    """,
                    (job_id, json.dumps(job), priority, time.time()),
                )
            return job_id

        jobs = list(job_or_jobs)
        if not jobs:
            return []

        entries: List[tuple] = []
        job_ids: List[str] = []
        for job in jobs:
            self._validate_job(job)
            job_id = str(uuid.uuid4())
            job_ids.append(job_id)
            entries.append((job_id, json.dumps(job), priority, time.time()))

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO jobs (job_id, job_data, skipped, priority, inserted_at)
                VALUES (?, ?, 0, ?, ?)
                """,
                entries,
            )
        return job_ids

    def get_next_job(self) -> Optional[JobReturn]:
        """Return the next available (not skipped) job based on priority and FIFO."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT job_id, job_data, skipped, priority, inserted_at
                FROM jobs
                WHERE skipped = 0
                ORDER BY priority DESC, inserted_at ASC
                LIMIT 1
                """
            )
            row = cur.fetchone()
        return self._row_to_job(row) if row else None

    def mark_skipped(self, job_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE jobs SET skipped = 1 WHERE job_id = ?", (job_id,)
            )

    def restore_all_skipped(self) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE jobs SET skipped = 0")

    def remove_job(self, job_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,))

    def promote_job(self, job_id: str, new_priority: int = 100) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET priority = ?, inserted_at = ?
                WHERE job_id = ?
                """,
                (new_priority, time.time(), job_id),
            )

    def list_jobs(self) -> List[JobReturn]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT job_id, job_data, skipped, priority, inserted_at
                FROM jobs
                ORDER BY priority DESC, inserted_at ASC
                """
            )
            rows = cur.fetchall()
        return [self._row_to_job(row) for row in rows]


from .worker_system import (
    CentralServer,
    WorkerServer,
    create_central_app,
    create_worker_app,
)


__all__ = [
    "JobQueue",
    "CentralServer",
    "WorkerServer",
    "create_central_app",
    "create_worker_app",
]
