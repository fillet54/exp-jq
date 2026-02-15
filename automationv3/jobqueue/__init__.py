"""
Core job queue persistence primitives.

SQLite schema overview for this module:

.. mermaid::

   erDiagram
       jobs ||--o| job_results : "job_id (logical link)"

       jobs {
           TEXT job_id PK
           TEXT job_data
           INTEGER skipped
           INTEGER priority
           REAL inserted_at
       }

       job_results {
           TEXT job_id PK
           TEXT job_data
           TEXT result_data
           INTEGER success
           TEXT worker_id
           TEXT worker_address
           REAL completed_at
           TEXT suite_run_id
           TEXT artifacts_manifest
           INTEGER artifacts_downloaded
       }

`job_results.job_id` is a logical join key to `jobs.job_id`; the schema does
not enforce a SQLite foreign key constraint.
"""

import json
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Optional, Union


JobInput = Dict[str, Any]
JobReturn = Dict[str, Any]
JobResult = Dict[str, Any]


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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_results (
                    job_id TEXT PRIMARY KEY,
                    job_data TEXT,
                    result_data TEXT,
                    success INTEGER,
                    worker_id TEXT,
                    worker_address TEXT,
                    completed_at REAL,
                    suite_run_id TEXT,
                    artifacts_manifest TEXT,
                    artifacts_downloaded INTEGER DEFAULT 0
                );
                """
            )
            # Best-effort schema upgrades when table already exists
            for column_def in [
                ("worker_address", "TEXT"),
                ("suite_run_id", "TEXT"),
                ("artifacts_manifest", "TEXT"),
                ("artifacts_downloaded", "INTEGER DEFAULT 0"),
            ]:
                try:
                    conn.execute(
                        f"ALTER TABLE job_results ADD COLUMN {column_def[0]} {column_def[1]}"
                    )
                except sqlite3.OperationalError:
                    pass

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

    def _row_to_result(self, row: sqlite3.Row) -> JobResult:
        job_data = json.loads(row["job_data"]) if row["job_data"] else {}
        result_data = json.loads(row["result_data"]) if row["result_data"] else None
        artifacts_manifest = (
            json.loads(row["artifacts_manifest"])
            if row["artifacts_manifest"]
            else []
        )
        suite_run_id = row["suite_run_id"] if "suite_run_id" in row.keys() else None
        return {
            "job_id": row["job_id"],
            "job_data": job_data,
            "result_data": result_data,
            "success": bool(row["success"]),
            "worker_id": row["worker_id"],
            "worker_address": row["worker_address"],
            "completed_at": row["completed_at"],
            "artifacts_manifest": artifacts_manifest,
            "artifacts_downloaded": bool(row["artifacts_downloaded"]),
            "suite_run_id": suite_run_id,
        }

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
            job_id = uuid7_str()
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
            job_id = uuid7_str()
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
        def _fetch(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
            cur = conn.execute(
                """
                SELECT job_id, job_data, skipped, priority, inserted_at
                FROM jobs
                WHERE skipped = 0
                ORDER BY priority DESC, inserted_at ASC
                LIMIT 1
                """
            )
            return cur.fetchone()

        with self._connect() as conn:
            row = _fetch(conn)
            if row:
                return self._row_to_job(row)
            # If everything is marked skipped, restore and try once more.
            skipped_count = conn.execute(
                "SELECT COUNT(1) FROM jobs WHERE skipped = 1"
            ).fetchone()[0]
            if skipped_count:
                conn.execute("UPDATE jobs SET skipped = 0")
                row = _fetch(conn)
                return self._row_to_job(row) if row else None
        return None

    def get_job(self, job_id: str) -> Optional[JobReturn]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT job_id, job_data, skipped, priority, inserted_at
                FROM jobs
                WHERE job_id = ?
                """,
                (job_id,),
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

    def record_result(
        self,
        job_id: str,
        result_data: Any,
        success: bool,
        worker_id: Optional[str],
        worker_address: Optional[str],
        artifacts_manifest: Optional[List[str]] = None,
        job_data_snapshot: Optional[JobInput] = None,
    ) -> None:
        artifact_list = [str(p) for p in (artifacts_manifest or [])]
        if job_data_snapshot is None:
            job_data_snapshot = self.get_job(job_id) or {}
        suite_run_id = None
        if isinstance(job_data_snapshot, dict):
            suite_run_id = job_data_snapshot.get("suite_run_id")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO job_results (
                    job_id,
                    job_data,
                    result_data,
                    success,
                    worker_id,
                    worker_address,
                    completed_at,
                    suite_run_id,
                    artifacts_manifest,
                    artifacts_downloaded
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    json.dumps(job_data_snapshot),
                    json.dumps(result_data),
                    1 if success else 0,
                    worker_id,
                    worker_address,
                    time.time(),
                    suite_run_id,
                    json.dumps(artifact_list),
                    0,
                ),
            )

    def count_results(self, suite_run_id: Optional[str] = None) -> int:
        with self._connect() as conn:
            if suite_run_id:
                row = conn.execute(
                    """
                    SELECT COUNT(1) AS result_count
                    FROM job_results
                    WHERE suite_run_id = ?
                    """,
                    (suite_run_id,),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT COUNT(1) AS result_count
                    FROM job_results
                    """
                ).fetchone()
        return int(row["result_count"] if row and "result_count" in row.keys() else 0)

    def list_results(
        self,
        limit: int = 50,
        suite_run_id: Optional[str] = None,
        offset: int = 0,
    ) -> List[JobResult]:
        safe_limit = max(1, int(limit))
        safe_offset = max(0, int(offset))
        with self._connect() as conn:
            if suite_run_id:
                cur = conn.execute(
                    """
                    SELECT job_id, job_data, result_data, success, worker_id, worker_address, completed_at, suite_run_id, artifacts_manifest, artifacts_downloaded
                    FROM job_results
                    WHERE suite_run_id = ?
                    ORDER BY completed_at DESC
                    LIMIT ?
                    OFFSET ?
                    """,
                    (suite_run_id, safe_limit, safe_offset),
                )
            else:
                cur = conn.execute(
                    """
                    SELECT job_id, job_data, result_data, success, worker_id, worker_address, completed_at, suite_run_id, artifacts_manifest, artifacts_downloaded
                    FROM job_results
                    ORDER BY completed_at DESC
                    LIMIT ?
                    OFFSET ?
                    """,
                    (safe_limit, safe_offset),
                )
            rows = cur.fetchall()
        return [self._row_to_result(row) for row in rows]

    def list_results_for_suite(self, suite_run_id: str, limit: int = 200) -> List[JobResult]:
        return self.list_results(limit=limit, suite_run_id=suite_run_id)

    def list_pending_artifacts(self) -> List[JobResult]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT job_id, job_data, result_data, success, worker_id, worker_address, completed_at, artifacts_manifest, artifacts_downloaded
                FROM job_results
                WHERE artifacts_downloaded = 0 AND artifacts_manifest IS NOT NULL
                """
            )
            rows = cur.fetchall()
        return [self._row_to_result(row) for row in rows]

    def mark_artifacts_downloaded(self, job_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE job_results SET artifacts_downloaded = 1 WHERE job_id = ?",
                (job_id,),
            )


from .worker_system import (
    CentralServer,
    WorkerServer,
    create_central_app,
    create_worker_app,
)
from .executor import run_job
from .uut import UUTStore, UUTConfig
from .suites import SuiteManager
from .ids import uuid7, uuid7_str


__all__ = [
    "JobQueue",
    "CentralServer",
    "WorkerServer",
    "create_central_app",
    "create_worker_app",
    "run_job",
    "UUTStore",
    "UUTConfig",
    "SuiteManager",
    "uuid7",
    "uuid7_str",
]
