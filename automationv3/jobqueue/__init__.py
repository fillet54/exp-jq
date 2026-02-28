"""Core job queue persistence and runtime exports.

This module owns queue and execution-result persistence primitives:

- ``jobs``
- ``job_results``
- ``pending_results``

Report metadata and report relationship tables are owned by
``automationv3.reporting`` and accessed here through a delegated repository.
"""

import json
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Optional, Union

from automationv3.reporting.repository import ReportingRepository


JobInput = Dict[str, Any]
JobReturn = Dict[str, Any]
JobResult = Dict[str, Any]
PendingResult = Dict[str, Any]
ReportRecord = Dict[str, Any]
ReportScript = Dict[str, Any]
ReportRequirement = Dict[str, Any]


class JobQueue:
    """SQLite-backed job queue."""

    def __init__(
        self,
        db_path: str = "jobqueue.db",
        reporting_repository: Optional[ReportingRepository] = None,
    ) -> None:
        self.db_path = db_path
        self.reporting_repository = reporting_repository or ReportingRepository(db_path=db_path)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_results (
                    job_id TEXT PRIMARY KEY,
                    report_id TEXT NOT NULL,
                    job_data TEXT,
                    result_data TEXT,
                    success INTEGER,
                    worker_id TEXT,
                    worker_address TEXT,
                    received_at REAL,
                    artifacts_manifest TEXT,
                    sync_attempts INTEGER DEFAULT 0,
                    last_error TEXT,
                    FOREIGN KEY (report_id) REFERENCES reports(report_id) ON DELETE CASCADE
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    report_id TEXT NOT NULL,
                    job_data TEXT,
                    skipped INTEGER DEFAULT 0,
                    priority INTEGER DEFAULT 0,
                    inserted_at REAL,
                    FOREIGN KEY (report_id) REFERENCES reports(report_id) ON DELETE CASCADE
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_results (
                    job_id TEXT PRIMARY KEY,
                    report_id TEXT NOT NULL,
                    job_data TEXT,
                    result_data TEXT,
                    success INTEGER,
                    worker_id TEXT,
                    worker_address TEXT,
                    completed_at REAL,
                    suite_run_id TEXT,
                    artifacts_manifest TEXT,
                    artifacts_downloaded INTEGER DEFAULT 0,
                    FOREIGN KEY (report_id) REFERENCES reports(report_id) ON DELETE CASCADE
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_report_id ON jobs (report_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_job_results_report_id_completed_at "
                "ON job_results (report_id, completed_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pending_results_report_id_received_at "
                "ON pending_results (report_id, received_at ASC)"
            )

    def _row_to_job(self, row: sqlite3.Row) -> JobReturn:
        job_data = json.loads(row["job_data"]) if row["job_data"] else {}
        report_id = str(row["report_id"] or "").strip()
        if report_id and not str(job_data.get("report_id") or "").strip():
            job_data["report_id"] = report_id
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
        report_id = str(row["report_id"] or "").strip()
        if report_id and not str(job_data.get("report_id") or "").strip():
            job_data["report_id"] = report_id
        result_data = json.loads(row["result_data"]) if row["result_data"] else None
        artifacts_manifest = (
            json.loads(row["artifacts_manifest"])
            if row["artifacts_manifest"]
            else []
        )
        suite_run_id = row["suite_run_id"]
        return {
            "job_id": row["job_id"],
            "report_id": report_id,
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

    def _row_to_pending_result(self, row: sqlite3.Row) -> PendingResult:
        job_data = json.loads(row["job_data"]) if row["job_data"] else {}
        report_id = str(row["report_id"] or "").strip()
        if report_id and not str(job_data.get("report_id") or "").strip():
            job_data["report_id"] = report_id
        result_data = json.loads(row["result_data"]) if row["result_data"] else None
        artifacts_manifest = (
            json.loads(row["artifacts_manifest"])
            if row["artifacts_manifest"]
            else []
        )
        return {
            "job_id": row["job_id"],
            "report_id": report_id,
            "job_data": job_data,
            "result_data": result_data,
            "success": bool(row["success"]),
            "worker_id": row["worker_id"],
            "worker_address": row["worker_address"],
            "received_at": row["received_at"],
            "artifacts_manifest": artifacts_manifest,
            "sync_attempts": int(row["sync_attempts"] or 0),
            "last_error": row["last_error"] or "",
        }

    def _validate_job(self, job: JobInput) -> None:
        if not isinstance(job, dict):
            raise ValueError("Job must be a dictionary.")
        for key in ("file", "uut", "report_id"):
            if key not in job or not str(job.get(key) or "").strip():
                raise ValueError(f"Missing required job key: {key}")

    def _normalize_report_script(self, report_id: Any, script_path: Any) -> tuple[str, str]:
        return str(report_id or "").strip(), str(script_path or "").strip()

    def _normalize_report_requirement(
        self, report_id: Any, requirement_id: Any
    ) -> tuple[str, str]:
        return str(report_id or "").strip(), str(requirement_id or "").strip()

    def _track_report_script_in_conn(
        self,
        conn: sqlite3.Connection,
        report_id: str,
        script_path: str,
        job_template: Optional[JobInput] = None,
    ) -> None:
        clean_report_id, clean_script = self._normalize_report_script(report_id, script_path)
        if not clean_report_id or not clean_script:
            return
        now = time.time()
        template = dict(job_template or {})
        conn.execute(
            """
            INSERT OR REPLACE INTO report_scripts (
                report_id,
                script_path,
                job_template,
                created_at,
                updated_at
            )
            VALUES (
                ?,
                ?,
                ?,
                COALESCE((SELECT created_at FROM report_scripts WHERE report_id = ? AND script_path = ?), ?),
                ?
            )
            """,
            (
                clean_report_id,
                clean_script,
                json.dumps(template),
                clean_report_id,
                clean_script,
                now,
                now,
            ),
        )

    def _track_report_requirement_in_conn(
        self,
        conn: sqlite3.Connection,
        report_id: str,
        requirement_id: str,
    ) -> None:
        clean_report_id, clean_requirement = self._normalize_report_requirement(
            report_id, requirement_id
        )
        if not clean_report_id or not clean_requirement:
            return
        now = time.time()
        conn.execute(
            """
            INSERT OR REPLACE INTO report_requirements (
                report_id,
                requirement_id,
                created_at,
                updated_at
            )
            VALUES (
                ?,
                ?,
                COALESCE((SELECT created_at FROM report_requirements WHERE report_id = ? AND requirement_id = ?), ?),
                ?
            )
            """,
            (
                clean_report_id,
                clean_requirement,
                clean_report_id,
                clean_requirement,
                now,
                now,
            ),
        )

    def _track_report_requirements_from_job_in_conn(
        self, conn: sqlite3.Connection, job: JobInput
    ) -> None:
        report_id = str(job.get("report_id") or "").strip()
        if not report_id:
            return
        meta = job.get("meta") if isinstance(job.get("meta"), dict) else {}
        raw_requirements = []
        if isinstance(meta, dict):
            raw_requirements = meta.get("requirements") or []
        if isinstance(raw_requirements, str):
            requirements = [part.strip() for part in raw_requirements.split(",") if part.strip()]
        elif isinstance(raw_requirements, list):
            requirements = [str(part).strip() for part in raw_requirements if str(part).strip()]
        else:
            requirements = []
        for requirement in requirements:
            self._track_report_requirement_in_conn(conn, report_id, requirement)

    def track_report_script(
        self,
        report_id: str,
        script_path: str,
        job_template: Optional[JobInput] = None,
    ) -> None:
        self.reporting_repository.track_report_script(
            report_id=report_id,
            script_path=script_path,
            job_template=job_template,
        )

    def add_report_requirement(self, report_id: str, requirement_id: str) -> None:
        self.reporting_repository.add_report_requirement(report_id, requirement_id)

    def add_report_requirements(self, report_id: str, requirement_ids: Iterable[str]) -> None:
        self.reporting_repository.add_report_requirements(report_id, requirement_ids)

    def add_job(
        self, job_or_jobs: Union[JobInput, Iterable[JobInput]], priority: int = 0
    ) -> Union[str, List[str]]:
        """Add a job or a batch of jobs. Returns job_id or list of job_ids."""
        if isinstance(job_or_jobs, dict):
            job = job_or_jobs
            self._validate_job(job)
            job_id = uuid7_str()
            clean_report_id = str(job.get("report_id") or "").strip()
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO jobs (job_id, report_id, job_data, skipped, priority, inserted_at)
                    VALUES (?, ?, ?, 0, ?, ?)
                    """,
                    (job_id, clean_report_id, json.dumps(job), priority, time.time()),
                )
                self._track_report_script_in_conn(
                    conn=conn,
                    report_id=clean_report_id,
                    script_path=str(job.get("file") or ""),
                    job_template=job,
                )
                self._track_report_requirements_from_job_in_conn(conn, job)
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
            entries.append(
                (
                    job_id,
                    str(job.get("report_id") or "").strip(),
                    json.dumps(job),
                    priority,
                    time.time(),
                )
            )

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO jobs (job_id, report_id, job_data, skipped, priority, inserted_at)
                VALUES (?, ?, ?, 0, ?, ?)
                """,
                entries,
            )
            for job in jobs:
                self._track_report_script_in_conn(
                    conn=conn,
                    report_id=str(job.get("report_id") or ""),
                    script_path=str(job.get("file") or ""),
                    job_template=job,
                )
                self._track_report_requirements_from_job_in_conn(conn, job)
        return job_ids

    def get_next_job(self) -> Optional[JobReturn]:
        """Return the next available (not skipped) job based on priority and FIFO."""
        def _fetch(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
            cur = conn.execute(
                """
                SELECT job_id, report_id, job_data, skipped, priority, inserted_at
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
                SELECT job_id, report_id, job_data, skipped, priority, inserted_at
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
                SELECT job_id, report_id, job_data, skipped, priority, inserted_at
                FROM jobs
                ORDER BY priority DESC, inserted_at ASC
                """
            )
            rows = cur.fetchall()
        return [self._row_to_job(row) for row in rows]

    def create_report(self, title: str, description: str = "") -> ReportRecord:
        clean_title = (title or "").strip()
        if not clean_title:
            raise ValueError("Report title is required.")
        report_id = uuid7_str()
        return self.reporting_repository.create_report(
            report_id=report_id,
            title=clean_title,
            description=(description or "").strip(),
        )

    def get_report(self, report_id: str) -> Optional[ReportRecord]:
        return self.reporting_repository.get_report(report_id)

    def list_reports(self, limit: int = 200) -> List[ReportRecord]:
        return self.reporting_repository.list_reports(limit=limit)

    def list_report_scripts(self, report_id: str) -> List[ReportScript]:
        return self.reporting_repository.list_report_scripts(report_id)

    def list_report_requirements(self, report_id: str) -> List[ReportRequirement]:
        return self.reporting_repository.list_report_requirements(report_id)

    def remove_report_requirement(self, report_id: str, requirement_id: str) -> None:
        self.reporting_repository.remove_report_requirement(report_id, requirement_id)

    def remove_report_script(self, report_id: str, script_path: str) -> None:
        self.reporting_repository.remove_report_script(report_id, script_path)

    def _job_matches_script(self, job: Dict[str, Any], script_path: str) -> bool:
        clean_script = str(script_path or "").strip()
        if not clean_script:
            return False
        return str(job.get("file") or "").strip() == clean_script

    def clear_results_for_report(
        self, report_id: str, script_path: Optional[str] = None
    ) -> int:
        clean_report_id = str(report_id or "").strip()
        if not clean_report_id:
            return 0
        with self._connect() as conn:
            if script_path is None:
                deleted = int(
                    conn.execute(
                        "DELETE FROM job_results WHERE report_id = ?",
                        (clean_report_id,),
                    ).rowcount
                    or 0
                )
                return deleted

            deleted = 0
            rows = conn.execute(
                """
                SELECT job_id, job_data
                FROM job_results
                WHERE report_id = ?
                """,
                (clean_report_id,),
            ).fetchall()
            for row in rows:
                job_data = {}
                if row["job_data"]:
                    try:
                        parsed = json.loads(row["job_data"])
                        if isinstance(parsed, dict):
                            job_data = parsed
                    except (TypeError, ValueError):
                        job_data = {}
                if not self._job_matches_script(job_data, str(script_path or "")):
                    continue
                conn.execute("DELETE FROM job_results WHERE job_id = ?", (row["job_id"],))
                deleted += 1
        return deleted

    def clear_pending_results_for_report(
        self, report_id: str, script_path: Optional[str] = None
    ) -> int:
        clean_report_id = str(report_id or "").strip()
        if not clean_report_id:
            return 0
        with self._connect() as conn:
            if script_path is None:
                deleted = int(
                    conn.execute(
                        "DELETE FROM pending_results WHERE report_id = ?",
                        (clean_report_id,),
                    ).rowcount
                    or 0
                )
                return deleted

            deleted = 0
            rows = conn.execute(
                """
                SELECT job_id, job_data
                FROM pending_results
                WHERE report_id = ?
                """,
                (clean_report_id,),
            ).fetchall()
            for row in rows:
                job_data = {}
                if row["job_data"]:
                    try:
                        parsed = json.loads(row["job_data"])
                        if isinstance(parsed, dict):
                            job_data = parsed
                    except (TypeError, ValueError):
                        job_data = {}
                if not self._job_matches_script(job_data, str(script_path or "")):
                    continue
                conn.execute("DELETE FROM pending_results WHERE job_id = ?", (row["job_id"],))
                deleted += 1
        return deleted

    def clear_queued_jobs_for_report(
        self, report_id: str, script_path: Optional[str] = None
    ) -> int:
        clean_report_id = str(report_id or "").strip()
        if not clean_report_id:
            return 0
        with self._connect() as conn:
            if script_path is None:
                deleted = int(
                    conn.execute(
                        "DELETE FROM jobs WHERE report_id = ?",
                        (clean_report_id,),
                    ).rowcount
                    or 0
                )
                return deleted

            deleted = 0
            rows = conn.execute(
                """
                SELECT job_id, job_data
                FROM jobs
                WHERE report_id = ?
                """,
                (clean_report_id,),
            ).fetchall()
            for row in rows:
                job_data = {}
                if row["job_data"]:
                    try:
                        parsed = json.loads(row["job_data"])
                        if isinstance(parsed, dict):
                            job_data = parsed
                    except (TypeError, ValueError):
                        job_data = {}
                if not self._job_matches_script(job_data, str(script_path or "")):
                    continue
                conn.execute("DELETE FROM jobs WHERE job_id = ?", (row["job_id"],))
                deleted += 1
        return deleted

    def remove_script_from_report(self, report_id: str, script_path: str) -> Dict[str, int]:
        clean_report_id, clean_script = self._normalize_report_script(report_id, script_path)
        if not clean_report_id or not clean_script:
            return {
                "removed_reference": 0,
                "removed_results": 0,
                "removed_queued_jobs": 0,
                "removed_pending_results": 0,
            }

        report_scripts = self.list_report_scripts(clean_report_id)
        has_reference = any(row.get("script_path") == clean_script for row in report_scripts)
        self.remove_report_script(clean_report_id, clean_script)
        removed_reference = 1 if has_reference else 0

        removed_results = self.clear_results_for_report(
            clean_report_id, script_path=clean_script
        )
        removed_queued_jobs = self.clear_queued_jobs_for_report(
            clean_report_id, script_path=clean_script
        )
        removed_pending_results = self.clear_pending_results_for_report(
            clean_report_id, script_path=clean_script
        )
        return {
            "removed_reference": removed_reference,
            "removed_results": removed_results,
            "removed_queued_jobs": removed_queued_jobs,
            "removed_pending_results": removed_pending_results,
        }

    def delete_report(self, report_id: str) -> Dict[str, int]:
        clean_report_id = str(report_id or "").strip()
        if not clean_report_id:
            return {
                "removed_report": 0,
                "removed_report_scripts": 0,
                "removed_results": 0,
                "removed_queued_jobs": 0,
                "removed_pending_results": 0,
            }

        removed_results = self.clear_results_for_report(clean_report_id)
        removed_queued_jobs = self.clear_queued_jobs_for_report(clean_report_id)
        removed_pending_results = self.clear_pending_results_for_report(clean_report_id)
        removed_report_scripts = len(self.reporting_repository.list_report_scripts(clean_report_id))
        removed_report = int(self.reporting_repository.delete_report(clean_report_id))
        return {
            "removed_report": removed_report,
            "removed_report_scripts": removed_report_scripts,
            "removed_results": removed_results,
            "removed_queued_jobs": removed_queued_jobs,
            "removed_pending_results": removed_pending_results,
        }

    def record_result(
        self,
        job_id: str,
        result_data: Any,
        success: bool,
        worker_id: Optional[str],
        worker_address: Optional[str],
        artifacts_manifest: Optional[List[str]] = None,
        job_data_snapshot: Optional[JobInput] = None,
        artifacts_downloaded: bool = False,
    ) -> None:
        artifact_list = [str(p) for p in (artifacts_manifest or [])]
        if job_data_snapshot is None:
            job_data_snapshot = self.get_job(job_id) or {}
        report_id = str((job_data_snapshot or {}).get("report_id") or "").strip()
        if not report_id:
            raise ValueError("job_data_snapshot.report_id is required to record a result.")
        suite_run_id = None
        if isinstance(job_data_snapshot, dict):
            suite_run_id = job_data_snapshot.get("suite_run_id")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO job_results (
                    job_id,
                    report_id,
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    report_id,
                    json.dumps(job_data_snapshot),
                    json.dumps(result_data),
                    1 if success else 0,
                    worker_id,
                    worker_address,
                    time.time(),
                    suite_run_id,
                    json.dumps(artifact_list),
                    1 if artifacts_downloaded else 0,
                ),
            )

    def store_pending_result(
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
        report_id = str((job_data_snapshot or {}).get("report_id") or "").strip()
        if not report_id:
            raise ValueError("job_data_snapshot.report_id is required to store pending results.")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO pending_results (
                    job_id,
                    report_id,
                    job_data,
                    result_data,
                    success,
                    worker_id,
                    worker_address,
                    received_at,
                    artifacts_manifest,
                    sync_attempts,
                    last_error
                )
                VALUES (
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    COALESCE((SELECT sync_attempts FROM pending_results WHERE job_id = ?), 0),
                    ''
                )
                """,
                (
                    job_id,
                    report_id,
                    json.dumps(job_data_snapshot),
                    json.dumps(result_data),
                    1 if success else 0,
                    worker_id,
                    worker_address,
                    time.time(),
                    json.dumps(artifact_list),
                    job_id,
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
                    SELECT job_id, report_id, job_data, result_data, success, worker_id, worker_address, completed_at, suite_run_id, artifacts_manifest, artifacts_downloaded
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
                    SELECT job_id, report_id, job_data, result_data, success, worker_id, worker_address, completed_at, suite_run_id, artifacts_manifest, artifacts_downloaded
                    FROM job_results
                    ORDER BY completed_at DESC
                    LIMIT ?
                    OFFSET ?
                    """,
                    (safe_limit, safe_offset),
                )
            rows = cur.fetchall()
        return [self._row_to_result(row) for row in rows]

    def get_result(self, job_id: str) -> Optional[JobResult]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT job_id, report_id, job_data, result_data, success, worker_id, worker_address, completed_at, suite_run_id, artifacts_manifest, artifacts_downloaded
                FROM job_results
                WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
        return self._row_to_result(row) if row else None

    def list_results_for_suite(self, suite_run_id: str, limit: int = 200) -> List[JobResult]:
        return self.list_results(limit=limit, suite_run_id=suite_run_id)

    def list_results_for_report(self, report_id: str, limit: int = 5000) -> List[JobResult]:
        clean_report_id = str(report_id or "").strip()
        if not clean_report_id:
            return []
        safe_limit = max(1, int(limit))
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT job_id, report_id, job_data, result_data, success, worker_id, worker_address, completed_at, suite_run_id, artifacts_manifest, artifacts_downloaded
                FROM job_results
                WHERE report_id = ?
                ORDER BY completed_at DESC
                LIMIT ?
                """,
                (clean_report_id, safe_limit),
            )
            rows = cur.fetchall()
        return [self._row_to_result(row) for row in rows]

    def list_pending_artifacts(self) -> List[JobResult]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT job_id, report_id, job_data, result_data, success, worker_id, worker_address, completed_at, artifacts_manifest, artifacts_downloaded, suite_run_id
                FROM job_results
                WHERE artifacts_downloaded = 0 AND artifacts_manifest IS NOT NULL
                """
            )
            rows = cur.fetchall()
        return [self._row_to_result(row) for row in rows]

    def list_pending_results(self) -> List[PendingResult]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT job_id, report_id, job_data, result_data, success, worker_id, worker_address, received_at, artifacts_manifest, sync_attempts, last_error
                FROM pending_results
                ORDER BY received_at ASC
                """
            )
            rows = cur.fetchall()
        return [self._row_to_pending_result(row) for row in rows]

    def get_pending_result(self, job_id: str) -> Optional[PendingResult]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT job_id, report_id, job_data, result_data, success, worker_id, worker_address, received_at, artifacts_manifest, sync_attempts, last_error
                FROM pending_results
                WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
        return self._row_to_pending_result(row) if row else None

    def mark_pending_result_error(self, job_id: str, last_error: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE pending_results
                SET sync_attempts = sync_attempts + 1,
                    last_error = ?
                WHERE job_id = ?
                """,
                ((last_error or "")[:2000], job_id),
            )

    def delete_pending_result(self, job_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM pending_results WHERE job_id = ?", (job_id,))

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
