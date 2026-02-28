"""Reporting persistence layer.

Owns report metadata, report-to-script tracking, and report-to-requirement
tracking in SQLite.
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Optional


ReportRecord = Dict[str, Any]
ReportScript = Dict[str, Any]
ReportRequirement = Dict[str, Any]


class ReportingRepository:
    """SQLite-backed repository for reporting entities."""

    def __init__(self, db_path: str = "jobqueue.db") -> None:
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _normalize_report_script(self, report_id: Any, script_path: Any) -> tuple[str, str]:
        return str(report_id or "").strip(), str(script_path or "").strip()

    def _normalize_report_requirement(
        self, report_id: Any, requirement_id: Any
    ) -> tuple[str, str]:
        return str(report_id or "").strip(), str(requirement_id or "").strip()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reports (
                    report_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT,
                    created_at REAL NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS report_scripts (
                    report_id TEXT NOT NULL,
                    script_path TEXT NOT NULL,
                    job_template TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY (report_id) REFERENCES reports(report_id) ON DELETE CASCADE,
                    PRIMARY KEY (report_id, script_path)
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS report_requirements (
                    report_id TEXT NOT NULL,
                    requirement_id TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY (report_id) REFERENCES reports(report_id) ON DELETE CASCADE,
                    PRIMARY KEY (report_id, requirement_id)
                );
                """
            )

    def create_report(self, report_id: str, title: str, description: str = "") -> ReportRecord:
        clean_report_id = str(report_id or "").strip()
        clean_title = (title or "").strip()
        if not clean_report_id:
            raise ValueError("report_id is required")
        if not clean_title:
            raise ValueError("Report title is required.")
        created_at = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO reports (report_id, title, description, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (clean_report_id, clean_title, (description or "").strip(), created_at),
            )
        return {
            "report_id": clean_report_id,
            "title": clean_title,
            "description": (description or "").strip(),
            "created_at": created_at,
        }

    def get_report(self, report_id: str) -> Optional[ReportRecord]:
        clean_report_id = str(report_id or "").strip()
        if not clean_report_id:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT report_id, title, description, created_at
                FROM reports
                WHERE report_id = ?
                """,
                (clean_report_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_reports(self, limit: int = 200) -> List[ReportRecord]:
        safe_limit = max(1, int(limit))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT report_id, title, description, created_at
                FROM reports
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def delete_report(self, report_id: str) -> int:
        clean_report_id = str(report_id or "").strip()
        if not clean_report_id:
            return 0
        with self._connect() as conn:
            conn.execute("DELETE FROM report_scripts WHERE report_id = ?", (clean_report_id,))
            conn.execute("DELETE FROM report_requirements WHERE report_id = ?", (clean_report_id,))
            removed = int(
                conn.execute(
                    "DELETE FROM reports WHERE report_id = ?",
                    (clean_report_id,),
                ).rowcount
                or 0
            )
        return removed

    def track_report_script(
        self,
        report_id: str,
        script_path: str,
        job_template: Optional[Dict[str, Any]] = None,
    ) -> None:
        clean_report_id, clean_script = self._normalize_report_script(report_id, script_path)
        if not clean_report_id or not clean_script:
            return
        now = time.time()
        template = dict(job_template or {})
        with self._connect() as conn:
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

    def list_report_scripts(self, report_id: str) -> List[ReportScript]:
        clean_report_id = str(report_id or "").strip()
        if not clean_report_id:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT report_id, script_path, job_template, created_at, updated_at
                FROM report_scripts
                WHERE report_id = ?
                ORDER BY script_path ASC
                """,
                (clean_report_id,),
            ).fetchall()
        out: List[ReportScript] = []
        for row in rows:
            template: Dict[str, Any] = {}
            raw_template = row["job_template"]
            if raw_template:
                try:
                    parsed = json.loads(raw_template)
                    if isinstance(parsed, dict):
                        template = parsed
                except (TypeError, ValueError):
                    template = {}
            out.append(
                {
                    "report_id": row["report_id"],
                    "script_path": row["script_path"],
                    "job_template": template,
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
            )
        return out

    def remove_report_script(self, report_id: str, script_path: str) -> int:
        clean_report_id, clean_script = self._normalize_report_script(report_id, script_path)
        if not clean_report_id or not clean_script:
            return 0
        with self._connect() as conn:
            removed = int(
                conn.execute(
                    """
                    DELETE FROM report_scripts
                    WHERE report_id = ? AND script_path = ?
                    """,
                    (clean_report_id, clean_script),
                ).rowcount
                or 0
            )
        return removed

    def add_report_requirement(self, report_id: str, requirement_id: str) -> None:
        clean_report_id, clean_requirement = self._normalize_report_requirement(
            report_id, requirement_id
        )
        if not clean_report_id or not clean_requirement:
            return
        now = time.time()
        with self._connect() as conn:
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

    def add_report_requirements(self, report_id: str, requirement_ids: Iterable[str]) -> None:
        for requirement_id in requirement_ids:
            self.add_report_requirement(report_id, requirement_id)

    def list_report_requirements(self, report_id: str) -> List[ReportRequirement]:
        clean_report_id = str(report_id or "").strip()
        if not clean_report_id:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT report_id, requirement_id, created_at, updated_at
                FROM report_requirements
                WHERE report_id = ?
                ORDER BY requirement_id ASC
                """,
                (clean_report_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def remove_report_requirement(self, report_id: str, requirement_id: str) -> int:
        clean_report_id, clean_requirement = self._normalize_report_requirement(
            report_id, requirement_id
        )
        if not clean_report_id or not clean_requirement:
            return 0
        with self._connect() as conn:
            removed = int(
                conn.execute(
                    """
                    DELETE FROM report_requirements
                    WHERE report_id = ? AND requirement_id = ?
                    """,
                    (clean_report_id, clean_requirement),
                ).rowcount
                or 0
            )
        return removed


__all__ = [
    "ReportingRepository",
    "ReportRecord",
    "ReportScript",
    "ReportRequirement",
]
