"""Reporting application service.

Coordinates report persistence with queue data cleanup operations.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, Iterable, List


class ReportingService:
    """High-level reporting operations composed over repository + queue."""

    def __init__(self, repository, queue) -> None:
        self.repository = repository
        self.queue = queue

    def create_report(
        self,
        title: str,
        description: str = "",
        report_id: str | None = None,
    ) -> Dict[str, Any]:
        return self.repository.create_report(
            report_id=str(report_id or uuid.uuid4().hex),
            title=title,
            description=description,
        )

    def get_report(self, report_id: str):
        return self.repository.get_report(report_id)

    def list_reports(self, limit: int = 200):
        return self.repository.list_reports(limit=limit)

    def list_report_scripts(self, report_id: str):
        return self.repository.list_report_scripts(report_id)

    def track_report_script(
        self,
        report_id: str,
        script_path: str,
        job_template: Dict[str, Any] | None = None,
    ) -> None:
        self.repository.track_report_script(
            report_id=report_id,
            script_path=script_path,
            job_template=job_template,
        )

    def remove_report_script(self, report_id: str, script_path: str) -> int:
        return self.repository.remove_report_script(report_id, script_path)

    def list_report_requirements(self, report_id: str):
        return self.repository.list_report_requirements(report_id)

    def add_report_requirement(self, report_id: str, requirement_id: str) -> None:
        self.repository.add_report_requirement(report_id, requirement_id)

    def add_report_requirements(self, report_id: str, requirement_ids: Iterable[str]) -> None:
        self.repository.add_report_requirements(report_id, requirement_ids)

    def remove_report_requirement(self, report_id: str, requirement_id: str) -> int:
        return self.repository.remove_report_requirement(report_id, requirement_id)

    def remove_script_from_report(self, report_id: str, script_path: str) -> Dict[str, int]:
        removed_reference = self.repository.remove_report_script(report_id, script_path)
        removed_results = self.queue.clear_results_for_report(report_id, script_path=script_path)
        removed_queued_jobs = self.queue.clear_queued_jobs_for_report(report_id, script_path=script_path)
        removed_pending_results = self.queue.clear_pending_results_for_report(
            report_id, script_path=script_path
        )
        return {
            "removed_reference": int(removed_reference),
            "removed_results": int(removed_results),
            "removed_queued_jobs": int(removed_queued_jobs),
            "removed_pending_results": int(removed_pending_results),
        }

    def clear_report_results(self, report_id: str) -> Dict[str, int]:
        removed_results = self.queue.clear_results_for_report(report_id)
        removed_pending_results = self.queue.clear_pending_results_for_report(report_id)
        return {
            "removed_results": int(removed_results),
            "removed_pending_results": int(removed_pending_results),
        }

    def delete_report(self, report_id: str) -> Dict[str, int]:
        removed_results = self.queue.clear_results_for_report(report_id)
        removed_queued_jobs = self.queue.clear_queued_jobs_for_report(report_id)
        removed_pending_results = self.queue.clear_pending_results_for_report(report_id)

        report_scripts = self.repository.list_report_scripts(report_id)
        removed_report_scripts = len(report_scripts)
        removed_report = self.repository.delete_report(report_id)
        return {
            "removed_report": int(removed_report),
            "removed_report_scripts": int(removed_report_scripts),
            "removed_results": int(removed_results),
            "removed_queued_jobs": int(removed_queued_jobs),
            "removed_pending_results": int(removed_pending_results),
        }


__all__ = ["ReportingService"]
