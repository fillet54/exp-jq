"""Interactive local CLI TUI for running scripts with one in-process worker.

Usage:
  automationv3-local [--db=<path>] [--config=<path>] [--artifacts-dir=<path>] [--cache-dir=<path>]

Options:
  --db=<path>            SQLite database path [default: jobqueue.db]
  --config=<path>        Persistent TUI config path [default: ~/.automationv3/local_tui.json]
  --artifacts-dir=<path> Directory for run artifacts [default: artifacts]
  --cache-dir=<path>     FS cache directory for UUT snapshots [default: .fscache]
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from automationv3.framework.rst import expand_rvt_variations
from automationv3.jobqueue import JobQueue
from automationv3.jobqueue.executor import run_job
from automationv3.jobqueue.fscache import snapshot_tree
from automationv3.reporting import ReportingRepository, ReportingService, UUTConfig, UUTStore

try:  # pragma: no cover - optional at runtime
    from prompt_toolkit import prompt as pt_prompt
    from prompt_toolkit.completion import FuzzyCompleter, WordCompleter
except Exception:  # pragma: no cover - fallback behavior is tested
    pt_prompt = None
    FuzzyCompleter = None
    WordCompleter = None


SCRATCH_REPORT_ID = "__scratch__"
SCRATCH_REPORT_TITLE = "Scratch"
LOCAL_WORKER_ID = "local-worker"
LOCAL_WORKER_ADDRESS = "local://worker"


@dataclass
class LocalTUIConfig:
    scripts_root: str = ""
    uut_path: str = ""
    uut_name: str = "Local UUT"


@dataclass
class ScriptEntry:
    relpath: str
    title: str
    meta: Dict[str, List[str]]
    path: str


def _expand_path(value: str) -> str:
    return str(Path(os.path.expanduser(str(value or "").strip())).resolve())


def _normalize_path(value: str) -> str:
    return os.path.normcase(os.path.abspath(value))


def _same_path(left: str, right: str) -> bool:
    return _normalize_path(left) == _normalize_path(right)


class LocalTUIConfigStore:
    """Persist/load TUI config to a JSON file."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> LocalTUIConfig:
        if not self.path.exists():
            return LocalTUIConfig()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (TypeError, ValueError):
            return LocalTUIConfig()
        if not isinstance(payload, dict):
            return LocalTUIConfig()
        return LocalTUIConfig(
            scripts_root=str(payload.get("scripts_root") or "").strip(),
            uut_path=str(payload.get("uut_path") or "").strip(),
            uut_name=str(payload.get("uut_name") or "Local UUT").strip() or "Local UUT",
        )

    def save(self, config: LocalTUIConfig) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(asdict(config), indent=2), encoding="utf-8")


def parse_meta_from_lines(lines: List[str]) -> Dict[str, List[str]]:
    meta: Dict[str, List[str]] = {"requirements": [], "tags": [], "subsystem": []}
    in_meta = False
    for line in lines:
        if line.strip().startswith(".. meta::"):
            in_meta = True
            continue
        if not in_meta:
            continue
        if not line.startswith("   "):
            break
        stripped = line.strip()
        if not stripped.startswith(":") or ":" not in stripped[1:]:
            continue
        key, raw_value = stripped[1:].split(":", 1)
        values = [part.strip() for part in raw_value.split(",") if part.strip()]
        if key.strip() in meta:
            meta[key.strip()].extend(values)
        else:
            meta[key.strip()] = values
    return meta


def extract_rst_title(lines: List[str], fallback: str = "") -> str:
    adornments = set("=-~^\"`*+#:.")
    for idx in range(len(lines) - 1):
        title = lines[idx].strip()
        underline = lines[idx + 1].strip()
        if (
            title
            and underline
            and len(underline) >= len(title)
            and len(set(underline)) == 1
            and underline[0] in adornments
        ):
            return title
    return fallback


def discover_scripts(root: Path) -> List[ScriptEntry]:
    entries: List[ScriptEntry] = []
    if not root.exists():
        return entries
    for path in sorted(root.rglob("*.rst")):
        try:
            text = path.read_text(encoding="utf-8")
        except (FileNotFoundError, OSError):
            continue
        lines = text.splitlines()
        relpath = path.relative_to(root).as_posix()
        title = extract_rst_title(lines, fallback=path.stem)
        entries.append(
            ScriptEntry(
                relpath=relpath,
                title=title,
                meta=parse_meta_from_lines(lines),
                path=str(path),
            )
        )
    return entries


def ensure_scratch_report(reporting: ReportingService) -> Dict[str, Any]:
    report = reporting.get_report(SCRATCH_REPORT_ID)
    if report:
        return report
    return reporting.create_report(
        title=SCRATCH_REPORT_TITLE,
        description="Implicit report for ad-hoc local runs.",
        report_id=SCRATCH_REPORT_ID,
    )


def ensure_uut_config(store: UUTStore, uut_path: str, uut_name: str = "Local UUT") -> UUTConfig:
    resolved_path = _expand_path(uut_path)
    for row in store.list():
        if _same_path(row.path, resolved_path):
            snap = store.snapshot(row.uut_id)
            return snap or row
    config = store.add(name=(uut_name or "Local UUT"), path=resolved_path)
    snap = store.snapshot(config.uut_id)
    return snap or config


def build_jobs_for_script(
    script_entry: ScriptEntry,
    *,
    report_id: str,
    uut_config: UUTConfig,
    scripts_root: Path,
    scripts_tree: str | None,
    framework_version: str = "",
) -> List[Dict[str, Any]]:
    script_path = Path(script_entry.path)
    text = script_path.read_text(encoding="utf-8")
    base_job: Dict[str, Any] = {
        "file": script_entry.relpath,
        "uut": uut_config.name,
        "report_id": report_id,
        "uut_tree": uut_config.last_tree_sha,
        "uut_id": uut_config.uut_id,
        "meta": script_entry.meta,
        "framework_version": framework_version,
        "scripts_tree": scripts_tree,
        "scripts_root": str(scripts_root),
        "suite_name": "",
        "suite_run_id": "",
    }

    variations = expand_rvt_variations(text)
    if not variations:
        return [base_job]

    total = len(variations)
    jobs: List[Dict[str, Any]] = []
    for index, variation in enumerate(variations, start=1):
        variation_name = str(variation.get("name") or "").strip() or f"variation-{index}"
        components = [
            str(component).strip()
            for component in (variation.get("components") or [])
            if str(component).strip()
        ]
        bindings = {
            str(symbol): str(value)
            for symbol, value in (variation.get("bindings") or {}).items()
            if str(symbol).strip()
        }
        row = dict(base_job)
        row["is_variation_job"] = True
        row["variation_name"] = variation_name
        row["variation_components"] = components
        row["variation_bindings"] = bindings
        row["variation_index"] = index
        row["variation_total"] = total
        jobs.append(row)
    return jobs


class LocalWorkerRuntime:
    """In-process worker loop that executes queued jobs and records results."""

    def __init__(
        self,
        queue: JobQueue,
        artifacts_dir: Path,
        worker_id: str = LOCAL_WORKER_ID,
        worker_address: str = LOCAL_WORKER_ADDRESS,
    ) -> None:
        self.queue = queue
        self.worker_id = worker_id
        self.worker_address = worker_address
        self.artifacts_dir = Path(artifacts_dir)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

        self._events: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._started = False
        self._busy = False
        self._current_job_id: Optional[str] = None

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=timeout)

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "busy": bool(self._busy),
                "current_job_id": self._current_job_id,
            }

    def events_since(self, job_id: str, last_seq: int = -1) -> List[Dict[str, Any]]:
        with self._lock:
            rows = list(self._events.get(job_id, []))
        if last_seq < 0:
            return rows
        out: List[Dict[str, Any]] = []
        for row in rows:
            seq = row.get("seq")
            if isinstance(seq, int):
                if seq > last_seq:
                    out.append(row)
            else:
                out.append(row)
        return out

    def _append_event(self, job_id: str, event: Dict[str, Any]) -> None:
        with self._lock:
            self._events.setdefault(job_id, []).append(dict(event))

    def _set_active_job(self, job_id: Optional[str]) -> None:
        with self._lock:
            self._current_job_id = job_id
            self._busy = bool(job_id)

    def _run_one(self, job: Dict[str, Any]) -> None:
        job_id = str(job.get("job_id") or "").strip()
        if not job_id:
            return
        self._set_active_job(job_id)

        try:
            result_payload = run_job(
                job,
                artifacts_dir=str(self.artifacts_dir),
                observer_callback=lambda event: self._append_event(job_id, event),
            )
            summary = dict(result_payload.get("summary") or {})
            success = bool(result_payload.get("success", True))
            artifacts = [str(p) for p in (result_payload.get("artifacts") or [])]
        except Exception as exc:  # pragma: no cover - defensive
            logging.getLogger("automationv3.local_tui").exception(
                "Local worker execution failed for job %s", job_id
            )
            success = False
            summary = {
                "status": "failed",
                "error": str(exc),
                "observer_events": self.events_since(job_id, -1),
            }
            artifacts = []

        self.queue.record_result(
            job_id=job_id,
            result_data=summary,
            success=success,
            worker_id=self.worker_id,
            worker_address=self.worker_address,
            artifacts_manifest=artifacts,
            job_data_snapshot=job,
            artifacts_downloaded=True,
        )
        self.queue.remove_job(job_id)
        self._set_active_job(None)

    def _loop(self) -> None:
        while not self._stop.is_set():
            job = self.queue.get_next_job()
            if not job:
                time.sleep(0.1)
                continue
            self._run_one(job)


class LocalAutomationTUI:
    """Menu-driven local runner interface."""

    def __init__(
        self,
        *,
        db_path: str,
        config_path: Path,
        artifacts_dir: str,
        cache_dir: str,
    ) -> None:
        self.log = logging.getLogger("automationv3.local_tui")
        repository = ReportingRepository(db_path=db_path)
        self.queue = JobQueue(db_path=db_path, reporting_repository=repository)
        self.reporting = ReportingService(repository=repository, queue=self.queue)
        self.uut_store = UUTStore(db_path=db_path, cache_dir=cache_dir)

        self.config_store = LocalTUIConfigStore(config_path)
        self.config = self.config_store.load()
        self.runtime = LocalWorkerRuntime(self.queue, artifacts_dir=Path(artifacts_dir))

    def run(self) -> None:
        ensure_scratch_report(self.reporting)
        self.runtime.start()
        try:
            self._ensure_configured(force=False)
            self._main_loop()
        finally:
            self.runtime.stop()

    def _main_loop(self) -> None:
        while True:
            print("")
            print("AutomationV3 Local")
            print("------------------")
            print("1. Run script (Scratch)")
            print("2. Run script (Report)")
            print("3. Show scratch history")
            print("4. Configure paths")
            print("5. Exit")
            choice = input("Select: ").strip().lower()
            if choice == "1":
                self._run_script_interactive(use_scratch=True)
            elif choice == "2":
                self._run_script_interactive(use_scratch=False)
            elif choice == "3":
                self._show_scratch_history()
            elif choice == "4":
                self._ensure_configured(force=True)
            elif choice in {"5", "q", "quit", "exit"}:
                return
            else:
                print("Invalid selection.")

    def _prompt_text(self, message: str, default: str = "") -> str:
        if pt_prompt is None:
            raw = input(f"{message} [{default}]: ").strip()
            return raw or default
        return str(pt_prompt(f"{message}: ", default=default)).strip()

    def _prompt_existing_directory(self, message: str, default: str = "") -> str:
        while True:
            candidate = self._prompt_text(message, default=default).strip()
            expanded = _expand_path(candidate)
            if Path(expanded).exists() and Path(expanded).is_dir():
                return expanded
            print(f"Directory not found: {expanded}")

    def _ensure_configured(self, force: bool = False) -> None:
        scripts_root = self.config.scripts_root
        uut_path = self.config.uut_path

        if force or not scripts_root or not Path(scripts_root).is_dir():
            scripts_root = self._prompt_existing_directory(
                "Scripts folder",
                default=scripts_root or "scripts",
            )
        if force or not uut_path or not Path(uut_path).is_dir():
            uut_path = self._prompt_existing_directory(
                "UUT folder",
                default=uut_path or ".",
            )

        if force or not (self.config.uut_name or "").strip():
            uut_name = self._prompt_text(
                "UUT display name",
                default=self.config.uut_name or "Local UUT",
            ).strip() or "Local UUT"
        else:
            uut_name = self.config.uut_name

        self.config = LocalTUIConfig(
            scripts_root=scripts_root,
            uut_path=uut_path,
            uut_name=uut_name,
        )
        self.config_store.save(self.config)
        print(f"Saved config to {self.config_store.path}")

    def _select_script(self) -> Optional[ScriptEntry]:
        scripts_root = Path(self.config.scripts_root)
        scripts = discover_scripts(scripts_root)
        if not scripts:
            print(f"No .rst scripts found under {scripts_root}")
            return None

        mapping: Dict[str, ScriptEntry] = {}
        for script in scripts:
            label = f"{script.title} [{script.relpath}]"
            mapping[label] = script

        print(f"Discovered {len(mapping)} scripts.")
        if pt_prompt is None or FuzzyCompleter is None or WordCompleter is None:
            for idx, label in enumerate(mapping.keys(), start=1):
                print(f"{idx:3d}. {label}")
            raw = input("Pick script number: ").strip()
            if not raw:
                return None
            try:
                index = int(raw)
            except ValueError:
                return None
            labels = list(mapping.keys())
            if index < 1 or index > len(labels):
                return None
            return mapping[labels[index - 1]]

        labels = list(mapping.keys())
        completer = FuzzyCompleter(WordCompleter(labels, sentence=True))
        while True:
            value = str(
                pt_prompt(
                    "Script (fuzzy search): ",
                    completer=completer,
                    complete_while_typing=True,
                )
            ).strip()
            if not value:
                return None
            if value in mapping:
                return mapping[value]
            for label, script in mapping.items():
                if script.relpath == value:
                    return script
                if value.lower() in label.lower():
                    return script
            print("No match found. Enter a full label/path or try another search.")

    def _choose_report_id(self) -> Optional[str]:
        reports = [
            row
            for row in self.reporting.list_reports(limit=500)
            if str(row.get("report_id") or "") != SCRATCH_REPORT_ID
        ]
        print("")
        if reports:
            print("Existing reports:")
            for index, row in enumerate(reports, start=1):
                title = str(row.get("title") or row.get("report_id") or "").strip()
                report_id = str(row.get("report_id") or "").strip()
                print(f"{index:3d}. {title} ({report_id})")
        else:
            print("No existing reports.")

        raw = input("Choose number, 'n' for new report, or Enter to cancel: ").strip().lower()
        if not raw:
            return None
        if raw == "n":
            title = input("New report title: ").strip()
            if not title:
                print("Title required.")
                return None
            description = input("Description (optional): ").strip()
            report = self.reporting.create_report(title=title, description=description)
            return str(report.get("report_id") or "").strip() or None
        try:
            index = int(raw)
        except ValueError:
            print("Invalid selection.")
            return None
        if index < 1 or index > len(reports):
            print("Invalid selection.")
            return None
        return str(reports[index - 1].get("report_id") or "").strip() or None

    def _build_jobs(
        self,
        script_entry: ScriptEntry,
        report_id: str,
        uut_config: UUTConfig,
    ) -> List[Dict[str, Any]]:
        scripts_root = Path(self.config.scripts_root).resolve()
        try:
            scripts_tree = str(snapshot_tree(str(scripts_root), cache_dir=".fscache_scripts"))
        except Exception:
            self.log.exception("Failed to snapshot scripts tree at %s", scripts_root)
            scripts_tree = ""
        return build_jobs_for_script(
            script_entry,
            report_id=report_id,
            uut_config=uut_config,
            scripts_root=scripts_root,
            scripts_tree=scripts_tree,
        )

    def _run_script_interactive(self, use_scratch: bool) -> None:
        script_entry = self._select_script()
        if not script_entry:
            return

        if use_scratch:
            report_id = str(ensure_scratch_report(self.reporting).get("report_id") or "")
        else:
            report_id = str(self._choose_report_id() or "")
        if not report_id:
            return

        uut = ensure_uut_config(
            self.uut_store,
            uut_path=self.config.uut_path,
            uut_name=self.config.uut_name,
        )
        jobs = self._build_jobs(script_entry, report_id, uut)
        queued = self.queue.add_job(jobs, priority=0)
        job_ids = [queued] if isinstance(queued, str) else list(queued)
        if not job_ids:
            print("No jobs queued.")
            return
        print(f"Queued {len(job_ids)} job(s) for {script_entry.relpath}.")
        self._monitor_jobs(job_ids)

    def _format_event(self, event: Dict[str, Any]) -> str:
        kind = str(event.get("kind") or "")
        if kind == "script_begin":
            return "script started"
        if kind == "script_end":
            return f"script finished ({'PASS' if event.get('passed') else 'FAIL'})"
        if kind == "block_start":
            block = str(event.get("block") or "block")
            args = " ".join([str(item) for item in (event.get("args") or [])]).strip()
            return f"step: ({block}{(' ' + args) if args else ''})"
        if kind == "block_end":
            status = "PASS" if bool(event.get("passed")) else "FAIL"
            duration = event.get("duration")
            if isinstance(duration, (int, float)):
                return f"step result: {status} ({duration:.3f}s)"
            return f"step result: {status}"
        if kind == "execution_error":
            return f"execution error: {event.get('message')}"
        return ""

    def _monitor_jobs(self, job_ids: List[str]) -> None:
        pending = set([str(job_id) for job_id in job_ids if str(job_id).strip()])
        last_seq: Dict[str, int] = {job_id: -1 for job_id in pending}
        while pending:
            for job_id in list(pending):
                for event in self.runtime.events_since(job_id, last_seq=last_seq.get(job_id, -1)):
                    seq = event.get("seq")
                    if isinstance(seq, int):
                        last_seq[job_id] = max(last_seq.get(job_id, -1), seq)
                    line = self._format_event(event)
                    if line:
                        print(f"[{job_id}] {line}")
                result = self.queue.get_result(job_id)
                if not result:
                    continue
                result_data = result.get("result_data") or {}
                duration = result_data.get("duration_seconds")
                status = "PASS" if bool(result.get("success")) else "FAIL"
                if isinstance(duration, (int, float)):
                    print(f"[{job_id}] completed: {status} ({float(duration):.2f}s)")
                else:
                    print(f"[{job_id}] completed: {status}")
                pending.discard(job_id)
            if pending:
                time.sleep(0.1)

    def _show_scratch_history(self) -> None:
        ensure_scratch_report(self.reporting)
        queued = [
            row for row in self.queue.list_jobs() if str(row.get("report_id") or "") == SCRATCH_REPORT_ID
        ]
        results = self.queue.list_results_for_report(SCRATCH_REPORT_ID, limit=100)

        print("")
        print("Scratch Queue")
        print("-------------")
        if not queued:
            print("No queued scratch jobs.")
        else:
            for row in queued:
                script = str(row.get("file") or "unknown")
                job_id = str(row.get("job_id") or "")
                print(f"- queued {job_id} :: {script}")

        print("")
        print("Scratch Results")
        print("---------------")
        if not results:
            print("No scratch results yet.")
            return
        for row in results[:25]:
            job_id = str(row.get("job_id") or "")
            script = str((row.get("job_data") or {}).get("file") or "unknown")
            success = "PASS" if bool(row.get("success")) else "FAIL"
            completed = row.get("completed_at")
            if isinstance(completed, (int, float)):
                completed_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(completed))
            else:
                completed_text = "unknown"
            print(f"- {completed_text} :: {success} :: {script} ({job_id})")


def main() -> None:
    from docopt import docopt

    logging.basicConfig(level=logging.INFO)
    args = docopt(__doc__)
    db_path = str(args.get("--db") or "jobqueue.db").strip()
    config_path = Path(_expand_path(str(args.get("--config") or "~/.automationv3/local_tui.json")))
    artifacts_dir = _expand_path(str(args.get("--artifacts-dir") or "artifacts"))
    cache_dir = _expand_path(str(args.get("--cache-dir") or ".fscache"))

    app = LocalAutomationTUI(
        db_path=db_path,
        config_path=config_path,
        artifacts_dir=artifacts_dir,
        cache_dir=cache_dir,
    )
    app.run()


if __name__ == "__main__":
    main()
