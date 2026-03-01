"""Interactive local CLI TUI for running scripts with one in-process worker.

Usage:
  automationv3-local [--db=<path>] [--config=<path>] [--artifacts-dir=<path>] [--cache-dir=<path>]

Options:
  --db=<path>            SQLite database path [default: jobqueue.db]
  --config=<path>        Persistent TUI config path [default: ~/.automationv3/local_tui.toml]
  --artifacts-dir=<path> Directory for run artifacts [default: artifacts]
  --cache-dir=<path>     FS cache directory for UUT snapshots [default: .fscache]
"""

from __future__ import annotations

import json
import logging
import os
import re
import select
import shutil
import sys
import threading
import textwrap
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from automationv3.framework.rst import expand_rvt_variations, rvt_script
from automationv3.jobqueue import JobQueue
from automationv3.jobqueue.executor import run_job
from automationv3.jobqueue.fscache import snapshot_tree
from automationv3.reporting import ReportingRepository, ReportingService, UUTConfig, UUTStore
from docutils import core as docutils_core
from docutils import nodes as docutils_nodes

try:  # pragma: no cover - optional at runtime
    from prompt_toolkit import PromptSession, prompt as pt_prompt
    from prompt_toolkit.completion import FuzzyCompleter, WordCompleter
    from prompt_toolkit.styles import Style
except Exception:  # pragma: no cover - fallback behavior is tested
    PromptSession = None
    pt_prompt = None
    FuzzyCompleter = None
    WordCompleter = None
    Style = None

try:  # Python 3.11+
    import tomllib as _toml  # type: ignore[attr-defined]
except ModuleNotFoundError:  # Python 3.9/3.10
    import tomli as _toml


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
    """Persist/load TUI config to a TOML file."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> LocalTUIConfig:
        if not self.path.exists():
            return LocalTUIConfig()
        try:
            payload = _toml.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
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
        self.path.write_text(self._dump_toml_payload(asdict(config)), encoding="utf-8")

    @staticmethod
    def _dump_toml_payload(payload: Dict[str, Any]) -> str:
        ordered_keys = ["scripts_root", "uut_path", "uut_name"]
        lines = []
        for key in ordered_keys:
            value = str(payload.get(key) or "")
            lines.append(f"{key} = {json.dumps(value)}")
        return "\n".join(lines) + "\n"


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


class _EscapeDetector:
    """Non-blocking escape key detector for live-follow mode."""

    def __init__(self) -> None:
        self._fd = None
        self._old_termios = None
        self._enabled = False

    def __enter__(self):
        if os.name == "nt":
            self._enabled = True
            return self

        try:
            import termios
            import tty
        except Exception:
            return self
        try:
            if not sys.stdin.isatty():
                return self
            fd = sys.stdin.fileno()
            old_attrs = termios.tcgetattr(fd)
            tty.setcbreak(fd)
            self._fd = fd
            self._old_termios = old_attrs
            self._enabled = True
        except Exception:
            self._fd = None
            self._old_termios = None
            self._enabled = False
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if os.name == "nt":
            return
        if self._fd is None or self._old_termios is None:
            return
        try:
            import termios

            termios.tcsetattr(self._fd, termios.TCSADRAIN, self._old_termios)
        except Exception:
            pass

    def pressed(self) -> bool:
        if not self._enabled:
            return False
        if os.name == "nt":
            try:
                import msvcrt
            except Exception:
                return False
            while msvcrt.kbhit():
                ch = msvcrt.getch()
                if ch == b"\x1b":
                    return True
            return False

        if self._fd is None:
            return False
        try:
            ready, _, _ = select.select([self._fd], [], [], 0)
            if not ready:
                return False
            ch = os.read(self._fd, 1)
            return ch == b"\x1b"
        except Exception:
            return False


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
        self._current_job_ids: List[str] = []
        self._current_last_seq: Dict[str, int] = {}
        self._current_completion_announced: set[str] = set()
        self._current_focus_job_id: Optional[str] = None
        self._current_job_files: Dict[str, str] = {}
        self._current_prompt_hint_shown = False
        self._active_step_state: Dict[str, Dict[str, Any]] = {}
        self._transient_line_visible = False
        self._transient_line_text = ""
        self._prompt_session = None
        if PromptSession is not None and Style is not None:
            self._prompt_session = PromptSession(
                message=[("class:promptchar", "> ")],
                style=Style.from_dict(
                    {
                        "promptchar": "#8dd6ff bold",
                        "placeholder": "#6b7280 italic",
                    }
                ),
            )

    def run(self) -> None:
        ensure_scratch_report(self.reporting)
        self.runtime.start()
        try:
            self._ensure_configured(force=False)
            self._main_loop()
        finally:
            self.runtime.stop()

    def _main_loop(self) -> None:
        print("Quick Run mode: type a script title/path to run in Scratch.")
        print("Commands: /report  /current  /history  /config  /exit")
        while True:
            if self._current_has_pending() and not self._current_prompt_hint_shown:
                print("Run in progress. Type /current to follow output.")
                self._current_prompt_hint_shown = True
            action, script_entry = self._prompt_quick_run_or_action()
            if action == "scratch":
                if script_entry:
                    self._run_script_interactive(use_scratch=True, script_entry=script_entry)
                continue
            if action in {"report", "2"}:
                self._run_script_interactive(use_scratch=False)
            elif action in {"history", "3"}:
                self._show_scratch_history()
            elif action in {"current"}:
                self._show_current_run()
            elif action in {"config", "4"}:
                self._ensure_configured(force=True)
            elif action in {"exit", "5", "q", "quit"}:
                return
            elif action:
                print("Invalid selection.")

    def _prompt_quick_run_or_action(self) -> tuple[str, Optional[ScriptEntry]]:
        scripts_root = Path(self.config.scripts_root)
        scripts = discover_scripts(scripts_root)
        if not scripts:
            print(f"No .rst scripts found under {scripts_root}")
            return ("config", None)

        mapping: Dict[str, ScriptEntry] = {}
        for script in scripts:
            label = f"{script.title} [{script.relpath}]"
            mapping[label] = script
        labels = list(mapping.keys())

        if pt_prompt is not None and FuzzyCompleter is not None and WordCompleter is not None:
            command_words = ["/report", "/current", "/history", "/config", "/exit"]
            completer = FuzzyCompleter(WordCompleter(labels + command_words, sentence=True))

            value = self._prompt_line(
                placeholder="Run script in scratch, or /report /current /history /config /exit",
                completer=completer,
            ).strip()
            if not value:
                return ("", None)
            if value in mapping:
                return ("scratch", mapping[value])
            for label, script in mapping.items():
                if script.relpath == value:
                    return ("scratch", script)
                if value.lower() in label.lower():
                    return ("scratch", script)
            if value in {"/report", "report"}:
                return ("report", None)
            if value in {"/current", "current"}:
                return ("current", None)
            if value in {"/history", "history"}:
                return ("history", None)
            if value in {"/config", "config"}:
                return ("config", None)
            if value in {"/exit", "exit", "quit", "q"}:
                return ("exit", None)
            return (value, None)

        print("")
        for idx, label in enumerate(labels, start=1):
            print(f"{idx:3d}. {label}")
        raw = input("> ").strip().lower()
        if not raw:
            return ("", None)
        if raw in {"/report", "report"}:
            return ("report", None)
        if raw in {"/current", "current"}:
            return ("current", None)
        if raw in {"/history", "history"}:
            return ("history", None)
        if raw in {"/config", "config"}:
            return ("config", None)
        if raw in {"/exit", "exit", "quit", "q"}:
            return ("exit", None)
        try:
            index = int(raw)
        except ValueError:
            return (raw, None)
        if index < 1 or index > len(labels):
            return (raw, None)
        return ("scratch", mapping[labels[index - 1]])

    def _prompt_line(self, placeholder: str, completer=None, default: str = "") -> str:
        if self._prompt_session is not None:
            return str(
                self._prompt_session.prompt(
                    default=default,
                    completer=completer,
                    complete_while_typing=True if completer is not None else None,
                    placeholder=[("class:placeholder", placeholder)],
                )
            )
        if pt_prompt is not None:
            return str(
                pt_prompt(
                    "> ",
                    default=default,
                    completer=completer,
                    complete_while_typing=True if completer is not None else None,
                    placeholder=placeholder,
                )
            )
        return input("> ")

    def _prompt_text(self, message: str, default: str = "") -> str:
        placeholder = message
        if default:
            placeholder = f"{message} (default: {default})"
        raw = self._prompt_line(placeholder=placeholder, default=default).strip()
        return raw or default

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

        if raw in {"n"}:
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

    def _run_script_interactive(
        self,
        use_scratch: bool,
        script_entry: Optional[ScriptEntry] = None,
    ) -> None:
        selected_script = script_entry or self._select_script()
        if not selected_script:
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
        jobs = self._build_jobs(selected_script, report_id, uut)
        queued = self.queue.add_job(jobs, priority=0)
        job_ids = [queued] if isinstance(queued, str) else list(queued)
        if not job_ids:
            print("No jobs queued.")
            return
        print(f"Queued {len(job_ids)} job(s) for {selected_script.relpath}.")
        self._set_current_jobs(job_ids)
        self._current_prompt_hint_shown = False
        self._follow_current_run(continue_to_next=True)

    def _ansi(self, code: str) -> str:
        return f"\x1b[{code}m"

    def _strip_ansi(self, text: str) -> str:
        return re.sub(r"\x1b\[[0-9;]*m", "", text)

    def _visible_len(self, text: str) -> int:
        return len(self._strip_ansi(text))

    def _terminal_width(self) -> int:
        return max(40, shutil.get_terminal_size(fallback=(120, 24)).columns)

    def _rst_wrap_width(self) -> int:
        width = self._terminal_width()
        if width >= 80:
            return 80
        return max(30, width - 4)

    def _format_right_label_line(self, left: str, label_plain: str, label_styled: str) -> str:
        width = self._terminal_width()
        left_len = self._visible_len(left)
        right_len = len(label_plain)
        spaces = max(1, width - left_len - right_len)
        return f"{left}{' ' * spaces}{label_styled}"

    def _print_output_line(self, line: str = "") -> None:
        if self._transient_line_visible:
            self._clear_transient_line()
        print(line)

    def _print_transient_line(self, line: str) -> None:
        if not sys.stdout.isatty():
            if line != self._transient_line_text:
                print(line)
            self._transient_line_text = line
            return
        width = self._terminal_width()
        visible = self._visible_len(line)
        padded = line + (" " * max(0, width - visible))
        sys.stdout.write("\r" + padded)
        sys.stdout.flush()
        self._transient_line_visible = True
        self._transient_line_text = line

    def _clear_transient_line(self) -> None:
        if not self._transient_line_visible and not self._transient_line_text:
            return
        if sys.stdout.isatty():
            width = self._terminal_width()
            sys.stdout.write("\r" + (" " * width) + "\r")
            sys.stdout.flush()
        self._transient_line_visible = False
        self._transient_line_text = ""

    def _style_inline_rst(self, text: str) -> str:
        text = re.sub(
            r"``([^`]+)``",
            lambda m: f"{self._ansi('36')}{m.group(1)}{self._ansi('0')}",
            text,
        )
        text = re.sub(
            r"\*\*([^*]+)\*\*",
            lambda m: f"{self._ansi('1')}{m.group(1)}{self._ansi('0')}",
            text,
        )
        text = re.sub(
            r"(?<!\*)\*([^*\n]+)\*(?!\*)",
            lambda m: f"{self._ansi('3')}{m.group(1)}{self._ansi('0')}",
            text,
        )
        return text

    def _rewrite_meta_directive_to_list_table(self, content: str) -> str:
        lines = content.splitlines()
        out: List[str] = []
        i = 0
        while i < len(lines):
            line = lines[i]
            if not line.strip().startswith(".. meta::"):
                out.append(line)
                i += 1
                continue
            rows, next_index = self._parse_meta_rows(lines, i)
            if rows:
                out.extend(
                    [
                        ".. list-table:: Meta",
                        "   :header-rows: 1",
                        "",
                        "   * - Key",
                        "     - Value",
                    ]
                )
                for key, value in rows:
                    out.append(f"   * - {key}")
                    out.append(f"     - {value}")
                out.append("")
            i = next_index
        return "\n".join(out) + ("\n" if content.endswith("\n") else "")

    def _render_inline_node(self, node: docutils_nodes.Node) -> str:
        if isinstance(node, docutils_nodes.Text):
            return str(node)
        rendered = "".join(self._render_inline_node(child) for child in getattr(node, "children", []))
        if isinstance(node, docutils_nodes.strong):
            return f"{self._ansi('1')}{rendered}{self._ansi('0')}"
        if isinstance(node, docutils_nodes.emphasis):
            return f"{self._ansi('3')}{rendered}{self._ansi('0')}"
        if isinstance(node, docutils_nodes.literal):
            return f"{self._ansi('36')}{rendered}{self._ansi('0')}"
        if isinstance(node, docutils_nodes.reference):
            return f"{self._ansi('4;36')}{rendered}{self._ansi('0')}"
        return rendered

    def _append_wrapped_text(
        self,
        output: List[str],
        text: str,
        width: int,
        indent: str = "",
        first_prefix: str = "",
    ) -> None:
        clean = " ".join(text.split())
        if not clean:
            output.append("")
            return
        wraps = self._wrap_ansi_visible(
            clean,
            width=max(20, width),
            initial_indent=indent + first_prefix,
            subsequent_indent=indent + (" " * len(first_prefix)),
        )
        output.extend(wraps or [(indent + first_prefix).rstrip()])

    def _wrap_ansi_visible(
        self,
        text: str,
        width: int,
        initial_indent: str = "",
        subsequent_indent: str = "",
    ) -> List[str]:
        words = text.split()
        if not words:
            return [initial_indent.rstrip()]

        lines: List[str] = []
        prefix = initial_indent
        current_words: List[str] = []
        current_visible = self._visible_len(prefix)

        for word in words:
            word_visible = self._visible_len(word)
            sep = 1 if current_words else 0
            if current_words and (current_visible + sep + word_visible) > width:
                lines.append(prefix + " ".join(current_words))
                prefix = subsequent_indent
                current_words = [word]
                current_visible = self._visible_len(prefix) + word_visible
                continue
            if sep:
                current_visible += 1
            current_words.append(word)
            current_visible += word_visible

        if current_words:
            lines.append(prefix + " ".join(current_words))
        return lines

    def _table_cell_text(self, entry: docutils_nodes.Node) -> str:
        parts: List[str] = []
        for child in entry.children:
            if isinstance(child, docutils_nodes.paragraph):
                parts.append(self._render_inline_node(child))
            else:
                parts.append(self._render_inline_node(child))
        return " ".join(part.strip() for part in parts if part.strip())

    def _render_table_node(self, table: docutils_nodes.table, width: int) -> List[str]:
        rows: List[List[str]] = []
        for row in table.findall(docutils_nodes.row):
            cells: List[str] = []
            for entry in row.children:
                if not isinstance(entry, docutils_nodes.entry):
                    continue
                cells.append(self._table_cell_text(entry))
            if cells:
                rows.append(cells)
        if not rows:
            return []
        col_count = max(len(row) for row in rows)
        normalized = [row + [""] * (col_count - len(row)) for row in rows]

        # Prefer compact two-column layout for meta/doc info tables.
        if col_count == 2:
            key_width = max(8, min(24, max(len(r[0]) for r in normalized)))
            value_width = max(20, width - key_width - 7)
            border = f"+-{'-' * key_width}-+-{'-' * value_width}-+"
            out = [border]
            for ridx, row in enumerate(normalized):
                key_lines = self._wrap_ansi_visible(row[0], width=key_width) or [""]
                value_lines = self._wrap_ansi_visible(row[1], width=value_width) or [""]
                height = max(len(key_lines), len(value_lines))
                for i in range(height):
                    left = key_lines[i] if i < len(key_lines) else ""
                    right = value_lines[i] if i < len(value_lines) else ""
                    out.append(
                        f"| {left}{' ' * max(0, key_width - self._visible_len(left))} | "
                        f"{right}{' ' * max(0, value_width - self._visible_len(right))} |"
                    )
                if ridx == 0:
                    out.append(border)
            out.append(border)
            return out

        # Generic fallback for wider tables.
        col_width = max(8, (width - (3 * col_count) - 1) // col_count)
        border = "+" + "+".join(["-" * (col_width + 2) for _ in range(col_count)]) + "+"
        out = [border]
        for ridx, row in enumerate(normalized):
            wrapped_cols = [
                self._wrap_ansi_visible(cell, width=col_width) or [""]
                for cell in row
            ]
            height = max(len(col) for col in wrapped_cols)
            for i in range(height):
                parts = []
                for col in wrapped_cols:
                    cell = col[i] if i < len(col) else ""
                    parts.append(
                        f" {cell}{' ' * max(0, col_width - self._visible_len(cell))} "
                    )
                out.append("|" + "|".join(parts) + "|")
            if ridx == 0:
                out.append(border)
        out.append(border)
        return out

    def _admonition_palette(self, node: docutils_nodes.Admonition) -> str:
        if isinstance(node, (docutils_nodes.warning, docutils_nodes.caution)):
            return "33"  # yellow
        if isinstance(node, (docutils_nodes.danger, docutils_nodes.error)):
            return "31"  # red
        if isinstance(node, (docutils_nodes.note, docutils_nodes.tip, docutils_nodes.hint)):
            return "34"  # blue
        if isinstance(node, (docutils_nodes.attention, docutils_nodes.important)):
            return "35"  # magenta
        return "36"  # cyan

    def _render_admonition_node(
        self,
        node: docutils_nodes.Admonition,
        width: int,
    ) -> List[str]:
        color = self._admonition_palette(node)
        box_width = max(28, min(width, 80))
        inner_width = max(20, box_width - 4)

        title_text = ""
        content_children: List[docutils_nodes.Node] = []
        for child in node.children:
            if isinstance(child, docutils_nodes.title):
                title_text = self._render_inline_node(child).strip()
                continue
            content_children.append(child)
        # Simple admonitions like ``.. warning:: Title`` are parsed by docutils
        # as the first paragraph, not a title node. Promote that paragraph so
        # titled admonitions consistently render a bold title line.
        if (
            not title_text
            and len(content_children) >= 2
            and isinstance(content_children[0], docutils_nodes.paragraph)
        ):
            candidate = self._render_inline_node(content_children[0]).strip()
            if candidate:
                title_text = candidate
                content_children = content_children[1:]

        content_lines: List[str] = []
        for child in content_children:
            self._render_doctree_node(child, content_lines, inner_width, indent="")

        while content_lines and content_lines[0] == "":
            content_lines.pop(0)
        while content_lines and content_lines[-1] == "":
            content_lines.pop()

        boxed_lines: List[str] = []
        if title_text:
            boxed_lines.extend(
                self._wrap_ansi_visible(
                    f"{self._ansi('1')}{title_text}{self._ansi('0')}",
                    width=inner_width,
                )
            )
            if content_lines:
                boxed_lines.append("")

        if content_lines:
            boxed_lines.extend(content_lines)
        else:
            boxed_lines.append("")

        border = f"{self._ansi(color)}+{'-' * (box_width - 2)}+{self._ansi('0')}"
        out = [border]
        for raw_line in boxed_lines:
            wrapped = (
                self._wrap_ansi_visible(raw_line, width=inner_width)
                if raw_line
                else [""]
            )
            for line in wrapped:
                pad = " " * max(0, inner_width - self._visible_len(line))
                out.append(
                    f"{self._ansi(color)}|{self._ansi('0')} "
                    f"{line}{pad} "
                    f"{self._ansi(color)}|{self._ansi('0')}"
                )
        out.append(border)
        return out

    def _render_doctree_node(
        self,
        node: docutils_nodes.Node,
        output: List[str],
        width: int,
        indent: str = "",
    ) -> None:
        if isinstance(node, docutils_nodes.document):
            for child in node.children:
                self._render_doctree_node(child, output, width, indent=indent)
            return
        if isinstance(node, docutils_nodes.section):
            for child in node.children:
                self._render_doctree_node(child, output, width, indent=indent)
            return
        if isinstance(node, docutils_nodes.Admonition):
            if output and output[-1] != "":
                output.append("")
            output.extend(self._render_admonition_node(node, width=width))
            if output and output[-1] != "":
                output.append("")
            return
        if isinstance(node, docutils_nodes.title):
            if output and output[-1] != "":
                output.append("")
            raw_title = self._render_inline_node(node).strip()
            if raw_title:
                output.append(f"{self._ansi('1;3')}{raw_title}{self._ansi('0')}")
                output.append(f"{self._ansi('2')}{'=' * max(len(self._strip_ansi(raw_title)), 3)}{self._ansi('0')}")
            return
        if isinstance(node, rvt_script):
            body = str(node.get("body") or "")
            if output and output[-1] != "":
                output.append("")
            for raw in body.splitlines():
                output.append(f"{indent}{self._ansi('36')}{raw}{self._ansi('0')}")
            if output and output[-1] != "":
                output.append("")
            return
        if isinstance(node, docutils_nodes.paragraph):
            self._append_wrapped_text(output, self._render_inline_node(node), width=width, indent=indent)
            parent = getattr(node, "parent", None)
            if isinstance(parent, (docutils_nodes.document, docutils_nodes.section)):
                if output and output[-1] != "":
                    output.append("")
            return
        if isinstance(node, docutils_nodes.bullet_list):
            for item in node.children:
                self._render_doctree_node(item, output, width, indent=indent)
            return
        if isinstance(node, docutils_nodes.enumerated_list):
            start = int(node.get("start", 1) or 1)
            for index, item in enumerate(node.children, start=start):
                self._render_doctree_node(item, output, width, indent=indent + f"{index}. ")
            return
        if isinstance(node, docutils_nodes.list_item):
            marker = "- " if not indent.strip().endswith(".") else ""
            first_para = True
            for child in node.children:
                if isinstance(child, docutils_nodes.paragraph) and first_para:
                    self._append_wrapped_text(
                        output,
                        self._render_inline_node(child),
                        width=width,
                        indent="",
                        first_prefix=indent + marker,
                    )
                    first_para = False
                else:
                    self._render_doctree_node(
                        child,
                        output,
                        width,
                        indent=indent + ("  " if marker else "   "),
                    )
            return
        if isinstance(node, docutils_nodes.literal_block):
            for line in node.astext().splitlines():
                output.append(f"{indent}{self._ansi('36')}{line}{self._ansi('0')}")
            return
        if isinstance(node, docutils_nodes.table):
            output.extend(self._render_table_node(node, width=width))
            return
        if isinstance(node, docutils_nodes.transition):
            output.append(f"{self._ansi('2')}{'-' * min(width, 80)}{self._ansi('0')}")
            return
        if isinstance(node, docutils_nodes.system_message):
            msg = node.astext().strip()
            if msg:
                self._append_wrapped_text(
                    output,
                    f"RST warning: {msg}",
                    width=width,
                    indent="",
                )
            return
        for child in getattr(node, "children", []):
            self._render_doctree_node(child, output, width, indent=indent)

    def _render_rst_docutils_lines(self, content: str) -> List[str]:
        rewritten = self._rewrite_meta_directive_to_list_table(content)
        document = docutils_core.publish_doctree(
            source=rewritten,
            settings_overrides={
                "halt_level": 6,
                "report_level": 5,
                "file_insertion_enabled": False,
                "raw_enabled": False,
                "warning_stream": None,
            },
        )
        width = self._rst_wrap_width()
        output: List[str] = []
        self._render_doctree_node(document, output, width=width)
        # Normalize consecutive blank lines.
        normalized: List[str] = []
        last_blank = False
        for line in output:
            blank = line == ""
            if blank and last_blank:
                continue
            normalized.append(line)
            last_blank = blank
        while normalized and normalized[-1] == "":
            normalized.pop()
        return normalized

    def _render_two_column_table(self, rows: List[tuple[str, str]]) -> List[str]:
        if not rows:
            return []
        key_header = "Key"
        value_header = "Value"
        key_width = max(len(key_header), *(len(str(key)) for key, _ in rows))
        value_width = max(len(value_header), *(len(str(value)) for _, value in rows))
        border = f"+-{'-' * key_width}-+-{'-' * value_width}-+"
        lines = [
            border,
            f"| {key_header.ljust(key_width)} | {value_header.ljust(value_width)} |",
            border,
        ]
        for key, value in rows:
            lines.append(f"| {str(key).ljust(key_width)} | {str(value).ljust(value_width)} |")
        lines.append(border)
        return lines

    def _parse_meta_rows(self, lines: List[str], start: int) -> tuple[List[tuple[str, str]], int]:
        rows: List[tuple[str, str]] = []
        index = start + 1
        while index < len(lines):
            raw = lines[index]
            stripped = raw.strip()
            if not stripped:
                index += 1
                continue
            if not raw.startswith("   "):
                break
            if stripped.startswith(":") and ":" in stripped[1:]:
                key, value = stripped[1:].split(":", 1)
                rows.append((key.strip(), value.strip()))
            index += 1
        return rows, index

    def _render_rst_fragment_lines(self, content: str) -> List[str]:
        if not content:
            return []
        try:
            return self._render_rst_docutils_lines(content)
        except Exception:
            pass
        lines = content.splitlines()
        rendered: List[str] = []
        i = 0
        heading_adorn = re.compile(r"^([=\-~^`:#\"'+*])\1{2,}$")
        while i < len(lines):
            line = lines[i]
            if line.strip().startswith(".. meta::"):
                meta_rows, next_index = self._parse_meta_rows(lines, i)
                if meta_rows:
                    rendered.extend(self._render_two_column_table(meta_rows))
                i = next_index
                continue
            if i + 1 < len(lines):
                underline = lines[i + 1].strip()
                if line.strip() and heading_adorn.match(underline) and len(underline) >= len(line.strip()):
                    if rendered and rendered[-1] != "":
                        rendered.append("")
                    raw_heading = line.strip()
                    heading = self._style_inline_rst(raw_heading)
                    adorn = underline[0]
                    underline_text = adorn * max(len(raw_heading), 3)
                    rendered.append(f"{self._ansi('1;3')}{heading}{self._ansi('0')}")
                    rendered.append(f"{self._ansi('2')}{underline_text}{self._ansi('0')}")
                    i += 2
                    continue
            rendered.append(self._style_inline_rst(line))
            i += 1
        return rendered

    def _source_rst_from_event(self, event: Dict[str, Any]) -> str:
        return str(event.get("source_rst") or "").strip("\n")

    def _block_source_lines(self, event: Dict[str, Any]) -> List[str]:
        source_rst = self._source_rst_from_event(event)
        if source_rst:
            lines = source_rst.splitlines()
            return lines if lines else [source_rst]
        block = str(event.get("block") or "block").strip() or "block"
        args = [str(item) for item in (event.get("args") or [])]
        return [f"({block}{(' ' + ' '.join(args)) if args else ''})"]

    def _format_block_result_line(
        self,
        event: Dict[str, Any],
        invocation: Optional[str] = None,
    ) -> str:
        block = str(event.get("block") or "block").strip() or "block"
        args = [str(item) for item in (event.get("args") or [])]
        invocation_text = invocation or f"({block}{(' ' + ' '.join(args)) if args else ''})"

        ts_value = event.get("timestamp")
        try:
            ts_float = float(ts_value) if ts_value is not None else time.time()
        except (TypeError, ValueError):
            ts_float = time.time()
        time_text = time.strftime("%H:%M:%S", time.localtime(ts_float))
        status_word = "PASS" if bool(event.get("passed")) else "FAIL"
        status_color = self._ansi("32") if status_word == "PASS" else self._ansi("31")
        label_plain = f"{time_text} {status_word}"
        label_styled = (
            f"{self._ansi('90')}{time_text}{self._ansi('0')} "
            f"{status_color}{status_word}{self._ansi('0')}"
        )
        return self._format_right_label_line(invocation_text, label_plain, label_styled)

    def _print_script_completion_summary(self, focus_job_id: str, result: Dict[str, Any]) -> None:
        result_data = result.get("result_data") or {}
        duration = result_data.get("duration_seconds")
        status_word = "PASS" if bool(result.get("success")) else "FAIL"
        status_color = self._ansi("32") if status_word == "PASS" else self._ansi("31")

        script_path = (
            str((result.get("job_data") or {}).get("file") or "").strip()
            or self._current_job_files.get(focus_job_id)
            or focus_job_id
        )
        summary = (
            f"{self._ansi('1')}Script Result{self._ansi('0')}: "
            f"{script_path} -> {status_color}{status_word}{self._ansi('0')}"
        )
        if isinstance(duration, (int, float)):
            summary += f" {self._ansi('90')}({float(duration):.2f}s){self._ansi('0')}"

        self._print_output_line("")
        self._print_output_line(summary)
        self._print_output_line("")
        self._print_output_line(f"{self._ansi('2')}{'-' * self._terminal_width()}{self._ansi('0')}")
        self._print_output_line("")

    def _print_script_completion_summary_from_event(self, focus_job_id: str, passed: bool) -> None:
        status_word = "PASS" if passed else "FAIL"
        status_color = self._ansi("32") if passed else self._ansi("31")
        script_path = self._current_job_files.get(focus_job_id) or focus_job_id
        summary = (
            f"{self._ansi('1')}Script Result{self._ansi('0')}: "
            f"{script_path} -> {status_color}{status_word}{self._ansi('0')}"
        )
        self._print_output_line("")
        self._print_output_line(summary)
        self._print_output_line("")
        self._print_output_line(f"{self._ansi('2')}{'-' * self._terminal_width()}{self._ansi('0')}")
        self._print_output_line("")

    def _format_active_block_line(self, job_id: str) -> Optional[str]:
        state = self._active_step_state.get(job_id)
        if not state:
            return None
        invocation = str(state.get("invocation") or "").strip()
        if not invocation:
            return None
        started_at = state.get("started_at")
        try:
            started = float(started_at) if started_at is not None else time.time()
        except (TypeError, ValueError):
            started = time.time()
        elapsed = max(0, int(time.time() - started))
        state["last_elapsed"] = elapsed
        label_plain = f"{elapsed}s"
        label_styled = f"{self._ansi('33')}{elapsed}s{self._ansi('0')}"
        return self._format_right_label_line(invocation, label_plain, label_styled)

    def _render_active_step_progress(self, job_id: str, force: bool = False) -> bool:
        state = self._active_step_state.get(job_id)
        if not state:
            return False
        started_at = state.get("started_at")
        try:
            started = float(started_at) if started_at is not None else time.time()
        except (TypeError, ValueError):
            started = time.time()
        elapsed = max(0, int(time.time() - started))
        last_elapsed = int(state.get("last_elapsed", -1))
        if not force and elapsed == last_elapsed:
            return False
        state["last_elapsed"] = elapsed
        line = self._format_active_block_line(job_id)
        if not line:
            return False
        self._print_transient_line(line)
        return True

    def _set_current_jobs(self, job_ids: List[str]) -> None:
        self._current_job_ids = [str(job_id) for job_id in job_ids if str(job_id).strip()]
        self._current_last_seq = {job_id: -1 for job_id in self._current_job_ids}
        self._current_completion_announced = set()
        self._current_focus_job_id = self._current_job_ids[-1] if self._current_job_ids else None
        self._current_job_files = {}
        for job_id in self._current_job_ids:
            job = self.queue.get_job(job_id) or {}
            script_path = str((job or {}).get("file") or "").strip()
            if script_path:
                self._current_job_files[job_id] = script_path
        self._active_step_state = {}
        self._clear_transient_line()

    def _current_has_pending(self) -> bool:
        if not self._current_job_ids:
            return False
        for job_id in self._current_job_ids:
            if self.queue.get_result(job_id) is None:
                return True
        return False

    def _resolve_current_focus_job_id(self) -> Optional[str]:
        if not self._current_job_ids:
            return None

        # Keep focus pinned while there is still active or unread output for
        # the current script, then allow advancing immediately.
        if self._current_focus_job_id and self._current_focus_job_id in self._current_job_ids:
            focus = self._current_focus_job_id
            if focus in self._active_step_state:
                return focus
            last_seq = self._current_last_seq.get(focus, -1)
            if self.runtime.events_since(focus, last_seq=last_seq):
                return focus

        status = self.runtime.status()
        current_job_id = str(status.get("current_job_id") or "").strip()
        if current_job_id and current_job_id in self._current_job_ids:
            self._current_focus_job_id = current_job_id
            return current_job_id

        if self._current_focus_job_id and self._current_focus_job_id in self._current_job_ids:
            return self._current_focus_job_id

        for job_id in reversed(self._current_job_ids):
            if self.runtime.events_since(job_id, last_seq=-1):
                self._current_focus_job_id = job_id
                return job_id
            if self.queue.get_result(job_id) is not None:
                self._current_focus_job_id = job_id
                return job_id
        return self._current_job_ids[-1]

    def _resolve_next_pending_focus(self, after_job_id: Optional[str] = None) -> Optional[str]:
        if not self._current_job_ids:
            return None

        pending = [
            job_id
            for job_id in self._current_job_ids
            if self.queue.get_result(job_id) is None
        ]
        if not pending:
            return None

        status = self.runtime.status()
        current_job_id = str(status.get("current_job_id") or "").strip()
        if current_job_id and current_job_id in pending:
            return current_job_id

        if after_job_id and after_job_id in self._current_job_ids:
            start_index = self._current_job_ids.index(after_job_id) + 1
            for job_id in self._current_job_ids[start_index:]:
                if job_id in pending:
                    return job_id

        return pending[0]

    def _print_current_output(self, replay: bool = False) -> bool:
        focus_job_id = self._resolve_current_focus_job_id()
        if not focus_job_id:
            return False
        printed = False
        last_seq = -1 if replay else self._current_last_seq.get(focus_job_id, -1)
        events = self.runtime.events_since(focus_job_id, last_seq=last_seq)
        for event in events:
            seq = event.get("seq")
            if isinstance(seq, int):
                self._current_last_seq[focus_job_id] = max(
                    self._current_last_seq.get(focus_job_id, -1), seq
                )
            kind = str(event.get("kind") or "")
            if kind == "block_start":
                source_lines = self._block_source_lines(event)
                invocation = source_lines[0] if source_lines else "(block)"
                started_at = event.get("timestamp")
                try:
                    started = float(started_at) if started_at is not None else time.time()
                except (TypeError, ValueError):
                    started = time.time()
                self._active_step_state[focus_job_id] = {
                    "invocation": invocation,
                    "started_at": started,
                    "last_elapsed": -1,
                }
                if not replay:
                    self._render_active_step_progress(focus_job_id, force=True)
                continue
            if kind == "text_chunk":
                content = str(event.get("content") or "")
                for line in self._render_rst_fragment_lines(content):
                    self._print_output_line(line)
                    printed = True
                continue
            if kind == "block_end":
                self._active_step_state.pop(focus_job_id, None)
                source_rst = self._source_rst_from_event(event)
                if source_rst:
                    source_lines = self._block_source_lines(event)
                    for raw_line in source_lines[:-1]:
                        self._print_output_line(f"{self._ansi('36')}{raw_line}{self._ansi('0')}")
                    last_line = source_lines[-1] if source_lines else ""
                    if last_line:
                        styled_last = f"{self._ansi('36')}{last_line}{self._ansi('0')}"
                        self._print_output_line(
                            self._format_block_result_line(event, invocation=styled_last)
                        )
                    else:
                        self._print_output_line(self._format_block_result_line(event))
                else:
                    self._print_output_line(self._format_block_result_line(event))
                printed = True
                continue
            if kind == "script_end":
                passed = bool(event.get("passed"))
                self._active_step_state.pop(focus_job_id, None)
                self._clear_transient_line()
                if replay or focus_job_id not in self._current_completion_announced:
                    self._print_script_completion_summary_from_event(focus_job_id, passed)
                    printed = True
                self._current_completion_announced.add(focus_job_id)
                continue
            if kind == "execution_error":
                message = str(event.get("message") or "execution error")
                self._print_output_line(
                    f"{self._ansi('31')}execution error: {message}{self._ansi('0')}"
                )
                printed = True
                continue

        result = self.queue.get_result(focus_job_id)
        if not result:
            if not replay:
                self._render_active_step_progress(focus_job_id, force=False)
            return printed
        if replay or focus_job_id not in self._current_completion_announced:
            self._print_script_completion_summary(focus_job_id, result)
            printed = True
        self._current_completion_announced.add(focus_job_id)
        self._active_step_state.pop(focus_job_id, None)
        self._clear_transient_line()
        return printed

    def _follow_current_run(self, continue_to_next: bool = False) -> None:
        if not self._current_job_ids:
            return
        with _EscapeDetector() as esc_detector:
            while True:
                self._print_current_output(replay=False)
                focus_job_id = self._resolve_current_focus_job_id()
                if focus_job_id and self.queue.get_result(focus_job_id) is not None:
                    if continue_to_next:
                        next_focus = self._resolve_next_pending_focus(after_job_id=focus_job_id)
                        if next_focus:
                            self._current_focus_job_id = next_focus
                            self._active_step_state.pop(focus_job_id, None)
                            self._clear_transient_line()
                            continue
                    self._clear_transient_line()
                    self._print_output_line("Current script completed. Type /current to replay output.")
                    self._current_prompt_hint_shown = False
                    return
                if esc_detector.pressed():
                    self._clear_transient_line()
                    self._print_output_line("Returning to prompt. Type /current to continue viewing output.")
                    self._current_prompt_hint_shown = True
                    return
                time.sleep(0.1)

    def _show_current_run(self) -> None:
        if not self._current_job_ids:
            self._print_output_line("No current run.")
            return
        focus_job_id = self._resolve_current_focus_job_id()
        if focus_job_id:
            self._print_output_line(f"Replaying output for {focus_job_id}.")
        else:
            self._print_output_line("Replaying current output.")
        self._print_current_output(replay=True)
        if self._current_has_pending():
            self._follow_current_run(continue_to_next=True)

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
    for logger_name in (
        "jobqueue.executor",
        "jobqueue.central",
        "jobqueue.worker",
    ):
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    args = docopt(__doc__)
    db_path = str(args.get("--db") or "jobqueue.db").strip()
    config_path = Path(_expand_path(str(args.get("--config") or "~/.automationv3/local_tui.toml")))
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
