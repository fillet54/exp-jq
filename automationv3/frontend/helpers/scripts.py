"""Script-centric frontend helper functions."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List

from flask import url_for

from automationv3.framework.requirements import REQUIREMENT_ID_PATTERN
from automationv3.framework.rst import (
    collect_script_syntax_issues,
    expand_rvt_variations,
    parse_rst_chunks,
    render_script_rst_html,
)

from .context import FrontendHelperContext

UNSPECIFIED_SYSTEM = "UNSPECIFIED"
UNKNOWN_SYSTEM = "UNKNOWN"


def parse_meta_from_rst(path: Path) -> Dict[str, List[str]]:
    try:
        content = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return {"requirements": [], "tags": [], "subsystem": []}
    return parse_meta_from_lines(content)


def parse_meta_from_lines(content: List[str]) -> Dict[str, List[str]]:
    meta: Dict[str, List[str]] = {"requirements": [], "tags": [], "subsystem": []}
    try:
        content = list(content)
    except Exception:
        return meta
    in_meta = False
    for line in content:
        if line.strip().startswith(".. meta::"):
            in_meta = True
            continue
        if in_meta:
            if not line.startswith("   "):
                break
            stripped = line.strip()
            if stripped.startswith(":") and ":" in stripped[1:]:
                key, val = stripped[1:].split(":", 1)
                key = key.strip()
                val_list = [v.strip() for v in val.split(",") if v.strip()]
                if key in meta:
                    meta[key].extend(val_list)
                else:
                    meta[key] = val_list
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


def discover_scripts(root: Path) -> List[Dict[str, Any]]:
    scripts: List[Dict[str, Any]] = []
    if not root.exists():
        return scripts
    for path in sorted(root.rglob("*.rst")):
        rel = path.relative_to(root)
        try:
            content = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            continue
        lines = content.splitlines()
        meta = parse_meta_from_lines(lines)
        title = extract_rst_title(lines, fallback=path.stem)
        syntax_issues = collect_script_syntax_issues(content)
        syntax_error_count = sum(1 for issue in syntax_issues if issue.get("is_error"))
        scripts.append(
            {
                "path": str(path),
                "relpath": str(rel),
                "meta": meta,
                "name": path.stem,
                "title": title,
                "syntax_issues": syntax_issues,
                "syntax_error_count": syntax_error_count,
                "has_syntax_errors": syntax_error_count > 0,
            }
        )
    return scripts


def build_raw_source_rows(script_content: str, issues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    line_to_issues: Dict[int, List[Dict[str, Any]]] = {}
    for issue in issues:
        line = issue.get("line")
        if isinstance(line, int) and line > 0:
            line_to_issues.setdefault(line, []).append(issue)

    rows: List[Dict[str, Any]] = []
    for index, line_text in enumerate(script_content.splitlines(), start=1):
        row_issues = line_to_issues.get(index, [])
        rows.append(
            {
                "line": index,
                "text": line_text,
                "issues": row_issues,
                "has_error": any(bool(issue.get("is_error")) for issue in row_issues),
            }
        )
    return rows


def _normalize_script_directory(relpath: str) -> str:
    parent = str(PurePosixPath(relpath).parent)
    return "" if parent == "." else parent


def parent_directory(dirpath: str) -> str | None:
    if not dirpath:
        return None
    parent = str(PurePosixPath(dirpath).parent)
    return "" if parent == "." else parent


def build_script_directory_index(
    scripts: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], Dict[str, List[str]], Dict[str, List[Dict[str, Any]]], Dict[str, int]]:
    directory_to_scripts: Dict[str, List[Dict[str, Any]]] = {"": []}
    directory_children: Dict[str, set[str]] = {"": set()}
    directories = {""}

    for script in scripts:
        relpath = script.get("relpath") or ""
        dirpath = _normalize_script_directory(relpath)
        directory_to_scripts.setdefault(dirpath, []).append(script)
        directories.add(dirpath)

        current = ""
        for part in (PurePosixPath(dirpath).parts if dirpath else ()):
            child = part if not current else f"{current}/{part}"
            directory_children.setdefault(current, set()).add(child)
            directory_children.setdefault(child, set())
            directory_to_scripts.setdefault(child, [])
            directories.add(child)
            current = child

    for dirpath in directories:
        directory_children.setdefault(dirpath, set())
        directory_to_scripts.setdefault(dirpath, [])
        directory_to_scripts[dirpath] = sorted(
            directory_to_scripts[dirpath],
            key=lambda row: ((row.get("title") or "").lower(), row.get("relpath") or ""),
        )

    recursive_counts: Dict[str, int] = {}
    for dirpath in sorted(directories, key=lambda d: (d.count("/"), len(d)), reverse=True):
        recursive_counts[dirpath] = len(directory_to_scripts.get(dirpath, [])) + sum(
            recursive_counts.get(child, 0)
            for child in sorted(directory_children.get(dirpath, set()))
        )

    directory_nodes: List[Dict[str, Any]] = [
        {
            "path": "",
            "name": ".",
            "depth": 0,
            "script_count": len(directory_to_scripts.get("", [])),
            "total_script_count": recursive_counts.get("", 0),
        }
    ]

    def _walk(parent: str, depth: int):
        for child in sorted(directory_children.get(parent, set())):
            directory_nodes.append(
                {
                    "path": child,
                    "name": PurePosixPath(child).name,
                    "depth": depth,
                    "script_count": len(directory_to_scripts.get(child, [])),
                    "total_script_count": recursive_counts.get(child, 0),
                }
            )
            _walk(child, depth + 1)

    _walk("", 1)
    return (
        directory_nodes,
        {key: sorted(value) for key, value in directory_children.items()},
        directory_to_scripts,
        recursive_counts,
    )


def requirement_to_system(requirement_id: str) -> str:
    req = (requirement_id or "").strip().upper()
    if not req:
        return UNKNOWN_SYSTEM
    match = REQUIREMENT_ID_PATTERN.fullmatch(req)
    if not match:
        return UNKNOWN_SYSTEM
    return match.group("system")


def sorted_systems(systems: List[str] | set[str]) -> List[str]:
    ordered = sorted(systems)
    if UNSPECIFIED_SYSTEM in ordered:
        ordered.remove(UNSPECIFIED_SYSTEM)
        ordered.append(UNSPECIFIED_SYSTEM)
    if UNKNOWN_SYSTEM in ordered:
        ordered.remove(UNKNOWN_SYSTEM)
        ordered.append(UNKNOWN_SYSTEM)
    return ordered


def build_script_system_index(
    scripts: List[Dict[str, Any]],
) -> tuple[List[str], Dict[str, int], Dict[str, Dict[str, List[Dict[str, Any]]]]]:
    system_to_requirement_to_scripts: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

    for script in scripts:
        meta = script.get("meta") or {}
        requirements = [r.strip() for r in (meta.get("requirements") or []) if r.strip()]
        script["requirements"] = requirements

        if not requirements:
            system_to_requirement_to_scripts.setdefault(UNSPECIFIED_SYSTEM, {}).setdefault(
                "No Requirement Declared", []
            ).append(script)
            continue

        for req in requirements:
            system = requirement_to_system(req)
            system_to_requirement_to_scripts.setdefault(system, {}).setdefault(req, []).append(script)

    for req_map in system_to_requirement_to_scripts.values():
        for req, items in list(req_map.items()):
            seen = set()
            deduped = []
            for item in items:
                relpath = item.get("relpath")
                if relpath in seen:
                    continue
                seen.add(relpath)
                deduped.append(item)
            req_map[req] = sorted(deduped, key=lambda row: row.get("relpath", ""))

    systems = sorted_systems(system_to_requirement_to_scripts.keys())

    system_counts: Dict[str, int] = {}
    for system, req_map in system_to_requirement_to_scripts.items():
        unique_paths = set()
        for rows in req_map.values():
            for row in rows:
                unique_paths.add(row.get("relpath"))
        system_counts[system] = len(unique_paths)

    return systems, system_counts, system_to_requirement_to_scripts


def safe_return_to(target: str) -> str | None:
    clean = (target or "").strip()
    if clean.startswith("/") and not clean.startswith("//"):
        return clean
    return None


def resolve_rel_script_path(script_path: str, base_path: Path) -> str:
    clean_path = (script_path or "").strip()
    if not clean_path:
        raise ValueError("script_path required")
    resolved_base = base_path.resolve()
    candidate = Path(clean_path)
    resolved_path = (
        candidate.resolve() if candidate.is_absolute() else (resolved_base / candidate).resolve()
    )
    if not resolved_path.is_file():
        raise ValueError("script_path not found")
    try:
        relpath = resolved_path.relative_to(resolved_base)
    except ValueError as exc:
        raise ValueError("script_path must be inside base_path") from exc
    return relpath.as_posix()


def normalize_requirements(raw: Any) -> List[str]:
    if raw is None:
        return []
    values: List[str] = []
    if isinstance(raw, str):
        values = [part.strip() for part in raw.split(",") if part.strip()]
    elif isinstance(raw, list):
        for item in raw:
            if item is None:
                continue
            text = str(item).strip()
            if text:
                values.append(text)
    else:
        text = str(raw).strip()
        if text:
            values = [text]
    deduped: List[str] = []
    seen = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def variation_key_from_bindings(bindings: Dict[str, Any]) -> str:
    if not isinstance(bindings, dict) or not bindings:
        return ""
    parts = [
        f"{str(key).strip()}={str(value).strip()}"
        for key, value in sorted(bindings.items(), key=lambda item: str(item[0]))
        if str(key).strip()
    ]
    return "|".join(parts)


def expand_job_variations_from_script(
    base_job: Dict[str, Any],
    script_path: Path,
) -> List[Dict[str, Any]]:
    if not script_path.exists() or not script_path.is_file():
        return [base_job]

    script_text = script_path.read_text(encoding="utf-8")
    variations = expand_rvt_variations(script_text)
    if not variations:
        return [base_job]

    total = len(variations)
    expanded: List[Dict[str, Any]] = []
    for index, variation in enumerate(variations, start=1):
        variation_bindings = variation.get("bindings") or {}
        variation_components = [
            str(component).strip()
            for component in (variation.get("components") or [])
            if str(component).strip()
        ]
        variation_name = str(variation.get("name") or "").strip()
        if not variation_name and variation_components:
            variation_name = " / ".join(variation_components)
        if not variation_name:
            variation_name = f"variation-{index}"

        job = dict(base_job)
        job["is_variation_job"] = True
        job["variation_name"] = variation_name
        job["variation_components"] = variation_components
        job["variation_bindings"] = {
            str(symbol): str(value)
            for symbol, value in variation_bindings.items()
            if str(symbol).strip()
        }
        job["variation_index"] = index
        job["variation_total"] = total
        expanded.append(job)
    return expanded


def build_jobs_for_relpath(
    ctx: FrontendHelperContext,
    rel_script_path: str,
    base_path: Path,
    config: Any,
    report_id: str,
    framework_version: str,
    scripts_tree: str | None,
    suite_name: str = "",
    suite_run_id: str = "",
) -> List[Dict[str, Any]]:
    script_path = (base_path / rel_script_path).resolve()
    meta = parse_meta_from_rst(script_path)
    base_job = {
        "file": rel_script_path,
        "uut": config.name,
        "report_id": report_id,
        "uut_tree": config.last_tree_sha,
        "uut_id": config.uut_id,
        "meta": meta,
        "framework_version": framework_version,
        "scripts_tree": scripts_tree,
        "scripts_root": str(base_path),
        "suite_name": suite_name,
        "suite_run_id": suite_run_id,
    }
    try:
        return expand_job_variations_from_script(base_job, script_path)
    except ValueError as exc:
        raise ValueError(f"Invalid variation data in script '{rel_script_path}': {exc}") from exc


def build_requeue_job_from_result_job_data(
    ctx: FrontendHelperContext,
    source_job: Dict[str, Any],
    report_id: str,
) -> Dict[str, Any] | None:
    script = str(source_job.get("file") or "").strip()
    uut = str(source_job.get("uut") or "").strip()
    if not script or not uut:
        return None

    job: Dict[str, Any] = {
        "file": script,
        "uut": uut,
        "report_id": report_id,
        "suite_run_id": "",
        "suite_name": "",
    }
    for key in (
        "uut_tree",
        "uut_id",
        "framework_version",
        "scripts_tree",
        "scripts_root",
    ):
        value = source_job.get(key)
        if value not in (None, ""):
            job[key] = value

    source_meta = source_job.get("meta")
    meta_copy: Dict[str, List[str]] = {}
    if isinstance(source_meta, dict):
        for key, value in source_meta.items():
            if isinstance(value, list):
                meta_copy[str(key)] = [str(item) for item in value if str(item).strip()]
            elif value not in (None, ""):
                meta_copy[str(key)] = [str(value)]
    job["meta"] = meta_copy

    script_candidates: List[Path] = []
    source_scripts_root = str(source_job.get("scripts_root") or "").strip()
    if source_scripts_root:
        script_candidates.append((Path(source_scripts_root).resolve() / script).resolve())
    script_candidates.append((ctx.scripts_root.resolve() / script).resolve())
    for candidate in script_candidates:
        try:
            if candidate.exists() and candidate.is_file():
                job["meta"] = parse_meta_from_rst(candidate)
                break
        except Exception:
            continue
    return job


def build_legacy_result_document(job_data: Dict[str, Any], result_data: Dict[str, Any]) -> str:
    if not isinstance(result_data, dict):
        return ""
    rvt = result_data.get("rvt")
    if not isinstance(rvt, dict):
        return ""

    output = (result_data.get("output") or "").strip()
    lines = [
        "Execution Output",
        "================",
        "",
    ]
    default_timestamp_value = result_data.get("timestamp") or result_data.get("completed_at")
    if isinstance(default_timestamp_value, (int, float)):
        default_timestamp = datetime.fromtimestamp(
            float(default_timestamp_value), tz=timezone.utc
        ).isoformat()
    else:
        default_timestamp = str(default_timestamp_value or datetime.now(timezone.utc).isoformat())
    duration = float(result_data.get("duration_seconds") or result_data.get("duration") or 0.0)

    def _make_rvt_result(
        step_text: str,
        status: str,
        detail_text: str,
        step_timestamp: Any = None,
        step_duration: Any = None,
    ) -> str:
        safe_step = step_text.strip() or "(legacy-step)"
        safe_detail = detail_text.strip() or "No output captured."
        if isinstance(step_timestamp, (int, float)):
            timestamp_value = datetime.fromtimestamp(
                float(step_timestamp), tz=timezone.utc
            ).isoformat()
        else:
            timestamp_value = str(step_timestamp or default_timestamp)
        try:
            duration_value = float(step_duration) if step_duration is not None else duration
        except (TypeError, ValueError):
            duration_value = duration
        directive_lines = [
            ".. rvt-result::",
            f"   :status: {status}",
            f"   :timestamp: {timestamp_value}",
            f"   :duration: {duration_value:.6f}",
            "",
            "   .. rvt::",
            "",
            *[f"      {line}" for line in safe_step.splitlines()],
            "",
            "   .. code-block:: text",
            "",
            *[f"      {line}" for line in safe_detail.splitlines()],
            "",
        ]
        return "\n".join(directive_lines)

    directives: List[str] = []
    invocations = rvt.get("invocations") or []
    for row in invocations:
        block = str((row or {}).get("block") or "block").strip()
        args = [str(arg) for arg in ((row or {}).get("args") or [])]
        result_text = str((row or {}).get("result") or "").strip()
        status = "pass" if bool((row or {}).get("passed")) else "fail"
        call_repr = f"({block}{(' ' + ' '.join(args)) if args else ''})"
        directives.append(
            _make_rvt_result(
                call_repr,
                status,
                result_text,
                step_timestamp=(row or {}).get("timestamp"),
                step_duration=(row or {}).get("duration"),
            )
        )

    if not directives:
        results = rvt.get("results") or []
        for row in results:
            form_text = str((row or {}).get("form") or "").strip()
            result_text = str((row or {}).get("result") or "").strip()
            status = "pass" if bool((row or {}).get("passed")) else "fail"
            if not form_text and not result_text:
                continue
            directives.append(_make_rvt_result(form_text, status, result_text))

    if not directives:
        fallback_file = str(job_data.get("file") or "unknown")
        status = "pass" if bool(rvt.get("passed", True)) else "fail"
        directives.append(
            _make_rvt_result(
                "(legacy-step)",
                status,
                output or f"No detailed RVT rows captured for {fallback_file}.",
            )
        )
    elif output:
        lines.extend(
            [
                ".. note::",
                f"   {output}",
                "",
            ]
        )

    script_file = str(job_data.get("file") or "").strip()
    scripts_root = str(job_data.get("scripts_root") or "").strip()
    if script_file and scripts_root:
        base_path = Path(scripts_root).resolve()
        script_path = (base_path / script_file).resolve()
        try:
            script_path.relative_to(base_path)
            if script_path.exists() and script_path.is_file():
                script_text = script_path.read_text(encoding="utf-8")
                chunks = parse_rst_chunks(script_text)
                rvt_count = sum(1 for chunk in chunks if chunk.kind == "rvt")
                if rvt_count > 0:
                    if len(directives) <= rvt_count:
                        grouped = []
                        cursor = 0
                        for _ in range(rvt_count):
                            if cursor < len(directives):
                                grouped.append([directives[cursor]])
                                cursor += 1
                            else:
                                grouped.append([])
                    else:
                        grouped = []
                        total = len(directives)
                        for idx in range(rvt_count):
                            start = round((idx * total) / rvt_count)
                            end = round(((idx + 1) * total) / rvt_count)
                            grouped.append(directives[start:end])
                    rendered_parts: List[str] = []
                    rvt_index = 0
                    for chunk in chunks:
                        if chunk.kind == "text":
                            rendered_parts.append(chunk.content)
                        else:
                            group = grouped[rvt_index] if rvt_index < len(grouped) else []
                            if group:
                                rendered_parts.append("\n".join(group).rstrip() + "\n\n")
                            rvt_index += 1
                    return "".join(rendered_parts).rstrip() + "\n"
        except Exception:
            pass

    lines.extend(directives)
    return "\n".join(lines).rstrip() + "\n"


def build_job_output_context(ctx: FrontendHelperContext, job_id: str) -> Dict[str, Any] | None:
    queued_job = ctx.queue.get_job(job_id)
    result_row = ctx.queue.get_result(job_id)
    live_output = ctx.central.get_live_job_output(job_id)

    if not queued_job and not result_row and not (live_output.get("events") or live_output.get("result_document")):
        return None

    workers = ctx.central.get_workers_snapshot()
    in_progress_worker = None
    for worker in workers:
        if getattr(worker, "current_job", None) == job_id and bool(getattr(worker, "busy", False)):
            in_progress_worker = worker
            break

    state = "queued"
    if result_row:
        state = "completed"
    elif in_progress_worker:
        state = "in_progress"

    job_data = {}
    if queued_job:
        job_data.update(queued_job)
    if result_row and isinstance(result_row.get("job_data"), dict):
        job_data.update(result_row.get("job_data") or {})

    result_data = result_row.get("result_data") if result_row else {}
    if not isinstance(result_data, dict):
        result_data = {}

    nested_rvt = result_data.get("rvt") if isinstance(result_data.get("rvt"), dict) else {}
    result_document = (
        result_data.get("result_document")
        or (nested_rvt or {}).get("result_document")
        or live_output.get("result_document")
        or ""
    )
    if not result_document and result_row:
        result_document = build_legacy_result_document(job_data, result_data)
    observer_events = result_data.get("observer_events") or live_output.get("events") or []

    rendered_output_html = ""
    if result_document.strip():
        attachment_name_to_path: Dict[str, str] = {}
        nested_invocations = []
        if isinstance(nested_rvt, dict):
            nested_invocations = nested_rvt.get("invocations") or []
        if isinstance(nested_invocations, list):
            for invocation in nested_invocations:
                if not isinstance(invocation, dict):
                    continue
                for attachment in invocation.get("attachments") or []:
                    if not isinstance(attachment, dict):
                        continue
                    path = str(attachment.get("path") or "").strip()
                    name = str(attachment.get("name") or "").strip()
                    if not name and path:
                        name = str(PurePosixPath(path).name)
                    if path:
                        normalized_path = str(PurePosixPath(path.lstrip("/")))
                        if name and name not in attachment_name_to_path:
                            attachment_name_to_path[name] = normalized_path

        def _resolve_attachment_ref(ref: str) -> str | None:
            name = str(ref or "").strip()
            if not name:
                return None
            artifact_rel = attachment_name_to_path.get(name, name)
            normalized = str(PurePosixPath(str(artifact_rel).lstrip("/")))
            if not normalized or normalized == ".":
                return None
            if any(part in {"..", ""} for part in PurePosixPath(normalized).parts):
                return None
            return url_for("scripts.job_output_artifact", job_id=job_id, artifact_path=normalized)

        try:
            rendered_output_html = render_script_rst_html(
                result_document,
                artifact_href_resolver=_resolve_attachment_ref,
            )
        except Exception as exc:
            rendered_output_html = (
                '<div class="alert alert-error">'
                f"<span>Render failed: {exc}</span>"
                "</div>"
            )

    return {
        "job_id": job_id,
        "job_data": job_data,
        "result_row": result_row,
        "result_data": result_data,
        "result_document": result_document,
        "rendered_output_html": rendered_output_html,
        "observer_events": observer_events,
        "state": state,
        "is_live": state in {"queued", "in_progress"},
        "worker_id": getattr(in_progress_worker, "worker_id", None) if in_progress_worker else None,
    }
