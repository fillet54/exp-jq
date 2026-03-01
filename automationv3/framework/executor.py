from pathlib import Path
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

from . import edn, lisp
from .block import BlockResult, all_blocks
from .rst import parse_rst_chunks

_INVOCATION_KEY = "__block_invocations__"
_RUN_CONTEXT_KEY = "__run_context__"


def _notify(observer, method: str, *args, **kwargs):
    if observer and hasattr(observer, method):
        getattr(observer, method)(*args, **kwargs)


def _result_passed(result):
    if isinstance(result, BlockResult):
        return bool(result)
    if isinstance(result, bool):
        return result
    return True


def _format_timestamp(timestamp: float | None) -> str:
    if timestamp is None:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(float(timestamp), timezone.utc).isoformat()


def _normalize_result_attachments(result: Any) -> List[Dict[str, Any]]:
    raw_items = getattr(result, "attachments", None)
    if not isinstance(raw_items, list):
        return []
    attachments: List[Dict[str, Any]] = []
    for raw in raw_items:
        if isinstance(raw, dict):
            name = str(raw.get("name") or "").strip()
            path = str(raw.get("path") or "").strip()
            mime_type = str(raw.get("mime_type") or raw.get("mime") or "").strip()
            kind = str(raw.get("kind") or "").strip()
            description = str(raw.get("description") or "").strip()
        else:
            name = str(getattr(raw, "name", "") or "").strip()
            path = str(getattr(raw, "path", "") or "").strip()
            mime_type = str(getattr(raw, "mime_type", "") or "").strip()
            kind = str(getattr(raw, "kind", "") or "").strip()
            description = str(getattr(raw, "description", "") or "").strip()
        if not name and not path:
            continue
        if not name:
            name = path
        if not path:
            path = name
        attachments.append(
            {
                "name": name,
                "path": path,
                "mime_type": mime_type or "application/octet-stream",
                "kind": kind or "blob",
                "description": description,
            }
        )
    return attachments


def _result_to_rst_directives(
    result: Any,
    block_name: str,
    args: List[Any],
    timestamp: float | None = None,
    duration: float | None = None,
    source_rst: str = "",
) -> List[str]:
    if hasattr(result, "as_rst_directives") and callable(getattr(result, "as_rst_directives")):
        try:
            directives = result.as_rst_directives(
                block_name=block_name,
                args=args,
                timestamp=timestamp,
                duration=duration,
                source_rst=source_rst,
            )
        except TypeError:
            try:
                directives = result.as_rst_directives(
                    block_name=block_name,
                    args=args,
                    timestamp=timestamp,
                    duration=duration,
                )
            except TypeError:
                directives = result.as_rst_directives(block_name=block_name, args=args)
        except Exception:
            directives = []
        if isinstance(directives, list):
            return [str(item) for item in directives if str(item).strip()]
    if isinstance(result, BlockResult):
        return result.as_rst_directives(
            block_name=block_name,
            args=args,
            timestamp=timestamp,
            duration=duration,
            source_rst=source_rst,
        )

    status = "pass" if _result_passed(result) else "fail"
    source_text = str(source_rst or "").strip("\n")
    if not source_text:
        source_text = "(" + block_name + "".join(f" {edn.writes(arg)}" for arg in args) + ")"
    output = str(result)
    timestamp_text = _format_timestamp(timestamp)
    duration_value = float(duration if duration is not None else 0.0)
    lines = [
        ".. rvt-result::",
        f"   :status: {status}",
        f"   :timestamp: {timestamp_text}",
        f"   :duration: {duration_value:.6f}",
        "",
        "   .. rvt::",
        "",
        *[f"      {line}" for line in source_text.splitlines()],
        "",
        "   .. code-block:: text",
        "",
    ]
    if output:
        lines.extend([f"      {line}" for line in output.splitlines()])
    else:
        lines.append("      ")
    return ["\n".join(lines).rstrip() + "\n\n"]


def _render_block_source_rst(block: Any, args: List[Any], block_name: str) -> str:
    source_text = ""
    try:
        rendered = block.as_rst(*args)
        if isinstance(rendered, str):
            source_text = rendered.strip("\n")
        elif rendered is not None:
            source_text = str(rendered).strip("\n")
    except Exception:
        source_text = ""
    if source_text:
        return source_text
    return "(" + block_name + "".join(f" {edn.writes(arg)}" for arg in args) + ")"


def build_script_env(extra_env=None, invocations=None, observer=None):
    """Create a Lisp environment and inject all discovered blocks as functions."""
    env = lisp.Env(outer=lisp.global_env)
    call_log = invocations if invocations is not None else []

    def _record_invocation(
        block_name: str,
        args: List[Any],
        result: Any,
        error: str = "",
        timestamp: float | None = None,
        duration: float | None = None,
        source_rst: str = "",
    ) -> Any:
        passed = _result_passed(result)
        directives = _result_to_rst_directives(
            result,
            block_name,
            args,
            timestamp=timestamp,
            duration=duration,
            source_rst=source_rst,
        )
        attachments = _normalize_result_attachments(result)
        call_log.append(
            {
                "block": block_name,
                "args": list(args),
                "source_rst": source_rst,
                "passed": passed,
                "result": str(result),
                "error": error,
                "timestamp": timestamp,
                "duration": duration,
                "directives": directives,
                "attachments": attachments,
            }
        )
        _notify(
            observer,
            "on_block_end",
            block_name,
            list(args),
            str(result),
            passed,
            error,
            timestamp,
            duration,
            source_rst,
        )
        return result

    def _invoke(block, *args):
        block_name = block.name()
        arg_list = list(args)
        source_rst = _render_block_source_rst(block, arg_list, block_name)
        _notify(observer, "on_block_start", block_name, arg_list, source_rst)
        started = time.perf_counter()
        run_context = env.get(_RUN_CONTEXT_KEY, {})
        if not block.check_syntax(*args):
            return _record_invocation(
                block_name,
                arg_list,
                BlockResult(False, stderr="syntax validation failed"),
                error="syntax validation failed",
                timestamp=time.time(),
                duration=(time.perf_counter() - started),
                source_rst=source_rst,
            )
        try:
            if hasattr(block, "execute_with_context") and callable(getattr(block, "execute_with_context")):
                result = block.execute_with_context(run_context, *args)
            else:
                result = block.execute(*args)
        except Exception as exc:
            return _record_invocation(
                block_name,
                arg_list,
                BlockResult(False, stderr=str(exc)),
                error=str(exc),
                timestamp=time.time(),
                duration=(time.perf_counter() - started),
                source_rst=source_rst,
            )
        return _record_invocation(
            block_name,
            arg_list,
            result,
            error="",
            timestamp=time.time(),
            duration=(time.perf_counter() - started),
            source_rst=source_rst,
        )

    for block in all_blocks:
        env[block.name()] = lambda *args, _block=block: _invoke(_block, *args)

    if extra_env:
        env.update(extra_env)
    env[_INVOCATION_KEY] = call_log
    return env


def execute_text(text, observer=None, env=None):
    """
    Execute one or more EDN/Lisp forms from ``text`` in sequence.

    Multiple forms in a single RVT body are treated as an implicit ``do``.
    Returns ``{"passed": bool, "results": [...], "invocations": [...]}``.
    """
    active_env = env if env is not None else build_script_env(observer=observer)
    block_invocations = active_env.get(_INVOCATION_KEY)
    if not isinstance(block_invocations, list):
        block_invocations = []
        active_env[_INVOCATION_KEY] = block_invocations
    start_invocation_index = len(block_invocations)

    forms = list(edn.read_all(text))
    _notify(observer, "on_test_begin")

    results = []
    error_message = ""
    for index, form in enumerate(forms):
        _notify(observer, "on_step_start", index, form)
        try:
            value = lisp.eval(form, active_env)
            passed = _result_passed(value)
            results.append(
                {
                    "form": edn.writes(form),
                    "passed": passed,
                    "result": str(value),
                }
            )
            _notify(observer, "on_step_end", index, form, value)
        except Exception as exc:
            error_message = str(exc).strip() or "Expression execution failed."
            results.append(
                {
                    "form": edn.writes(form),
                    "passed": False,
                    "result": error_message,
                }
            )
            _notify(observer, "on_step_end", index, form, error_message)
            break

    _notify(observer, "on_test_end")

    invoked = list(block_invocations[start_invocation_index:])
    expression_passed = all(r["passed"] for r in results)
    block_passed = all(i["passed"] for i in invoked)
    response = {
        "passed": expression_passed and block_passed,
        "results": results,
        "invocations": invoked,
    }
    if error_message:
        response["error"] = error_message
    return response


def _format_invocation_result_fragments(invocations: List[Dict[str, Any]]) -> str:
    fragments: List[str] = []
    for invocation in invocations:
        directives = invocation.get("directives") or []
        if directives:
            for directive in directives:
                text = str(directive)
                if not text.endswith("\n"):
                    text += "\n"
                if not text.endswith("\n\n"):
                    text += "\n"
                fragments.append(text)
            continue

        status = "pass" if invocation.get("passed") else "fail"
        block = invocation.get("block", "block")
        args = invocation.get("args", [])
        source_text = str(invocation.get("source_rst") or "").strip("\n")
        if not source_text:
            source_text = "(" + str(block) + "".join(f" {edn.writes(arg)}" for arg in args) + ")"
        output = invocation.get("error") or invocation.get("result") or ""
        timestamp_text = _format_timestamp(invocation.get("timestamp"))
        duration_value = float(invocation.get("duration") or 0.0)
        lines = [
            ".. rvt-result::",
            f"   :status: {status}",
            f"   :timestamp: {timestamp_text}",
            f"   :duration: {duration_value:.6f}",
            "",
            "   .. rvt::",
            "",
            *[f"      {line}" for line in source_text.splitlines()],
            "",
            "   .. code-block:: text",
            "",
        ]
        if output:
            lines.extend([f"      {line}" for line in str(output).splitlines()])
        else:
            lines.append("      ")
        lines.append("")
        fragments.append("\n".join(lines))
    return "".join(fragments)


def run_script_document_text(script_text, observer=None, env=None):
    """
    Run a full RST script by preserving text chunks and executing RVT chunks.

    Output document interleaves original RST text with per-block ``rvt-result``
    directives emitted by block results.
    """
    chunks = parse_rst_chunks(script_text)
    if env is None:
        block_invocations: List[Dict[str, Any]] = []
        active_env = build_script_env(
            invocations=block_invocations,
            observer=observer,
        )
    else:
        active_env = env
        existing = active_env.get(_INVOCATION_KEY)
        if isinstance(existing, list):
            block_invocations = existing
        else:
            block_invocations = []
            active_env[_INVOCATION_KEY] = block_invocations

    all_results: List[Dict[str, Any]] = []
    result_document_parts: List[str] = []
    passed = True
    rvt_index = 0

    _notify(observer, "on_script_begin")
    for chunk_index, chunk in enumerate(chunks):
        if chunk.kind == "text":
            if chunk.content:
                result_document_parts.append(chunk.content)
                _notify(observer, "on_text_chunk", chunk_index, chunk.content, chunk.line)
                _notify(
                    observer,
                    "on_content",
                    chunk.content,
                    "text/rst",
                    {
                        "kind": "text",
                        "chunk_index": chunk_index,
                        "line": chunk.line,
                    },
                )
            continue

        if chunk.kind != "rvt":
            continue
        body = (chunk.content or "").strip()
        if not body:
            continue
        _notify(observer, "on_rvt_start", rvt_index, body, chunk.line)
        report = execute_text(body, observer=observer, env=active_env)
        block_fragments = _format_invocation_result_fragments(report.get("invocations") or [])
        for invocation in report.get("invocations") or []:
            for attachment in invocation.get("attachments") or []:
                _notify(
                    observer,
                    "on_content",
                    "",
                    str(attachment.get("mime_type") or "application/octet-stream"),
                    {
                        "kind": "attachment",
                        "name": str(attachment.get("name") or ""),
                        "path": str(attachment.get("path") or ""),
                        "content_type": str(attachment.get("mime_type") or "application/octet-stream"),
                    },
                )
        if report.get("error") and not block_fragments:
            timestamp_text = _format_timestamp(time.time())
            block_fragments = (
                ".. rvt-result::\n"
                "   :status: fail\n"
                f"   :timestamp: {timestamp_text}\n"
                "   :duration: 0.000000\n\n"
                "   .. rvt::\n\n"
                "      (evaluation-error)\n\n"
                "   .. code-block:: text\n\n"
                f"      {report.get('error')}\n\n"
            )
        if block_fragments:
            result_document_parts.append(block_fragments)
            _notify(
                observer,
                "on_content",
                block_fragments,
                "text/rst",
                {
                    "kind": "rvt_result",
                    "rvt_index": rvt_index,
                    "line": chunk.line,
                },
            )
        _notify(observer, "on_rvt_result", rvt_index, body, report, block_fragments)
        _notify(observer, "on_rvt_end", rvt_index, body, report)
        all_results.extend(report.get("results") or [])
        passed = passed and bool(report.get("passed"))
        rvt_index += 1

    _notify(observer, "on_script_end", passed)
    return {
        "passed": passed,
        "results": all_results,
        "invocations": block_invocations,
        "body_count": rvt_index,
        "result_document": "".join(result_document_parts),
    }


def run_script_document(path, observer=None, env=None):
    """Load and run full script document preserving text sections + RVT results."""
    script_text = Path(path).read_text(encoding="utf-8")
    return run_script_document_text(script_text, observer=observer, env=env)
