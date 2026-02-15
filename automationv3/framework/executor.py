from pathlib import Path
from typing import Any, Dict, List

from . import edn, lisp
from .block import BlockResult, all_blocks
from .rst import extract_rvt_bodies, parse_rst_chunks

_INVOCATION_KEY = "__block_invocations__"


def _notify(observer, method: str, *args, **kwargs):
    if observer and hasattr(observer, method):
        getattr(observer, method)(*args, **kwargs)


def _result_passed(result):
    if isinstance(result, BlockResult):
        return bool(result)
    if isinstance(result, bool):
        return result
    return True


def _result_to_rst_directives(result: Any, block_name: str, args: List[Any]) -> List[str]:
    if hasattr(result, "as_rst_directives") and callable(getattr(result, "as_rst_directives")):
        try:
            directives = result.as_rst_directives(block_name=block_name, args=args)
            if isinstance(directives, list):
                return [str(item) for item in directives if str(item).strip()]
        except Exception:
            pass
    if isinstance(result, BlockResult):
        return result.as_rst_directives(block_name=block_name, args=args)

    status = "pass" if _result_passed(result) else "fail"
    invocation = "(" + block_name + "".join(f" {edn.writes(arg)}" for arg in args) + ")"
    output = str(result)
    lines = [
        ".. rvt-result::",
        f"   :status: {status}",
    ]
    if output:
        lines.append(f"   :output: {output}")
    lines.extend(["", f"   {invocation}"])
    return ["\n".join(lines).rstrip() + "\n\n"]


def build_script_env(extra_env=None, invocations=None, observer=None):
    """Create a Lisp environment and inject all discovered blocks as functions."""
    env = lisp.Env(outer=lisp.global_env)
    call_log = invocations if invocations is not None else []

    def _record_invocation(block_name: str, args: List[Any], result: Any, error: str = "") -> Any:
        passed = _result_passed(result)
        directives = _result_to_rst_directives(result, block_name, args)
        call_log.append(
            {
                "block": block_name,
                "args": list(args),
                "passed": passed,
                "result": str(result),
                "error": error,
                "directives": directives,
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
        )
        return result

    def _invoke(block, *args):
        block_name = block.name()
        arg_list = list(args)
        _notify(observer, "on_block_start", block_name, arg_list)
        if not block.check_syntax(*args):
            return _record_invocation(
                block_name,
                arg_list,
                BlockResult(False, stderr="syntax validation failed"),
                error="syntax validation failed",
            )
        try:
            result = block.execute(*args)
        except Exception as exc:
            return _record_invocation(
                block_name,
                arg_list,
                BlockResult(False, stderr=str(exc)),
                error=str(exc),
            )
        return _record_invocation(block_name, arg_list, result, error="")

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
        output = invocation.get("error") or invocation.get("result") or ""
        invocation_text = "(" + str(block) + "".join(f" {edn.writes(arg)}" for arg in args) + ")"
        lines = [
            ".. rvt-result::",
            f"   :status: {status}",
        ]
        if output:
            lines.append(f"   :output: {output}")
        lines.extend(["", f"   {invocation_text}", ""])
        fragments.append("\n".join(lines))
    return "".join(fragments)


def run_script_document_text(script_text, observer=None, env=None):
    """
    Run a full RST script by preserving text chunks and executing RVT chunks.

    Output document interleaves original RST text with per-block ``rvt-result``
    directives emitted by block results.
    """
    chunks = parse_rst_chunks(script_text)
    block_invocations: List[Dict[str, Any]] = []
    active_env = env or build_script_env(
        invocations=block_invocations,
        observer=observer,
    )

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
            continue

        if chunk.kind != "rvt":
            continue
        body = (chunk.content or "").strip()
        if not body:
            continue
        _notify(observer, "on_rvt_start", rvt_index, body, chunk.line)
        report = execute_text(body, observer=observer, env=active_env)
        block_fragments = _format_invocation_result_fragments(report.get("invocations") or [])
        if report.get("error") and not block_fragments:
            block_fragments = (
                ".. rvt-result::\n"
                "   :status: fail\n"
                f"   :output: {report.get('error')}\n\n"
                "   ;; RVT evaluation failed before block invocation.\n\n"
            )
        if block_fragments:
            result_document_parts.append(block_fragments)
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


def run_rvt_script_text(script_text, observer=None, env=None):
    """
    Run all ``.. rvt::`` directive bodies found in ``script_text`` as Lisp.
    """
    bodies = extract_rvt_bodies(script_text)
    if not bodies:
        return {"passed": True, "results": [], "body_count": 0}

    results = []
    invocations = []
    passed = True
    for body in bodies:
        report = execute_text(body, observer=observer, env=env)
        results.extend(report["results"])
        invocations.extend(report.get("invocations", []))
        passed = passed and report["passed"]
    return {
        "passed": passed,
        "results": results,
        "invocations": invocations,
        "body_count": len(bodies),
    }


def run_rvt_script(path, observer=None, env=None):
    """Load an RST script file and run all ``.. rvt::`` directive bodies."""
    script_text = Path(path).read_text(encoding="utf-8")
    return run_rvt_script_text(script_text, observer=observer, env=env)


def run_script_document(path, observer=None, env=None):
    """Load and run full script document preserving text sections + RVT results."""
    script_text = Path(path).read_text(encoding="utf-8")
    return run_script_document_text(script_text, observer=observer, env=env)
