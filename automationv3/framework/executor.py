import re
from pathlib import Path

from . import edn, lisp
from .block import BlockResult, all_blocks

RVT_DIRECTIVE = re.compile(r"^\.\.\s+rvt::\s*$")


def _extract_rvt_bodies(text):
    """Extract the body text for each ``.. rvt::`` directive in an RST document."""
    bodies = []
    lines = text.splitlines()
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        if RVT_DIRECTIVE.match(line):
            idx += 1
            body = []
            while idx < len(lines):
                raw = lines[idx]
                if raw.strip() == "":
                    if body:
                        body.append("")
                    idx += 1
                    continue
                if not raw.startswith("   "):
                    break
                content = raw[3:]
                # Support directive options but ignore them for now.
                if not body and content.startswith(":"):
                    idx += 1
                    continue
                body.append(content)
                idx += 1
            body_text = "\n".join(body).strip()
            if body_text:
                bodies.append(body_text)
            continue
        idx += 1
    return bodies


def build_script_env(extra_env=None, invocations=None):
    """Create a Lisp environment and inject all discovered blocks as functions."""
    env = lisp.Env(outer=lisp.global_env)
    call_log = invocations if invocations is not None else []

    def _invoke(block, *args):
        if not block.check_syntax(*args):
            raise TypeError(
                f"Invalid arguments for block '{block.name()}': {args}"
            )
        result = block.execute(*args)
        call_log.append(
            {
                "block": block.name(),
                "args": list(args),
                "passed": _result_passed(result),
                "result": str(result),
            }
        )
        return result

    for block in all_blocks:
        env[block.name()] = lambda *args, _block=block: _invoke(_block, *args)

    if extra_env:
        env.update(extra_env)
    return env


def _result_passed(result):
    if isinstance(result, BlockResult):
        return bool(result)
    if isinstance(result, bool):
        return result
    return True


def execute_text(text, observer=None, env=None):
    """
    Execute one or more EDN/Lisp forms from ``text``.

    Returns ``{"passed": bool, "results": [...]}`` where each result contains
    a serialized form, pass/fail, and stringified result.
    """
    block_invocations = []
    active_env = env or build_script_env(invocations=block_invocations)
    forms = list(edn.read_all(text))
    if observer and hasattr(observer, "on_test_begin"):
        observer.on_test_begin()

    results = []
    for index, form in enumerate(forms):
        if observer and hasattr(observer, "on_step_start"):
            observer.on_step_start(index, form)
        value = lisp.eval(form, active_env)
        passed = _result_passed(value)
        results.append(
            {
                "form": edn.writes(form),
                "passed": passed,
                "result": str(value),
            }
        )
        if observer and hasattr(observer, "on_step_end"):
            observer.on_step_end(index, form, value)

    if observer and hasattr(observer, "on_test_end"):
        observer.on_test_end()

    expression_passed = all(r["passed"] for r in results)
    block_passed = all(i["passed"] for i in block_invocations)
    return {
        "passed": expression_passed and block_passed,
        "results": results,
        "invocations": block_invocations,
    }


def run_rvt_script_text(script_text, observer=None, env=None):
    """
    Run all ``.. rvt::`` directive bodies found in ``script_text`` as Lisp.
    """
    bodies = _extract_rvt_bodies(script_text)
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
