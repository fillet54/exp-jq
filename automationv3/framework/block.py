import importlib
import pkgutil
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import PurePosixPath

import automationv3.plugins

from . import edn


class BuildingBlock:
    """
    Base class for RVT-exposed callable BuildingBlocks.

    A subclass is discovered at import time and injected into the Lisp
    execution environment by :func:`automationv3.framework.executor.build_script_env`.

    Contract
    --------
    ``name()``
      Lisp-visible function name (defaults to class name).
    ``check_syntax(*args)``
      Fast arity/type check before execute is called.
    ``execute(*args)``
      Business logic. Return :class:`BlockResult` or another value type.
    ``as_rst(*args)``
      Optional rendering helper used by some legacy/reporting paths.
    """

    def name(self):
        """Return the Lisp-visible block function name.

        Override when you need a stable/legacy name that differs from class
        name, for example ``"random-fail"``.
        """
        return type(self).__name__

    def check_syntax(self, *args):
        """Validate call arguments before execution.

        Return ``True`` when ``execute`` should be attempted, otherwise
        ``False``. Invalid syntax is reported as a failed block invocation.
        """
        return True

    def execute(self, *args):
        """Execute the block and return a result object.

        The default implementation returns a failing :class:`BlockResult` so
        subclasses are expected to override this method.
        """
        return BlockResult(False)

    def as_rst(self, *args):
        """Convert this invocation to an RST snippet.

        This primarily exists for compatibility with older rendering paths.
        Current execution/reporting is centered on ``rvt-result`` directives
        generated from execution results.
        """
        src = edn.writes(edn.List([self.name(), *args]))
        return (
            "\n".join(
                [
                    ".. code-block:: clojure",
                    "",
                    *["  " + line for line in src.splitlines()],
                ]
            )
            + "\n\n"
        )


class BuildingBlockInst:
    """Small wrapper that binds a block object to call arguments.

    The execution layer uses this abstraction when it needs both the block
    object and its positional arguments as one unit.
    """

    def __init__(self, block, args):
        self.block = block
        self.args = args

    def name(self):
        return self.block.name()

    def valid(self):
        return self.block.check_syntax(*self.args)

    def execute(self):
        return self.block.execute(*self.args)

    def __repr_rst__(self):
        return self.block.as_rst(*self.args)


class BlockResult(object):
    """
    Canonical result object returned by most BuildingBlocks.

    Parameters
    ----------
    passed:
        Boolean pass/fail state.
    stdout:
        Human-readable output text.
    stderr:
        Human-readable error text.
    attachments:
        Optional sequence of attachment metadata dictionaries or
        :class:`Attachment` objects.
    """

    def __init__(self, passed, stdout="", stderr="", attachments=None):
        self.passed = passed
        self.stdout = stdout
        self.stderr = stderr
        self.attachments = _normalize_attachments(attachments or [])

    def __bool__(self):
        return self.passed

    def __str__(self):
        result = "PASS" if self.passed else "FAIL"
        return f"<BlockResult: {result}, {self.stdout}, {self.stderr}>"

    def as_rst_directives(self, block_name="", args=None, timestamp=None, duration=None):
        """
        Render this result as one or more RST directives.

        Default behavior emits a single ``.. rvt-result::`` directive for the
        block invocation. Custom result types can override this to emit
        additional directives (artifacts, raw HTML, etc.).
        """
        args = list(args or [])
        status = "pass" if bool(self) else "fail"
        timestamp_value = float(timestamp if timestamp is not None else time.time())
        duration_value = float(duration if duration is not None else 0.0)
        timestamp_text = datetime.fromtimestamp(
            timestamp_value, tz=timezone.utc
        ).isoformat()

        invocation_parts = [f"({block_name or 'step'}"]
        for arg in args:
            invocation_parts.append(f" {edn.writes(arg)}")
        invocation_parts.append(")")
        invocation = "".join(invocation_parts)

        details = [str(self)]
        if self.stdout:
            details.append(f"stdout: {self.stdout}")
        if self.stderr:
            details.append(f"stderr: {self.stderr}")

        lines = [
            ".. rvt-result::",
            f"   :status: {status}",
            f"   :timestamp: {timestamp_text}",
            f"   :duration: {duration_value:.6f}",
            "",
            "   .. rvt::",
            "",
            *[f"      {line}" for line in invocation.splitlines()],
            "",
            "   .. code-block:: text",
            "",
            *[f"      {line}" for line in details],
        ]
        if self.attachments:
            lines.extend(
                [
                    "",
                    "   Attachments:",
                    "",
                ]
            )
        for attachment in self.attachments:
            label = attachment.name or attachment.path or "attachment"
            extra = f" ({attachment.mime_type})" if attachment.mime_type else ""
            lines.append(f"   - :attachment:`{label}`{extra}")
            if attachment.description:
                lines.extend(
                    [
                        "",
                        *[f"     {line}" for line in str(attachment.description).splitlines()],
                    ]
                )
        return ["\n".join(lines).rstrip() + "\n\n"]


@dataclass(frozen=True)
class Attachment:
    """Attachment metadata emitted by blocks for run-scoped artifacts."""

    name: str = ""
    path: str = ""
    kind: str = "blob"
    mime_type: str = "application/octet-stream"
    description: str = ""


def _normalize_attachments(raw_items):
    normalized = []
    for raw in raw_items:
        if isinstance(raw, Attachment):
            item = raw
        elif isinstance(raw, dict):
            path = str(raw.get("path") or "").strip()
            name = str(raw.get("name") or "").strip()
            if not name and path:
                name = PurePosixPath(path).name or path
            if not name and not path:
                continue
            item = Attachment(
                name=name,
                path=path,
                kind=str(raw.get("kind") or "blob").strip() or "blob",
                mime_type=(
                    str(raw.get("mime_type") or raw.get("mime") or "application/octet-stream").strip()
                    or "application/octet-stream"
                ),
                description=str(raw.get("description") or "").strip(),
            )
        else:
            continue

        if not item.name:
            default_name = PurePosixPath(item.path).name if item.path else ""
            item = Attachment(
                name=default_name or item.path or "attachment",
                path=item.path,
                kind=item.kind or "blob",
                mime_type=item.mime_type or "application/octet-stream",
                description=item.description or "",
            )
        elif not item.path:
            item = Attachment(
                name=item.name,
                path=item.name,
                kind=item.kind or "blob",
                mime_type=item.mime_type or "application/octet-stream",
                description=item.description or "",
            )
        normalized.append(item)
    return normalized


def find_block(form):
    name, *args = form

    for block in all_blocks:
        if block.name() == name and block.check_syntax(*args):
            return BuildingBlockInst(block, args)


def iter_namespace(ns_pkg):
    return pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + ".")


discovered_plugins = {
    name: importlib.import_module(name)
    for finder, name, ispkg in iter_namespace(automationv3.plugins)
}

all_blocks = [block() for block in BuildingBlock.__subclasses__()]
