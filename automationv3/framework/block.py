import importlib
import pkgutil
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, List

import automationv3.plugins

from . import edn

_EDN_LINE_LIMIT = 80
_EDN_INDENT_STEP = 2


def _edn_items(value: Any) -> List[Any]:
    if isinstance(value, (edn.Vector,)):
        return list(value)
    if isinstance(value, (edn.List, list, tuple)):
        return list(value)
    if isinstance(value, (edn.Set, set)):
        return sorted(list(value), key=lambda item: edn.writes(item))
    return []


def _compact_edn(value: Any) -> str:
    if isinstance(value, (edn.Map, dict)):
        if not value:
            return "{}"
        parts = []
        for key, item in dict(value).items():
            parts.append(f"{_compact_edn(key)} {_compact_edn(item)}")
        return "{" + " ".join(parts) + "}"
    if isinstance(value, edn.List):
        return "(" + " ".join(_compact_edn(item) for item in _edn_items(value)) + ")"
    if isinstance(value, (edn.Vector, list, tuple)):
        return "[" + " ".join(_compact_edn(item) for item in _edn_items(value)) + "]"
    if isinstance(value, (edn.Set, set)):
        return "#{" + " ".join(_compact_edn(item) for item in _edn_items(value)) + "}"
    return edn.writes(value)


def _pretty_sequence_lines(
    values: List[Any],
    start: str,
    end: str,
    indent: int,
    indent_step: int,
    inline_first: bool = False,
) -> List[str]:
    if not values:
        return [f"{start}{end}"]

    lines: List[str] = []
    start_index = 0
    if inline_first:
        first = _pretty_edn_lines(values[0], indent + indent_step, indent_step)
        lines.append(f"{start}{first[0]}")
        for extra in first[1:]:
            lines.append((" " * (indent + indent_step)) + extra)
        start_index = 1
    else:
        lines.append(start)

    for item in values[start_index:]:
        rendered = _pretty_edn_lines(item, indent + indent_step, indent_step)
        lines.append((" " * (indent + indent_step)) + rendered[0])
        for extra in rendered[1:]:
            lines.append((" " * (indent + indent_step)) + extra)

    lines.append((" " * indent) + end)
    return lines


def _pretty_map_lines(value: dict[Any, Any], indent: int, indent_step: int) -> List[str]:
    if not value:
        return ["{}"]
    lines = ["{"]
    for key, item in value.items():
        key_text = edn.writes(key)
        rendered = _pretty_edn_lines(item, 0, indent_step)
        lines.append((" " * (indent + indent_step)) + f"{key_text} {rendered[0]}")
        continuation_indent = " " * (indent + indent_step + indent_step)
        for extra in rendered[1:]:
            lines.append(continuation_indent + extra.lstrip())
    lines.append((" " * indent) + "}")
    return lines


def _pretty_edn_lines(value: Any, indent: int = 0, indent_step: int = 2) -> List[str]:
    compact = _compact_edn(value)
    if "\n" not in compact and (indent + len(compact) <= _EDN_LINE_LIMIT):
        return [compact]

    if isinstance(value, (edn.Map, dict)):
        return _pretty_map_lines(dict(value), indent, indent_step)
    if isinstance(value, edn.List):
        items = _edn_items(value)
        # Lists are function/invocation heavy in this project, so keep head inline
        # and break subsequent items onto separate lines.
        return _pretty_sequence_lines(
            items, "(", ")", indent, indent_step, inline_first=True
        )
    if isinstance(value, (edn.Vector, list, tuple)):
        items = _edn_items(value)
        return _pretty_sequence_lines(items, "[", "]", indent, indent_step)
    if isinstance(value, (edn.Set, set)):
        items = _edn_items(value)
        return _pretty_sequence_lines(items, "#{", "}", indent, indent_step)
    return [edn.writes(value)]


def format_block_invocation_rst(block_name: str, args: List[Any]) -> str:
    form = edn.List([edn.Symbol(block_name), *list(args)])
    return "\n".join(_pretty_edn_lines(form, indent=0, indent_step=_EDN_INDENT_STEP))


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
        """Convert this invocation into RVT source line text.

        The returned string is injected as the source section inside
        ``.. rvt-result::`` output. Subclasses can override this to present
        custom source formatting.
        """
        return format_block_invocation_rst(self.name(), list(args))


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

    def as_rst_directives(
        self,
        block_name="",
        args=None,
        timestamp=None,
        duration=None,
        source_rst=None,
    ):
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

        source_text = str(source_rst or "").strip("\n")
        if not source_text:
            invocation_parts = [f"({block_name or 'step'}"]
            for arg in args:
                invocation_parts.append(f" {edn.writes(arg)}")
            invocation_parts.append(")")
            source_text = "".join(invocation_parts)

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
            *[f"      {line}" for line in source_text.splitlines()],
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
