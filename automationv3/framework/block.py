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
    The 'BuildingBlock' of the automation framework. Registers as a function to
    be run during text execution.
    """

    def name(self):
        """Returns the name of the building block. The name is used
        as a first order lookup for the block"""
        return type(self).__name__

    def check_syntax(self, *args):
        """Returns True if this BuildingBlock can support the
        arguments and False otherwise"""
        return True

    def execute(self, *args):
        """Executes the block.

        Returns a BlockResult"""
        return BlockResult(False)

    def as_rst(self, *args):
        """Converts block with arguments to RST

        Note: We don't use the something like __repr_rst__
        here due to maintaining backwards compatibility. A
        block is sort of treated as a singleton which then
        provides member functions that take a specific list
        of arguments. A building block typically will
        get wrapped up in a class that

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
    """Building block `instance` which packs block together with arguments

    This provides a mechanism to make a BuildingBlock
    more pythonic without breaking backwards compatibility.

    New blocks are free to implement either this or BuildingBlock.
    The framework will mostly be interfacing with blocks via this
    interface.
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
    The result of executing a BuildingBlock
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
