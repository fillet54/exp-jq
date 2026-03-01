"""Core BuildingBlocks used by RVT execution.

This module contains timing, setup, and presentation-oriented blocks that are
available in the script execution environment.
"""

import time
from pathlib import Path
from datetime import datetime, timezone

from automationv3.framework.block import (
    Attachment,
    BuildingBlock,
    BlockResult,
    format_block_invocation_rst,
)


class Wait(BuildingBlock):
    """Pause execution for a number of seconds.

    Usage in ``.. rvt::``:

    ``(Wait 2)``

    Notes
    -----
    ``seconds`` is passed directly to :func:`time.sleep`.
    """

    def check_syntax(self, *args):
        """Require exactly one argument."""
        return len(args) == 1

    def execute(self, seconds):
        """Sleep for ``seconds`` and return a passing result."""
        time.sleep(seconds)
        return BlockResult(True)

    def as_rst(self, seconds):
        """Render invocation source lines used by ``rvt-result`` output."""
        return format_block_invocation_rst(self.name(), [seconds])


class SetupSimulation(BuildingBlock):
    """Create a lightweight simulation setup result and optional attachment.

    Expected argument form is key/value pairs:

    ``(SetupSimulation "mode" "nominal" "seed" "42")``

    When run with context that includes ``artifacts_dir``, this block writes
    ``attachments/setup-simulation.log`` and returns it as an attachment in the
    emitted ``rvt-result`` content.
    """

    def check_syntax(self, *args):
        """Accept either a single dict or key/value token pairs."""
        if len(args) == 1 and isinstance(args[0], dict):
            return True
        return len(args) >= 2 and (len(args) % 2 == 0)

    def execute(self, *arg):
        """Fallback execution path when no run context is provided."""
        return BlockResult(True)

    def execute_with_context(self, context, *args):
        """Execute with run context and produce setup attachment metadata.

        Parameters
        ----------
        context:
            Execution context dictionary. ``artifacts_dir`` is used when
            present to persist attachment content.
        *args:
            Alternating key/value tokens describing the simulation setup.
        """
        context = context or {}
        run_artifacts_dir = str(context.get("artifacts_dir") or "").strip()
        details = {}
        if len(args) == 1 and isinstance(args[0], dict):
            for key, value in args[0].items():
                details[str(key)] = str(value)
        else:
            for key, value in zip(args[::2], args[1::2]):
                details[str(key)] = str(value)

        attachments = []
        if run_artifacts_dir:
            root = Path(run_artifacts_dir)
            relpath = Path("attachments") / "setup-simulation.log"
            target = root / relpath
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(
                "\n".join(
                    [
                        "SetupSimulation static log",
                        f"generated_utc={datetime.now(timezone.utc).isoformat()}",
                        *[f"{key}={value}" for key, value in sorted(details.items())],
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            attachments.append(
                Attachment(
                    name="setup-simulation.log",
                    path=relpath.as_posix(),
                    kind="text",
                    mime_type="text/plain",
                    description="Example simulation setup log artifact.",
                )
            )

        stdout = "setup simulation complete"
        if details:
            stdout += " (" + ", ".join(f"{k}={v}" for k, v in sorted(details.items())) + ")"
        return BlockResult(True, stdout=stdout, attachments=attachments)

    def as_rst(self, *args):
        """Render invocation source lines used by ``rvt-result`` output."""
        return format_block_invocation_rst(self.name(), list(args))


class TableDriven(BuildingBlock):
    """Render a data table as HTML for report-style visualization.

    Usage in ``.. rvt::`` with EDN collections:

    ``(Table-Driven ["name" "status"] [["boot" "PASS"] ["io" "FAIL"]])``

    The first argument is the header row; the second is a list of rows.
    """

    def name(self):
        """Expose the historical block name with a dash."""
        return "Table-Driven"

    def execute(self, *args):
        """Return success; formatting is handled by :meth:`as_rst`."""
        return BlockResult(True)

    def as_rst(self, *args):
        """Render invocation source lines used by ``rvt-result`` output."""
        return format_block_invocation_rst(self.name(), list(args))
