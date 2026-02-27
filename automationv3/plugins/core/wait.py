"""Core BuildingBlocks used by RVT execution.

This module contains timing, setup, and presentation-oriented blocks that are
available in the script execution environment.
"""

import io
import time
from pathlib import Path
from datetime import datetime, timezone

from automationv3.framework.block import Attachment, BuildingBlock, BlockResult
from automationv3.framework import edn


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
        """Render a compact HTML representation for non-result contexts."""
        return f".. raw:: html\n\n   <span><strong>Wait</strong> {seconds} seconds</span>\n\n"  # noqa: E501


class SetupSimulation(BuildingBlock):
    """Create a lightweight simulation setup result and optional attachment.

    Expected argument form is key/value pairs:

    ``(SetupSimulation "mode" "nominal" "seed" "42")``

    When run with context that includes ``artifacts_dir``, this block writes
    ``attachments/setup-simulation.log`` and returns it as an attachment in the
    emitted ``rvt-result`` content.
    """

    def check_syntax(self, *args):
        """Require an even number of arguments (key/value pairs)."""
        return (len(args) % 2) == 0

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
        """Render this block invocation as a multi-line Lisp form snippet."""
        lines = [".. code-block:: clojure", "", "   (SetupSimulation"]
        # TODO: Clean this up
        for arg1, arg2 in zip(args[::2], args[1::2]):
            if isinstance(arg1, str) and not isinstance(
                arg1, (edn.Symbol, edn.Keyword)
            ):
                arg1 = f'"{arg1}"'
            if isinstance(arg2, str) and not isinstance(
                arg2, (edn.Symbol, edn.Keyword)
            ):
                arg2 = f'"{arg2}"'
            lines.append(f"      {arg1} {arg2}")
        lines[-1] += ")\n\n"

        return "\n".join(lines)


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
        """Render the provided tabular data into a styled HTML table."""
        headers, rows = args
        s = io.StringIO("")
        s.write('   <div class="-mx-4 -my-2 overflow-x-auto sm:-mx-6 lg:-mx-8">\n')
        s.write(
            '      <div class="inline-block min-w-full py-2 align-middle sm:px-6 lg:px-8">\n'  # noqa: E501
        )
        s.write(
            '         <div class="overflow-hidden shadow ring-1 ring-black ring-opacity-5 sm:rounded-lg">\n'  # noqa: E501
        )
        s.write(
            '            <table class="my-0 min-w-full divide-y divide-gray-300">\n'
        )
        s.write('               <thead class="bg-gray-50">\n')
        s.write('                  <tr class="divide-x divide-gray-200">\n')
        s.write("                     ")
        s.write(
            "                     ".join(
                f'<td class="px-3 py-3.5 text-left text-sm font-semibold text-gray-900">{header}</td>\n'  # noqa: E501
                for header in headers
            )
        )
        s.write("                  </tr>\n")
        s.write("                </thead>\n")
        s.write("                <tbody>\n")
        for row in rows:
            s.write('                   <tr class="divide-x divide-gray-200">\n')
            s.write("                      ")
            s.write(
                "                      ".join(
                    f'<td class="whitespace-nowrap py-4 pl-4 pr-3 text-sm font-medium text-gray-900 sm:pl-6">{data}</td>\n'  # noqa: E501
                    for data in row
                )
            )
            s.write("                   </tr>\n")
        s.write("                </tbody>\n")
        s.write("             </table>\n")
        s.write("          </div>\n")
        s.write("      </div>\n")
        s.write("   </div>\n")

        html = s.getvalue()

        rst = f".. raw:: html\n\n{html}\n\n"
        return rst
