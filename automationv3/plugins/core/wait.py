import io
import time
from pathlib import Path
from datetime import datetime, timezone

from automationv3.framework.block import Attachment, BuildingBlock, BlockResult
from automationv3.framework import edn


class Wait(BuildingBlock):
    def check_syntax(self, *args):
        return len(args) == 1

    def execute(self, seconds):
        time.sleep(seconds)
        return BlockResult(True)

    def as_rst(self, seconds):
        return f".. raw:: html\n\n   <span><strong>Wait</strong> {seconds} seconds</span>\n\n"  # noqa: E501


class SetupSimulation(BuildingBlock):
    def check_syntax(self, *args):
        return (len(args) % 2) == 0

    def execute(self, *arg):
        return BlockResult(True)

    def execute_with_context(self, context, *args):
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
    def name(self):
        return "Table-Driven"

    def execute(self, *args):
        return BlockResult(True)

    def as_rst(self, *args):
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
