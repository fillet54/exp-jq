"""Utilities for reStructuredText script parsing."""

import re
from dataclasses import dataclass

import docutils.core
from docutils import nodes
from docutils.parsers.rst import Directive, directives


class rvt_script(nodes.General, nodes.Element):
    """Docutils node storing a parsed ``.. rvt::`` body."""


class RvtDirective(Directive):
    """Docutils directive for RVT Lisp snippets."""

    required_arguments = 0
    optional_arguments = 1
    final_argument_whitespace = True
    has_content = True
    option_spec = {
        "table-driven": directives.flag,
        "name": directives.unchanged,
        "id": directives.unchanged,
        "tags": directives.unchanged,
    }
    option_line = re.compile(r"^:[\w-]+:\s*.*$")

    def run(self):
        body_lines = list(self.content)
        while body_lines and self.option_line.match(body_lines[0].strip()):
            body_lines.pop(0)
        while body_lines and not body_lines[0].strip():
            body_lines.pop(0)

        node = rvt_script()
        node["body"] = "\n".join(body_lines)
        node["title"] = self.arguments[0].strip() if self.arguments else ""
        node["options"] = dict(self.options)
        node["line"] = int(self.lineno)
        node["start_line"] = int(self.lineno)
        node["end_line"] = max(
            int(self.lineno),
            int(self.content_offset) + len(self.content),
        )
        return [node]


directives.register_directive("rvt", RvtDirective)


@dataclass
class RstChunk:
    """One parsed chunk of an RST document in source order."""

    kind: str
    content: str
    line: int | None = None


class RvtNodeVisitor(nodes.GenericNodeVisitor):
    """Collect ``rvt_script`` nodes using standard Docutils traversal."""

    def __init__(self, document):
        super().__init__(document)
        self.nodes = []

    def default_visit(self, node):
        return None

    def default_departure(self, node):
        return None

    def visit_rvt_script(self, node):
        self.nodes.append(node)
        raise nodes.SkipNode


def parse_rst_chunks(text):
    """
    Parse an RST script and return ordered chunks.

    Currently materializes ``text`` and ``rvt`` chunks.
    """
    document = docutils.core.publish_doctree(
        source=text,
        settings_overrides={
            "halt_level": 6,
            "report_level": 5,
            "file_insertion_enabled": False,
            "raw_enabled": False,
            "warning_stream": None,
        },
    )
    visitor = RvtNodeVisitor(document)
    document.walkabout(visitor)
    rvt_nodes = sorted(
        visitor.nodes,
        key=lambda node: int(node.get("start_line", 0) or 0),
    )
    if not rvt_nodes:
        return [RstChunk("text", text, 1)] if text else []

    lines = text.splitlines(keepends=True)
    chunks = []
    cursor = 1
    for node in rvt_nodes:
        start_line = int(node.get("start_line", cursor))
        end_line = int(node.get("end_line", start_line))
        if cursor < start_line:
            text_chunk = "".join(lines[cursor - 1 : start_line - 1])
            if text_chunk:
                chunks.append(RstChunk("text", text_chunk, cursor))
        chunks.append(RstChunk("rvt", node.get("body", ""), start_line))
        cursor = end_line + 1

    if cursor <= len(lines):
        tail = "".join(lines[cursor - 1 :])
        if tail:
            chunks.append(RstChunk("text", tail, cursor))
    return chunks


def extract_rvt_bodies(text):
    """Extract each ``.. rvt::`` directive body from an RST document."""
    chunks = parse_rst_chunks(text)
    return [
        chunk.content.strip()
        for chunk in chunks
        if chunk.kind == "rvt" and chunk.content.strip()
    ]


# Backward-compatible aliases for older imports.
_parse_rst_chunks = parse_rst_chunks
_extract_rvt_bodies = extract_rvt_bodies
