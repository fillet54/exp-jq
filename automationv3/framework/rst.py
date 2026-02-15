"""Utilities for reStructuredText script parsing."""

import re
from html import escape
from dataclasses import dataclass

import docutils.core
from docutils import nodes
from docutils.parsers.rst import Directive, directives
from docutils.writers.html4css1 import HTMLTranslator, Writer

from .requirements import load_default_requirements


class rvt_script(nodes.General, nodes.Element):
    """Docutils node storing a parsed ``.. rvt::`` body."""


class script_meta(nodes.General, nodes.Element):
    """Docutils node for visible script metadata."""


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


class ScriptMetaDirective(Directive):
    """Visible metadata directive used for script rendering."""

    required_arguments = 0
    optional_arguments = 0
    has_content = True
    option_spec = {}
    field_line = re.compile(r"^:([\w-]+):\s*(.*)$")

    def run(self):
        fields = {}
        for raw in self.content:
            match = self.field_line.match(raw.strip())
            if not match:
                continue
            key = match.group(1).strip()
            values = [item.strip() for item in match.group(2).split(",") if item.strip()]
            fields[key] = values

        node = script_meta()
        node["fields"] = fields
        return [node]


directives.register_directive("script-meta", ScriptMetaDirective)


class ScriptHTMLTranslator(HTMLTranslator):
    """Custom HTML translator for script detail rendering."""

    def visit_script_meta(self, node):
        fields = node.get("fields", {})
        requirements = fields.get("requirements", [])
        tags = fields.get("tags", [])
        subsystem = fields.get("subsystem", [])
        requirement_text_map = _get_requirement_text_map()

        parts = ['<section class="script-meta-block">']
        if requirements:
            parts.append("<h3>Requirements</h3><ul>")
            for req in requirements:
                req_text = requirement_text_map.get(req, "")
                if req_text:
                    parts.append(
                        f"<li><code>{escape(req)}</code>: {escape(req_text)}</li>"
                    )
                else:
                    parts.append(f"<li><code>{escape(req)}</code></li>")
            parts.append("</ul>")
        if tags:
            tag_html = "".join(
                f'<span class="badge badge-info badge-outline badge-sm">{escape(tag)}</span>'
                for tag in tags
            )
            parts.append(f'<div><strong>Tags:</strong> {tag_html}</div>')
        if subsystem:
            sub_html = "".join(
                f'<span class="badge badge-accent badge-outline badge-sm">{escape(item)}</span>'
                for item in subsystem
            )
            parts.append(f'<div><strong>Subsystem:</strong> {sub_html}</div>')
        parts.append("</section>")
        self.body.append("".join(parts))
        raise nodes.SkipNode

    def depart_script_meta(self, node):
        return None

    def visit_rvt_script(self, node):
        body = escape(node.get("body", ""))
        html = (
            '<div class="rvt-block">'
            f'<pre><code class="language-clojure">{body}</code></pre>'
            "</div>"
        )
        self.body.append(html)
        raise nodes.SkipNode

    def depart_rvt_script(self, node):
        return None


class ScriptHTMLWriter(Writer):
    def __init__(self):
        super().__init__()
        self.translator_class = ScriptHTMLTranslator


_REQUIREMENT_TEXT_MAP: dict[str, str] | None = None


def _get_requirement_text_map() -> dict[str, str]:
    global _REQUIREMENT_TEXT_MAP
    if _REQUIREMENT_TEXT_MAP is not None:
        return _REQUIREMENT_TEXT_MAP
    try:
        _REQUIREMENT_TEXT_MAP = {req.id: req.text for req in load_default_requirements()}
    except Exception:
        _REQUIREMENT_TEXT_MAP = {}
    return _REQUIREMENT_TEXT_MAP


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


def _rewrite_meta_directive_for_rendering(text: str) -> str:
    """
    Rewrite ``.. meta::`` blocks to ``.. script-meta::`` so metadata is visible
    in rendered HTML instead of being emitted as head-only meta tags.
    """
    lines = text.splitlines()
    out = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.strip().startswith(".. meta::"):
            indent = line[: len(line) - len(line.lstrip())]
            out.append(f"{indent}.. script-meta::")
            i += 1
            while i < len(lines):
                body_line = lines[i]
                if body_line.strip() == "":
                    out.append(body_line)
                    i += 1
                    continue
                if not body_line.startswith(indent + "   "):
                    break
                out.append(body_line)
                i += 1
            continue
        out.append(line)
        i += 1
    return "\n".join(out) + ("\n" if text.endswith("\n") else "")


def render_script_rst_html(text: str) -> str:
    """Render script RST to HTML with visible metadata + RVT blocks."""
    rewritten = _rewrite_meta_directive_for_rendering(text)
    parts = docutils.core.publish_parts(
        source=rewritten,
        writer=ScriptHTMLWriter(),
        settings_overrides={
            "initial_header_level": "2",
            "halt_level": 6,
            "report_level": 5,
            "file_insertion_enabled": False,
            "raw_enabled": False,
            "warning_stream": None,
        },
    )
    return parts.get("html_body", "")


# Backward-compatible aliases for older imports.
_parse_rst_chunks = parse_rst_chunks
_extract_rvt_bodies = extract_rvt_bodies
