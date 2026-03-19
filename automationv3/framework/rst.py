"""Utilities for reStructuredText script parsing."""

import io
import itertools
import re
from html import escape
from dataclasses import dataclass
from typing import Any, Callable, Dict

import docutils.core
from docutils import nodes
from docutils.parsers.rst import Directive, directives, roles
from docutils.writers.html4css1 import HTMLTranslator, Writer

from . import edn
from . import lisp
from .block import all_blocks
from .requirements import load_default_requirements


DOCUTILS_MESSAGE_RE = re.compile(
    r"^<[^>]+>:(?P<line>\d+): \((?P<level_name>[A-Z]+)/(?P<level>\d+)\) (?P<message>.+)$"
)
PARSE_ERROR_SUFFIX_RE = re.compile(r"\s*\(line:\s*\d+,\s*col:\s*\d+\)\s*$")


class rvt_script(nodes.General, nodes.Element):
    """Docutils node storing a parsed ``.. rvt::`` body."""


class script_meta(nodes.General, nodes.Element):
    """Docutils node for visible script metadata."""


class rvt_result(nodes.General, nodes.Element):
    """Docutils node for rendered RVT execution outcomes."""


class attachment_ref(nodes.Inline, nodes.TextElement):
    """Inline role node that points to a run attachment by name."""


class RvtDirective(Directive):
    """Docutils directive for RVT Lisp snippets."""

    required_arguments = 0
    optional_arguments = 1
    final_argument_whitespace = True
    has_content = True
    option_spec = {
        "table-driven": directives.flag,
        "variation": directives.flag,
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
        node["body_start_line"] = int(self.content_offset) + 1
        node["end_line"] = max(
            int(self.lineno),
            int(self.content_offset) + len(self.content),
        )
        return [node]


directives.register_directive("rvt", RvtDirective)


def attachment_role(role_name, rawtext, text, lineno, inliner, options=None, content=None):
    target = (text or "").strip()
    node = attachment_ref(rawtext, target)
    node["name"] = target
    return [node], []


roles.register_local_role("attachment", attachment_role)


class RvtResultDirective(Directive):
    """Directive used to render RVT execution results in output documents."""

    required_arguments = 0
    optional_arguments = 0
    has_content = True
    option_spec = {
        "status": directives.unchanged,
        "timestamp": directives.unchanged,
        "duration": directives.unchanged,
        # Backward compatibility for older generated output.
        "output": directives.unchanged,
    }

    def run(self):
        node = rvt_result()
        node["status"] = (self.options.get("status") or "").strip().lower()
        node["timestamp"] = (self.options.get("timestamp") or "").strip()
        node["duration"] = (self.options.get("duration") or "").strip()
        node["output"] = (self.options.get("output") or "").strip()
        self.state.nested_parse(self.content, self.content_offset, node)
        return [node]


directives.register_directive("rvt-result", RvtResultDirective)


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


def _clean_parse_error_message(message: str) -> str:
    return PARSE_ERROR_SUFFIX_RE.sub("", message).strip()


def _validate_rvt_body_strict(body: str):
    stream = edn.PushBackCharStream(body)
    while True:
        form = edn.read(stream)
        if form == edn.READ_EOF:
            return


def _get_rvt_reader_error(body: str):
    if not body.strip():
        return None
    try:
        _validate_rvt_body_strict(body)
        return None
    except edn.ParseError as exc:
        return exc
    except Exception as exc:
        return exc


def _format_rvt_reader_error_text(body: str, parse_error, body_start_line: int | None = None) -> str:
    if isinstance(parse_error, edn.ParseError):
        rel_line = max(int(parse_error.line), 0)
        rel_col = max(int(parse_error.col), 0)
        message = _clean_parse_error_message(str(parse_error))
    else:
        rel_line = 0
        rel_col = 0
        message = str(parse_error).strip() or "Unknown RVT reader error."

    abs_line = body_start_line + rel_line if body_start_line else None
    header = f";; RVT reader syntax error at line {rel_line + 1}, col {rel_col + 1}"
    if abs_line is not None:
        header += f" (script line {abs_line})"

    body_lines = body.splitlines()
    source_line = body_lines[rel_line] if 0 <= rel_line < len(body_lines) else ""
    pointer = (" " * rel_col) + "^"

    return "\n".join(
        [
            header,
            f";; {message}",
            source_line,
            pointer,
        ]
    )


class ScriptHTMLTranslator(HTMLTranslator):
    """Custom HTML translator for script detail rendering."""

    def __init__(self, document, artifact_href_resolver: Callable[[str], str | None] | None = None):
        self.artifact_href_resolver = artifact_href_resolver
        super().__init__(document)

    def _render_attachment_html(
        self,
        name: str,
        href_ref: str,
        mime: str = "",
        description: str = "",
    ) -> str:
        href = href_ref
        resolver = self.artifact_href_resolver
        if callable(resolver):
            try:
                resolved = resolver(href_ref)
                if isinstance(resolved, str) and resolved.strip():
                    href = resolved.strip()
            except Exception:
                href = href_ref

        label = name or href_ref or "attachment"
        if href:
            link_html = (
                f'<a class="link link-primary font-mono break-all" href="{escape(href)}" '
                f'target="_blank" rel="noopener">{escape(label)}</a>'
            )
        else:
            link_html = f'<span class="font-mono break-all">{escape(label)}</span>'

        bits = [
            '<span class="inline-flex items-center gap-1">',
            link_html,
        ]
        if mime:
            bits.append(f'<span class="opacity-70 text-[11px]">({escape(mime)})</span>')
        if description:
            bits.append(f'<span class="opacity-80 text-[11px]">{escape(description)}</span>')
        bits.append("</span>")
        return "".join(bits)

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
        raw_body = node.get("body", "")
        parse_error = _get_rvt_reader_error(raw_body)
        block_class = "rvt-block"
        if parse_error is not None:
            raw_body = _format_rvt_reader_error_text(
                raw_body,
                parse_error,
                int(node.get("body_start_line", 0) or 0),
            )
            block_class += " rvt-block-error"
        body = escape(raw_body)
        html = (
            f'<div class="{block_class}">'
            f'<pre><code class="language-clojure">{body}</code></pre>'
            "</div>"
        )
        self.body.append(html)
        raise nodes.SkipNode

    def depart_rvt_script(self, node):
        return None

    def visit_rvt_result(self, node):
        status = (node.get("status") or "").strip().lower()
        timestamp = (node.get("timestamp") or "").strip()
        duration = (node.get("duration") or "").strip()
        output = node.get("output", "")
        if status == "pass":
            status_label = "PASS"
            badge_class = "badge-success"
        elif status == "fail":
            status_label = "FAIL"
            badge_class = "badge-error"
        else:
            status_label = status.upper() if status else "UNKNOWN"
            badge_class = "badge-outline"

        meta_bits = [f'<span class="badge badge-sm {badge_class}">{status_label}</span>']
        if timestamp:
            meta_bits.append(f'<span class="text-[11px] opacity-75">ts: {escape(timestamp)}</span>')
        if duration:
            meta_bits.append(f'<span class="text-[11px] opacity-75">dur: {escape(duration)}s</span>')

        header_html = '<div class="mb-2 flex flex-wrap items-center gap-2">' + "".join(meta_bits) + "</div>"
        output_html = ""
        if output:
            output_html = (
                '<div class="text-xs mb-2">'
                f"<strong>Output:</strong> {escape(output)}"
                "</div>"
            )
        self.body.append(
            f'<div class="rvt-block rvt-result-block rvt-result-{status or "unknown"}">'
            f"{header_html}"
            f"{output_html}"
        )

    def depart_rvt_result(self, node):
        self.body.append("</div>")

    def visit_attachment_ref(self, node):
        name = (node.get("name") or node.astext() or "").strip()
        self.body.append(self._render_attachment_html(name=name, href_ref=name))
        raise nodes.SkipNode

    def depart_attachment_ref(self, node):
        return None


class ScriptHTMLWriter(Writer):
    def __init__(self, artifact_href_resolver: Callable[[str], str | None] | None = None):
        super().__init__()
        self.artifact_href_resolver = artifact_href_resolver
        self.translator_class = ScriptHTMLTranslator

    def translate(self):
        self.visitor = visitor = self.translator_class(
            self.document,
            artifact_href_resolver=self.artifact_href_resolver,
        )
        self.document.walkabout(visitor)
        for attr in self.visitor_attributes:
            setattr(self, attr, getattr(visitor, attr))
        self.output = self.apply_template()


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

    # Docutils may invoke unknown_visit/unknown_departure for custom inline
    # nodes depending on version/writer behavior. Keep traversal permissive.
    def unknown_visit(self, node):
        return None

    def unknown_departure(self, node):
        return None

    def visit_rvt_script(self, node):
        self.nodes.append(node)
        raise nodes.SkipNode


def _publish_doctree(text: str, report_level: int = 5, warning_stream=None):
    return docutils.core.publish_doctree(
        source=text,
        settings_overrides={
            "halt_level": 6,
            "report_level": report_level,
            "file_insertion_enabled": False,
            "raw_enabled": False,
            "warning_stream": warning_stream,
        },
    )


def _collect_rvt_nodes(text: str):
    document = _publish_doctree(text, report_level=5, warning_stream=None)
    visitor = RvtNodeVisitor(document)
    document.walkabout(visitor)
    return sorted(
        visitor.nodes,
        key=lambda node: int(node.get("start_line", 0) or 0),
    )


def collect_rst_syntax_issues(text: str) -> list[dict]:
    warning_stream = io.StringIO()
    try:
        _publish_doctree(text, report_level=1, warning_stream=warning_stream)
    except Exception as exc:
        return [
            {
                "source": "rst",
                "level": 4,
                "level_name": "SEVERE",
                "line": None,
                "column": None,
                "message": str(exc).strip() or "Docutils parse failure.",
                "is_error": True,
            }
        ]

    issues = []
    for raw in warning_stream.getvalue().splitlines():
        match = DOCUTILS_MESSAGE_RE.match(raw.strip())
        if not match:
            continue
        level = int(match.group("level"))
        if level < 2:
            continue
        issues.append(
            {
                "source": "rst",
                "level": level,
                "level_name": match.group("level_name"),
                "line": int(match.group("line")),
                "column": 1,
                "message": match.group("message").strip(),
                "is_error": level >= 3,
            }
        )
    return issues


def collect_rvt_syntax_issues(text: str) -> list[dict]:
    issues = []
    try:
        rvt_nodes = _collect_rvt_nodes(text)
    except Exception as exc:
        issues.append(
            {
                "source": "rvt",
                "level": 4,
                "level_name": "SEVERE",
                "line": None,
                "column": None,
                "message": str(exc).strip() or "Failed to locate RVT blocks.",
                "is_error": True,
            }
        )
        return issues

    for node in rvt_nodes:
        body = node.get("body", "")
        parse_error = _get_rvt_reader_error(body)
        if parse_error is None:
            continue

        if isinstance(parse_error, edn.ParseError):
            rel_line = max(int(parse_error.line), 0)
            rel_col = max(int(parse_error.col), 0)
            body_start_line = int(node.get("body_start_line", 0) or 0)
            abs_line = body_start_line + rel_line if body_start_line else None
            issue = {
                "source": "rvt",
                "level": 3,
                "level_name": "ERROR",
                "line": abs_line,
                "column": rel_col + 1,
                "message": _clean_parse_error_message(str(parse_error)),
                "is_error": True,
            }
        else:
            body_start_line = int(node.get("body_start_line", 0) or 0)
            issue = {
                "source": "rvt",
                "level": 3,
                "level_name": "ERROR",
                "line": body_start_line or None,
                "column": 1,
                "message": str(parse_error).strip() or "Unknown RVT reader error.",
                "is_error": True,
            }
        issues.append(issue)
    return issues


class _SemanticScope:
    def __init__(self, parent: "_SemanticScope | None" = None, initial: set[str] | None = None):
        self.parent = parent
        self.symbols = set(initial or [])

    def define(self, symbol: str) -> None:
        if symbol:
            self.symbols.add(symbol)

    def contains(self, symbol: str) -> bool:
        if symbol in self.symbols:
            return True
        if self.parent is not None:
            return self.parent.contains(symbol)
        return False

    def child(self) -> "_SemanticScope":
        return _SemanticScope(parent=self)


def _default_semantic_symbols() -> set[str]:
    symbols = set()
    for key in lisp.global_env.keys():
        if isinstance(key, str) and key.strip():
            symbols.add(key)
    for key in lisp.special_forms.keys():
        if isinstance(key, str) and key.strip():
            symbols.add(key)
    for block in all_blocks:
        try:
            name = block.name()
        except Exception:
            continue
        if isinstance(name, str) and name.strip():
            symbols.add(name)
    return symbols


def _read_all_forms_strict(text: str) -> list[Any]:
    stream = edn.PushBackCharStream(text)
    forms: list[Any] = []
    while True:
        form = edn.read(stream)
        if form == edn.READ_EOF:
            return forms
        if form == stream:
            continue
        forms.append(form)


def _symbol_issue(symbol: edn.Symbol, body_start_line: int | None) -> dict:
    rel_line = int(symbol.meta.get("start_row", 0) or 0)
    rel_col = int(symbol.meta.get("start_col", 0) or 0)
    abs_line = body_start_line + rel_line if body_start_line else None
    return {
        "source": "rvt",
        "level": 2,
        "level_name": "WARNING",
        "line": abs_line,
        "column": rel_col + 1,
        "message": f"Unknown symbol '{symbol}' may fail at runtime.",
        "is_error": False,
    }


def _bind_vector_symbols(vector_form: Any, scope: _SemanticScope) -> None:
    if not isinstance(vector_form, edn.Vector):
        return
    bind_next_as_rest = False
    for item in vector_form:
        if not isinstance(item, edn.Symbol):
            continue
        name = str(item)
        if name == "&":
            bind_next_as_rest = True
            continue
        scope.define(name)
        if bind_next_as_rest:
            bind_next_as_rest = False


def _analyze_fn_like(form: edn.List, scope: _SemanticScope, issues: list[dict], body_start_line: int | None) -> None:
    if not form:
        return
    head_name = str(form[0]) if isinstance(form[0], edn.Symbol) else ""
    cursor = 1
    if head_name == "defn":
        if len(form) > 1 and isinstance(form[1], edn.Symbol):
            scope.define(str(form[1]))
        cursor = 2
    elif len(form) > 1 and isinstance(form[1], edn.Symbol):
        cursor = 2

    if len(form) <= cursor:
        return

    clauses = []
    if isinstance(form[cursor], edn.Vector):
        clauses.append((form[cursor], list(form[cursor + 1 :])))
    else:
        for clause in form[cursor:]:
            if isinstance(clause, edn.List) and clause and isinstance(clause[0], edn.Vector):
                clauses.append((clause[0], list(clause[1:])))

    for params, body_forms in clauses:
        fn_scope = scope.child()
        if head_name == "defn" and len(form) > 1 and isinstance(form[1], edn.Symbol):
            fn_scope.define(str(form[1]))
        _bind_vector_symbols(params, fn_scope)
        for body_form in body_forms:
            _analyze_form(body_form, fn_scope, issues, body_start_line)


def _analyze_form(form: Any, scope: _SemanticScope, issues: list[dict], body_start_line: int | None) -> None:
    if isinstance(form, edn.Keyword):
        return

    if isinstance(form, edn.Symbol):
        symbol_name = str(form)
        if not scope.contains(symbol_name):
            issues.append(_symbol_issue(form, body_start_line))
        return

    if isinstance(form, edn.Vector):
        for item in form:
            _analyze_form(item, scope, issues, body_start_line)
        return

    if isinstance(form, edn.Set):
        for item in form:
            _analyze_form(item, scope, issues, body_start_line)
        return

    if isinstance(form, (edn.Map, dict)):
        for key, value in form.items():
            _analyze_form(key, scope, issues, body_start_line)
            _analyze_form(value, scope, issues, body_start_line)
        return

    if not isinstance(form, edn.List):
        return

    if not form:
        return

    head = form[0]
    if isinstance(head, edn.Symbol):
        head_name = str(head)

        if head_name == "quote":
            return

        if head_name == "def":
            if len(form) >= 3:
                _analyze_form(form[2], scope, issues, body_start_line)
            if len(form) >= 2 and isinstance(form[1], edn.Symbol):
                scope.define(str(form[1]))
            return

        if head_name == "let":
            let_scope = scope.child()
            bindings = form[1] if len(form) >= 2 else None
            if isinstance(bindings, edn.Vector):
                binding_items = list(bindings)
                for idx in range(0, len(binding_items), 2):
                    binding_symbol = binding_items[idx]
                    binding_expr = binding_items[idx + 1] if idx + 1 < len(binding_items) else None
                    if binding_expr is not None:
                        _analyze_form(binding_expr, let_scope, issues, body_start_line)
                    if isinstance(binding_symbol, edn.Symbol):
                        let_scope.define(str(binding_symbol))
            for body_form in form[2:]:
                _analyze_form(body_form, let_scope, issues, body_start_line)
            return

        if head_name == "do":
            for body_form in form[1:]:
                _analyze_form(body_form, scope, issues, body_start_line)
            return

        if head_name == "if":
            if len(form) >= 2:
                _analyze_form(form[1], scope, issues, body_start_line)
            if len(form) >= 3:
                _analyze_form(form[2], scope.child(), issues, body_start_line)
            if len(form) >= 4:
                _analyze_form(form[3], scope.child(), issues, body_start_line)
            return

        if head_name in {"fn", "defn"}:
            _analyze_fn_like(form, scope, issues, body_start_line)
            return

        if head_name.startswith("."):
            for arg in form[1:]:
                _analyze_form(arg, scope, issues, body_start_line)
            return

    _analyze_form(head, scope, issues, body_start_line)
    for arg in form[1:]:
        _analyze_form(arg, scope, issues, body_start_line)


def collect_rvt_semantic_issues(text: str) -> list[dict]:
    issues: list[dict] = []
    base_symbols = _default_semantic_symbols()
    try:
        for dimension in extract_rvt_variation_dimensions(text):
            for symbol in dimension.get("symbols") or []:
                if isinstance(symbol, str) and symbol.strip():
                    base_symbols.add(symbol)
    except Exception:
        pass

    try:
        rvt_nodes = _collect_rvt_nodes(text)
    except Exception:
        return issues

    for node in rvt_nodes:
        body = node.get("body", "")
        if not body.strip():
            continue
        if _get_rvt_reader_error(body) is not None:
            continue
        try:
            forms = _read_all_forms_strict(body)
        except Exception:
            continue
        scope = _SemanticScope(initial=base_symbols)
        body_start_line = int(node.get("body_start_line", 0) or 0)
        for form in forms:
            _analyze_form(form, scope, issues, body_start_line)
    return issues


def collect_script_syntax_issues(text: str) -> list[dict]:
    issues = collect_rst_syntax_issues(text)
    issues.extend(collect_rvt_syntax_issues(text))
    issues.extend(collect_rvt_semantic_issues(text))
    return sorted(
        issues,
        key=lambda issue: (
            issue.get("line") is None,
            issue.get("line") or 0,
            issue.get("column") or 0,
            issue.get("source") or "",
        ),
    )


def parse_rst_chunks(text):
    """
    Parse an RST script and return ordered chunks.

    Currently materializes ``text`` and ``rvt`` chunks.
    """
    rvt_nodes = _collect_rvt_nodes(text)
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


def _is_sequence_value(value: Any) -> bool:
    return isinstance(value, (list, tuple))


def _parse_rvt_variation_table(body: str, start_line: int | None = None) -> Dict[str, Any]:
    line_hint = f" near line {start_line}" if start_line else ""

    def fail(message: str) -> None:
        raise ValueError(f"{message}{line_hint}.")

    forms = list(edn.read_all(body) or [])
    if len(forms) != 1:
        fail("Variation directive must contain exactly one EDN table expression")

    table = forms[0]
    if not _is_sequence_value(table) or len(table) < 2:
        fail("Variation table must be a list/vector with a header row and at least one variation row")

    header = table[0]
    if not _is_sequence_value(header) or not header:
        fail("Variation header row must be a non-empty list of symbols")

    symbols = [str(raw_symbol or "").strip() for raw_symbol in header]
    if any(not symbol for symbol in symbols):
        fail("Variation header contains an empty symbol")

    variants: list[Dict[str, Any]] = []
    expected_row_len = len(symbols) + 1
    for row_index, row in enumerate(table[1:], start=1):
        if not _is_sequence_value(row):
            fail(f"Variation row {row_index} must be a list/vector")
        if len(row) != expected_row_len:
            fail(
                f"Variation row {row_index} expected {expected_row_len} items "
                f"(name + {len(symbols)} values), got {len(row)}"
            )

        variation_name, *values = row
        variation_name = str(variation_name or "").strip()
        if not variation_name:
            fail(f"Variation row {row_index} has an empty name")

        bindings_edn = {
            symbol: edn.writes(value)
            for symbol, value in zip(symbols, values)
        }
        variants.append(
            {
                "name": variation_name,
                "values": values,
                "bindings": bindings_edn,
            }
        )

    if not variants:
        fail("Variation table must include at least one variation row")

    return {
        "symbols": symbols,
        "variants": variants,
    }


def extract_rvt_variation_dimensions(text: str) -> list[Dict[str, Any]]:
    """Return variation dimensions declared via ``.. rvt::`` directives.

    A directive is considered a variation source when it includes the
    ``:variation:`` option and its body contains a single EDN table expression:

    - header row: symbol names
    - each following row: ``[variation-name value1 value2 ...]``
    """
    dimensions: list[Dict[str, Any]] = []
    for node in _collect_rvt_nodes(text):
        options = node.get("options") or {}
        if "variation" not in options:
            continue
        dimensions.append(
            _parse_rvt_variation_table(
                body=str(node.get("body") or ""),
                start_line=int(node.get("start_line", 0) or 0) or None,
            )
        )
    return dimensions


def expand_rvt_variations(text: str) -> list[Dict[str, Any]]:
    """Expand all declared variation dimensions into cartesian combinations.

    Returns list items shaped as:

    - ``name``: combined human-readable variation name
    - ``components``: per-dimension variation names
    - ``bindings``: mapping ``symbol -> edn-literal-string``
    """
    dimensions = extract_rvt_variation_dimensions(text)
    if not dimensions:
        return []

    seen_symbols: set[str] = set()
    for dim in dimensions:
        for symbol in dim.get("symbols") or []:
            if symbol in seen_symbols:
                raise ValueError(
                    f"Duplicate variation symbol '{symbol}' across variation directives."
                )
            seen_symbols.add(symbol)

    combinations: list[Dict[str, Any]] = []
    variant_sets = [dim.get("variants") or [] for dim in dimensions]
    for combo in itertools.product(*variant_sets):
        components = [str(item.get("name") or "").strip() for item in combo]
        bindings: Dict[str, str] = {}
        for item in combo:
            for symbol, value in (item.get("bindings") or {}).items():
                bindings[str(symbol)] = str(value)
        combined_name = " / ".join([name for name in components if name]).strip()
        combinations.append(
            {
                "name": combined_name,
                "components": components,
                "bindings": bindings,
            }
        )
    return combinations


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


def render_script_rst_html(
    text: str,
    artifact_href_resolver: Callable[[str], str | None] | None = None,
) -> str:
    """Render script RST to HTML with visible metadata + RVT blocks."""
    rewritten = _rewrite_meta_directive_for_rendering(text)
    parts = docutils.core.publish_parts(
        source=rewritten,
        writer=ScriptHTMLWriter(artifact_href_resolver=artifact_href_resolver),
        settings_overrides={
            "initial_header_level": "2",
            "halt_level": 6,
            "report_level": 5,
            "file_insertion_enabled": False,
            "raw_enabled": True,
            "warning_stream": None,
        },
    )
    return parts.get("html_body", "")


# Backward-compatible aliases for older imports.
_parse_rst_chunks = parse_rst_chunks
_extract_rvt_bodies = extract_rvt_bodies
