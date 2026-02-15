"""Utilities for reStructuredText

"""

import re
from dataclasses import dataclass

import docutils.core
from docutils import writers, nodes
from docutils.parsers.rst import roles, Directive, directives
from docutils.writers.html4css1 import Writer, HTMLTranslator

from . import edn
from .block import find_block

try:
    from ..database import db
    from ..requirements.models import Requirement
except Exception:
    db = None
    Requirement = None


def requirement_reference_role(
    role, rawtext, text, lineno, inliner, options=None, content=None
):
    """rst role to support software requirement references"""
    try:
        node = requirement(text)
        return [node], []
    except Exception as e:
        print(e)
    return [], []


class requirement(nodes.Inline, nodes.TextElement):
    def __init__(self, id):
        super().__init__()
        if db is not None and Requirement is not None:
            with db.session as session:
                self.req = Requirement.find_by_id(session, id)
                if self.req is None:
                    self.req = Requirement(id=id)
            return

        # Optional dependency fallback for environments without
        # requirements/database modules.
        self.req = type(
            "RequirementPlaceholder",
            (),
            {
                "id": id,
                "__repr_html__": lambda self: f'<code class="req">{self.id}</code>',
            },
        )()


# Register requirement role
roles.register_canonical_role("REQ", requirement_reference_role)


class endstatement(nodes.Inline, nodes.TextElement):
    pass


class EndStatement(Directive):
    """This `Directive` will split up statements"""

    required_arguments = 0
    optional_arguments = 0
    has_content = False

    def run(self):
        thenode = endstatement()
        return [thenode]


directives.register_directive("endstatement", EndStatement)


class rvt_script(nodes.General, nodes.Element):
    """Docutils node storing a parsed ``.. rvt::`` body."""


class RvtDirective(Directive):
    """Docutils directive for RVT Lisp snippets."""

    required_arguments = 0
    optional_arguments = 0
    final_argument_whitespace = False
    has_content = True
    option_spec = {}
    option_line = re.compile(r"^:[\w-]+:\s*.*$")

    def run(self):
        body_lines = list(self.content)
        while body_lines and self.option_line.match(body_lines[0].strip()):
            body_lines.pop(0)
        while body_lines and not body_lines[0].strip():
            body_lines.pop(0)

        node = rvt_script()
        node["body"] = "\n".join(body_lines)
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


class TestcaseHTMLTranslator(HTMLTranslator):
    documenttag_args = {
        "tagname": "div",
        "CLASS": "document prose prose-li:mt-0 prose-li:mb-0 prose-p:mb-1 prose-p:mt-1 prose-headings:mb-2 prose-headings:mt-5",  # noqa: E501
    }

    # Delimiters for endstatement directives
    ENDSTATEMENT_RST = "\n.. endstatement::\n\n"
    ENDSTATEMENT_DIV = '<splitter id="1234567890!!!!"/>'

    def __init__(self, document):
        HTMLTranslator.__init__(self, document)

    def visit_document(self, node):
        super().visit_document(node)
        self.body.append(self.ENDSTATEMENT_DIV)

    def depart_document(self, node):
        self.body.append(self.ENDSTATEMENT_DIV)
        super().depart_document(node)

    # Don't want nested sections since we might split
    # a section
    def visit_section(self, node):
        pass

    def depart_section(self, node):
        pass

    def visit_endstatement(self, node):
        self.body.append(self.ENDSTATEMENT_DIV)

    def depart_endstatement(self, node):
        pass

    def visit_requirement(self, node):
        return self.body.append(node.req.__repr_html__())

    def depart_requirement(self, node):
        pass


class TestcaseHTMLWriter(Writer):
    def __init__(self, requirement_by_id=None):
        Writer.__init__(self)
        self.translator_class = TestcaseHTMLTranslator


def rst_codeblock(src):
    return (
        "\n".join(
            [".. code-block:: clojure", "", *["  " + line for line in src.splitlines()]]
        )
        + "\n\n"
    )


def repr_rst(form):
    """Convert object to RST"""
    if isinstance(form, str):
        return form
    elif block := find_block(form):
        return block.__repr_rst__()
    else:
        return rst_codeblock(edn.writes(form))


def write_html_parts(rst_statements):
    # At this point we can assume all of our statements
    # are in rst format. To allow us to split up the rendered
    # html we need to insert some marker so we can split on
    # that after. To do this we will use a custom rst
    # directive.
    rst_text = TestcaseHTMLTranslator.ENDSTATEMENT_RST.join(rst_statements)
    html = docutils.core.publish_parts(
        rst_text,
        writer=TestcaseHTMLWriter(),
        settings_overrides={"initial_header_level": "3"},
    )

    # Now we should be able to split the HTML on
    # our custom div pattern. Throw away the first
    # and last as thats the wrapping 'document' divs
    return html["html_body"].split(TestcaseHTMLTranslator.ENDSTATEMENT_DIV)[1:-1]


class TestCaseFieldWriter(writers.Writer):
    """Writes test case fields to a dictionary"""

    def __init__(self):
        writers.Writer.__init__(self)
        self.translator_class = TestCaseTranslator
        self.visitor = None

    def translate(self):
        self.visitor = visitor = self.translator_class(self.document)
        self.document.walkabout(visitor)
        self.output = visitor.output


class TestCaseTranslator(nodes.GenericNodeVisitor):
    def __init__(self, document):
        nodes.NodeVisitor.__init__(self, document)
        self.output = {"title": "", "requirements": set()}

    # GenericNodeVisitor methods
    def default_visit(self, node):
        """Default node visit method."""
        pass

    def default_departure(self, node):
        """Default node depart method."""
        pass

    # NodeVisitor methods
    def unknown_departure(self, node):
        pass

    def unknown_visit(self, node):
        pass

    # Test case fields
    def visit_title(self, node):
        if isinstance(node.parent, nodes.document):
            self.output["title"] = node.astext()

    def visit_requirement(self, node):
        self.output["requirements"].add(node.req)


def extract_testcase_fields(text):
    """Extracts testcase fields from reStructuredText"""
    writer = TestCaseFieldWriter()
    parts = docutils.core.publish_parts(text, writer=writer)
    return parts["whole"]

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
