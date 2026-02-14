"""Utilities for reStructuredText

"""

import re
import docutils.core
from docutils import writers, nodes
from docutils.parsers.rst import roles, Directive, directives
from docutils.writers.html4css1 import Writer, HTMLTranslator

from . import edn
from .block import find_block
from ..database import db
from ..requirements.models import Requirement


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
        with db.session as session:
            self.req = Requirement.find_by_id(session, id)

            if self.req is None:
                self.req = Requirement(id=id)


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


directive_start = re.compile(r"^\.\. (\w+)::(\s*|\s+\w+)$")


def _is_directive_start(line):
    m = directive_start.match(line)
    return m is not None


indent_pattern = re.compile(r"^\s+")


def _indent_width(line):
    m = indent_pattern.match(line)
    if m:
        return m.group()
    return 0


def split_rst_by_directives(text):
    """Splits rst file by directives"""

    parts = [""]
    state = "normal"
    for line in text.splitlines():
        if state == "normal":
            if _is_directive_start(line):
                if parts[-1].endswith("\n"):
                    parts[-1] = parts[-1][:-1]
                parts.append(line + "\n")
                state = "body"
            else:
                parts[-1] += line + "\n"
        elif state == "body":
            if _indent_width(line) == 0 and line.strip() != "":
                if parts[-1].endswith("\n"):
                    parts[-1] = parts[-1][:-1]
                parts.append("")
                state = "normal"
            else:
                parts[-1] += line + "\n"

    if parts[-1] == "":
        parts = parts[:-1]

    return parts


def parse_directive(text):
    """Parses directive into arguments, options, body"""

    pass
