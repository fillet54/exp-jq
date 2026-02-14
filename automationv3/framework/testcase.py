"""Models and utilites for reading and representing test cases"""
import functools
from abc import ABC, abstractmethod

from . import edn
from .block import find_block
from .rst import extract_testcase_fields, write_html_parts, repr_rst


# TODO: This likely should actually extend a job class.
#       TestCase will be only one type of job
class TestCase(ABC):
    """Test case"""

    @property
    @abstractmethod
    def id(self):
        pass

    @property
    @abstractmethod
    def title(self):
        pass

    @property
    @abstractmethod
    def requirements(self):
        pass

    @property
    @abstractmethod
    def statements(self):
        pass


class TestCaseStatement:
    """Statement of a test case

    This class essentially wraps a test case statement's
    various representations."""

    def __init__(self, statement, html=None, rst=None):
        self.statement = statement
        self.html = html or ""
        self.rst = rst or ""

        # TODO: This is just for quick examples
        #       In the future we will look up `BuildingBlockInst` and
        #       if it exists then delegate to that for reprenstations
        # special lookups
        # if len(statement) > 0 and statement[0] in html_repr:
        #    self.html = html_repr[statement[0]](statement)

    def __str__(self):
        return edn.writes(self.statement).replace("\\n", "\n").strip('"')

    def __repr__(self):
        return str(self)

    def _repr_html_(self):
        return self.html

    def _repr_rst_(self):
        return self.rst

    def _repr_edn_(self):
        text = edn.writes(self.statement).strip()

        # Clean up string formatting. This should
        # always be raw RST so just clean it up for
        # consistency in the edn file
        if isinstance(self.statement, str):
            # turn escaped newlines to actual newlines and remove whitespace
            text = text.replace("\\n", "\n").strip()
            # strip off quotes
            text = text.strip('"')
            # strip any other whitespace
            text = text.strip()
            # add back in quotes
            text = f'"\n{text}\n"'

            # newlines as real newlines
            # text = text.replace('\\n', '\n')
            # leading and trailing double-quote on ownline
            # if not text.startswith('"\n'):
            #    text = f'"\n{text[1:]}'
            # if not text.endswith('\n"'):
            #    text = f'{text[:-1]}\n"'
        return text + "\n"


# Basic caching of text content to Testcase statements
# This improves performance quite a bit as usually from
# the webside there is a request for EACH statement which
# required reparsing everything. Now we only reparse on
# change of text content
@functools.lru_cache(maxsize=128)
def get_statements(text):
    edn_statements = list(edn.read_all(text))
    rst_statements = [repr_rst(stmt) for stmt in edn_statements]
    html_statements = write_html_parts(rst_statements)

    # Wrap our statements
    statements = [
        TestCaseStatement(stmt, html, rst)
        for stmt, html, rst in zip(edn_statements, html_statements, rst_statements)
    ]

    return statements


class EdnTestCase(TestCase):
    """Test case represented in edn

    An edn test case consist of a sequence of edn forms. The forms
    are limited to the types of `edn.List` and `edn.String`. All other
    forms are ignored/skipped.

    Forms of type `edn.List` are interpreted as representing building
    blocks while forms of type `edn.String` are considered
    documentation.

    Documentation is written in the reStructuredText(rst) format with
    the entire testcase getting converted to a rst document for
    interpretting and rendering. The various fields of the test case
    are extracted the rst document. For example the title comes from
    the title of the rst document and the requirements are extracted
    from references

    """

    def __init__(self, id, text):
        self._id = id
        self.text = text

        self.fields = extract_testcase_fields(self.__repr_rst__())

    @property
    def id(self):
        return self._id

    @property
    def title(self):
        return self.fields["title"]

    @property
    def requirements(self):
        return self.fields["requirements"]

    @property
    def statements(self):
        return list(get_statements(self.text))

    def update_statement(self, index, value):
        # simple detection of rst or code
        # very rare should a rst step start with a (
        # otherwise need to have the GUI handle cell
        # types
        if not value.strip().startswith("("):
            value = f'"{value}"'

        statements = get_statements(value)

        all_statements = self.statements
        original_len = len(all_statements)

        if index != -1:
            all_statements[index : index + 1] = statements
        else:
            all_statements += statements

        # Now we need to just write out each sections
        text = "\n".join([stmt._repr_edn_() for stmt in all_statements])
        self.text = text

        # Still not sure how I handle this.
        # return the sections updated
        # if statements read is of len == 1 then its only this index
        # otherwise its current index until the end
        if index == -1:  # added at end
            modified = list(range(original_len, original_len + len(statements)))
            shifted = []
        elif len(statements) <= 1:
            modified = [index]
            shifted = []
        else:
            modified = [i for i in range(index, index + len(statements))]
            shifted = [
                (i, i + len(statements) - 1) for i in range(index + 1, original_len)
            ]

        return modified, shifted

    def __repr_rst__(self):
        return edn_to_rst(self.text)


def edn_to_rst(text):
    """Convert edn forms to rst"""

    def form_to_rst(form):
        if isinstance(form, str):
            return form
        elif block := find_block(form):
            return block.__repr_rst__()
        else:
            return repr_rst(form)

    rst = [
        form_to_rst(form)
        for form in edn.read_all(text)
        if isinstance(form, (str, list))
    ]

    return "\n".join(rst)
