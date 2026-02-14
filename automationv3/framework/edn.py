"""
==========
The Reader
==========
This section will go over building a reader for the Extensible Data Notation (EDN).
Most of the descriptions on this page are directly from
`edn-format spec<https://github.com/edn-format/edn>` While the
spec itself is pretty informal it looks to provide enough detail to implement a
satisfactory reader.

    **edn** is an extensible data notation. A superset of edn is used by Clojure to
    represent programs, and it is used by Datomic and other applications as a
    data transfer format. This spec describes edn in isolation from those and
    other specific use cases, to help facilitate implementation of readers and
    writers in other languages, and for other uses.

    edn supports a rich set of built-in elements, and the definition of
    extension elements in terms of the others. Users of data formats without
    such facilities must rely on either convention or context to convey
    elements not included in the base set. This greatly complicates application
    logic, betraying the apparent simplicity of the format. edn is simple, yet
    powerful enough to meet the demands of applications without convention or
    complex context-sensitive logic.

    edn is a system for the conveyance of values. It is not a type system, and
    has no schemas. Nor is it a system for representing objects - there are no
    reference types, nor should a consumer have an expectation that two
    equivalent elements in some body of edn will yield distinct object
    identities when read, unless a reader implementation goes out of its way to
    make such a promise. Thus the resulting values should be considered
    immutable, and a reader implementation should yield values that ensure
    this, to the extent possible.

    edn is a set of definitions for acceptable elements. A use of edn might be
    a stream or file containing elements, but it could be as small as the
    conveyance of a single element in e.g. an HTTP query param.

    There is no enclosing element at the top level. Thus edn is suitable for
    streaming and interactive applications.

    The base set of elements in edn is meant to cover the basic set of data
    structures common to most programming languages. While edn specifies how
    those elements are formatted in text, it does not dictate the
    representation that results on the consumer side. A well behaved reader
    library should endeavor to map the elements to programming language types
    with similar semantics
"""

import re
import io
from collections import abc


class ParseError(Exception):
    def __init__(self, msg, line_info):
        super().__init__(f"{msg} (line: {line_info[0]}, col: {line_info[1]})")
        self.line = line_info[0]
        self.col = line_info[1]


# Helper
class PushBackCharStream:
    def __init__(self, chars):
        self.iterator = chars if isinstance(chars, abc.Iterator) else iter(chars)
        self.pushed_back = []
        self.line = 0
        self.col = 0
        self.line_history = [0]
        self.__reached_end = False

    def __iter__(self):
        return self

    def __next__(self):
        if self.__reached_end:
            raise StopIteration

        if self.pushed_back:
            char = self.pushed_back.pop()
        else:
            try:
                char = next(self.iterator)
            except StopIteration:
                self.__reached_end = True
                return None

        # advanced character position
        if char == "\n":
            self.line += 1
            self.col = 0
        else:
            self.col += 1

        # Save last line history for pushback
        if self.line >= len(self.line_history):
            self.line_history.append(0)
        self.line_history[self.line] = self.col

        return char

    def push_back_single(self, char: str):
        if char is None:
            return

        self.pushed_back.append(char)

        # Reverse the position
        if self.col == 0:
            self.line -= 1
            self.col = self.line_history[self.line]
        else:
            self.col -= 1

        # Reset EOF since we know we are not there anymore
        self.__reached_end = False

    def push_back(self, chars: str):
        if chars is None:
            self.__reached_end = False
            return

        for c in reversed(chars):
            self.push_back_single(c)

    @property
    def empty(self):
        return len(self.pushed_back) == 0 and self.__reached_end

    def starting_line_col_info(self):
        return (self.line, self.col - 1)

    def ending_line_col_info(self):
        return (self.line, self.col)


"""
Classes
=======
The classes
"""


class Symbol(str):
    def __new__(cls, val, *args, **kwargs):
        return str.__new__(cls, val)

    def __init__(self, val, namespace=None):
        self.namespace = namespace
        self.meta = {}

    def __eq__(self, val):
        if isinstance(val, type(self)):
            return super().__eq__(val) and self.namespace == val.namespace
        elif isinstance(val, str):
            if self.namespace is not None:
                return f"{self.namespace}/{self}" == val
            else:
                return super().__eq__(val)
        return False

    @property
    def name(self):
        return self

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        if self.namespace is None:
            return super().__str__()
        else:
            return f"{self.namespace}/{super().__str__()}"

    def __hash__(self):
        return hash(str(self))


class Keyword(Symbol):
    def __repr__(self):
        return ":" + super().__repr__()


class List(list):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.meta = {}

    def __hash__(self):
        return hash(tuple(self))


class Vector(list):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.meta = {}


class Map(dict):
    def __init__(self, vals, linerange=None):
        dict.__init__(self, vals)
        self.meta = {}


class Set(set):
    def __init__(self, *vals):
        set.__init__(self, vals)
        self.meta = {}


def is_whitespace(ch):
    return ch in " \t\n,"


def is_ending(ch):
    return ch in '";@^`()[]{}\\'


def is_special(ch):
    return ch in "-+."


def is_non_numeric(ch):
    return ch.isalpha() or ch in ".*+!-_?$%&=<>@:#"


def is_start(ch):
    return is_non_numeric(ch) and ch not in ":#"


def is_numeric(ch, base=10):
    try:
        int(ch, base)
        return True
    except ValueError:
        return False


def is_number_literal(ch, lookahead):
    if is_numeric(ch):
        return True
    elif ch == "-" or ch == "+":
        return is_numeric(lookahead)


def read_symbol(stream, initch):
    starting_info = stream.starting_line_col_info()
    token = read_token(stream, initch)

    # Special Symbols
    if token == "nil":
        return None
    elif token == "true":
        return True
    elif token == "false":
        return False
    elif token == "/":
        return Symbol("/")
    elif token is None:
        return None

    ns, name = parse_symbol(token)
    symbol = Symbol(name, ns)

    # attach line/col info
    ending_info = stream.ending_line_col_info()
    symbol.meta["start_row"] = starting_info[0]
    symbol.meta["start_col"] = starting_info[1]
    symbol.meta["ending_row"] = ending_info[0]
    symbol.meta["ending_col"] = ending_info[1]

    return symbol


invalid_token = re.compile(r"(^::|.*:$)")
invalid_namespace = re.compile(r".*:$")


def parse_symbol(token):
    "Parses a string into a tuple of the namespace and symbol"
    if not token or invalid_token.match(token):
        raise Exception("Invalid symbol: '{}'".format(token))

    # If no namespace just return None as ns and token as symbol
    if token == "/" or "/" not in token:
        return None, token

    ns, sym = token.split("/", 1)

    if (
        sym
        and not is_numeric(sym[0])
        and not invalid_namespace.match(ns)
        and (sym == "/" or "/" not in sym)
    ):
        return ns, sym
    raise Exception("Invalid symbol: '{}'".format(token))


def read_token(stream, initch):
    token = ""

    ch = initch
    while True:
        if ch is None or is_whitespace(ch):
            break
        elif is_ending(ch):
            stream.push_back(ch)
            break
        else:
            token += ch
            ch = next(stream)

    if token == "":
        token = None

    return token


def read_string(stream, initch):
    s = ""
    for ch in stream:
        if ch == '"':
            break
        elif ch == "\\":
            s += escape_char(stream)
        elif ch is None:
            raise Exception("EOF in middle of string")
        else:
            s += ch
    return s


escape_chars = {
    "t": "\t",
    "r": "\r",
    "n": "\n",
    "\\": "\\",
    '"': '"',
    "b": "\b",
    "f": "\f",
}


def escape_char(stream):
    ch = next(stream)

    # Normal character
    if ch in escape_chars:
        ch = escape_chars[ch]
    elif ch == "u":  # Hex unicode escape
        ch = read_unicode_char(stream, base=16, length=4)
    elif is_numeric(ch):  # Octal Unicode escape
        stream.push_back(ch)
        ch = read_unicode_char(stream, base=8, length=3)
    else:
        raise Exception("Invalid escape '\\{}'".format(ch))
    return ch


def read_unicode_char(stream, base, length):
    unicode_bytes = b"\\"

    if base == 16:
        unicode_bytes += b"u"

    for _ in range(length):
        ch = next(stream)
        if not is_numeric(ch, base=base):
            raise Exception("Invalid unicode escape '{}'".format(ch))
        unicode_bytes += bytes(ch, "utf-8")

    try:
        return unicode_bytes.decode("unicode-escape")
    except UnicodeError:
        raise Exception("Invalid unicode escape '{}'".format(unicode_bytes))


def read_char(stream, initch):
    ch = next(stream)
    if ch is None:
        raise Exception("EOF in character")

    if is_whitespace(ch):
        raise Exception("Backslash cannot be followed by whitespace")

    if is_ending(ch):
        token = ch
    else:
        token = read_token(stream, ch)

    if len(token) == 1:
        ch = token
    elif token == "newline":
        ch = "\n"
    elif token == "space":
        ch = " "
    elif token == "tab":
        ch = "\t"
    elif token == "backspace":
        ch = "\b"
    elif token == "formfeed":
        ch = "\f"
    elif token == "return":
        ch = "\r"
    elif token.startswith("u"):
        stream.push_back(token[1:])
        ch = read_unicode_char(stream, base=16, length=4)
    elif token.startswith("o"):
        stream.push_back(token[1:])
        ch = read_unicode_char(stream, base=8, length=len(token) - 1)
    else:
        raise Exception("Invalid character escape '{}'".format(token))

    return ch


def read_keyword(stream, initch):
    starting_info = stream.starting_line_col_info()

    ch = next(stream)
    if is_whitespace(ch):
        raise Exception("Single colon not allowed")

    token = read_token(stream, ch)
    ns, kw = parse_symbol(token)

    if ns is not None and ns.startswith(":"):
        raise Exception("Namespace alias not supported")

    keyword = Keyword(kw, ns)

    # attach line/col info
    ending_info = stream.ending_line_col_info()
    keyword.meta["start_row"] = starting_info[0]
    keyword.meta["start_col"] = starting_info[1]
    keyword.meta["ending_row"] = ending_info[0]
    keyword.meta["ending_col"] = ending_info[1]

    return keyword


int_pattern = re.compile(
    r"^([-+]?)(?:(0)|([1-9][0-9]*)|0[xX]([0-9A-Fa-f]+)|0([0-7]+)|([1-9][0-9]?)[rR]([0-9A-Za-z]+)|0[0-9]+)(N)?$"  # noqa: E501
)
float_pattern = re.compile(r"^([-+]?[0-9]+(\.[0-9]*)?([eE][-+]?[0-9]+)?)(M)?$")
ratio_pattern = re.compile(r"^([-+]?[0-9]+)/([0-9]+)$")


def read_number(stream, initch):
    s = initch
    for ch in stream:
        if ch is None or is_whitespace(ch) or is_ending(ch):
            stream.push_back(ch)
            return match_number(s)
        else:
            s += ch


def match_number(s):
    if int_pattern.match(s):
        return match_int(s)
    elif float_pattern.match(s):
        return match_float(s)
    elif ratio_pattern.match(s):
        return match_ratio(s)


def match_int(s):
    m = int_pattern.match(s).groups()
    if m[1] is not None:
        return 0

    negate = m[0] == "-"

    if m[2] is not None:
        base = 10
        n = m[2]
    elif m[3] is not None:
        base = 16
        n = m[3]
    elif m[4] is not None:
        base = 8
        n = m[4]
    elif m[6] is not None:
        base = int(m[5])
        n = m[6]
    else:
        base = None
        n = None

    number = int(n, base)
    number = -1 * number if negate else number
    return number


def match_float(s):
    m = float_pattern.match(s).groups()
    if m[3] is not None:
        return float(
            m[0]
        )  # TODO: Should we support exact precision? This would be decimal.Decimal
    else:
        return float(s)


def match_ratio(s):
    m = ratio_pattern.match(s).groups()
    numerator = m[0][1:] if m[0].startswith("+") else m[0]
    denominator = m[1]

    # no ratio in python
    return int(numerator) / int(denominator)


def read_list(stream, initch):
    starting_info = stream.starting_line_col_info()

    forms = read_delimited(stream, initch, sentinel=")")
    thelist = List(forms)

    # attach line/col info
    ending_info = stream.ending_line_col_info()
    thelist.meta["start_row"] = starting_info[0]
    thelist.meta["start_col"] = starting_info[1]
    thelist.meta["ending_row"] = ending_info[0]
    thelist.meta["ending_col"] = ending_info[1]

    return thelist


def read_delimited(stream, initch, sentinel):
    forms = []
    while True:
        form = read(stream, sentinel)
        if form == READ_EOF:
            raise Exception("EOF in middle of list")
        elif form == READ_FINISHED:
            return forms
        elif form == stream:
            pass
        elif form is None:
            # non-sentinel ending (i.e. ERROR!)
            raise ParseError(
                f"ParseError: Missing closing '{sentinel}'",
                stream.ending_line_col_info(),
            )
        else:
            forms.append(form)


def read_vector(stream, initch):
    starting_info = stream.starting_line_col_info()
    forms = read_delimited(stream, initch, sentinel="]")
    thevector = Vector(forms)

    # attach line/col info
    ending_info = stream.ending_line_col_info()
    thevector.meta["start_row"] = starting_info[0]
    thevector.meta["start_col"] = starting_info[1]
    thevector.meta["ending_row"] = ending_info[0]
    thevector.meta["ending_col"] = ending_info[1]

    return thevector


def read_map(stream, initch):
    starting_info = stream.starting_line_col_info()
    forms = read_delimited(stream, initch, sentinel="}")

    assert len(forms) % 2 == 0, "Map must have value for every key"

    pairs = [forms[i : i + 2] for i in range(0, len(forms), 2)]
    themap = Map(pairs)

    # attach line/col info
    ending_info = stream.ending_line_col_info()
    themap.meta["start_row"] = starting_info[0]
    themap.meta["start_col"] = starting_info[1]
    themap.meta["ending_row"] = ending_info[0]
    themap.meta["ending_col"] = ending_info[1]

    return themap


def read_dispatch(stream, initch):
    ch = next(stream)
    if ch in dispatch_macros:
        return dispatch_macros[ch](stream, ch)
    raise Exception("Invalid Dispatch")


def read_set(stream, initch):
    starting_info = stream.starting_line_col_info()

    forms = read_delimited(stream, initch, sentinel="}")
    theset = Set(*forms)

    # attach line/col info
    ending_info = stream.ending_line_col_info()
    theset.meta["start_row"] = starting_info[0]
    theset.meta["start_col"] = starting_info[1] - 1
    theset.meta["ending_row"] = ending_info[0]
    theset.meta["ending_col"] = ending_info[1]

    return theset


def read_comment(stream, initch):
    for ch in stream:
        if ch == "\n":
            break
        # ignore everything else
    # returning the stream will allow higher level
    # macros ignore the form.
    return stream


def read_quote(stream, initch):
    return List([Symbol("quote"), read(stream)])


macros = {
    '"': read_string,
    "\\": read_char,
    ":": read_keyword,
    "(": read_list,
    "[": read_vector,
    "{": read_map,
    ";": read_comment,
    "'": read_quote,
    "#": read_dispatch,
}

dispatch_macros = {"{": read_set}

READ_EOF = "READ_EOF"
READ_FINISHED = "READ_FINISHED"


def read(stream_or_str, sentinel=None):
    """Reads EDN returning the first form as a python datastructure"""

    if isinstance(stream_or_str, str):
        stream = PushBackCharStream(stream_or_str)
    else:
        stream = stream_or_str

    for ch in stream:
        if ch is None:
            return READ_EOF
        if is_whitespace(ch):
            continue
        if ch == sentinel:
            return READ_FINISHED

        # Possibly need 1 lookahead
        lookahead = next(stream)
        stream.push_back(lookahead)

        if is_number_literal(ch, lookahead):
            return read_number(stream, ch)
        elif ch in macros:
            form = macros[ch](stream, ch)

            # if the actual stream is the form then the form was a comment and we should
            # continue on
            if form != stream:
                return form
        else:
            return read_symbol(stream, ch)


def read_all(text):
    """Reads EDN returning the all forms"""

    try:
        forms = []
        stream = PushBackCharStream(text)

        while (statement := read(stream)) != READ_EOF:
            forms.append(statement)

        return forms
    except ParseError as e:
        # hacky way to point out error
        start = max(e.line - 5, 0)
        end = e.line + 1
        pointer = " " * e.col + "^"

        for line in text.splitlines()[start:end]:
            print(line)
        print(pointer)
        print(e)
        return


# Write functions
# This is trickier than it looks to get it to output
# clean looking code.
def complex_type(obj):
    return type(obj) in [List, Vector, list, Map, dict, Set, set]


def write_indent(s, indent_level):
    if indent_level is None:
        return
    else:
        s.write(" " * indent_level)


def write_map(obj, s, indent_level=0):
    s.write("{")

    if any(complex_type(o) for o in obj.values()):
        s.write("\n")
        item_indent = indent_level + 2
        write_indent(s, item_indent)
    else:
        item_indent = None

    for (
        i,
        kv,
    ) in enumerate(obj.items()):
        k, v = kv
        if i != 0:
            s.write("\n") if item_indent is not None else s.write(" ")
            write_indent(s, item_indent)

        write(k, s, item_indent)
        s.write(" ")
        write(v, s, item_indent)

    if item_indent is not None:
        s.write("\n")
        write_indent(s, indent_level)
    s.write("}")


def write_sequence(obj, s, chars, indent_level=0):
    start, end = chars
    is_list = chars == ("(", ")")
    indent_amount = 4 if is_list else 1

    stemp = s
    s = io.StringIO("")

    s.write(start)
    if any(complex_type(o) for o in obj):
        item_indent = indent_level + indent_amount
    else:
        item_indent = None

    for (
        i,
        v,
    ) in enumerate(obj):
        if i != 0:
            if item_indent is not None:
                s.write("\n")
                write_indent(s, item_indent)
            else:
                s.write(" ")
        write(v, s, item_indent)

    s.write(end)

    if len(s.getvalue()) <= (100 - indent_level):
        s = " ".join(s.getvalue().split())
    else:
        s = s.getvalue()

    stemp.write(s)


def write_list(obj, s, indent_level=0):
    # There are a few special cases to eventually handle
    # 1. function def
    # 2. quote
    # 3. Simple types no indent
    if obj[0] in ["fn", "defn", "if", "if-not"]:
        parts = iter(obj)
        s.write(" " * indent_level + "(")  # indent
        write(next(parts), s)  # fn or defn
        s.write(" ")
        if obj[0] == "defn":
            write(next(parts), s)  # name
            s.write(" ")
        write(next(parts), s)  # arguments
        s.write("\n")
        count = 0
        for x in parts:  # body
            write_indent(s, indent_level + 2)
            write(x, s, indent_level=indent_level + 2)
            s.write("\n")
        s.seek(s.tell() - 1)
        s.write(")")
    elif obj[0] in ["StartSimulation"]:
        s.write(" " * indent_level + "(")  # indent
        write(obj[0], s)  # fn or defn
        s.write("\n")
        # write pairs
        count = 0
        for left, right in zip(obj[1::2], obj[2::2]):
            write_indent(s, indent_level + 2)
            write(left, s, indent_level=indent_level + 2)
            s.write(" ")
            write(right, s)
            count += 1
            if count < len(obj[1:]) // 2:
                s.write("\n")
        s.write(")")
    elif obj[0] in ["Table-Driven"]:
        write_indent(s, indent_level + 2)
        s.write("(Table-Driven\n")

        # find column widths
        headers = obj[1]
        rows = obj[2]
        widths = [len(h) for h in headers]

        # header row
        row_str = ""
        for header, width in zip(headers, widths):
            row_str += header
            row_str += " "
        write_indent(s, indent_level + 2)
        s.write(f"[{row_str.strip()}]\n")

        write_indent(s, indent_level + 2)
        s.write("[")
        for i, row in enumerate(rows):
            row_str = ""
            for col, width in zip(row, widths):
                col_str = writes(col).strip()
                row_str += col_str + (" " * (width - len(col_str)))
            if i != 0:
                write_indent(s, indent_level + 3)
            s.write(f"[{row_str}]\n")
        s.seek(s.tell() - 1)
        s.write("])")
    else:
        write_sequence(obj, s, chars=("(", ")"), indent_level=indent_level)


def write_vector(obj, s, indent_level=0):
    write_sequence(obj, s, chars=("[", "]"), indent_level=indent_level)


def write_set(obj, s, indent_level=0):
    write_sequence(obj, s, chars=("#{", "}"), indent_level=indent_level)


escape_chars_reverse = {v: k for k, v in escape_chars.items()}


def write_char(obj, s, indent_level=0):
    # Common escapes
    if obj in escape_chars_reverse:
        s.write("\\")
        s.write(escape_chars_reverse[obj])
    else:
        # handle \x escapes and convert to \u0000
        x = obj.encode("unicode_escape")
        if x.startswith(b"\\x"):
            s.write("\\u00")
            s.write(x[2:].decode("utf-8"))
        else:
            # Everything else
            s.write(obj)


def write_string(obj, s, indent_level=0):
    s.write('"')
    for char in obj:
        write_char(char, s, indent_level)
    s.write('"')


def write(obj, stream, indent_level=0):
    """Writes obj in edn"""
    if obj is None:
        stream.write("nil")
    elif isinstance(obj, (Map, dict)):
        write_map(obj, stream, indent_level)
    elif isinstance(obj, List):
        write_list(obj, stream, indent_level)
    elif isinstance(obj, (Vector, list)):
        write_vector(obj, stream, indent_level)
    elif isinstance(obj, (Set, set)):
        write_set(obj, stream, indent_level)
    elif isinstance(obj, bool):
        stream.write(str(obj).lower())
    elif isinstance(obj, Keyword):
        stream.write(repr(obj))
    elif isinstance(obj, Symbol):
        stream.write(repr(obj))
    elif isinstance(obj, str):
        write_string(obj, stream, indent_level)
    else:
        # Try our best
        stream.write(repr(obj))


def writes(obj, indent_level=0):
    stream = io.StringIO("")
    write(obj, stream, indent_level)
    return stream.getvalue()


if __name__ == "__main__":
    s = read("""(abc 123 [3 4 5])""")

    print(s.meta)
    print(s[2].meta)
