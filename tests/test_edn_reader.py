from __future__ import annotations

import pytest

from automationv3.framework import edn


def test_read_special_literals():
    assert edn.read("nil") is None
    assert edn.read("true") is True
    assert edn.read("false") is False
    assert isinstance(edn.read("/"), edn.Symbol)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("0", 0),
        ("42", 42),
        ("-7", -7),
        ("+9", 9),
        ("0xff", 255),
        ("077", 63),
        ("2r101", 5),
        ("3.14", 3.14),
        ("6.02e2", 602.0),
        ("3/4", 0.75),
    ],
)
def test_read_numeric_forms(text, expected):
    assert edn.read(text) == expected


def test_read_symbol_and_keyword_with_meta():
    sym = edn.read("hello/world")
    kw = edn.read(":alpha/beta")

    assert isinstance(sym, edn.Symbol)
    assert isinstance(kw, edn.Keyword)
    assert sym.namespace == "hello"
    assert kw.namespace == "alpha"

    for form in (sym, kw):
        assert form.meta["start_row"] == 0
        assert form.meta["start_col"] == 0
        assert form.meta["ending_row"] == 0
        assert form.meta["ending_col"] > 0


def test_read_string_escape_sequences():
    parsed = edn.read(r'"line1\nline2\t\"\\\u0041\101"')
    assert parsed == 'line1\nline2\t"\\AA'


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (r"\newline", "\n"),
        (r"\space", " "),
        (r"\tab", "\t"),
        (r"\return", "\r"),
        (r"\formfeed", "\f"),
        (r"\backspace", "\b"),
        (r"\u0041", "A"),
        (r"\o101", "A"),
        (r"\A", "A"),
    ],
)
def test_read_character_forms(text, expected):
    assert edn.read(text) == expected


def test_read_nested_collections():
    form = edn.read("(job [:steps {:count 2}] #{1 2})")

    assert isinstance(form, edn.List)
    assert isinstance(form[1], edn.Vector)
    assert isinstance(form[1][1], edn.Map)
    assert isinstance(form[2], edn.Set)
    assert form[1][1][edn.Keyword("count")] == 2
    assert set(form[2]) == {1, 2}


def test_read_quote_expands_to_quote_list():
    form = edn.read("'abc")
    assert isinstance(form, edn.List)
    assert form[0] == edn.Symbol("quote")
    assert form[1] == edn.Symbol("abc")


def test_read_comment_is_skipped():
    assert edn.read("; ignored line\n42") == 42


def test_read_all_handles_mixed_forms():
    forms = edn.read_all("1 :x [3] #{4} 'a")
    assert forms[0] == 1
    assert forms[1] == edn.Keyword("x")
    assert isinstance(forms[2], edn.Vector)
    assert isinstance(forms[3], edn.Set)
    assert isinstance(forms[4], edn.List)


def test_read_all_symbol_at_eof_does_not_loop():
    forms = edn.read_all("alpha beta")
    assert [str(f) for f in forms] == ["alpha", "beta"]


def test_parse_symbol_valid_cases():
    assert edn.parse_symbol("alpha") == (None, "alpha")
    assert edn.parse_symbol("/") == (None, "/")
    assert edn.parse_symbol("ns/value") == ("ns", "value")


@pytest.mark.parametrize("token", ["", "::x", "abc:", "ns/"])
def test_parse_symbol_invalid_cases(token):
    with pytest.raises(Exception, match="Invalid symbol"):
        edn.parse_symbol(token)


def test_read_keyword_rejects_namespace_alias():
    with pytest.raises(Exception, match="Namespace alias not supported"):
        edn.read("::ns/name")


def test_read_raises_parseerror_for_mismatched_delimiter():
    with pytest.raises(edn.ParseError) as exc:
        edn.read("(]")
    assert exc.value.line == 0
    assert exc.value.col == 1


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("(1 2", "EOF in middle of list"),
        ("[1 2", "EOF in middle of list"),
        ('"abc', "EOF in middle of string"),
        (r"\ ", "Backslash cannot be followed by whitespace"),
        ("#x", "Invalid Dispatch"),
    ],
)
def test_read_raises_for_invalid_input(text, message):
    with pytest.raises(Exception, match=message):
        edn.read(text)


def test_read_map_odd_number_of_forms_raises():
    with pytest.raises(AssertionError, match="Map must have value for every key"):
        edn.read("{:a}")


def test_read_all_returns_none_on_parseerror_and_prints_pointer(capsys):
    result = edn.read_all("(]")
    captured = capsys.readouterr()

    assert result is None
    assert "^" in captured.out
    assert "Missing closing ')'" in captured.out
