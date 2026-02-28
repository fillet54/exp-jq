from textwrap import dedent

import pytest

from automationv3.framework.rst import expand_rvt_variations, extract_rvt_bodies, parse_rst_chunks


def test_parse_rst_chunks_no_directives_returns_single_text_chunk():
    text = dedent(
        """\
        Title
        =====

        Intro paragraph.
        """
    )

    chunks = parse_rst_chunks(text)

    assert len(chunks) == 1
    assert chunks[0].kind == "text"
    assert chunks[0].line == 1
    assert chunks[0].content == text
    assert extract_rvt_bodies(text) == []


def test_parse_rst_chunks_splits_text_and_rvt_in_order():
    text = dedent(
        """\
        Title
        =====

        Intro paragraph.

        .. rvt::

           (always-pass)

        Outro paragraph.
        """
    )

    chunks = parse_rst_chunks(text)

    assert [chunk.kind for chunk in chunks] == ["text", "rvt", "text"]
    assert chunks[1].content == "(always-pass)"
    assert chunks[1].line == 6
    assert extract_rvt_bodies(text) == ["(always-pass)"]


def test_parse_rst_chunks_handles_multiple_rvt_blocks_and_ignores_option_lines():
    text = dedent(
        """\
        Header
        ======

        Before first block.

        .. rvt::

           :note: ignored option-like line

           (always-pass)

        Between.

        .. rvt::

           (do
             (always-pass)
             (random-fail 0))

        After second block.
        """
    )

    chunks = parse_rst_chunks(text)
    bodies = extract_rvt_bodies(text)

    assert [chunk.kind for chunk in chunks] == ["text", "rvt", "text", "rvt", "text"]
    assert bodies == [
        "(always-pass)",
        "(do\n  (always-pass)\n  (random-fail 0))",
    ]


def test_parse_rst_chunks_supports_back_to_back_rvt_directives():
    text = dedent(
        """\
        .. rvt::

           (always-pass)

        .. rvt::

           (always-fail)

        Tail.
        """
    )

    chunks = parse_rst_chunks(text)

    assert [chunk.kind for chunk in chunks] == ["rvt", "text", "rvt", "text"]
    assert chunks[1].content.strip() == ""
    assert extract_rvt_bodies(text) == ["(always-pass)", "(always-fail)"]


def test_expand_rvt_variations_single_dimension():
    text = dedent(
        """\
        .. rvt::
           :variation:

           [[mode]
            ["nominal" "nominal"]
            ["safe" "safe"]]
        """
    )

    rows = expand_rvt_variations(text)

    assert len(rows) == 2
    assert rows[0]["name"] == "nominal"
    assert rows[0]["bindings"] == {"mode": '"nominal"'}
    assert rows[1]["name"] == "safe"
    assert rows[1]["bindings"] == {"mode": '"safe"'}


def test_expand_rvt_variations_cartesian_product():
    text = dedent(
        """\
        .. rvt::
           :variation:

           [[mode]
            ["nominal" "nominal"]
            ["safe" "safe"]]

        .. rvt::
           :variation:

           [[seed]
            ["s1" 1]
            ["s2" 2]]
        """
    )

    rows = expand_rvt_variations(text)

    assert len(rows) == 4
    names = {row["name"] for row in rows}
    assert names == {
        "nominal / s1",
        "nominal / s2",
        "safe / s1",
        "safe / s2",
    }


def test_expand_rvt_variations_rejects_duplicate_symbols():
    text = dedent(
        """\
        .. rvt::
           :variation:

           [[mode]
            ["m1" "nominal"]]

        .. rvt::
           :variation:

           [[mode]
            ["m2" "safe"]]
        """
    )

    with pytest.raises(ValueError, match="Duplicate variation symbol"):
        expand_rvt_variations(text)
