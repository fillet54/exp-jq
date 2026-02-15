from textwrap import dedent

from docutils.core import publish_doctree

from automationv3.framework.rst import extract_rvt_bodies, rvt_script


def _single_rvt_node(text):
    document = publish_doctree(text)
    nodes = list(document.findall(rvt_script))
    assert len(nodes) == 1
    return nodes[0]


def test_rvt_directive_without_title_or_options():
    text = dedent(
        """\
        .. rvt::

           (always-pass)
        """
    )

    node = _single_rvt_node(text)

    assert node["title"] == ""
    assert node["options"] == {}
    assert extract_rvt_bodies(text) == ["(always-pass)"]


def test_rvt_directive_with_title_only_keeps_body_clean():
    text = dedent(
        """\
        .. rvt:: Smoke title

           (always-pass)
        """
    )

    node = _single_rvt_node(text)

    assert node["title"] == "Smoke title"
    assert node["options"] == {}
    assert extract_rvt_bodies(text) == ["(always-pass)"]


def test_rvt_directive_parses_flag_and_value_options():
    text = dedent(
        """\
        .. rvt:: Table Smoke
           :table-driven:
           :name: smoke
           :id: tc-001
           :tags: smoke, core

           (always-pass)
        """
    )

    node = _single_rvt_node(text)
    options = node["options"]

    assert node["title"] == "Table Smoke"
    assert "table-driven" in options
    assert options["name"] == "smoke"
    assert options["id"] == "tc-001"
    assert options["tags"] == "smoke, core"
    assert extract_rvt_bodies(text) == ["(always-pass)"]


def test_rvt_directive_no_title_with_options():
    text = dedent(
        """\
        .. rvt::
           :table-driven:
           :name: untitled
           :id: tc-untitled

           (always-pass)
        """
    )

    node = _single_rvt_node(text)
    options = node["options"]

    assert node["title"] == ""
    assert "table-driven" in options
    assert options["name"] == "untitled"
    assert options["id"] == "tc-untitled"
    assert extract_rvt_bodies(text) == ["(always-pass)"]
