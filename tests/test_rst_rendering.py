from textwrap import dedent

from automationv3.framework.rst import collect_script_syntax_issues, render_script_rst_html
from automationv3.jobqueue.views import (
    _build_raw_source_rows,
    _build_script_directory_index,
    _discover_scripts,
    _parent_directory,
)


def test_render_script_rst_html_shows_meta_requirements_and_tags():
    text = dedent(
        """\
        Sample Script
        =============

        .. meta::
           :requirements: ECSCOM00008, ECSCOM00009
           :tags: smoke, comms
           :subsystem: com

        Paragraph text.
        """
    )

    html = render_script_rst_html(text)

    assert "Requirements" in html
    assert "ECSCOM00008" in html
    assert "ECSCOM00009" in html
    assert "accept telecommands over the primary bus" in html
    assert "Tags" in html
    assert "smoke" in html
    assert "comms" in html
    assert "badge-info" in html
    assert "Subsystem" in html
    # We want visible metadata, not HTML head-only <meta> tags.
    assert "<meta " not in html.lower()


def test_render_script_rst_html_renders_rvt_block():
    text = dedent(
        """\
        RVT Script
        ==========

        .. rvt::

           (do
             (always-pass))
        """
    )

    html = render_script_rst_html(text)

    assert "always-pass" in html
    assert "rvt-block" in html
    assert "<pre>" in html


def test_collect_script_syntax_issues_reports_rst_errors():
    text = dedent(
        """\
        .. unknown_directive::
        """
    )

    issues = collect_script_syntax_issues(text)

    assert issues
    assert any(issue["source"] == "rst" for issue in issues)
    assert any("Unknown directive type" in issue["message"] for issue in issues)


def test_collect_script_syntax_issues_reports_rvt_reader_line_and_column():
    text = dedent(
        """\
        Bad RVT
        =======

        .. rvt::

           (]
        """
    )

    issues = collect_script_syntax_issues(text)
    rvt_issues = [issue for issue in issues if issue["source"] == "rvt"]

    assert len(rvt_issues) == 1
    assert rvt_issues[0]["is_error"] is True
    assert rvt_issues[0]["line"] is not None
    assert rvt_issues[0]["column"] == 2
    assert "Missing closing ')'" in rvt_issues[0]["message"]


def test_render_script_rst_html_replaces_invalid_rvt_with_location_hint():
    text = dedent(
        """\
        Bad RVT
        =======

        .. rvt::

           (]
        """
    )

    html = render_script_rst_html(text)

    assert "RVT reader syntax error" in html
    assert "script line" in html
    assert "^" in html
    assert "rvt-block-error" in html


def test_discover_scripts_adds_static_syntax_flags(tmp_path):
    (tmp_path / "bad.rst").write_text(
        dedent(
            """\
            Bad RVT
            =======

            .. rvt::

               (]
            """
        ),
        encoding="utf-8",
    )
    (tmp_path / "ok.rst").write_text(
        dedent(
            """\
            Good RVT
            ========

            .. rvt::

               (always-pass)
            """
        ),
        encoding="utf-8",
    )

    scripts = _discover_scripts(tmp_path)
    indexed = {row["name"]: row for row in scripts}

    assert indexed["bad"]["has_syntax_errors"] is True
    assert indexed["bad"]["syntax_error_count"] >= 1
    assert any(issue["source"] == "rvt" for issue in indexed["bad"]["syntax_issues"])

    assert indexed["ok"]["has_syntax_errors"] is False
    assert indexed["ok"]["syntax_error_count"] == 0


def test_build_raw_source_rows_marks_issue_lines():
    text = dedent(
        """\
        Line one
        Line two
        Line three
        """
    )
    issues = [
        {"source": "rst", "line": 2, "column": 1, "message": "Bad directive", "is_error": True},
        {"source": "rvt", "line": 3, "column": 2, "message": "Reader error", "is_error": True},
    ]

    rows = _build_raw_source_rows(text, issues)

    assert len(rows) == 3
    assert rows[0]["has_error"] is False
    assert rows[1]["has_error"] is True
    assert rows[1]["issues"][0]["message"] == "Bad directive"
    assert rows[2]["has_error"] is True
    assert rows[2]["issues"][0]["source"] == "rvt"


def test_build_script_directory_index_creates_tree_and_counts():
    scripts = [
        {"relpath": "alpha/a1.rst", "title": "A1"},
        {"relpath": "alpha/beta/b1.rst", "title": "B1"},
        {"relpath": "root.rst", "title": "Root"},
    ]

    nodes, children, by_dir, totals = _build_script_directory_index(scripts)

    node_paths = [node["path"] for node in nodes]
    assert "" in node_paths
    assert "alpha" in node_paths
    assert "alpha/beta" in node_paths
    assert children[""] == ["alpha"]
    assert children["alpha"] == ["alpha/beta"]
    assert len(by_dir[""]) == 1
    assert len(by_dir["alpha"]) == 1
    assert len(by_dir["alpha/beta"]) == 1
    assert totals[""] == 3
    assert totals["alpha"] == 2
    assert totals["alpha/beta"] == 1


def test_parent_directory_handles_root_and_nested_paths():
    assert _parent_directory("") is None
    assert _parent_directory("alpha") == ""
    assert _parent_directory("alpha/beta") == "alpha"
