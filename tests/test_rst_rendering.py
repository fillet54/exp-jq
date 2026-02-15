from textwrap import dedent

from automationv3.framework.rst import render_script_rst_html


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
