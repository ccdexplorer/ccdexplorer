"""A chart with nothing to draw should say so.

An empty figure renders as a bare pair of axes, which a reader takes for a
flat line at zero -- a claim about the data rather than an absence of it. The
agent registry chart begins in May 2026, so any window before that is
legitimately empty, and so is any range the nightly job has not filled.
"""

import plotly.graph_objects as go

from ccdexplorer.ccdexplorer_site.app.utils import empty_chart_figure


def test_an_empty_figure_carries_an_explanation():
    fig = empty_chart_figure("light")
    assert any("No data" in a.text for a in fig.layout.annotations)


def test_the_message_can_say_which_range_was_empty():
    fig = empty_chart_figure("light", "No data for last 30 days")
    assert any("last 30 days" in a.text for a in fig.layout.annotations)


def test_it_draws_no_traces():
    assert empty_chart_figure("light").data == ()


def test_it_hides_the_axes():
    """Axes with no data on them are the thing that reads as a zero line."""
    fig = empty_chart_figure("light")
    assert fig.layout.xaxis.visible is False
    assert fig.layout.yaxis.visible is False


def test_it_is_still_a_figure_the_image_route_can_render():
    assert isinstance(empty_chart_figure("dark"), go.Figure)
