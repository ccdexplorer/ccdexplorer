"""The graph settings reflect the state the page was asked for.

The range lives on the slider, not in a button row -- the two set the same
thing and the row always won, so dragging the slider did nothing. What the
panel still carries is the grouping and the traces.

The radios used to be hardcoded -- daily always checked, no range control at
all -- so a link carrying a configuration showed the chart in that
configuration while the controls claimed something else. Rendered through
Jinja rather than asserted against the template source, because the thing
being checked is what the branch actually produces.
"""

import jinja2
import pytest

from ccdexplorer.charts import ChartState
from ccdexplorer.charts.registry import BY_NAME

TEMPLATES = "projects/ccdexplorer_site/templates"
SPEC = BY_NAME["transactions_count"]


def _render(params):
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATES))
    source = env.loader.get_source(env, "charts/standalone_chart.html")[0]
    # Only the settings block is under test; the surrounding page pulls in
    # base.html and the slider, which need the whole app.
    # From the guard, not from the div inside it: the row is wrapped in
    # `{% if not spec.automatic_grouping %}` now, and slicing past that
    # left the fragment with an orphan {% endif %}.
    start = source.index("{% if not is_live and not (spec.automatic_grouping")
    end = source.index("{% if include_kpis %}")
    # is_live=False: this is a calendar chart, which is what the panel below
    # belongs to. The live charts' one control is covered by the e2e tests.
    return env.from_string(source[start:end]).render(
        spec=SPEC, state=ChartState.from_query(SPEC, params), is_live=False
    )


def _checked(html):
    """The ids of the inputs rendered with `checked`."""
    import re

    return {m.group(1) for m in re.finditer(r'id="([^"]+)"[^>]*\bchecked\b', html)}


def test_a_page_with_no_query_opens_weekly():
    assert "weekly" in _checked(_render({}))


@pytest.mark.parametrize("grouping", ["daily", "weekly", "monthly"])
def test_the_requested_grouping_is_the_checked_one(grouping):
    checked = _checked(_render({"grouping": grouping}))
    assert grouping in checked
    assert len({"daily", "weekly", "monthly"} & checked) == 1


def test_a_stale_link_still_renders_a_checked_grouping():
    """A page is not an API: nonsense falls back rather than erroring."""
    assert "weekly" in _checked(_render({"grouping": "sideways"}))


def test_the_panel_carries_no_range_buttons():
    """They duplicated the slider and beat it, so the slider did nothing."""
    assert 'name="window"' not in _render({})


def test_a_page_that_passes_no_spec_still_renders():
    """standalone_chart.html is shared with the handwritten pages, and the
    PLT one renders it without a `spec` in the context. Guarding the
    grouping row on `spec.automatic_grouping` made that an UndefinedError,
    so /mainnet/charts/plt-transfers answered "Something's not quite
    right!" -- a 500 on a page that had been fine.

    A caller with no spec keeps its controls, which is what it had before.
    Rendered with no `is_live` either, for the same reason: that page does
    not pass one, and an undefined value has to read as "not live" rather
    than hiding every control on it.
    """
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATES))
    source = env.loader.get_source(env, "charts/standalone_chart.html")[0]
    start = source.index("{% if not is_live and not (spec.automatic_grouping")
    end = source.index("{% if include_kpis %}")

    html = env.from_string(source[start:end]).render(state=None)

    assert "Group By" in html
