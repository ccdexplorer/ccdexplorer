"""Seven Kraken intervals filled most of a phone screen for one chart.

The picker shows a family once, at its default. Everything in `other` keeps its
own entry, because those are eighteen different charts, not one chart eighteen
ways.
"""

from ccdexplorer.ccdexplorer_chart_bot.catalogue import BY_NAME, CHARTS
from ccdexplorer.ccdexplorer_chart_bot.inline import pickable


def test_a_family_appears_once():
    names = {c.name for c in pickable(CHARTS)}
    price = {c.name for c in CHARTS if c.group == "price"}

    assert len(price & names) == 1


def test_the_entry_is_the_family_default():
    assert BY_NAME["ccd_kraken_4h"] in pickable(CHARTS)


def test_everything_in_other_still_appears():
    others = [c for c in CHARTS if c.group == "other"]
    picked = set(pickable(CHARTS))

    assert all(c in picked for c in others)


def test_the_picker_is_one_entry_per_family_plus_all_of_other():
    families = {c.group for c in CHARTS if c.period}
    others = [c for c in CHARTS if c.group == "other"]

    assert len(pickable(CHARTS)) == len(families) + len(others)


def test_collapsing_preserves_catalogue_order():
    """The picker's order is editorial; collapsing must not reshuffle it."""
    picked = pickable(CHARTS)
    order = [c.name for c in CHARTS]

    assert [order.index(c.name) for c in picked] == sorted(order.index(c.name) for c in picked)


def test_a_search_result_collapses_too():
    """`pickable` is applied to whatever list the handler has, not only CHARTS."""
    price_charts = [c for c in CHARTS if c.group == "price"]

    assert len(pickable(price_charts)) == 1
