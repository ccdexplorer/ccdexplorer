"""The gallery opens on categories, not on every chart at once.

Twenty thumbnails meant twenty chart images on one page, each a kaleido
render when its cache was cold. It was slow enough to be the first thing
anyone said about it. /statistics has always opened on a handful of
categories and that is the shape this wants: six tiles, then the charts.
"""

from pathlib import Path

import pytest
import jinja2

from ccdexplorer.ccdexplorer_site.app.routers.charts.charts_home import (
    CATEGORY_ORDER,
    categories_with_counts,
    cover_chart,
    specs_in,
)
from ccdexplorer.charts.registry import ALL_SPECS

PROJECT = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"


def _render(name: str, **context) -> str:
    """One template's content block, without base.html.

    TestClient cannot render these pages: the lifespan that populates app
    state never runs under it, and the 500 handler needs that state too, so
    every failure arrives as the same AttributeError. The content is what is
    under test, so it is rendered directly.
    """
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(PROJECT / "templates")))
    source = env.loader.get_source(env, name)[0]
    body = source.split("{% block content %}")[1].rsplit("{% endblock %}", 1)[0]
    for include in (
        "{% include 'base/search_bar_non_home_inclusion.html' %}",
        "{% include 'base/box_title.html' %}",
    ):
        body = body.replace(include, "")
    return env.from_string(body).render(**context)


def test_the_index_lists_every_category_that_has_charts():
    listed = {c for c, _n in categories_with_counts()}
    assert listed == {s.category for s in ALL_SPECS}


def test_a_category_with_no_charts_is_not_listed():
    """An empty tile is a promise of nothing."""
    for _category, count in categories_with_counts():
        assert count > 0


def test_the_categories_read_in_a_fixed_order():
    listed = [c for c, _n in categories_with_counts()]
    assert listed == [c for c in CATEGORY_ORDER if c in listed]


def test_each_category_has_at_most_one_cover_chart():
    """One image per category rather than one per chart. PLT has none: its
    only chart is refused by the pipeline."""
    # By name: a ChartSpec carries a dict and so is not hashable.
    covers = [cover_chart(c) for c, _n in categories_with_counts()]
    names = [c.name for c in covers if c is not None]
    assert names, "no category has a cover"
    assert len(names) == len(set(names)), "two categories share a cover"


def test_every_category_names_a_cover_chart():
    """Was: a cover has to be a chart the generic builder can draw.

    That tied the picture to a limitation of the generator. The covers come
    through each chart's own image route now, so the Kraken candles and
    PLT's TVL can be covers too -- which is the whole reason exchanges
    showed a chart nobody chose and PLT showed nothing.
    """
    for category, _n in categories_with_counts():
        assert cover_chart(category) is not None, category


def test_the_index_draws_stored_files_rather_than_rendering():
    """The whole point: six full-size kaleido renders became five files."""
    from ccdexplorer.ccdexplorer_site.app.routers.charts.charts_home import (
        category_label,
        cover_image,
    )

    html = _render(
        "charts/charts_home.html",
        net="mainnet",
        categories=categories_with_counts(),
        cover_image=cover_image,
        category_label=category_label,
        theme="dark",
    )
    assert "/plots/" not in html
    # Two references per tile: the src for this theme, and the pattern the
    # switcher swaps when the reader picks the other one.
    assert html.count("/static/charts/") == 2 * sum(
        1 for c, _n in categories_with_counts() if cover_image(c, "dark")
    )


def test_the_index_links_into_each_category():
    from ccdexplorer.ccdexplorer_site.app.routers.charts.charts_home import (
        category_label,
        cover_image,
    )

    html = _render(
        "charts/charts_home.html",
        net="mainnet",
        categories=categories_with_counts(),
        cover_image=cover_image,
        category_label=category_label,
    )
    for category, _n in categories_with_counts():
        assert f"/mainnet/charts/category/{category}" in html, category


def test_a_category_page_lists_only_its_own_charts():
    html = _render(
        "charts/charts_category.html",
        net="mainnet",
        category="staking",
        specs=specs_in("staking"),
        has_image=lambda s: s.has_image,
        tile_href=lambda s, net: f"/{net}/charts/{s.slug}",
    )
    assert "/mainnet/charts/staking-open-pool-count" in html
    assert "/mainnet/charts/transaction-fees" not in html


def test_a_category_page_offers_a_way_back():
    html = _render(
        "charts/charts_category.html",
        net="mainnet",
        category="staking",
        specs=specs_in("staking"),
        has_image=lambda s: s.has_image,
        tile_href=lambda s, net: f"/{net}/charts/{s.slug}",
    )
    assert "/mainnet/charts" in html


async def test_an_unknown_category_is_refused():
    """Not a 404 page built from an empty list -- a 404."""
    from fastapi import HTTPException

    from ccdexplorer.ccdexplorer_site.app.routers.charts.charts_home import (
        get_charts_category,
    )

    with pytest.raises(HTTPException) as caught:
        await get_charts_category(None, "mainnet", "nonsense", {}, None)
    assert caught.value.status_code == 404
