"""The charts index draws stored images, not live renders.

Each cover used to be a full chart drawn through kaleido on request: six
renders of six full-size figures before the page appeared, and the index is
the first thing anyone opens. They are generated once and served as files.

They were greyscale at first, on the theory that a cover is a signpost
rather than a reading. That threw away the one thing telling these charts
apart -- six tiles of grey marks on black looked like the same picture six
times -- and a stored file cannot follow the theme toggle, so the index kept
showing dark tiles to a reader who had just asked for light. One file per
theme now, in the chart's own colours.
"""

from pathlib import Path

import pytest

from ccdexplorer.ccdexplorer_site.app.routers.charts.charts_home import (
    COVER_CHART,
    categories_with_counts,
    cover_chart,
    cover_image,
)

PROJECT = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"
COVERS = PROJECT / "static" / "charts"
THEMES = ("light", "dark")


def test_every_listed_category_has_a_cover():
    """Including PLT, which had none: its chart is refused by the generic
    pipeline, and the covers used to be limited to what that could draw."""
    missing = [c for c, _n in categories_with_counts() if not cover_image(c, "light")]
    assert missing == []


@pytest.mark.parametrize(
    "category, chart",
    [
        ("chain", "transactions_count"),
        ("accounts", "accounts_growth"),
        ("staking", "staking_avg_delegator_stake"),
        ("exchanges", "ccd_kraken_4h"),
        ("plt", "plt_tvl"),
        ("agents", "agent_registries"),
    ],
)
def test_each_category_shows_the_chart_it_was_given(category, chart):
    """Chosen rather than taken in registry order, which gave chain and
    accounts the same shape and picked nothing at all for PLT."""
    spec = cover_chart(category)
    assert spec is not None, category
    assert spec.name == chart


def test_the_mapping_names_every_category():
    assert {c for c, _n in categories_with_counts()} <= set(COVER_CHART)


def test_a_cover_path_is_a_static_file_not_a_render():
    path = cover_image("staking", "light")
    assert path.startswith("/static/charts/")
    assert "/plots/" not in path
    assert path.endswith(".png")


@pytest.mark.parametrize("theme", THEMES)
def test_there_is_one_cover_per_theme(theme):
    """A stored file cannot react to the toggle, so there are two of them
    and the switcher picks."""
    for category, _n in categories_with_counts():
        path = cover_image(category, theme)
        assert path.endswith(f"-{theme}.png"), path


@pytest.mark.parametrize("theme", THEMES)
def test_the_cover_files_exist(theme):
    """A missing file is a broken image on the first page anyone opens."""
    missing = [
        category
        for category, _n in categories_with_counts()
        if not (COVERS / f"{category}-{theme}.png").exists()
    ]
    assert not missing, f"run `just chart-covers`: {missing}"


def test_the_two_themes_are_actually_different_pictures():
    """Writing the same render under both names would look like it worked
    and change nothing when the reader toggles."""
    for category, _n in categories_with_counts():
        light = (COVERS / f"{category}-light.png").read_bytes()
        dark = (COVERS / f"{category}-dark.png").read_bytes()
        assert light != dark, category


def test_the_covers_carry_their_chart_colours():
    """Greyscale was the old rule. It is what made six categories look like
    one, so a cover with no colour in it now means the generator regressed."""
    from PIL import Image

    for category, _n in categories_with_counts():
        with Image.open(COVERS / f"{category}-light.png") as image:
            sample = list(image.convert("RGB").getdata())[::97]
            coloured = [p for p in sample if not (p[0] == p[1] == p[2])]
            assert coloured, f"{category} is still greyscale"


@pytest.mark.parametrize("theme", THEMES)
def test_the_covers_are_small_enough_to_be_thumbnails(theme):
    """The point of the change was the index loading quickly."""
    for category, _n in categories_with_counts():
        size = (COVERS / f"{category}-{theme}.png").stat().st_size
        assert size < 120_000, f"{category}-{theme}.png is {size} bytes"


def test_an_unknown_category_has_no_cover():
    assert cover_image("nonsense", "light") == ""
