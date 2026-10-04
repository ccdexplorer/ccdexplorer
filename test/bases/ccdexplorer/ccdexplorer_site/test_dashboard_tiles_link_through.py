"""A dashboard tile should lead to the chart it shows.

/statistics/* stays a place to scan: fixed previews, two to a row. What it
gained is a way out of each tile into the page where that chart can be
configured -- which is the whole point of having built those pages.

A tile whose chart has no page is left alone rather than linked somewhere
approximate.
"""

import re
from pathlib import Path

import pytest

from ccdexplorer.charts.registry import spec_for_plot

TEMPLATES = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site" / "templates"
DASHBOARDS = sorted((TEMPLATES / "statistics").glob("statistics-chain-*.html"))
SHARED = TEMPLATES / "statistics" / "chain" / "generic_plot_with_share.html"


def _plot_names(path: Path) -> list[str]:
    return re.findall(r'set plot_name\s*=\s*"(\w+)"', path.read_text())


def test_the_shared_tile_renders_a_link_when_there_is_one():
    body = SHARED.read_text()
    assert "page_slug" in body, "the tile has no link at all"
    assert "{% if page_slug %}" in body, "the link is not conditional"


@pytest.mark.parametrize("path", DASHBOARDS, ids=[p.stem for p in DASHBOARDS])
def test_every_tile_resolves_or_is_deliberately_unlinked(path):
    """Each tile either finds a spec with a page, or is one of the few that
    genuinely has none."""
    unlinked = {"staking_validator_staked_amounts"}
    for name in _plot_names(path):
        spec = spec_for_plot(name)
        if name in unlinked:
            assert spec is None or not spec.has_page, f"{name} is linkable now"
        else:
            assert spec is not None, f"{name} resolves to no spec"
            assert spec.has_page, f"{name} has a spec but no page"


def test_the_three_renamed_tiles_find_their_page():
    """These name a route that became a spec under another name."""
    for old, new in (
        ("accounts_per_day", "accounts-growth"),
        ("network_activity_tps", "network-activity"),
        ("transaction_types", "transactions-count"),
    ):
        spec = spec_for_plot(old)
        assert spec is not None and spec.slug == new, old
