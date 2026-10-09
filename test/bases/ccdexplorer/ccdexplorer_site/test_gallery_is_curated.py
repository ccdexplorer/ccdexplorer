"""The gallery lists charts, not every variant of one.

Seven Kraken candle charts are one chart at seven intervals; listing all
seven filled the exchanges category with the same picture. One is listed and
the rest are reachable from it.

The three CCD price charts are gone from the listing: they plot the same
price the Kraken charts do, from the chain's own rate rather than the order
book, and three more tiles of it is not three more charts.

A tile has to lead somewhere. An intraday chart has no configurable page, so
it links to the chart's own page instead of being a dead tile.
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ccdexplorer.ccdexplorer_site.app.factory import AppSettings, create_app
from ccdexplorer.ccdexplorer_site.app.routers.charts.charts_home import (
    listed_specs,
    tile_href,
)
from ccdexplorer.charts.registry import ALL_SPECS, BY_NAME

PROJECT = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"


@pytest.fixture(scope="module")
def client():
    app = create_app(
        AppSettings(
            static_dir=PROJECT / "static",
            templates_dir=PROJECT / "templates",
            node_modules_dir=PROJECT / "node_modules",
            addresses_dir=PROJECT / "addresses",
        )
    )
    return TestClient(app, follow_redirects=False)


def test_only_one_kraken_chart_is_listed():
    listed = [s.name for s in listed_specs("exchanges")]
    kraken = [n for n in listed if n.startswith("ccd_kraken")]
    assert len(kraken) == 1, kraken


def test_the_listed_kraken_chart_is_the_one_the_bot_opens_at():
    """Four hours: short enough to be current, long enough to have shape."""
    listed = [s.name for s in listed_specs("exchanges")]
    assert "ccd_kraken_4h" in listed


def test_the_other_intervals_are_still_registered():
    """They are reachable, just not six more tiles of the same picture."""
    for code in ("1m", "5m", "15m", "30m", "1h", "1d"):
        assert f"ccd_kraken_{code}" in BY_NAME


def test_the_ccd_price_charts_are_not_listed():
    listed = {s.name for s in listed_specs("exchanges")}
    assert not any(n.startswith("ccd_price") for n in listed)


def test_the_exchanges_category_is_a_readable_length():
    assert len(listed_specs("exchanges")) <= 4


def test_every_listed_tile_leads_somewhere():
    for spec in ALL_SPECS:
        if spec in listed_specs(spec.category):
            assert tile_href(spec, "mainnet"), spec.name


def test_an_intraday_tile_leads_to_its_own_chart_page():
    assert tile_href(BY_NAME["ccd_kraken_4h"], "mainnet") == "/mainnet/charts/ccd-kraken"


def test_a_configurable_tile_leads_to_its_configurable_page():
    assert tile_href(BY_NAME["daily_limits"], "mainnet") == "/mainnet/charts/daily-limits"


def test_the_kraken_tile_points_at_a_registered_route(client):
    """A link to a page that does not exist is worse than no link.

    The route, not a request: rendering it fetches the order book through
    the app's own client, which the lifespan sets up and TestClient never
    runs.
    """
    paths = {getattr(r, "path", "") for r in client.app.routes}
    assert "/plots/{net}/ccd_kraken_4h" in paths
