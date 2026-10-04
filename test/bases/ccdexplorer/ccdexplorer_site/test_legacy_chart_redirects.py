"""The twelve route names the bot used to use, and the gallery.

transactions_count_90d and its siblings are in Telegram's own file cache and
in links people have already shared. They redirect to the parameterised route
rather than 404.

Routes only -- no chart is drawn here. Rendering one needs the API and
kaleido; what is under test is where a url goes.
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ccdexplorer.ccdexplorer_site.app.factory import AppSettings, create_app
from ccdexplorer.charts.registry import ALL_SPECS

PROJECT = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"

#: 180d has no exact Window -- the enum offers 30d, 90d, 1y and all -- so it
#: redirects to 90d, the nearest that does not overstate the range.
LEGACY = [
    ("transactions_count_30d", "transactions_count", "30d"),
    ("transactions_count_90d", "transactions_count", "90d"),
    ("transactions_count_180d", "transactions_count", "90d"),
    ("transactions_count_365d", "transactions_count", "1y"),
    ("plt_tvl_30d", "plt_tvl", "30d"),
    ("plt_tvl_90d", "plt_tvl", "90d"),
    ("plt_tvl_180d", "plt_tvl", "90d"),
    ("plt_tvl_365d", "plt_tvl", "1y"),
    ("agent_registries_30d", "agent_registries", "30d"),
    ("agent_registries_90d", "agent_registries", "90d"),
    ("agent_registries_180d", "agent_registries", "90d"),
    ("agent_registries_365d", "agent_registries", "1y"),
]


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


@pytest.mark.parametrize("old,name,window", LEGACY, ids=[row[0] for row in LEGACY])
def test_legacy_image_route_redirects_to_the_parameterised_one(client, old, name, window):
    response = client.get(f"/plots/mainnet/{old}/image.png")
    assert response.status_code in (301, 307, 308)
    location = response.headers["location"]
    assert f"/plots/mainnet/{name}/image.png" in location
    assert f"window={window}" in location


@pytest.mark.parametrize("old,name,window", LEGACY, ids=[row[0] for row in LEGACY])
def test_the_bare_legacy_route_redirects_too(client, old, name, window):
    """Both spellings were registered, so both have to keep working."""
    response = client.get(f"/plots/mainnet/{old}")
    assert response.status_code in (301, 307, 308)


def test_the_parameterised_route_exists_for_every_spec_with_an_image(client):
    paths = {getattr(r, "path", "") for r in client.app.routes}
    for name in ("transactions_count", "plt_tvl", "agent_registries"):
        assert f"/plots/{{net}}/{name}/image.png" in paths, name


def test_every_spec_claiming_a_page_has_one(client):
    """A gallery tile linking to a 404 is worse than no tile.

    Only the specs that claim a page: a chart gets a spec before it gets a
    page, and statistics.py still draws the rest."""
    paths = {getattr(r, "path", "") for r in client.app.routes}
    for spec in ALL_SPECS:
        if spec.has_page:
            assert f"/{{net}}/charts/{spec.slug}" in paths, spec.slug


def test_a_spec_without_a_page_really_has_no_route(client):
    """The flag has to be honest in both directions, or it is just a comment."""
    paths = {getattr(r, "path", "") for r in client.app.routes}
    for spec in ALL_SPECS:
        if not spec.has_page:
            assert f"/{{net}}/charts/{spec.slug}" not in paths, spec.slug


def test_the_category_home_routes_redirect_into_the_gallery(client):
    for path, anchor in [
        ("/mainnet/charts-plt", "plt"),
        ("/mainnet/charts-agents", "agents"),
    ]:
        response = client.get(path)
        assert response.status_code in (301, 307, 308)
        assert response.headers["location"].endswith(f"/mainnet/charts#{anchor}")


def test_a_spec_without_an_image_flag_really_has_no_image_route(client):
    """The complement of the test below: the flag must be honest in both
    directions, or a chart that has a thumbnail silently stops showing one.
    """
    from ccdexplorer.charts.registry import ALL_SPECS
    from ccdexplorer.ccdexplorer_site.app.routers.charts.charts_home import has_image

    paths = {getattr(r, "path", "") for r in client.app.routes}
    for spec in ALL_SPECS:
        if not has_image(spec):
            assert f"/plots/{{net}}/{spec.name}/image.png" not in paths, spec.name


def test_every_spec_the_gallery_thumbnails_really_has_that_route(client):
    from ccdexplorer.charts.registry import ALL_SPECS
    from ccdexplorer.ccdexplorer_site.app.routers.charts.charts_home import has_image

    paths = {getattr(r, "path", "") for r in client.app.routes}
    for spec in ALL_SPECS:
        if has_image(spec):
            assert f"/plots/{{net}}/{spec.name}/image.png" in paths, spec.name


def test_the_legacy_redirect_keeps_the_theme_it_was_asked_for(client):
    """Every Telegram-cached legacy url carries theme=light. Dropping it
    means a black rectangle in a light chat -- the exact population the
    redirect exists to serve."""
    response = client.get("/plots/mainnet/transactions_count_90d/image.png?theme=light&t=99")
    location = response.headers["location"]
    assert "theme=light" in location
    assert "window=90d" in location


def test_the_redirect_does_not_duplicate_window_or_grouping(client):
    response = client.get("/plots/mainnet/transactions_count_90d/image.png?theme=light&window=30d")
    location = response.headers["location"]
    assert location.count("window=") == 1
    assert "window=90d" in location
