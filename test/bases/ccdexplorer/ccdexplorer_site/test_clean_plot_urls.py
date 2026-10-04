"""Plot images follow the same path shape as the pages.

/plots/mainnet/transactions_count/weekly/202306/202604/image.png

Same reasoning as the pages: state belongs in the path. Two things stay in
the query string and are not addresses --

`theme` is a rendering preference with a working default, like asking for a
different stylesheet; the canonical image is the one without it.

`t` is a cache-busting bucket. Telegram downloads a photo url once and serves
its own copy forever after, so the url has to change when the chart does. It
is the opposite of an address: it is there precisely so the address stops
matching.
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ccdexplorer.ccdexplorer_site.app.factory import AppSettings, create_app

PROJECT = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"
PARAMETERISED = ["transactions_count", "plt_tvl", "agent_registries"]


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


def _paths(client):
    return {getattr(r, "path", "") for r in client.app.routes}


@pytest.mark.parametrize("name", PARAMETERISED)
def test_the_image_has_a_path_form(client, name):
    assert (
        f"/plots/{{net}}/{name}/{{grouping}}/{{start}}/{{end}}/image.png" in _paths(client)
    )


@pytest.mark.parametrize("name", PARAMETERISED)
def test_the_image_path_takes_traces_too(client, name):
    assert (
        f"/plots/{{net}}/{name}/{{grouping}}/{{start}}/{{end}}/{{traces}}/image.png"
        in _paths(client)
    )


@pytest.mark.parametrize("name", PARAMETERISED)
def test_the_bare_image_still_works(client, name):
    """What the share button copies, and the chart as it opens."""
    assert f"/plots/{{net}}/{name}/image.png" in _paths(client)


def test_the_query_form_redirects_to_the_path(client):
    response = client.get(
        "/plots/mainnet/transactions_count/image.png?grouping=weekly&window=90d"
    )
    assert response.status_code in (301, 307, 308)
    location = response.headers["location"]
    assert "/plots/mainnet/transactions_count/weekly/" in location
    assert "grouping=" not in location


def test_the_redirect_keeps_theme_and_the_cache_bucket(client):
    """Dropping theme returns a dark chart to a light chat, and dropping the
    bucket is how Telegram ends up serving a four-day-old image."""
    response = client.get(
        "/plots/mainnet/transactions_count/image.png?grouping=weekly&window=90d"
        "&theme=light&t=42"
    )
    location = response.headers["location"]
    assert "theme=light" in location
    assert "t=42" in location


def test_a_nonsense_grouping_in_an_image_path_is_not_found(client):
    assert (
        client.get(
            "/plots/mainnet/transactions_count/sideways/202306/202604/image.png"
        ).status_code
        == 404
    )


def test_a_nonsense_month_in_an_image_path_is_not_found(client):
    assert (
        client.get(
            "/plots/mainnet/transactions_count/weekly/nope/202604/image.png"
        ).status_code
        == 404
    )
