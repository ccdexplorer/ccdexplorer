"""A page that is not there answers 404, not 200.

The handler rendered the error template through TemplateResponse without a
status, which defaults to 200 -- so every missing page on the site claimed to
be a page. It matters more now that chart state lives in the path: a crawler
can walk any number of made-up groupings and months, and a soft 404 invites
every one of them into an index.
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ccdexplorer.ccdexplorer_site.app.factory import AppSettings, create_app

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


def test_an_unrouted_path_is_a_404(client):
    assert client.get("/mainnet/no-such-page-at-all").status_code == 404


def test_a_made_up_chart_view_is_a_404(client):
    assert client.get("/mainnet/charts/daily-limits/sideways/202306/202604").status_code == 404


def test_a_made_up_month_is_a_404(client):
    assert client.get("/mainnet/charts/daily-limits/weekly/nope/202604").status_code == 404


# An unknown category is covered in test_charts_gallery_by_category, at the
# function level: that route takes a dependency needing app state the
# lifespan sets, so through TestClient it fails before reaching its 404.


def test_the_reader_still_gets_the_error_page(client):
    """A bare 404 body would be a worse page than the one we had."""
    body = client.get("/mainnet/no-such-page-at-all").text
    assert "find the page" in body.lower()


# --- and the same omission in its sibling ----------------------------------
#
# The 404 handler was given a status; the 500 handler beside it was not, and
# TemplateResponse still defaults to 200. So every unhandled error on the
# site reported success: a monitor watching status codes saw none of them,
# and an htmx swap replaced a chart with the error page as though it had
# worked.


def test_an_unhandled_error_says_500(client):
    from fastapi import APIRouter

    router = APIRouter()

    @router.get("/mainnet/boom")
    async def boom():
        raise RuntimeError("the kind of thing that reaches Sentry")

    client.app.include_router(router)

    response = TestClient(client.app, raise_server_exceptions=False).get("/mainnet/boom")
    assert response.status_code == 500
    assert "not quite right" in response.text.lower()
