"""The validator page's three charts are each fetchable as a png.

The chart bot's /v command posts them as photos, and Telegram fetches a
photo url itself -- it will not POST to get one, which is how the page
draws them. So each figure needs a GET that answers with image bytes.

"image.png" is not decoration: return_plot_response decides between a png
and a full interactive page by looking for exactly that in the path. A
route named ".../png" answers 200 with five megabytes of html, which
Telegram downloads as a photo until it times out. That is what these
routes did when they shipped, so the name is the thing under test.
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ccdexplorer.ccdexplorer_site.app.factory import AppSettings, create_app

PROJECT = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"

#: The three boxes on the validator tab, as the bot's buttons offer them.
VALIDATOR_IMAGES = [
    "/{net}/account/apy-graph/{index_or_hash}/{request_type}/image.png",
    "/{net}/account/baker-performance/{validator_id}/{days}/image.png",
    "/{net}/account/rewards-bucketed/{account_id}/{account_index}/image.png",
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


def _paths(client):
    return {getattr(r, "path", "") for r in client.app.routes}


@pytest.mark.parametrize("path", VALIDATOR_IMAGES)
def test_each_validator_chart_has_a_get_image_route(client, path):
    assert path in _paths(client)


def test_none_of_them_is_named_png(client):
    """The suffix return_plot_response looks for is image.png, not png."""
    offenders = [p for p in _paths(client) if p.endswith("/png")]

    assert not offenders, offenders
