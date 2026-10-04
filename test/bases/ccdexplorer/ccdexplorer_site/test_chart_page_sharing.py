"""A chart page has to survive being pasted into a chat.

The statistics tiles had a share button that copied /plots/<net>/<name>,
which answers with a small og-tagged page, so Telegram drew a card with the
chart in it. The configurable pages replaced those tiles and arrived with
neither: no button, and no og tags of their own. Pasting the url people
would actually copy -- the one in the address bar, carrying the grouping
and the range they chose -- got a bare link with the site's generic title.

The state matters as much as the picture. A link shared at monthly over
three years must preview monthly over three years, or the card contradicts
the page it opens.
"""

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ccdexplorer.ccdexplorer_site.app.factory import AppSettings, create_app

PROJECT = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"

PAGE = "/mainnet/charts/transaction-fees/monthly/202301/202412"
IMAGE = "/plots/mainnet/transaction_fees/monthly/202301/202412/image.png"


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


def _meta(html: str, prop: str) -> str | None:
    for pattern in (
        rf'<meta[^>]+property="{prop}"[^>]+content="([^"]*)"',
        rf'<meta[^>]+content="([^"]*)"[^>]+property="{prop}"',
        rf'<meta[^>]+name="{prop}"[^>]+content="([^"]*)"',
        rf'<meta[^>]+content="([^"]*)"[^>]+name="{prop}"',
    ):
        found = re.search(pattern, html)
        if found:
            return found.group(1)
    return None


@pytest.fixture(scope="module")
def page(client):
    response = client.get(PAGE)
    assert response.status_code == 200, response.status_code
    return response.text


def test_the_page_carries_an_og_image(page):
    assert _meta(page, "og:image")


def test_the_preview_is_the_chart_in_the_state_the_url_names(page):
    """Not the default. A link shared at monthly over 2023-2024 that
    previews the last year weekly is a card describing a different chart."""
    assert _meta(page, "og:image").endswith(IMAGE)


def test_the_og_url_is_the_page_itself(page):
    assert _meta(page, "og:url").endswith(PAGE)


def test_the_card_says_which_chart_it_is(page):
    assert "Transaction fees" in (_meta(page, "og:title") or "")
    assert _meta(page, "og:description")


def test_the_card_says_the_grouping_and_the_range_the_url_names(page):
    """The same words the figure puts above itself. A card reading "per
    Week" over a chart of monthly bars is the mislabelling this whole
    migration was about, moved one layer out."""
    assert "Month" in (_meta(page, "og:title") or "")
    assert "2023-01-01" in (_meta(page, "og:description") or "")
    assert "2024-12-31" in (_meta(page, "og:description") or "")


def test_telegram_and_twitter_get_a_large_image(page):
    assert _meta(page, "twitter:card") == "summary_large_image"
    assert _meta(
        page,
        "twitter:image",
    ) == _meta(page, "og:image")


def test_the_page_offers_the_url_to_copy(page):
    """The address bar holds it, but a button is what people look for --
    the statistics tiles had one and these replaced those tiles."""
    found = re.search(r'data-share-url="([^"]*)"', page)
    assert found, "no share button on the chart page"
    assert found.group(1).endswith(PAGE)


def test_a_default_page_shares_its_own_canonical_url(client):
    """Opened without a state in the path, the page still has one, and the
    link it hands out should say so rather than being bare."""
    response = client.get("/mainnet/charts/transaction-fees")
    assert response.status_code == 200
    found = re.search(r'data-share-url="([^"]*)"', response.text)
    assert found
    assert re.search(r"/mainnet/charts/transaction-fees/weekly/\d{6}/\d{6}$", found.group(1)), (
        found.group(1)
    )
