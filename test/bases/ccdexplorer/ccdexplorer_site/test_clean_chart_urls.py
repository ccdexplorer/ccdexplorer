"""Chart pages live at paths, not at query strings.

The site keeps its urls clean, and the chart pages had grown a
?grouping=&from=&to= tail. The state goes in the path instead, and the old
form redirects so that anything already shared still lands.
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ccdexplorer.ccdexplorer_site.app.factory import AppSettings, create_app
from ccdexplorer.ccdexplorer_site.app.routers.charts.generated import can_be_generated
from ccdexplorer.charts.registry import ALL_SPECS

PROJECT = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"
GENERATED = [s for s in ALL_SPECS if can_be_generated(s)]


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


@pytest.mark.parametrize("spec", GENERATED, ids=[s.name for s in GENERATED])
def test_every_chart_has_a_bare_path(client, spec):
    assert f"/{{net}}/charts/{spec.slug}" in _paths(client)


@pytest.mark.parametrize("spec", GENERATED, ids=[s.name for s in GENERATED])
def test_every_chart_has_a_stateful_path(client, spec):
    assert f"/{{net}}/charts/{spec.slug}/{{grouping}}/{{start}}/{{end}}" in _paths(client)


def test_the_old_query_form_redirects_to_the_path(client):
    response = client.get(
        "/mainnet/charts/daily-limits?grouping=weekly&from=2023-06-01&to=2026-04-30"
    )
    assert response.status_code in (301, 307, 308)
    location = response.headers["location"]
    assert location == "/mainnet/charts/daily-limits/weekly/202306/202604"
    assert "?" not in location


def test_a_bare_url_is_left_alone(client):
    """Nothing to clean up, so no redirect to sit in front of every visit."""
    assert client.get("/mainnet/charts/daily-limits").status_code == 200


def test_a_grouping_only_query_still_redirects(client):
    # transaction-fees, because daily-limits picks its own resolution now
    # and would redirect to whatever the span implies rather than to the
    # grouping the query named.
    response = client.get("/mainnet/charts/transaction-fees?grouping=monthly")
    assert response.status_code in (301, 307, 308)
    assert "/monthly/" in response.headers["location"]


def test_a_grouping_query_on_an_automatic_chart_redirects_to_what_it_drew(client):
    """Not to what was asked: daily-limits takes its resolution from the
    span, so the path records the picture rather than the request."""
    response = client.get("/mainnet/charts/daily-limits?grouping=monthly")
    assert response.status_code in (301, 307, 308)
    assert "/weekly/" in response.headers["location"], response.headers["location"]


def test_a_nonsense_month_is_not_found(client):
    """A path is the address; drawing something else would make it a lie."""
    assert client.get("/mainnet/charts/daily-limits/weekly/nope/202604").status_code == 404


def test_a_nonsense_grouping_is_not_found(client):
    assert client.get("/mainnet/charts/daily-limits/sideways/202306/202604").status_code == 404


def test_the_page_carries_no_query_string_anywhere(client):
    """Including the links it renders back to itself."""
    body = client.get("/mainnet/charts/daily-limits").text
    assert "?grouping=" not in body
    assert "&from=" not in body
