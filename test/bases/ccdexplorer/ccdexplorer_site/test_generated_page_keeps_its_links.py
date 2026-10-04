"""The generated page carries what the handwritten ones carried.

Five charts moved from a handwritten page to a generated one. Each had a
download link under the chart and, where one existed, a link to its
documentation. Losing either on the way across would be a quiet downgrade.

The download is a real endpoint now rather than a tempfile written as a side
effect of drawing: the old pages dropped a csv into /tmp per request and
linked to it, and nothing ever removed them.
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


@pytest.mark.parametrize("spec", GENERATED, ids=[s.name for s in GENERATED])
def test_every_generated_chart_has_a_csv_endpoint(client, spec):
    paths = {getattr(r, "path", "") for r in client.app.routes}
    assert (
        f"/{{net}}/charts/{spec.slug}/{{grouping}}/{{start}}/{{end}}/data.csv" in paths
    )


def test_the_page_offers_the_download(client):
    body = client.get("/mainnet/charts/agent-registries").text
    assert "Download Data" in body
    assert "/mainnet/charts/agent-registries/weekly/" in body
    assert "/data.csv" in body


def test_the_download_is_not_a_tempfile(client):
    """The old pages linked to /tmp/<uuid>.csv, written while drawing."""
    body = client.get("/mainnet/charts/agent-registries").text
    assert "/tmp/" not in body


def test_the_csv_carries_the_state_it_was_asked_for(client):
    body = client.get("/mainnet/charts/transactions-count/monthly/202601/202606").text
    assert "/transactions-count/monthly/202601/202606/data.csv" in body


def test_the_csv_link_has_no_query_string(client):
    body = client.get("/mainnet/charts/agent-registries").text
    assert "data.csv?" not in body


def test_a_chart_with_documentation_links_to_it(client):
    body = client.get("/mainnet/charts/transactions-count").text
    assert "docs" in body.lower()


# --- and the link has to actually resolve --------------------------------
#
# Every test above passes while the download is broken. The route is
# registered, the page prints it, and nothing asks whether the url answers.
# It did not: `.../{grouping}/{start}/{end}/{traces}` is registered before
# `.../{grouping}/{start}/{end}/data.csv`, FastAPI matches in registration
# order, and so `{traces}` captured the string "data.csv" and every chart's
# Download Data link answered 404.


def test_the_csv_url_is_not_captured_by_the_trace_route(client):
    """The order the two routes are registered in is the whole bug."""
    paths = [getattr(r, "path", "") for r in client.app.routes]
    csv = paths.index("/{net}/charts/transaction-fees/{grouping}/{start}/{end}/data.csv")
    traces = paths.index("/{net}/charts/transaction-fees/{grouping}/{start}/{end}/{traces}")
    assert csv < traces, "data.csv must be registered before the trace route, or it never matches"


@pytest.fixture
def csv_client(monkeypatch):
    """An app built from the same module object the patch is applied to.

    test_app_imports_and_docs deletes every ccdexplorer_site.app module from
    sys.modules to prove the import order is safe, so by the time this runs
    `generated` may be a second, freshly imported module -- and the routes a
    module-scoped app holds would be the first one's, whose _fetch no patch
    here can reach.
    """
    from fastapi import FastAPI

    from ccdexplorer.ccdexplorer_site.app.routers.charts import generated

    async def rows(_spec, _app, state):
        return [{"date": "2024-01-01", "_days": 7}]

    monkeypatch.setattr(generated, "_fetch", rows)

    app = FastAPI()
    app.include_router(generated.router)
    return TestClient(app, follow_redirects=False)


@pytest.mark.parametrize("spec", GENERATED, ids=[s.name for s in GENERATED])
def test_the_download_answers_with_a_csv(csv_client, spec):
    """Asked for the way the page asks for it."""
    response = csv_client.get(f"/mainnet/charts/{spec.slug}/weekly/202401/202412/data.csv")
    assert response.status_code == 200, response.status_code
    assert response.headers["content-type"].startswith("text/csv")
    assert response.text.startswith("date,")
