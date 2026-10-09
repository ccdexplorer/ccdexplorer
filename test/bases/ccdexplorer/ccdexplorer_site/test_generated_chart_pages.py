"""Pages generated from the specs, rather than one handler each.

Fifteen charts had a bespoke page handler in statistics.py and no way to
change their range or grouping. The spec already says everything a page needs
-- which fields, what they are called, how they group -- so the routes are
generated from it and the handlers become redundant.
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ccdexplorer.ccdexplorer_site.app.factory import AppSettings, create_app
from ccdexplorer.charts import Axis
from ccdexplorer.charts.registry import ALL_SPECS
from ccdexplorer.ccdexplorer_site.app.routers.charts.generated import (
    can_be_generated,
)

PROJECT = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"

GENERATED = [s for s in ALL_SPECS if s.source and not s.has_image or s.source]


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


@pytest.mark.parametrize(
    "spec", [s for s in ALL_SPECS if s.source], ids=[s.name for s in ALL_SPECS if s.source]
)
def test_every_spec_with_a_source_has_a_page(client, spec):
    """A chart that can be grouped can be configured."""
    paths = {getattr(r, "path", "") for r in client.app.routes}
    assert f"/{{net}}/charts/{spec.slug}" in paths, spec.slug


@pytest.mark.parametrize(
    "spec", [s for s in ALL_SPECS if s.source], ids=[s.name for s in ALL_SPECS if s.source]
)
def test_every_such_spec_says_it_has_a_page(client, spec):
    """has_page has to track the routes, or the gallery links wrongly."""
    assert spec.has_page is True, spec.name


#: The calendar charts, which are what the invariants below are about: a
#: grouping and a date range in the path, and series for build_figure to
#: draw. A live chart is generated too, and configured by one segment
#: instead -- see test_generated_live_charts.py.
GENERATED_SPECS = [s for s in ALL_SPECS if can_be_generated(s) and s.axis is Axis.CALENDAR]


@pytest.mark.parametrize("spec", GENERATED_SPECS, ids=[s.name for s in GENERATED_SPECS])
def test_every_generated_page_has_a_data_endpoint(client, spec):
    paths = {getattr(r, "path", "") for r in client.app.routes}
    assert f"/{{net}}/charts/{spec.slug}/data" in paths, spec.slug


def test_a_chart_the_pipeline_refuses_gets_no_generated_data_endpoint(client):
    """statistics_plt keeps its handwritten page; a generated one would fail
    on every request."""
    paths = {getattr(r, "path", "") for r in client.app.routes}
    assert "/{net}/charts/plt-transfers/data" not in paths


def test_an_intraday_spec_gets_no_generated_page(client):
    """It has no series to draw generically."""
    paths = {getattr(r, "path", "") for r in client.app.routes}
    assert "/{net}/charts/ccd-kraken-1h" not in paths


def test_a_generated_page_renders(client):
    response = client.get("/mainnet/charts/staking-open-pool-count")
    assert response.status_code == 200
    assert "Graph Settings" in response.text


def test_a_generated_page_reflects_the_state_in_its_path(client):
    # transaction-fees: open pools is a closing value and has no radios to
    # check any more, so there is nothing for this to read there.
    response = client.get("/mainnet/charts/transaction-fees/monthly/202601/202606")
    assert response.status_code == 200
    body = response.text
    monthly = body[body.index('id="monthly"') :][:120]
    assert "checked" in monthly


def test_the_query_form_is_redirected_rather_than_served(client):
    """It used to answer 200. The state belongs in the path now."""
    response = client.get("/mainnet/charts/staking-open-pool-count?grouping=monthly")
    assert response.status_code in (301, 307, 308)
    assert "?" not in response.headers["location"]


def test_a_generated_page_is_mainnet_only(client):
    response = client.get("/testnet/charts/staking-open-pool-count")
    assert response.status_code in (200, 404)
    if response.status_code == 200:
        assert "not available" in response.text.lower()
