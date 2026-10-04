"""Every migrated chart serves a parameterised png.

Without one the chart bot cannot offer period buttons: a button that
produces the same picture it already showed is a dead control, and that is
what 18 of the bot's 28 charts would have got.

The bare url keeps working and keeps its name, because it is what the share
button copies and what Telegram has cached.
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
def test_every_migrated_chart_has_a_parameterised_image(client, spec):
    assert f"/plots/{{net}}/{spec.name}/{{grouping}}/{{start}}/{{end}}/image.png" in _paths(client)


@pytest.mark.parametrize("spec", GENERATED, ids=[s.name for s in GENERATED])
def test_the_bare_image_url_survives(client, spec):
    """Shared links and Telegram's cache both use it."""
    assert f"/plots/{{net}}/{spec.name}/image.png" in _paths(client)


@pytest.mark.parametrize("spec", GENERATED, ids=[s.name for s in GENERATED])
def test_each_one_says_it_has_an_image(client, spec):
    assert spec.has_image, spec.name


def test_statistics_no_longer_claims_those_image_urls(client):
    """Two handlers for one url is a coin toss decided by import order."""
    import re

    source = (
        Path(__file__).resolve().parents[4]
        / "bases/ccdexplorer/ccdexplorer_site/app/routers/statistics.py"
    ).read_text()
    claimed = set(re.findall(r'@router\.get\("/plots/\{net\}/([a-z0-9_]+)"', source))
    migrated = {s.name for s in GENERATED}
    assert not (claimed & migrated), sorted(claimed & migrated)


def test_the_dashboards_still_have_their_ajax_endpoints(client):
    """Only the image decorators moved; /statistics keeps drawing."""
    paths = _paths(client)
    for endpoint in (
        "statistics_transaction_fees",
        "statistics_classified_pools_open_pool_count",
        "statistics_microccd",
    ):
        assert f"/{{net}}/ajax_statistics_plotly_py/{endpoint}" in paths, endpoint


# --- and the generated route has to be the one that answers ----------------
#
# Registering a path is not owning it. sc_transactions_count.py and
# sc_agent_registries.py registered the same /plots paths, and their routers
# are included first, so FastAPI matched theirs and the generated handler
# never ran. Invisible, except for the theme: the generated handler resolves
# it with theme_from_query, which falls back to light because Telegram sends
# neither a parameter nor a cookie, while those two use
# get_theme_from_request, which falls back to dark. So two charts arrived in
# Telegram as black rectangles while the rest arrived light.


def _endpoint_for(app, path):
    scope = {"type": "http", "method": "GET", "path": path, "headers": [], "query_string": b""}
    for route in app.routes:
        match, _ = route.matches(scope)
        if match.name == "FULL":
            return route.endpoint
    return None


@pytest.mark.parametrize("spec", GENERATED, ids=[s.name for s in GENERATED])
@pytest.mark.parametrize(
    "shape",
    ["/plots/mainnet/{name}/image.png", "/plots/mainnet/{name}/weekly/202401/202412/image.png"],
    ids=["stateless", "path-form"],
)
def test_the_generated_handler_is_the_one_that_answers(client, spec, shape):
    endpoint = _endpoint_for(client.app, shape.format(name=spec.name))
    assert endpoint is not None, f"nothing serves {shape.format(name=spec.name)}"
    assert endpoint.__module__.endswith("charts.generated"), (
        f"{spec.name} is served by {endpoint.__module__}.{endpoint.__name__}, "
        "which shadows the generated route"
    )


def test_the_two_theme_resolvers_agree():
    """There are two, and a chart got whichever its handler happened to call.

    The generated routes resolve with theme_from_query and answered light;
    the handlers in statistics.py and the sc_ modules use
    get_theme_from_request, which ended at a hardcoded dark. Same request,
    two answers, and which one a chart got came down to which router
    registered its path first -- so eight of the charts the bot offers
    arrived in chats as black rectangles and the rest arrived light.

    Telegram sends neither a parameter nor a cookie, so the no-op request
    below is the one that matters.
    """
    import asyncio

    from starlette.requests import Request

    from ccdexplorer.ccdexplorer_site.app import utils

    def request(query="", cookie=None):
        headers = [(b"cookie", cookie.encode())] if cookie else []
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/plots/mainnet/x/image.png",
            "query_string": query.encode(),
            "headers": headers,
            "scheme": "https",
            "server": ("ccdexplorer.io", 443),
            "root_path": "",
        }
        built = Request(scope)
        built._body = b""
        return built

    for query, cookie in [
        ("", None),  # Telegram, and any unfurler
        ("theme=light", None),
        ("theme=dark", None),
        ("", "bsTheme=dark"),  # a reader on the dark site
        ("", "bsTheme=light"),
        ("theme=chartreuse", None),  # refused, so it falls back
    ]:
        asked = asyncio.run(utils.get_theme_from_request(request(query, cookie)))
        direct = utils.theme_from_query(request(query, cookie))
        assert asked == direct, f"query={query!r} cookie={cookie!r}: {asked} vs {direct}"


def test_a_request_that_says_nothing_gets_light():
    """Named separately because it is the whole user-visible point: these
    land in other people's chats, which are mostly light."""
    import asyncio

    from starlette.requests import Request

    from ccdexplorer.ccdexplorer_site.app import utils

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/plots/mainnet/x/image.png",
        "query_string": b"",
        "headers": [],
        "scheme": "https",
        "server": ("ccdexplorer.io", 443),
        "root_path": "",
    }
    built = Request(scope)
    built._body = b""
    assert asyncio.run(utils.get_theme_from_request(built)) == "light"
