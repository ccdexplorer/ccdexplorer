"""The site fetches from the api over the tunnel; the footer links to it publicly.

API_URL was doing both jobs. So rendering any page on ccd-100 sent every one of
its api calls out to api.ccdexplorer.io and back -- public DNS, a TLS handshake
and a trip through the proxy -- to reach a container on the same host. Measured
on 2026-10-29, /v2/mainnet/account/0/info took 28ms over the tunnel and
105ms over the public name, and the two responses were byte-identical.

The reason this is not simply a change of value is the footer. base.html renders
request.state.api_calls into every page as links, and a link to
http://10.0.0.1:10101/docs is dead for everyone outside the tunnel -- which
is everyone. So the fetch url and the url a visitor is shown are two attributes
now, and the tests below are mostly about keeping them from being confused for
each other: the invariants at the bottom are the ones that would catch a new
handler getting it wrong, because nothing else would.
"""

import importlib
import os
import pathlib
import re
from types import SimpleNamespace

import pytest

from ccdexplorer.ccdexplorer_site.app.factory import AppSettings, resolve_api_urls
from ccdexplorer.ccdexplorer_site.app.routers.charts import sc_plt_transfers as plt_mod

REPO = pathlib.Path(__file__).resolve().parents[4]
SITE_SOURCES = sorted((REPO / "bases/ccdexplorer/ccdexplorer_site").rglob("*.py"))


def _settings(**kwargs):
    return AppSettings(
        static_dir=pathlib.Path("."),
        templates_dir=pathlib.Path("."),
        node_modules_dir=pathlib.Path("."),
        addresses_dir=pathlib.Path("."),
        **kwargs,
    )


# --- which url is which ----------------------------------------------------


def test_the_fetch_url_is_the_internal_one_and_the_shown_url_is_not():
    fetch, shown = resolve_api_urls(
        _settings(),
        {"API_URL": "https://api.ccdexplorer.io", "API_URL_INTERNAL": "http://10.0.0.1:10101"},
    )

    assert fetch == "http://10.0.0.1:10101"
    assert shown == "https://api.ccdexplorer.io"


def test_without_an_internal_url_both_are_the_public_one():
    """Every process that has no reason to care is unaffected by this change."""
    fetch, shown = resolve_api_urls(_settings(), {"API_URL": "https://api.ccdexplorer.io"})

    assert fetch == shown == "https://api.ccdexplorer.io"


def test_an_empty_internal_url_is_not_a_url():
    """An env var set to "" in a stack file would otherwise take the site down."""
    fetch, shown = resolve_api_urls(
        _settings(), {"API_URL": "https://api.ccdexplorer.io", "API_URL_INTERNAL": ""}
    )

    assert fetch == shown == "https://api.ccdexplorer.io"


def test_app_settings_still_overrides_both():
    """How the tests point the whole app at one stand-in."""
    fetch, shown = resolve_api_urls(
        _settings(api_url="http://testserver"),
        {"API_URL": "https://api.ccdexplorer.io", "API_URL_INTERNAL": "http://10.0.0.1:10101"},
    )

    assert fetch == shown == "http://testserver"


def test_the_env_component_answers_both_names():
    """API_URL_INTERNAL defaults to API_URL in ccdexplorer.env, not just here."""
    from ccdexplorer.env import settings

    reloaded = importlib.reload(settings)
    try:
        assert "API_URL_INTERNAL" in reloaded.environment
        if not os.environ.get("API_URL_INTERNAL"):
            assert reloaded.environment["API_URL_INTERNAL"] == reloaded.environment["API_URL"]
    finally:
        importlib.reload(settings)


# --- a real handler --------------------------------------------------------


@pytest.fixture
def plt_request(monkeypatch):
    """The PLT transfers page: a handler that both fetches and fills the footer."""
    fetched = []

    async def recording_get(url, client):
        fetched.append(url)
        return SimpleNamespace(ok=True, return_value={})

    monkeypatch.setattr(plt_mod, "get_url_from_api", recording_get)

    class _Templates:
        def TemplateResponse(self, request, name, context):  # noqa: N802 - starlette's name
            return "rendered"

    request = SimpleNamespace(
        app=SimpleNamespace(
            templates=_Templates(),
            api_url="http://10.0.0.1:10101",
            api_docs_url="https://api.ccdexplorer.io",
            httpx_client=None,
            env={},
        ),
        state=SimpleNamespace(),
        query_params={},
    )
    return request, fetched


async def test_the_footer_links_go_to_the_public_api(plt_request):
    request, _ = plt_request

    await plt_mod.get_plt_transfers_at(request, "mainnet", "monthly", "202510", "202610")

    assert request.state.api_calls
    for name, url in request.state.api_calls.items():
        assert url.startswith("https://api.ccdexplorer.io/"), f"{name} points at {url}"


async def test_the_calls_the_page_makes_go_over_the_tunnel(plt_request):
    request, fetched = plt_request

    await plt_mod.get_plt_transfers_at(request, "mainnet", "monthly", "202510", "202610")

    assert fetched, "the handler is supposed to call the api"
    for url in fetched:
        assert url.startswith("http://10.0.0.1:10101/"), url


# --- the invariants that keep the two apart --------------------------------
#
# 117 docs links were rewritten by hand. These are what catches the 118th
# going in wrong, in either direction -- and the second one is the dangerous
# direction, because a tunnel address in a footer link looks fine in a local
# browser and is dead for every visitor.

FETCH_URL_RENDERED = re.compile(r"\.api_url\}/docs")
SHOWN_URL_FETCHED = re.compile(r"\.api_docs_url\}/(?!docs)")


def test_no_docs_link_is_built_from_the_fetch_url():
    offenders = [
        f"{path.relative_to(REPO)}:{n}"
        for path in SITE_SOURCES
        for n, line in enumerate(path.read_text().splitlines(), 1)
        if FETCH_URL_RENDERED.search(line)
    ]

    assert offenders == [], "api_url is the tunnel; a clicked link needs api_docs_url"


def test_the_shown_url_is_never_fetched_from():
    offenders = [
        f"{path.relative_to(REPO)}:{n}"
        for path in SITE_SOURCES
        for n, line in enumerate(path.read_text().splitlines(), 1)
        if SHOWN_URL_FETCHED.search(line)
    ]

    assert offenders == [], "api_docs_url is for display; fetch over api_url"


def test_the_sources_being_scanned_are_really_there():
    """A path typo would make both invariants above pass on nothing."""
    assert len(SITE_SOURCES) > 20
    assert sum("api_docs_url" in p.read_text() for p in SITE_SOURCES) >= 15
