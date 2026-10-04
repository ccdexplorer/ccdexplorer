"""The PLT page answers the url shape everything hands out.

plt_tvl has has_page=True, so chart_path builds it a path-shaped address --
/mainnet/charts/plt-transfers/weekly/202510/202610 -- and that is what the
bot puts in its caption and what the share button copies. Only the bare
/mainnet/charts/plt-transfers was registered, so every one of those links
answered "Can't find the page you are looking for!".

It is served by hand rather than generated, because build_grouping_pipeline
refuses statistics_plt, so it did not get the path routes the generated
pages all have.
"""

import datetime as dt
from types import SimpleNamespace

import pytest

from ccdexplorer.ccdexplorer_site.app.routers.charts import sc_plt_transfers as mod


class _Templates:
    def __init__(self):
        self.context = None

    def TemplateResponse(self, request, name, context):  # noqa: N802 - starlette's name
        self.context = context
        return "rendered"


def _request():
    templates = _Templates()
    request = SimpleNamespace(
        app=SimpleNamespace(
            templates=templates,
            api_url="http://api",
            httpx_client=None,
            env={},
        ),
        state=SimpleNamespace(),
        query_params={},
    )
    return request, templates


@pytest.fixture(autouse=True)
def _no_api(monkeypatch):
    async def overview(url, client):
        return SimpleNamespace(ok=True, return_value={})

    monkeypatch.setattr(mod, "get_url_from_api", overview)


def test_the_path_form_is_registered():
    paths = {getattr(r, "path", "") for r in mod.router.routes}
    assert "/{net}/charts/plt-transfers/{grouping}/{start}/{end}" in paths


async def test_the_path_decides_the_state_the_page_opens_in():
    request, templates = _request()

    await mod.get_plt_transfers_at(request, "mainnet", "monthly", "202510", "202610")

    state = templates.context["state"]
    from ccdexplorer.charts.state import latest_complete_day

    assert state.start == dt.date(2025, 10, 1)
    # The path names October 2026, whose last day has not happened. Every
    # chart stops at the previous day, because each point comes from that
    # day's final block.
    assert state.end == min(dt.date(2026, 10, 31), latest_complete_day())
    # Not monthly: every series on this chart is a closing value, so it
    # takes its resolution from the span and the grouping in the path is a
    # record of what was drawn rather than a request.
    from ccdexplorer.charts.state import resolution_for

    assert state.grouping is resolution_for(state.start, state.end)


async def test_a_state_the_path_cannot_name_is_a_404():
    """A page is not an API, but a made-up grouping is a url that means
    nothing -- and a crawler can walk any number of them."""
    from fastapi import HTTPException

    request, _ = _request()
    with pytest.raises(HTTPException) as raised:
        await mod.get_plt_transfers_at(request, "mainnet", "sideways", "202510", "202610")
    assert raised.value.status_code == 404


async def test_the_bare_url_still_works():
    request, templates = _request()

    await mod.get_plt_transfers(request, "mainnet")

    assert templates.context["state"] is not None
