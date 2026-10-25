"""Fetching several api routes at once instead of one after another.

Sentry's consecutive-http detector fired 28 times in the week to
2026-10-24, 22 of them on /{net}/account/{index_or_hash}, naming
GET /v2/mainnet/account/0/info as the offending span. It was right: the
account handler made eighteen `await get_url_from_api` calls in a row and
no router in the site used asyncio.gather at all, so every round trip
waited for the one before it.

Only the first call is actually a dependency -- account/info yields the
address, index and validator id the rest are addressed by. After that the
calls are independent of each other, and the ones that are not (token
values, which need to know tokens exist) form one further wave.

The pattern already exists in the repo: fetch_validator in the chart bot
fans five calls out with gather, and get_last_accounts in the api records
that exactly this change took a request from ~850ms of serial work.
"""

import asyncio
from types import SimpleNamespace

import pytest


def _gather():
    """Resolved per call, never bound at collection time.

    A full-suite run re-imports the site app package, which leaves a
    reference captured at import time pointing at an orphaned module object
    whose globals no monkeypatch reaches -- the function would quietly call
    the real get_url_from_api and hit the network. Asserted below.
    """
    from ccdexplorer.ccdexplorer_site.app import utils

    return utils.gather_from_api


class _Recorder:
    """Stands in for get_url_from_api, measuring how many run at once."""

    def __init__(self, answers=None, hold=0.01):
        self.answers = answers or {}
        self.hold = hold
        self.in_flight = 0
        self.max_in_flight = 0
        self.order = []

    async def __call__(self, url, httpx_client):
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        self.order.append(url)
        try:
            await asyncio.sleep(self.hold)
        finally:
            self.in_flight -= 1
        return self.answers.get(url, SimpleNamespace(ok=True, return_value={"url": url}))


@pytest.fixture
def recorder(monkeypatch):
    from ccdexplorer.ccdexplorer_site.app import utils

    rec = _Recorder()
    monkeypatch.setattr(utils, "get_url_from_api", rec)
    return rec


# --- the helper -----------------------------------------------------------


async def test_every_url_is_fetched_at_the_same_time(recorder):
    """The whole point. Three urls, three calls in flight at once."""
    await _gather()({"a": "/a", "b": "/b", "c": "/c"}, None)

    assert recorder.max_in_flight == 3


async def test_results_come_back_keyed_by_name(recorder):
    got = await _gather()({"info": "/info", "pool": "/pool"}, None)

    assert sorted(got) == ["info", "pool"]
    assert got["info"].return_value == {"url": "/info"}
    assert got["pool"].return_value == {"url": "/pool"}


async def test_a_name_maps_to_its_own_url_not_a_neighbours(recorder):
    """gather preserves order; this is what makes zip safe, and getting it
    wrong would hand every value to the wrong variable."""
    urls = {f"k{i}": f"/u{i}" for i in range(12)}

    got = await _gather()(urls, None)

    for name, url in urls.items():
        assert got[name].return_value == {"url": url}, name


async def test_nothing_to_fetch_costs_no_calls(recorder):
    """The validator and delegation urls are conditional, so an ordinary
    account fetches a shorter set -- and an empty one must not error."""
    got = await _gather()({}, None)

    assert got == {}
    assert recorder.order == []


async def test_a_failing_route_does_not_lose_the_others(monkeypatch):
    """get_url_from_api answers not-ok rather than raising, so one dead
    route has to come back as its own not-ok result."""
    from ccdexplorer.ccdexplorer_site.app import utils

    async def mixed(url, client):
        if url == "/bad":
            return SimpleNamespace(ok=False, return_value=None)
        return SimpleNamespace(ok=True, return_value={"url": url})

    monkeypatch.setattr(utils, "get_url_from_api", mixed)
    got = await _gather()({"good": "/good", "bad": "/bad"}, None)

    assert got["good"].ok is True
    assert got["bad"].ok is False


async def test_it_is_faster_than_doing_them_one_at_a_time(monkeypatch):
    """Measured, not assumed: eight calls that each take 20ms finish in
    well under the 160ms they would cost in series."""
    from ccdexplorer.ccdexplorer_site.app import utils

    monkeypatch.setattr(utils, "get_url_from_api", _Recorder(hold=0.02))
    urls = {f"k{i}": f"/u{i}" for i in range(8)}

    loop = asyncio.get_running_loop()
    start = loop.time()
    await _gather()(urls, None)
    elapsed = loop.time() - start

    assert elapsed < 0.10, f"took {elapsed:.3f}s; serial would be ~0.16s"


# --- the handler actually uses it ----------------------------------------


def test_the_account_handler_fans_out():
    """A helper nothing calls leaves the Sentry issue exactly where it was."""
    import inspect

    from ccdexplorer.ccdexplorer_site.app.routers import account

    source = inspect.getsource(account.get_account)
    assert "gather_from_api(" in source, "get_account still fetches one at a time"


def test_the_handler_no_longer_awaits_a_long_chain():
    """Eighteen sequential awaits was the finding. A couple remain by
    necessity -- account/info first, and the smart-wallet fallback -- but
    the bulk has to be gone or nothing was fixed."""
    import inspect

    from ccdexplorer.ccdexplorer_site.app.routers import account

    source = inspect.getsource(account.get_account)
    sequential = source.count("await get_url_from_api")
    assert sequential <= 2, f"{sequential} sequential api awaits left in get_account"


def test_the_patch_reaches_the_function_that_matters():
    """The reason _gather exists. If these drift apart again, the tests
    above would pass against the real network instead of the recorder."""
    from ccdexplorer.ccdexplorer_site.app import utils

    assert utils.gather_from_api.__globals__ is utils.__dict__
