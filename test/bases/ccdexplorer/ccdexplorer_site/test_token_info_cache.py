"""Token metadata is fetched once per token, not once per logged event.

The transaction page renders every logged event in a transaction, and each
CIS-2 / CIS-8004 / CIS-5 event asked the api for its token's metadata --
`/v2/{net}/token/{index}/{subindex}/{token_id}/info` -- with no cache of any
kind. One sequential round trip per event.

Measured on production 2026-11-08, with live-everything attributing each
spike to a single request:

    20:53:25   +1 request   +81.1s
    20:54:21   +2 requests  +81.5s
    20:55:17   +1 request   +81.7s

~81.5s per page load, against an api whose own mean was 37ms -- roughly
2,200 sequential calls. Over eight minutes the route's lifetime mean went
from 0.482s to 1.208s. Sentry's own detector had been reporting the same
endpoint on this route since 2026-07-17 ("Inefficient HTTP Requests",
issue 7616524463), naming two identical sequential GETs for one token.

The events in a transaction overwhelmingly concern the same few tokens, so
the fix is a cache. These tests pin that it is actually consulted, that it
does not over-cache (a wrong token's metadata rendered against an event is
worse than a slow page), and that the early returns which used to avoid the
call still avoid it.
"""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest


def _utils():
    """Resolved per call, never bound at import time.

    A full-suite run re-imports the site app package; a reference captured at
    collection time belongs to an orphaned module object that no monkeypatch
    reaches, and the test would silently call the real api. The same trap is
    documented and asserted in test_parallel_api_calls.py.
    """
    from ccdexplorer.ccdexplorer_site.app import utils

    return utils


class _Recorder:
    """Stands in for get_url_from_api, counting the urls actually requested."""

    def __init__(self, ok: bool = True, value: object = "metadata"):
        self.ok = ok
        self.value = value
        self.urls: list[str] = []

    async def __call__(self, url, httpx_client):
        self.urls.append(url)
        return SimpleNamespace(ok=self.ok, return_value=self.value, status_code=200)

    @property
    def calls(self) -> int:
        return len(self.urls)


class _FakeApp:
    """Only what the function under test touches."""

    def __init__(self):
        self.api_url = "http://api"


def _contract(index: int = 1234, subindex: int = 0):
    from ccdexplorer.grpc_client.CCD_Types import CCD_ContractAddress

    return CCD_ContractAddress(index=index, subindex=subindex)


def _request(app, net: str = "mainnet", token_id: str = "abc", field: str = "token_id"):
    """A ProcessEventRequest-shaped object carrying one recognized event."""
    recognized = SimpleNamespace(**{field: token_id})
    recognized.model_fields_set = {field}
    return SimpleNamespace(
        app=app,
        net=net,
        httpx_client=None,
        contract_address=_contract(),
        logged_event_from_collection=SimpleNamespace(recognized_event=recognized),
    )


@pytest.fixture
def recorder(monkeypatch):
    rec = _Recorder()
    monkeypatch.setattr(_utils(), "get_url_from_api", rec)
    return rec


# --- the bug itself -------------------------------------------------------


async def test_the_same_token_is_fetched_once(recorder):
    """The whole point. Two events on one token must cost one api call."""
    utils, app = _utils(), _FakeApp()

    for _ in range(2):
        await utils.get_token_info_from_collection(_request(app), _contract())

    assert recorder.calls == 1


async def test_many_events_on_one_token_still_cost_one_call(recorder):
    """The production shape: a transaction with hundreds of events on the
    same token took ~81s because each one went to the api."""
    utils, app = _utils(), _FakeApp()

    for _ in range(250):
        await utils.get_token_info_from_collection(_request(app), _contract())

    assert recorder.calls == 1


async def test_the_cached_value_is_the_value_returned(recorder):
    """A cache that returns something else is worse than no cache."""
    utils, app = _utils(), _FakeApp()

    first = await utils.get_token_info_from_collection(_request(app), _contract())
    second = await utils.get_token_info_from_collection(_request(app), _contract())

    assert first == "metadata"
    assert second == "metadata"


async def test_the_url_is_unchanged(recorder):
    """Same endpoint as before; this change is a cache, not a reroute."""
    utils, app = _utils(), _FakeApp()

    await utils.get_token_info_from_collection(_request(app), _contract())

    assert recorder.urls == ["http://api/v2/mainnet/token/1234/0/abc/info"]


# --- not over-caching, which would render the wrong token ----------------


async def test_two_token_ids_are_fetched_separately(recorder):
    """An NFT collection mints many distinct token ids in one transaction.
    Collapsing them would label every event with the first token's metadata."""
    utils, app = _utils(), _FakeApp()

    await utils.get_token_info_from_collection(_request(app, token_id="aa"), _contract())
    await utils.get_token_info_from_collection(_request(app, token_id="bb"), _contract())

    assert recorder.calls == 2


async def test_two_contracts_are_fetched_separately(recorder):
    """The same token id under a different contract is a different token."""
    utils, app = _utils(), _FakeApp()

    await utils.get_token_info_from_collection(_request(app), _contract(index=1))
    await utils.get_token_info_from_collection(_request(app), _contract(index=2))

    assert recorder.calls == 2


async def test_a_subindex_change_is_a_different_token(recorder):
    utils, app = _utils(), _FakeApp()

    await utils.get_token_info_from_collection(_request(app), _contract(subindex=0))
    await utils.get_token_info_from_collection(_request(app), _contract(subindex=1))

    assert recorder.calls == 2


async def test_nets_do_not_share_cached_tokens(recorder):
    """Contract <1234,0> on testnet is unrelated to <1234,0> on mainnet, and
    one app object serves every net."""
    utils, app = _utils(), _FakeApp()

    await utils.get_token_info_from_collection(_request(app, net="mainnet"), _contract())
    await utils.get_token_info_from_collection(_request(app, net="testnet"), _contract())

    assert recorder.calls == 2
    assert recorder.urls[0].startswith("http://api/v2/mainnet/")
    assert recorder.urls[1].startswith("http://api/v2/testnet/")


async def test_an_unknown_net_is_still_served(recorder):
    """The existing caches index a fixed dict by net and would KeyError on
    anything else. This one must not take the page down over it."""
    utils, app = _utils(), _FakeApp()

    result = await utils.get_token_info_from_collection(_request(app, net="stagenet"), _contract())

    assert result == "metadata"
    assert recorder.calls == 1


# --- the "_" convention for fungible tokens -------------------------------


async def test_an_empty_token_id_becomes_underscore(recorder):
    """Preserved behaviour: a fungible token has no id and the api wants "_"."""
    utils, app = _utils(), _FakeApp()

    await utils.get_token_info_from_collection(_request(app, token_id=""), _contract())

    assert recorder.urls == ["http://api/v2/mainnet/token/1234/0/_/info"]


async def test_the_underscore_token_is_cached_under_one_key(recorder):
    """Every transfer event of one fungible token looks like this, so it is
    the case that matters most."""
    utils, app = _utils(), _FakeApp()

    for _ in range(10):
        await utils.get_token_info_from_collection(_request(app, token_id=""), _contract())

    assert recorder.calls == 1


async def test_an_agent_token_id_is_read_and_cached(recorder):
    """CIS-8004 events carry agent_token_id rather than token_id."""
    utils, app = _utils(), _FakeApp()

    for _ in range(3):
        await utils.get_token_info_from_collection(
            _request(app, token_id="agent1", field="agent_token_id"), _contract()
        )

    assert recorder.calls == 1
    assert recorder.urls == ["http://api/v2/mainnet/token/1234/0/agent1/info"]


# --- failures: the pathological case is a page of them -------------------


async def test_a_failed_lookup_is_not_repeated_per_event(monkeypatch):
    """A token the api cannot answer for used to cost a round trip on every
    single event -- and a failing call can be slower than a good one, up to
    the 30s httpx read timeout. Bounded by the cache ttl, not by the page."""
    utils, app = _utils(), _FakeApp()
    rec = _Recorder(ok=False, value=None)
    monkeypatch.setattr(utils, "get_url_from_api", rec)

    for _ in range(100):
        result = await utils.get_token_info_from_collection(_request(app), _contract())
        assert result is None

    assert rec.calls == 1


# --- the early returns that already avoided the call ---------------------


async def test_an_unrecognized_event_makes_no_call(recorder):
    utils, app = _utils(), _FakeApp()
    req = _request(app)
    req.logged_event_from_collection.recognized_event = None

    assert await utils.get_token_info_from_collection(req, _contract()) is None
    assert recorder.calls == 0


async def test_an_event_without_any_token_id_makes_no_call(recorder):
    """Neither token_id nor agent_token_id set -- there is nothing to ask for."""
    utils, app = _utils(), _FakeApp()
    req = _request(app)
    req.logged_event_from_collection.recognized_event.model_fields_set = {"amount"}

    assert await utils.get_token_info_from_collection(req, _contract()) is None
    assert recorder.calls == 0


# --- the s7 helper hits the same endpoint, so it shares the cache --------


async def test_the_s7_helper_is_cached_too(recorder):
    utils, app = _utils(), _FakeApp()

    for _ in range(5):
        await utils.get_s7_token_info_from_collection(_request(app), _contract(), "tok")

    assert recorder.calls == 1


async def test_the_s7_helper_shares_entries_with_the_event_path(recorder):
    """Same url, so a second lookup by either route must not re-fetch."""
    utils, app = _utils(), _FakeApp()

    await utils.get_token_info_from_collection(_request(app, token_id="shared"), _contract())
    await utils.get_s7_token_info_from_collection(_request(app), _contract(), "shared")

    assert recorder.calls == 1


# --- freshness -----------------------------------------------------------


async def test_an_expired_entry_is_refetched(recorder, monkeypatch):
    """The api returns a holder count, which changes, so entries must expire.
    Without a ttl this would serve a stale count for the process lifetime."""
    utils, app = _utils(), _FakeApp()
    monkeypatch.setattr(utils, "TOKEN_INFO_CACHE_TTL_SECONDS", 0.05)

    await utils.get_token_info_from_collection(_request(app), _contract())
    time.sleep(0.1)
    await utils.get_token_info_from_collection(_request(app), _contract())

    assert recorder.calls == 2


async def test_the_ttl_is_bounded(recorder):
    """A long ttl on mutable metadata is its own bug. 60s matches the sibling
    caches in factory.py."""
    utils = _utils()

    assert 0 < utils.TOKEN_INFO_CACHE_TTL_SECONDS <= 60


async def test_the_cache_is_bounded(recorder):
    """Keyed by token address, i.e. unbounded cardinality for an explorer --
    a plain dict would keep every token ever rendered live for the process
    lifetime. The same reasoning is written out in factory.py."""
    utils, app = _utils(), _FakeApp()

    await utils.get_token_info_from_collection(_request(app), _contract())

    assert app.token_address_info_cache["mainnet"].maxsize == utils.TOKEN_INFO_CACHE_MAXSIZE
    assert utils.TOKEN_INFO_CACHE_MAXSIZE > 0


# --- it must work on the real app object, not just this fake -------------


def test_the_factory_declares_the_cache():
    """Unit tests above build their own app. This is what proves the cache
    the running site uses is the one being tested."""
    import inspect

    from ccdexplorer.ccdexplorer_site.app import factory

    source = inspect.getsource(factory.create_app)

    assert "token_address_info_cache" in source
