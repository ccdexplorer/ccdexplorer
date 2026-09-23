"""The social card route must be cheap to hit and impossible to abuse.

/og/<net>/<kind>/<id>/image.png is public, unauthenticated, and every miss costs an
API call plus ~12 ms of PNG encoding on a site that runs one uvicorn worker.
Three properties keep that from being a way to hurt it, and each is asserted
here rather than assumed:

  * an identifier that is not a plausible height, hash or address never
    reaches the API at all,
  * a card is rendered once and then served from cache,
  * a card built from a failed lookup is not cached for a day, because the
    block it was asked for may simply not exist yet.
"""

import httpx2 as httpx
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from ccdexplorer.ccdexplorer_site.app import og_cards
from ccdexplorer.ccdexplorer_site.app.routers import og
from ccdexplorer.ccdexplorer_site.app.state import get_httpx_client

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"

BLOCK = {
    "height": 52029165,
    "hash": "b7fc3dd35e5668022fe641cc8d4d5d50743ad46b102da62637e9dd5c770393c1",
    "slot_time": "2026-09-23T06:05:50.779000Z",
    "transaction_count": 3,
    "baker": 8,
    "finalized": True,
}
ACCOUNT = {
    "index": 41827,
    "address": "3ZFGxLtnUUSJGW2WqjMh1DDjxyq5rnytCwkSqxvwU3AsmZ1x8u",
    "amount": 128456789012,
    "available_balance": 98456789012,
    "stake": {"baker": {"baker_info": {"baker_id": 8}, "staked_amount": 30000000000}},
}
TRANSACTION = {
    "hash": "6f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a6978a7b6c5d4e3f21201",
    "type": {"type": "account_transaction", "contents": "transfer"},
    "energy_cost": 501,
    "account_transaction": {"cost": 1980000, "sender": ACCOUNT["address"]},
    "block_info": {"height": 52029100, "slot_time": "2026-09-23T05:59:12Z"},
}


# --- which requests are worth any work at all -----------------------------


@pytest.mark.parametrize(
    "net, kind, ident",
    [
        ("mainnet", "block", "52029165"),
        ("mainnet", "block", BLOCK["hash"]),
        ("testnet", "account", "41827"),
        ("mainnet", "account", ACCOUNT["address"]),
        ("devnet", "transaction", TRANSACTION["hash"]),
    ],
)
def test_a_plausible_identifier_is_accepted(net, kind, ident):
    assert og_cards.accepts(net, kind, ident)


@pytest.mark.parametrize(
    "net, kind, ident",
    [
        ("mainnet", "block", "../../etc/passwd"),
        ("mainnet", "block", "not-a-height"),
        ("mainnet", "block", "1" * 40),  # too long to be a height
        ("mainnet", "transaction", "52029165"),  # a height is not a tx hash
        ("mainnet", "account", "short"),
        ("mainnet", "account", "0OIl" * 12),  # base58 excludes these characters
        ("mainnet", "wallet", "52029165"),  # unknown kind
        ("fakenet", "block", "52029165"),  # unknown net
    ],
)
def test_junk_is_refused_on_its_shape(net, kind, ident):
    assert not og_cards.accepts(net, kind, ident)


# --- the cards themselves -------------------------------------------------


@pytest.mark.parametrize(
    "builder, payload",
    [
        (og_cards.block_card, BLOCK),
        (og_cards.account_card, ACCOUNT),
        (og_cards.transaction_card, TRANSACTION),
    ],
)
def test_a_card_is_the_size_every_preview_consumer_expects(builder, payload):
    image = builder("mainnet", "x", payload)
    assert image.size == (og_cards.CARD_WIDTH, og_cards.CARD_HEIGHT) == (1200, 630)


def test_a_card_survives_a_payload_with_nothing_in_it():
    """The API can answer 200 with a shape we did not expect; draw what there is."""
    assert og_cards.build_png("mainnet", "block", "1", {}).startswith(PNG_MAGIC)


def test_an_unknown_kind_falls_back_rather_than_raising():
    assert og_cards.build_png("mainnet", "nonsense", "1", BLOCK).startswith(PNG_MAGIC)


# --- the cache ------------------------------------------------------------


def test_the_cache_returns_what_it_stored_with_its_max_age():
    cache = og_cards.PngCache()
    cache.put("k", b"png", 300)
    assert cache.get("k") == (b"png", 300)


def test_the_cache_forgets_an_entry_once_it_expires():
    cache = og_cards.PngCache()
    cache.put("k", b"png", 0)
    assert cache.get("k") is None


def test_the_cache_is_bounded():
    cache = og_cards.PngCache(max_entries=3)
    for index in range(10):
        cache.put(f"k{index}", b"png", 300)
    assert len(cache) == 3
    assert cache.get("k0") is None
    assert cache.get("k9") is not None


# --- the route ------------------------------------------------------------


@pytest.fixture
def client():
    """A site app carrying only the og router, over a stubbed API."""
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        path = request.url.path
        if path.endswith("/block/52029165"):
            return httpx.Response(200, json=BLOCK)
        if path.endswith("/account/41827/info"):
            return httpx.Response(200, json=ACCOUNT)
        return httpx.Response(404, json={"detail": "not found"})

    app = FastAPI()
    app.include_router(og.router)
    app.api_url = "https://api.example.test"
    stub = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    app.dependency_overrides[get_httpx_client] = lambda: stub

    og._CARDS = og_cards.PngCache()  # each test starts cold
    with TestClient(app) as test_client:
        test_client.api_calls = calls
        yield test_client


def test_a_known_block_is_served_as_a_png(client):
    response = client.get("/og/mainnet/block/52029165/image.png")
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"
    assert response.content.startswith(PNG_MAGIC)
    assert response.headers["cache-control"] == f"public, max-age={og_cards.TTL_IMMUTABLE}"


def test_an_account_card_is_not_cached_as_long_as_a_block(client):
    """A balance moves; a finalized block does not."""
    response = client.get("/og/mainnet/account/41827/image.png")
    assert response.headers["cache-control"] == f"public, max-age={og_cards.TTL_MUTABLE}"


def test_a_card_is_rendered_once_and_then_served_from_cache(client):
    first = client.get("/og/mainnet/block/52029165/image.png")
    second = client.get("/og/mainnet/block/52029165/image.png")
    assert first.content == second.content
    assert len(client.api_calls) == 1


def test_junk_never_reaches_the_api(client):
    response = client.get("/og/mainnet/block/not-a-height/image.png")
    assert response.status_code == 200
    assert response.content.startswith(PNG_MAGIC)
    assert client.api_calls == []


def test_an_unknown_entity_gets_a_card_rather_than_an_error(client):
    """A 404 would make the sharer's whole message show a bare link."""
    response = client.get("/og/mainnet/block/99999999999/image.png")
    assert response.status_code == 200
    assert response.content.startswith(PNG_MAGIC)
    # Short, because that block may simply not have been produced yet.
    assert response.headers["cache-control"] == f"public, max-age={og_cards.TTL_FALLBACK}"
