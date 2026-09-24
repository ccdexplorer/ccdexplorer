"""The social card route must be cheap to hit and impossible to abuse.

/<net>/og/<path>/image.png is public, unauthenticated, and every miss costs an
API call plus ~12 ms of PNG encoding on a site that runs one uvicorn worker.
Three properties keep that from being a way to hurt it, and each is asserted
here rather than assumed:

  * a path that is not a page we draw never reaches the API at all,
  * a card is rendered once and then served from cache,
  * a card built from a failed lookup is not cached for a day, because the
    entity it was asked for may simply not exist yet.

The payloads below are real responses, copied from the API rather than
invented, so the builders are tested against the field names they will
actually meet.
"""

from pathlib import Path

import httpx2 as httpx
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from ccdexplorer.ccdexplorer_site.app import og_cards
from ccdexplorer.ccdexplorer_site.app.routers import og
from ccdexplorer.ccdexplorer_site.app.state import get_httpx_client

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
ADDRESS = "3ZFGxLtnUUSJGW2WqjMh1DDjxyq5rnytCwkSqxvwU3AsmZ1x8u"

BLOCK = {
    "height": 52029165,
    "hash": "b7fc3dd35e5668022fe641cc8d4d5d50743ad46b102da62637e9dd5c770393c1",
    "slot_time": "2026-09-23T06:05:50.779000Z",
    "transaction_count": 0,
    "baker": 8,
    "finalized": True,
}
ACCOUNT = {
    "index": 41827,
    "address": ADDRESS,
    "amount": 128456789012,
    "available_balance": 98456789012,
    "sequence_number": 12522,
    "stake": {"baker": {"baker_info": {"baker_id": 8}, "staked_amount": 30000000000}},
}
POOL = {
    "baker": 8,
    "address": "44bxoGippBqpgseaiYPFnYgi5J5q58bQKfpQFeGbY9DHmDPD78",
    "equity_capital": 420000000000000,
    "delegated_capital": 0,
    "all_pool_total_capital": 9991138729021849,
    "current_payday_info": {"blocks_baked": 137, "lottery_power": 0.0421},
    "pool_info": {"open_status": "openForAll"},
    "is_suspended": False,
}
TRANSACTION = {
    "hash": "6f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a6978a7b6c5d4e3f21201",
    "type": {"type": "account_transaction", "contents": "transfer"},
    "energy_cost": 501,
    "account_transaction": {"cost": 1980000, "sender": ADDRESS},
    "block_info": {"height": 52029100, "slot_time": "2026-09-23T05:59:12Z"},
}
PLT = {
    "token_id": "t-USDT",
    "token_state": {
        "decimals": 6,
        "total_supply": {"value": "4728175", "decimals": 6},
        "module_state": {
            "name": "t-USDT",
            "governance_account": {"account": "3uBLKTZSQbFara6jB67kQi9JEATzhLnUNwVLUUv66M9NtAJD8J"},
            "mintable": True,
            "burnable": True,
            "paused": False,
        },
    },
}
CONTRACT = {
    "v1": {
        "owner": "4XMdmHd5bvcxqp1TLMeQhNjst5MJ6fydBhyMWqgFQtg9JUHUep",
        "amount": 0,
        "methods": ["bridge-manager.grantRole", "bridge-manager.hasRole"],
        "name": "init_bridge-manager",
    },
    "_id": "<9337,0>",
    "module_verification": {"verified": False},
}
LOCK = {
    "lock": {"account_index": 57},
    "recipients": "any",
    "expiry": "2026-08-30T19:41:10Z",
    "controller": {"tokens": ["tDKK", "IMT"], "keep_alive": True},
    "funds": [{"account": ADDRESS}],
    "status": "open",
}
TOKEN = {
    "contract": "<9352,0>",
    "token_id": "",
    "token_amount": "1000000",
    "token_holders": {"a": "1", "b": "2"},
    "token_metadata": {
        "name": "Bank Of Memories",
        "symbol": "wGBM",
        "decimals": 7,
        "description": "Wrapped GBM",
    },
    "current_holders_count": 7,
    "tag_information": {"_id": "wGBM"},
}


# --- which paths are worth any work at all --------------------------------


@pytest.mark.parametrize(
    "entity, kind, parts",
    [
        ("mainnet/block/52029165", "block", ("52029165",)),
        ("mainnet/block/" + BLOCK["hash"], "block", (BLOCK["hash"],)),
        ("mainnet/account/41827", "account", ("41827",)),
        ("mainnet/account/" + ADDRESS, "account", (ADDRESS,)),
        ("mainnet/account/8/validator", "validator", ("8",)),
        ("testnet/transaction/" + TRANSACTION["hash"], "transaction", (TRANSACTION["hash"],)),
        ("mainnet/instance/9337/0", "contract", ("9337", "0")),
        ("mainnet/token/9352/0", "token", ("9352", "0", "")),
        ("mainnet/token/9352/0/abcdef", "token", ("9352", "0", "abcdef")),
        ("mainnet/tokens/t-USDT", "plt", ("t-USDT",)),
        ("devnet/tokens/plt/lock/YGLResV1sY", "lock", ("YGLResV1sY",)),
    ],
)
def test_a_real_page_path_resolves(entity, kind, parts):
    net, resolved, got = og_cards.resolve(entity)
    assert (resolved.name, got) == (kind, parts)
    assert net == entity.split("/")[0]


@pytest.mark.parametrize(
    "entity",
    [
        "mainnet/block/../../etc/passwd",
        "mainnet/block/not-a-height",
        "mainnet/block/" + "1" * 40,
        "mainnet/transaction/52029165",
        "mainnet/account/short",
        "mainnet/account/8/rewards",
        "mainnet/wallet/72723",
        "mainnet/instance/9337",
        "mainnet/instance/9337/0/extra",
        "mainnet/token/9352/0/not-hex!",
        "fakenet/block/52029165",
        "mainnet",
        "",
    ],
)
def test_anything_else_is_refused_on_its_shape(entity):
    assert og_cards.resolve(entity) is None


# --- the cards themselves -------------------------------------------------


@pytest.mark.parametrize(
    "kind, parts, payload",
    [
        ("block", ("52029165",), BLOCK),
        ("account", ("41827",), ACCOUNT),
        ("validator", ("8",), POOL),
        ("transaction", (TRANSACTION["hash"],), TRANSACTION),
        ("plt", ("t-USDT",), PLT),
        ("contract", ("9337", "0"), CONTRACT),
        ("lock", ("YGLResV1sY",), LOCK),
        ("token", ("9352", "0", ""), TOKEN),
    ],
)
def test_every_kind_draws_a_card_of_the_expected_size(kind, parts, payload):
    image = og_cards.KINDS[kind].build("mainnet", parts, payload)
    assert image.size == (og_cards.CARD_WIDTH, og_cards.CARD_HEIGHT) == (1200, 630)


@pytest.mark.parametrize("kind", sorted(og_cards.KINDS))
def test_a_kind_survives_a_payload_with_nothing_in_it(kind):
    """The API can answer 200 with a shape we did not expect; draw what there is."""
    parts = ("1", "0", "")
    assert og_cards.build_png("mainnet", og_cards.KINDS[kind], parts, {"x": 1}).startswith(
        PNG_MAGIC
    )


def test_a_tag_and_a_token_address_share_one_builder():
    """/tokens/<tag> has one part, /token/<i>/<s>/<id> has three.

    The first version of this indexed parts[1] unconditionally and every tag
    card came out as the generic fallback, with the reason only in the log.
    """
    for parts in (("wGBM",), ("9352", "0", ""), ("9352", "0", "abcd")):
        assert og_cards.token_card("mainnet", parts, TOKEN) is not None
        assert og_cards.build_png("mainnet", og_cards.KINDS["tag"], parts, TOKEN).startswith(
            PNG_MAGIC
        )


def test_a_tag_falls_back_from_plt_to_cis2():
    """A tag names one or the other, and the path does not say which."""
    plt = og_cards.KINDS["plt"]
    assert plt.alt is og_cards.KINDS["tag"]
    assert plt.api("mainnet", ("wGBM",)) == "/v2/mainnet/plt/wGBM/info"
    assert plt.alt.api("mainnet", ("wGBM",)) == "/v2/mainnet/token/tag/wGBM/info"


def test_an_empty_cis2_token_id_is_sent_as_the_underscore_the_api_wants():
    assert (
        og_cards.KINDS["token"].api("mainnet", ("9352", "0", ""))
        == "/v2/mainnet/token/9352/0/_/info"
    )


def test_a_token_supply_is_divided_by_its_own_decimals():
    """wGBM has seven of them, so 2,239,000,001 base units is a supply of 223.90.

    Printing token_amount raw claimed 2.24 billion on a card people share.
    """
    assert og_cards.scaled_amount("2239000001", 7) == "223.90"
    assert og_cards.scaled_amount("1000", 0) == "1,000"
    assert og_cards.scaled_amount(None, 7) is None


def test_a_missing_payload_falls_back():
    assert og_cards.build_png("mainnet", og_cards.KINDS["block"], ("1",), None).startswith(
        PNG_MAGIC
    )


def test_the_nonce_becomes_the_count_of_transactions_sent():
    """sequence_number is the *next* nonce, so one less is what has been sent.

    It counts only what this account sent, never what it received, which is
    why the card says "transactions sent" rather than a bare count.
    """
    card = og_cards.account_card("mainnet", ("41827",), dict(ACCOUNT, sequence_number=12522))
    assert card is not None
    assert og_cards.build_png(
        "mainnet", og_cards.KINDS["account"], ("41827",), dict(ACCOUNT, sequence_number=12522)
    ).startswith(PNG_MAGIC)


@pytest.mark.parametrize(
    "nonce, expected",
    [(12522, "12,521"), (1, "0"), (2, "1"), (None, None), ("nonsense", None), (0, None)],
)
def test_transactions_sent_handles_every_nonce_the_api_can_send(nonce, expected):
    account = dict(ACCOUNT, sequence_number=nonce)
    account.pop("stake")
    account["available_balance"] = account["amount"]  # so only balance + sent remain
    stats = _stats_of(account)
    assert stats.get("Transactions sent") == expected


def _stats_of(account):
    """The label/value pairs account_card would draw, without drawing them."""
    captured = {}

    def fake_render(net, kicker, headline, subline=None, stats=(), footer=None):
        captured.update({label: value for label, value in stats})
        return "image"

    real, og_cards.render_card = og_cards.render_card, fake_render
    try:
        og_cards.account_card("mainnet", ("1",), account)
    finally:
        og_cards.render_card = real
    return captured


def test_available_is_dropped_when_it_only_repeats_the_balance():
    account = dict(ACCOUNT, available_balance=ACCOUNT["amount"])
    assert "Available" not in _stats_of(account)
    assert "Balance" in _stats_of(account)


def test_a_card_offered_four_stats_lays_out_exactly_as_if_offered_three():
    """The bug this exists to prevent.

    render_card sized its columns by how many stats it was handed and then drew
    only three, so every column came out narrower than the space it had. A
    validator's "538.49M CCD" missed the resulting 240px by one pixel and
    rendered as "538.49M CC...".
    """
    three = [("Balance", "538.49M CCD"), ("Staked", "420.00M CCD"), ("Transactions sent", "15")]
    four = three + [("Available", "118.49M CCD")]
    assert og_cards.to_png(og_cards.render_card("mainnet", "Account", "#8", None, three)) == (
        og_cards.to_png(og_cards.render_card("mainnet", "Account", "#8", None, four))
    )


def test_a_plt_amount_is_scaled_by_its_own_decimals():
    assert og_cards.token_amount({"value": "4728175", "decimals": 6}) == "4.728175"
    assert og_cards.token_amount({"value": "10000", "decimals": 2}) == "100.00"
    assert og_cards.token_amount("nonsense") is None


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
        calls.append(request.url.path)
        for suffix, payload in (
            ("/block/52029165", BLOCK),
            ("/account/41827/info", ACCOUNT),
            ("/account/8/pool-info", POOL),
            ("/plt/t-USDT/info", PLT),
            ("/contract/9337/0/info", CONTRACT),
        ):
            if request.url.path.endswith(suffix):
                return httpx.Response(200, json=payload)
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
    response = client.get("/mainnet/og/block/52029165/image.png")
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"
    assert response.content.startswith(PNG_MAGIC)
    assert response.headers["cache-control"] == f"public, max-age={og_cards.TTL_IMMUTABLE}"


@pytest.mark.parametrize(
    "path, api_suffix",
    [
        ("/mainnet/og/account/41827/image.png", "/account/41827/info"),
        ("/mainnet/og/account/8/validator/image.png", "/account/8/pool-info"),
        ("/mainnet/og/tokens/t-USDT/image.png", "/plt/t-USDT/info"),
        ("/mainnet/og/instance/9337/0/image.png", "/contract/9337/0/info"),
    ],
)
def test_each_kind_asks_the_right_endpoint(client, path, api_suffix):
    response = client.get(path)
    assert response.status_code == 200
    assert response.content.startswith(PNG_MAGIC)
    assert client.api_calls == [f"/v2{api_suffix}"] or client.api_calls[0].endswith(api_suffix)


def test_a_card_is_rendered_once_and_then_served_from_cache(client):
    first = client.get("/mainnet/og/block/52029165/image.png")
    second = client.get("/mainnet/og/block/52029165/image.png")
    assert first.content == second.content
    assert len(client.api_calls) == 1


def test_junk_never_reaches_the_api(client):
    response = client.get("/mainnet/og/block/not-a-height/image.png")
    assert response.status_code == 200
    assert response.content.startswith(PNG_MAGIC)
    assert client.api_calls == []


def test_an_unknown_entity_gets_a_card_rather_than_an_error(client):
    """A 404 would make the sharer's whole message show a bare link."""
    response = client.get("/mainnet/og/block/99999999999/image.png")
    assert response.status_code == 200
    assert response.content.startswith(PNG_MAGIC)
    assert response.headers["cache-control"] == f"public, max-age={og_cards.TTL_FALLBACK}"


def test_the_url_this_shipped_with_still_works(client):
    """Previews unfurled before the move have the old path baked into them."""
    legacy = client.get("/og/mainnet/block/52029165/image.png")
    current = client.get("/mainnet/og/block/52029165/image.png")
    assert legacy.status_code == 200
    assert legacy.content == current.content
    assert len(client.api_calls) == 1, "and they should share a cache entry"


# --- the meta tags --------------------------------------------------------


def _macros():
    from jinja2 import Environment, FileSystemLoader

    root = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site" / "templates"
    env = Environment(loader=FileSystemLoader(str(root)), autoescape=True)
    return env.get_template("base/ogp_card.html").module


def test_the_card_url_is_the_page_url_with_og_after_the_net():
    rendered = _macros().ogp_card(
        {"SITE_URL": "https://ccdexplorer.io"}, "mainnet", "instance/9337/0", "A contract", None
    )
    assert 'property="og:url" content="https://ccdexplorer.io/mainnet/instance/9337/0"' in rendered
    assert (
        'property="og:image" content="https://ccdexplorer.io/mainnet/og/instance/9337/0/image.png"'
        in rendered
    )


def test_an_unset_site_url_never_becomes_the_word_None():
    """SITE_URL is absent from a local .env, and Jinja renders None as "None".

    That is how a verification email once went out linking to None/verify/... .
    """
    rendered = _macros().ogp_card({"SITE_URL": None}, "mainnet", "block/1", "Block 1", None)
    assert "None/" not in rendered
    assert 'content="/mainnet/og/block/1/image.png"' in rendered


def test_markup_in_a_title_cannot_escape_the_attribute():
    rendered = _macros().ogp_card(
        {"SITE_URL": "https://ccdexplorer.io"},
        "mainnet",
        "block/1",
        "<script>alert(1)</script>",
        None,
    )
    assert "<script>" not in rendered
