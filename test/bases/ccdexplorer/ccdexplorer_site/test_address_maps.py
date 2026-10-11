"""The address maps the site holds, built from the api rather than a pickle.

They were two committed pickles -- 79MB of tracked binary, refreshed by hand
with `just addresses`, served publicly by accident, and 437MB resident once
unpickled, because the larger one rehydrated a whole document per account and
left 427,000 containers for the collector to re-scan on every pass. That is
what gc.freeze() in factory.py was working around.

Measured on 123,911 accounts: 6.6MB over the wire, 0.06s to build, 37MB
resident, 5,188 objects, no measurable gc pause.
"""

from types import SimpleNamespace

from ccdexplorer.ccdexplorer_site.app.utils import (
    account_address_is_alias,
    add_account_info_to_cache,
    address_maps_from_rows,
)

ADDRESS = "3Y1RLgi5pW3x96xZ7CiHUKTOrOrgnSk2tzMEXBhWhVvHDOwgkC"
OTHER = "4jxvYasaPncfmCFCLZCvuL5cZuvR5HAQezCHZH7ZA7AGsRYpix"


def test_both_maps_are_keyed_on_the_canonical_prefix():
    """29 characters, which is what identifies an account across its
    aliases, and what the collection's _id already is."""
    indexes, canonical = address_maps_from_rows([[ADDRESS, 1234]])

    assert indexes == {ADDRESS[:29]: 1234}
    assert canonical == {ADDRESS[:29]: ADDRESS}


def test_the_canonical_map_keeps_the_whole_address():
    """Its one job is telling an alias from the real address, which needs
    the 21 characters the key throws away."""
    _, canonical = address_maps_from_rows([[ADDRESS, 1]])

    assert canonical[ADDRESS[:29]] == ADDRESS
    assert len(canonical[ADDRESS[:29]]) == 50


def test_many_rows_build_both_maps():
    indexes, canonical = address_maps_from_rows([[ADDRESS, 1], [OTHER, 2]])

    assert indexes == {ADDRESS[:29]: 1, OTHER[:29]: 2}
    assert canonical == {ADDRESS[:29]: ADDRESS, OTHER[:29]: OTHER}


def test_no_rows_is_two_empty_maps_rather_than_a_failure():
    """A fresh devnet has no accounts, and the site still has to boot."""
    assert address_maps_from_rows([]) == ({}, {})


def test_a_missing_payload_is_two_empty_maps():
    """get_url_from_api answers None when the call failed. The site booted
    with empty maps before -- the pickle load was wrapped in try/except --
    and it has to keep doing that rather than refuse to start."""
    assert address_maps_from_rows(None) == ({}, {})


def test_a_malformed_row_is_skipped_rather_than_fatal():
    """One bad row should cost one account, not the whole map and with it
    every account link on the site."""
    indexes, canonical = address_maps_from_rows([[ADDRESS, 1], ["nope"], [OTHER, 2]])

    assert indexes == {ADDRESS[:29]: 1, OTHER[:29]: 2}
    assert canonical == {ADDRESS[:29]: ADDRESS, OTHER[:29]: OTHER}


def test_the_highest_index_is_discoverable_for_the_search_box():
    """home.py validates a numeric search term against max_index_known, and
    the old code took max() of the pickle's values -- which raises on an
    empty map, so a net with no accounts took the whole boot down."""
    indexes, _ = address_maps_from_rows([[ADDRESS, 7], [OTHER, 42]])

    assert max(indexes.values(), default=0) == 42
    assert max({}.values(), default=0) == 0


# --- what the canonical map is for ----------------------------------------


def _app(canonical: dict):
    return SimpleNamespace(
        addresses_to_indexes={"mainnet": {}},
        canonical_addresses={"mainnet": canonical},
        max_index_known={"mainnet": 0},
    )


def test_the_canonical_address_is_not_an_alias():
    app = _app({ADDRESS[:29]: ADDRESS})

    assert account_address_is_alias(ADDRESS, "mainnet", app) is False


def test_another_address_on_the_same_account_is_an_alias():
    """Aliases share the first 29 characters and differ after them."""
    alias = ADDRESS[:29] + "zzzzzzzzzzzzzzzzzzzzz"
    app = _app({ADDRESS[:29]: ADDRESS})

    assert account_address_is_alias(alias, "mainnet", app) is True


def test_a_bare_canonical_prefix_is_not_an_alias():
    app = _app({ADDRESS[:29]: ADDRESS})

    assert account_address_is_alias(ADDRESS[:29], "mainnet", app) is False


def test_an_account_the_map_has_never_heard_of_is_not_called_an_alias():
    """An account created since boot. Saying "alias" about it would be a
    claim, where not knowing is the truth."""
    assert account_address_is_alias(ADDRESS, "mainnet", _app({})) is False


# --- accounts that appear after boot --------------------------------------


def test_a_newly_seen_account_lands_in_both_maps():
    app = _app({})
    info = SimpleNamespace(address=ADDRESS, index=99)

    add_account_info_to_cache(info, app, "mainnet")

    assert app.addresses_to_indexes["mainnet"][ADDRESS[:29]] == 99
    assert app.canonical_addresses["mainnet"][ADDRESS[:29]] == ADDRESS


def test_caching_an_account_raises_the_known_ceiling():
    app = _app({})

    add_account_info_to_cache(SimpleNamespace(address=ADDRESS, index=99), app, "mainnet")

    assert app.max_index_known["mainnet"] == 99


def test_a_cached_account_is_then_known_not_to_be_an_alias():
    """The cache has to be good enough for the alias check too, or an
    account created after boot renders as an alias of itself."""
    app = _app({})

    add_account_info_to_cache(SimpleNamespace(address=ADDRESS, index=99), app, "mainnet")

    assert account_address_is_alias(ADDRESS, "mainnet", app) is False


# --- the boot path --------------------------------------------------------
#
# The maps are a cache in front of /accounts/get-indexes. An empty one costs
# a round trip per unknown address; a boot that refuses to start costs the
# site. So every failure here has to leave the maps empty and carry on --
# which is what the pickle version's try/except did, and what replacing it
# with a network call must not quietly give up.


async def _boot(monkeypatch, responder):
    from types import SimpleNamespace

    from ccdexplorer.ccdexplorer_site.app import factory

    monkeypatch.setattr(factory, "get_url_from_api", responder)
    app = SimpleNamespace(api_url="http://api", httpx_client=None)
    await factory.read_addresses_if_available(app)
    return app


async def test_boot_builds_a_map_per_net(monkeypatch):
    async def ok(url, client):
        return SimpleNamespace(ok=True, return_value=[[ADDRESS, 5], [OTHER, 9]])

    app = await _boot(monkeypatch, ok)

    for net in ("mainnet", "testnet", "devnet"):
        assert app.addresses_to_indexes[net][ADDRESS[:29]] == 5, net
        assert app.canonical_addresses[net][OTHER[:29]] == OTHER, net
        assert app.max_index_known[net] == 9, net


async def test_an_api_that_answers_badly_leaves_the_maps_empty(monkeypatch):
    async def not_ok(url, client):
        return SimpleNamespace(ok=False, return_value=None)

    app = await _boot(monkeypatch, not_ok)

    assert app.addresses_to_indexes["mainnet"] == {}
    assert app.canonical_addresses["mainnet"] == {}
    assert app.max_index_known["mainnet"] == 0


async def test_an_api_that_raises_does_not_stop_the_boot(monkeypatch):
    async def explode(url, client):
        raise RuntimeError("api is down")

    app = await _boot(monkeypatch, explode)

    assert app.addresses_to_indexes["mainnet"] == {}
    assert app.max_index_known["mainnet"] == 0


async def test_one_net_failing_does_not_cost_the_others(monkeypatch):
    async def mainnet_only(url, client):
        if "testnet" in url:
            raise RuntimeError("testnet is down")
        return SimpleNamespace(ok=True, return_value=[[ADDRESS, 5]])

    app = await _boot(monkeypatch, mainnet_only)

    assert app.addresses_to_indexes["mainnet"][ADDRESS[:29]] == 5
    assert app.addresses_to_indexes["testnet"] == {}
    assert app.addresses_to_indexes["devnet"][ADDRESS[:29]] == 5


async def test_a_net_with_no_accounts_does_not_take_the_boot_down(monkeypatch):
    """max() of an empty map raises, and the old code called it unguarded."""

    async def empty(url, client):
        return SimpleNamespace(ok=True, return_value=[])

    app = await _boot(monkeypatch, empty)

    assert app.max_index_known["devnet"] == 0


async def test_the_boot_asks_the_bulk_endpoint(monkeypatch):
    """Not get-indexes, which answers for a known list -- the wrong shape
    when the list is every account there is."""
    asked = []

    async def record(url, client):
        asked.append(url)
        return SimpleNamespace(ok=True, return_value=[])

    await _boot(monkeypatch, record)

    assert asked == [
        f"http://api/v2/{net}/accounts/address-index-map"
        for net in ("mainnet", "testnet", "devnet")
    ]
