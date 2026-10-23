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
    account_link,
    add_account_info_to_cache,
    address_maps_from_rows,
    from_address_to_index,
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


# --- a miss had nowhere to go --------------------------------------------
#
# The maps are a boot-time snapshot, so an account created since boot is not
# in them. from_address_to_index answered that by returning the address it
# was handed, and account_link renders a str as four characters of address --
# so mainnet/today-in, the one page guaranteed to list the newest accounts on
# the chain, showed addresses where indexes belong.
#
# The docstring on read_addresses_if_available had always called the maps "a
# cache in front of /accounts/get-indexes". That route existed and nothing
# ever called it; the cache had no miss path at all.


def _resolvable_app(known: dict | None = None):
    return SimpleNamespace(
        api_url="http://api",
        addresses_to_indexes={"mainnet": dict(known or {})},
        canonical_addresses={"mainnet": {}},
        max_index_known={"mainnet": 0},
    )


def test_an_unknown_address_comes_back_as_itself():
    """The behaviour the fix exists to stop being the end of the line."""
    app = _resolvable_app()

    assert from_address_to_index(ADDRESS, "mainnet", app) == ADDRESS


def test_an_unknown_address_renders_as_an_address_not_an_index():
    """What production showed: a person icon and four characters."""
    app = _resolvable_app()

    rendered = account_link(ADDRESS, "mainnet", None, None, app)

    assert ADDRESS[:4] in rendered
    assert f'href="/mainnet/account/{ADDRESS}"' in rendered


async def _resolve(monkeypatch, app, values, responder):
    from ccdexplorer.ccdexplorer_site.app import utils

    monkeypatch.setattr(utils, "post_url_from_api", responder)
    await utils.resolve_missing_indexes(values, "mainnet", app, None)


def _answers(mapping, calls=None):
    async def responder(url, client, content, headers=None):
        if calls is not None:
            calls.append((url, content))
        return SimpleNamespace(ok=True, return_value=mapping)

    return responder


async def test_an_unknown_address_is_resolved_and_remembered(monkeypatch):
    app = _resolvable_app()

    await _resolve(monkeypatch, app, [ADDRESS], _answers({ADDRESS[:29]: 104840}))

    assert app.addresses_to_indexes["mainnet"][ADDRESS[:29]] == 104840
    assert from_address_to_index(ADDRESS, "mainnet", app) == 104840


async def test_the_index_is_then_what_renders(monkeypatch):
    """The whole point, asserted on the markup the reader sees."""
    app = _resolvable_app()

    await _resolve(monkeypatch, app, [ADDRESS], _answers({ADDRESS[:29]: 104840}))
    rendered = account_link(ADDRESS, "mainnet", None, None, app)

    assert "104840" in rendered
    assert ADDRESS[:4] not in rendered


async def test_it_asks_the_bulk_route_with_canonical_ids(monkeypatch):
    """get-indexes matches on _id, which is the 29-character prefix."""
    calls = []
    app = _resolvable_app()

    await _resolve(monkeypatch, app, [ADDRESS, OTHER], _answers({}, calls))

    url, content = calls[0]
    assert url.endswith("/v2/mainnet/accounts/get-indexes")
    assert sorted(content) == sorted([ADDRESS[:29], OTHER[:29]])


async def test_one_request_covers_a_whole_page(monkeypatch):
    """today-in lists ~85 addresses. One round trip, not 85."""
    calls = []
    app = _resolvable_app()

    await _resolve(monkeypatch, app, [ADDRESS, OTHER], _answers({}, calls))

    assert len(calls) == 1


async def test_nothing_is_asked_when_everything_is_known(monkeypatch):
    """The maps are still the cache; a hit must not cost a request."""
    calls = []
    app = _resolvable_app({ADDRESS[:29]: 5})

    await _resolve(monkeypatch, app, [ADDRESS], _answers({}, calls))

    assert calls == []


async def test_contract_addresses_are_not_asked_after(monkeypatch):
    """today-in's impacted list carries `<10100,0>` alongside accounts, and
    account_link routes those to instance_link_from_str on its own."""
    calls = []
    app = _resolvable_app()

    await _resolve(monkeypatch, app, ["<10100,0>", "<10082,0>"], _answers({}, calls))

    assert calls == []


async def test_public_keys_are_not_asked_after(monkeypatch):
    """64 characters is a cis5 public key, not an account."""
    calls = []
    app = _resolvable_app()

    await _resolve(monkeypatch, app, ["a" * 64], _answers({}, calls))

    assert calls == []


async def test_resolving_raises_the_known_ceiling(monkeypatch):
    """max_index_known feeds the search box, and a new account raises it."""
    app = _resolvable_app()

    await _resolve(monkeypatch, app, [ADDRESS], _answers({ADDRESS[:29]: 104840}))

    assert app.max_index_known["mainnet"] == 104840


async def test_a_404_leaves_the_map_alone(monkeypatch):
    """get-indexes answers 404 when it matched nothing, which is its normal
    answer for an id that is not an account."""
    app = _resolvable_app()

    async def not_found(url, client, content, headers=None):
        return SimpleNamespace(ok=False, return_value=None)

    await _resolve(monkeypatch, app, [ADDRESS], not_found)

    assert app.addresses_to_indexes["mainnet"] == {}


async def test_a_nonsense_payload_does_not_poison_the_map(monkeypatch):
    app = _resolvable_app()

    await _resolve(monkeypatch, app, [ADDRESS], _answers(["not", "a", "mapping"]))

    assert app.addresses_to_indexes["mainnet"] == {}


async def test_a_non_integer_index_is_refused(monkeypatch):
    """from_address_to_index promises an int to callers that branch on type."""
    app = _resolvable_app()

    await _resolve(monkeypatch, app, [ADDRESS], _answers({ADDRESS[:29]: None}))

    assert app.addresses_to_indexes["mainnet"] == {}


async def test_no_api_url_is_survivable(monkeypatch):
    calls = []
    app = _resolvable_app()
    app.api_url = None

    await _resolve(monkeypatch, app, [ADDRESS], _answers({}, calls))

    assert calls == []


async def test_an_empty_list_asks_nothing(monkeypatch):
    calls = []
    app = _resolvable_app()

    await _resolve(monkeypatch, app, [], _answers({}, calls))
    await _resolve(monkeypatch, app, None, _answers({}, calls))

    assert calls == []


# --- the top-up that should have prevented this -------------------------
#
# The map is kept current by one loop in the 60s refresher, over the newest
# 50 accounts. It used to sit at the bottom of that block: below an
# identity-providers call whose `continue` on a None answer skipped it
# outright, and below every line that could raise into the net-wide except.
# Three independent ways for the map to silently stop being topped up.


def test_the_top_up_runs_before_the_identity_providers_gate():
    import inspect

    from ccdexplorer.ccdexplorer_site.app import factory

    source = inspect.getsource(factory.create_app)
    top_up = source.index("add_account_info_to_cache(account_info, app, net)")
    gate = source.index("if api_result.return_value is None:")

    assert top_up < gate, "the top-up is still behind the identity-providers continue"


def test_one_unparseable_account_does_not_abort_the_top_up():
    import inspect

    from ccdexplorer.ccdexplorer_site.app import factory

    source = inspect.getsource(factory.create_app)
    block = source[source.index("skipped an account in the map top-up") - 400 :]

    assert "continue" in block
