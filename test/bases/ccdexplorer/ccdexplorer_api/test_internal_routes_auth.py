"""The site's own routes are for the site's own key.

/site-auth/* and /v2/site_user/* are how the site logs people in and reads
and writes their settings. They sit on the public api host, and the check in
front of them asked only whether the key was a registered one -- which every
customer's key is, the free ones included. The key's group came back from
that check and nothing looked at it.

So these ask the question per group, and ask it of the routes as they are
mounted rather than of the dependency alone: a router that forgets to carry
the dependency passes every test of the dependency.
"""

import datetime as dt
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from ccdexplorer.ccdexplorer_api.app import ratelimiting
from ccdexplorer.ccdexplorer_api.app.models import APIPlans, User
from ccdexplorer.ccdexplorer_api.app.routers.account import account
from ccdexplorer.ccdexplorer_api.app.routers.site_auth import site_auth
from ccdexplorer.ccdexplorer_api.app.routers.v2 import site_user_v2
from ccdexplorer.ccdexplorer_api.app.state_getters import get_mongo_motor

INTERNAL = "ccdexplorer.io"
#: Every group a customer's key can be in, plus ones that only look right.
CUSTOMER_GROUPS = [plan.value.lower() for plan in APIPlans]
NOT_INTERNAL = [
    *CUSTOMER_GROUPS,
    "something-else",
    "CCDExplorer.io",
    "ccdexplorer.io ",
    " ccdexplorer.io",
    "ccdexplorer.io.evil.example",
    "",
    None,
]

KEYS = {
    "site-key": {"api_account_id": "site", "api_group": INTERNAL},
    **{
        f"{group}-key": {"api_account_id": f"acct-{group}", "api_group": group}
        for group in CUSTOMER_GROUPS
    },
}


def _request(api_key: str | None, keys: dict):
    async def fake_get_api_keys(*args, **kwargs):
        return keys

    app = SimpleNamespace(state=SimpleNamespace(get_api_keys_fn=fake_get_api_keys), motormongo=None)
    headers = [(b"x-ccdexplorer-key", api_key.encode())] if api_key else []
    return SimpleNamespace(scope={"app": app, "headers": headers})


# --- the dependency -------------------------------------------------------


def test_the_customer_groups_are_what_this_file_thinks_they_are():
    """If a plan is added, it is refused here without anyone remembering to."""
    assert set(CUSTOMER_GROUPS) >= {"free", "standard", "pro", "unrestricted"}
    assert INTERNAL not in CUSTOMER_GROUPS


async def test_the_sites_own_key_is_let_through():
    request = _request("site-key", KEYS)
    assert await ratelimiting.require_internal_api_auth(request) == ("site", INTERNAL)


@pytest.mark.parametrize("group", NOT_INTERNAL)
async def test_a_valid_key_from_any_other_group_is_refused(group):
    keys = {"their-key": {"api_account_id": "them", "api_group": group}}
    with pytest.raises(HTTPException) as exc:
        await ratelimiting.require_internal_api_auth(_request("their-key", keys))
    assert exc.value.status_code == 403


@pytest.mark.parametrize("api_key", [None, "no-such-key"])
async def test_no_key_is_still_unauthorised_rather_than_forbidden(api_key):
    """401 says "who are you", 403 says "not you". A caller with no key at all
    gets the first, as before."""
    with pytest.raises(HTTPException) as exc:
        await ratelimiting.require_internal_api_auth(_request(api_key, KEYS))
    assert exc.value.status_code == 401


async def test_the_refusal_does_not_name_the_group_it_wanted():
    """The group name is the whole of this check; a 403 that spells it out
    tells a caller exactly what to go looking for."""
    with pytest.raises(HTTPException) as exc:
        await ratelimiting.require_internal_api_auth(_request("free-key", KEYS))
    assert INTERNAL not in str(exc.value.detail)


# --- the routes, as mounted ----------------------------------------------


def _app() -> FastAPI:
    """Both routers on a bare app: the real routes and their real
    dependencies, with nothing behind them. No Mongo, no Redis -- so a
    request that gets past the key check fails loudly instead of quietly
    doing something."""

    async def fake_get_api_keys(*args, **kwargs):
        return KEYS

    app = FastAPI()
    app.state.get_api_keys_fn = fake_get_api_keys
    app.motormongo = None
    app.include_router(site_auth.router)
    app.include_router(site_user_v2.router)
    return app


def _every_route():
    """(method, path) for each route on both routers, path params filled in."""
    found = []
    for router in (site_auth.router, site_user_v2.router):
        for route in router.routes:
            assert isinstance(route, APIRoute)
            path = route.path
            for name in route.param_convertors:
                path = path.replace("{" + name + "}", "x")
            for method in sorted(route.methods - {"HEAD", "OPTIONS"}):
                found.append((method, path))
    return found


ROUTES = _every_route()


def test_there_are_routes_to_ask_about():
    """A loop over nothing passes. Both routers are in it."""
    paths = [path for _, path in ROUTES]
    assert len(ROUTES) >= 20
    assert any(path.startswith("/site-auth/") for path in paths)
    assert any(path.startswith("/v2/site_user/") for path in paths)


@pytest.mark.parametrize("method,path", ROUTES)
@pytest.mark.parametrize("group", CUSTOMER_GROUPS)
def test_a_customers_key_opens_none_of_them(method, path, group):
    """Sent with no body on purpose: the refusal has to come before the
    request is even read, let alone acted on."""
    client = TestClient(_app(), raise_server_exceptions=False)
    response = client.request(method, path, headers={"x-ccdexplorer-key": f"{group}-key"})
    assert response.status_code == 403, f"{group} key reached {method} {path}"


@pytest.mark.parametrize("method,path", ROUTES)
def test_no_key_opens_none_of_them(method, path):
    client = TestClient(_app(), raise_server_exceptions=False)
    assert client.request(method, path).status_code in (401, 403)
    response = client.request(method, path, headers={"x-ccdexplorer-key": "no-such-key"})
    assert response.status_code == 401


def test_the_sites_own_key_still_gets_an_answer():
    """Everything above would pass if the routes refused everyone."""
    app = _app()
    app.dependency_overrides[get_mongo_motor] = lambda: SimpleNamespace(utilities={})
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/v2/site_user/explanations", headers={"x-ccdexplorer-key": "site-key"})
    assert response.status_code == 200
    assert response.json() == {}


@pytest.mark.parametrize("method,path", ROUTES)
def test_the_sites_own_key_is_never_refused_at_the_door(method, path):
    """Whatever happens behind the check -- a missing body, no database --
    it is not a 401 or a 403."""
    client = TestClient(_app(), raise_server_exceptions=False)
    response = client.request(method, path, headers={"x-ccdexplorer-key": "site-key"})
    assert response.status_code not in (401, 403), f"{method} {path}"


# --- the group cannot be had through the account page ---------------------
#
# The check above trusts one string on the key, and a key takes that string
# from its owner's plan. The plan is whatever the owner last posted to
# /plans/set. Today such a key is born expired, which is an accident of how
# the end date is worked out and not a rule anyone wrote down.


class _Keys:
    def __init__(self):
        self.written = []

    async def bulk_write(self, operations):
        self.written.extend(operations)


def _account_request(keys: _Keys):
    app = SimpleNamespace(
        motormongo=SimpleNamespace(utilities_db={"api_api_keys": keys}),
        api_keys={},
    )
    return SimpleNamespace(app=app)


def _user(plan: str) -> User:
    return User(
        scope="http://testserver",
        token="t",
        alias_id=1,
        alias_account_id="a",
        api_account_id="acct",
        email="someone@example.com",
        password="hashed",
        plan=plan,
        plan_end_date=dt.datetime.now(dt.UTC) + dt.timedelta(days=30),
    )


@pytest.fixture
def minting(monkeypatch):
    async def fake_get_api_keys(*args, **kwargs):
        return {}

    monkeypatch.setattr(account, "get_api_keys", fake_get_api_keys)

    def as_user(plan: str):
        monkeypatch.setattr(account, "get_user_details", lambda request: _user(plan))
        keys = _Keys()
        return keys, _account_request(keys)

    return as_user


async def test_an_account_on_the_internal_group_is_given_no_key(minting):
    keys, request = minting(INTERNAL)
    with pytest.raises(HTTPException) as exc:
        await account.account_new_key(request, mongomotor=None)
    assert exc.value.status_code == 403
    assert keys.written == []


@pytest.mark.parametrize("plan", ["free", "standard", "pro"])
async def test_an_ordinary_account_still_gets_its_key(minting, plan):
    keys, request = minting(plan)
    await account.account_new_key(request, mongomotor=None)
    assert len(keys.written) == 1
    assert keys.written[0]._doc["api_group"] == plan
