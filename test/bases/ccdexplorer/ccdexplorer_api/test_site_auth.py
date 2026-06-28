"""Tests for the security fixes on the ``/site-auth`` router.

These cover finding #3 from the security review:

* The endpoints now require a *valid* API key (``require_api_auth``), not just a
  present header. Bogus keys are rejected; the site's registered key passes.
* Login / forgot-password / register are throttled per email to stop brute force
  and verification/reset email bombing.

They follow the unit style of ``test_mcp_auth_headers.py`` (call the functions
directly with in-memory fakes) so they need no live Mongo/Redis.
"""

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from ccdexplorer.ccdexplorer_api.app.ratelimiting import require_api_auth
from ccdexplorer.ccdexplorer_api.app.routers.site_auth import site_auth
from ccdexplorer.ccdexplorer_api.app.security import hash_password
from ccdexplorer.mongodb import CollectionsUtilities


# --------------------------------------------------------------------------- #
# In-memory fakes
# --------------------------------------------------------------------------- #
class FakeRedis:
    """Minimal async stand-in for the bits of redis the throttler uses."""

    def __init__(self):
        self.store: dict[str, int] = {}

    async def get(self, key):
        value = self.store.get(key)
        return None if value is None else str(value).encode()  # real client returns bytes

    async def incr(self, key):
        self.store[key] = int(self.store.get(key, 0)) + 1
        return self.store[key]

    async def expire(self, key, ttl):
        return True

    async def delete(self, key):
        self.store.pop(key, None)
        return 1


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    async def find_one(self, query, projection=None):
        for doc in self.docs:
            if all(doc.get(k) == v for k, v in query.items()):
                return dict(doc)
        return None

    async def bulk_write(self, operations):  # save_user() writes through this
        return None


class FakeMongo:
    def __init__(self, users):
        self.utilities = {CollectionsUtilities.users_v2_prod: users}


class FakeTooter:
    def __init__(self):
        self.emails = []

    def email_api(self, **kwargs):
        self.emails.append(kwargs)


def make_request(redis=None, tooter=None):
    app = SimpleNamespace(r=redis or FakeRedis(), tooter=tooter or FakeTooter())
    return SimpleNamespace(app=app)


# --------------------------------------------------------------------------- #
# Authentication: a valid API key is required (finding #3, part A)
# --------------------------------------------------------------------------- #
def test_site_auth_router_enforces_valid_api_key():
    """The router carries the validating dependency, not just presence checks."""
    dep_names = [d.dependency.__name__ for d in site_auth.router.dependencies]
    assert "require_api_auth" in dep_names


def _scope_for(api_key: str, keys: dict):
    async def fake_get_api_keys(*args, **kwargs):
        return keys

    app = SimpleNamespace(
        state=SimpleNamespace(get_api_keys_fn=fake_get_api_keys),
        motormongo=None,
    )
    return {"app": app, "headers": [(b"x-ccdexplorer-key", api_key.encode())]}


@pytest.mark.asyncio
async def test_require_api_auth_rejects_bogus_key():
    request = SimpleNamespace(scope=_scope_for("bogus-key", {}))
    with pytest.raises(HTTPException) as exc:
        await require_api_auth(request)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_require_api_auth_accepts_registered_key():
    keys = {"site-key": {"api_account_id": "acct", "api_group": "grp"}}
    request = SimpleNamespace(scope=_scope_for("site-key", keys))
    assert await require_api_auth(request) == ("acct", "grp")


# --------------------------------------------------------------------------- #
# Throttling (finding #3, part B)
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_login_throttles_after_max_attempts():
    """After LOGIN_MAX_ATTEMPTS failures the next attempt is 429, not 401."""
    email = "victim@example.com"
    # User has no password set, so each attempt fails fast (no bcrypt) but still
    # increments the throttle counter.
    users = FakeCollection([{"token": "tok", "email_address": email}])
    mongo = FakeMongo(users)
    request = make_request()
    body = site_auth.LoginRequest(email=email, password="guess")

    for _ in range(site_auth.LOGIN_MAX_ATTEMPTS):
        with pytest.raises(HTTPException) as exc:
            await site_auth.login(request=request, body=body, mongomotor=mongo)
        assert exc.value.status_code == 401

    with pytest.raises(HTTPException) as exc:
        await site_auth.login(request=request, body=body, mongomotor=mongo)
    assert exc.value.status_code == 429


@pytest.mark.asyncio
async def test_login_valid_key_reaches_credentials_check():
    """A request that passed auth but has no matching user gets the app-level
    401 ('Invalid email or password'), proving the valid-key path runs."""
    mongo = FakeMongo(FakeCollection([]))  # no users
    request = make_request()
    body = site_auth.LoginRequest(email="nobody@example.com", password="x")

    with pytest.raises(HTTPException) as exc:
        await site_auth.login(request=request, body=body, mongomotor=mongo)
    assert exc.value.status_code == 401
    assert exc.value.detail == "Invalid email or password."


@pytest.mark.asyncio
async def test_login_success_returns_token_and_clears_throttle():
    email = "user@example.com"
    users = FakeCollection(
        [
            {
                "token": "the-token",
                "email_address": email,
                "password": hash_password("correct-horse"),
                "email_verified": True,
            }
        ]
    )
    redis = FakeRedis()
    # Pre-load some prior failures; a successful login must wipe them.
    redis.store[site_auth._throttle_key("login", email)] = 3
    request = make_request(redis=redis)

    bad = site_auth.LoginRequest(email=email, password="wrong")
    with pytest.raises(HTTPException):
        await site_auth.login(request=request, body=bad, mongomotor=FakeMongo(users))

    good = site_auth.LoginRequest(email=email, password="correct-horse")
    result = await site_auth.login(request=request, body=good, mongomotor=FakeMongo(users))
    assert result == {"token": "the-token"}
    assert site_auth._throttle_key("login", email) not in redis.store


@pytest.mark.asyncio
async def test_forgot_password_stops_emailing_when_throttled_but_stays_silent():
    email = "reset@example.com"
    users = FakeCollection([{"token": "tok", "email_address": email}])
    tooter = FakeTooter()
    request = make_request(tooter=tooter)
    body = site_auth.ForgotPasswordRequest(email=email)

    # Calls up to the limit each send one email...
    for _ in range(site_auth.EMAIL_MAX_PER_WINDOW):
        assert await site_auth.forgot_password(
            request=request, body=body, mongomotor=FakeMongo(users)
        ) == {"ok": True}

    # ...the next is throttled: still 'ok' (no enumeration), but no extra email.
    assert await site_auth.forgot_password(
        request=request, body=body, mongomotor=FakeMongo(users)
    ) == {"ok": True}
    assert len(tooter.emails) == site_auth.EMAIL_MAX_PER_WINDOW


@pytest.mark.asyncio
async def test_register_throttled_returns_429():
    email = "spam-target@example.com"
    redis = FakeRedis()
    redis.store[site_auth._throttle_key("register", email)] = site_auth.EMAIL_MAX_PER_WINDOW
    request = make_request(redis=redis)
    body = site_auth.RegisterRequest(email=email, password="whatever")

    with pytest.raises(HTTPException) as exc:
        await site_auth.register(request=request, body=body, mongomotor=FakeMongo(FakeCollection([])))
    assert exc.value.status_code == 429
