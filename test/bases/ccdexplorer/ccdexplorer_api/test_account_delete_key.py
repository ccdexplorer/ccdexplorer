"""Deleting an API key is for whoever the key belongs to.

DELETE /account/key/{key} took the key from the path and deleted it. It never
looked at who was asking: no login, and no check that the key was the
caller's. A key is a string its owner pastes into scripts, configs and
support requests, so "knows the key" is a much larger set than "owns the
key" -- and it included every key the project runs on.
"""

import datetime as dt
from types import SimpleNamespace

import pytest

from ccdexplorer.ccdexplorer_api.app.models import User
from ccdexplorer.ccdexplorer_api.app.routers.account import account


class _Keys:
    """api_api_keys, far enough to apply a DeleteOne the way Mongo would:
    every field in the filter has to match."""

    def __init__(self, docs: list[dict]):
        self.docs = {doc["_id"]: doc for doc in docs}
        self.calls = 0

    async def bulk_write(self, operations):
        self.calls += 1
        for operation in operations:
            wanted = operation._filter
            for key_id, doc in list(self.docs.items()):
                if all(doc.get(field) == value for field, value in wanted.items()):
                    del self.docs[key_id]
                    break


def _user(api_account_id: str) -> User:
    return User(
        scope="http://testserver",
        token=f"token-of-{api_account_id}",
        alias_id=1,
        alias_account_id="a",
        api_account_id=api_account_id,
        email=f"{api_account_id}@example.com",
        password="hashed",
        plan="free",
        plan_end_date=dt.datetime.now(dt.UTC) + dt.timedelta(days=30),
    )


@pytest.fixture
def keys():
    return _Keys(
        [
            {"_id": "mine", "api_account_id": "me", "api_group": "free"},
            {"_id": "mine-too", "api_account_id": "me", "api_group": "free"},
            {"_id": "theirs", "api_account_id": "them", "api_group": "pro"},
            {"_id": "the-sites", "api_account_id": "site", "api_group": "ccdexplorer.io"},
        ]
    )


@pytest.fixture
def delete_as(monkeypatch, keys):
    """Call the route as a given account, or as nobody."""
    refreshed = []

    async def fake_get_api_keys(*args, **kwargs):
        refreshed.append(kwargs.get("for_"))
        return {}

    monkeypatch.setattr(account, "get_api_keys", fake_get_api_keys)

    async def call(api_account_id: str | None, key: str):
        user = _user(api_account_id) if api_account_id else None
        monkeypatch.setattr(account, "get_user_details", lambda request: user)
        app = SimpleNamespace(
            motormongo=SimpleNamespace(utilities_db={"api_api_keys": keys}), api_keys={}
        )
        return await account.account_delete_key(SimpleNamespace(app=app), key, mongomotor=None)

    call.refreshed = refreshed
    return call


async def test_nobody_logged_in_deletes_nothing(delete_as, keys):
    await delete_as(None, "theirs")
    assert sorted(keys.docs) == ["mine", "mine-too", "the-sites", "theirs"]


async def test_nobody_logged_in_does_not_reach_the_collection(delete_as, keys):
    """Not a delete that happened to match nothing: no write at all."""
    await delete_as(None, "theirs")
    assert keys.calls == 0


async def test_nobody_logged_in_is_sent_to_the_login_page(delete_as):
    response = await delete_as(None, "theirs")
    assert response.status_code == 303
    assert response.headers["location"] == "/auth/login"


async def test_an_account_deletes_its_own_key(delete_as, keys):
    await delete_as("me", "mine")
    assert sorted(keys.docs) == ["mine-too", "the-sites", "theirs"]


async def test_deleting_one_key_leaves_the_accounts_others(delete_as, keys):
    """The owner is part of the match, not the whole of it."""
    await delete_as("me", "mine")
    assert "mine-too" in keys.docs


@pytest.mark.parametrize("key", ["theirs", "the-sites"])
async def test_an_account_cannot_delete_a_key_that_is_not_its_own(delete_as, keys, key):
    await delete_as("me", key)
    assert sorted(keys.docs) == ["mine", "mine-too", "the-sites", "theirs"]


async def test_a_key_that_is_not_yours_looks_like_a_key_that_is_not_there(delete_as):
    """The same answer for both, or the route says which strings are keys."""
    not_mine = await delete_as("me", "theirs")
    not_there = await delete_as("me", "no-such-key")
    assert not_mine.status_code == not_there.status_code
    assert dict(not_mine.headers) == dict(not_there.headers)
    assert not_mine.body == not_there.body


async def test_the_owner_still_lands_back_on_the_account_page(delete_as):
    response = await delete_as("me", "mine")
    assert response.headers["HX-Redirect"] == "/account"


async def test_the_key_cache_is_reread_after_a_delete(delete_as):
    """A deleted key has to stop working, and the keys in use are a copy."""
    await delete_as("me", "mine")
    assert delete_as.refreshed == ["account_delete_key"]
