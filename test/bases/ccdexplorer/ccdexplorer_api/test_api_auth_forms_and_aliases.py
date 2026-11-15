"""The api's account forms, and the alias an account is given.

Two things were wrong at the edges of /auth:

* set-password and reset-password read the form body by hand. The password
  was never URL-decoded and an `&` in it split the body, so a password with
  an `@`, `%`, `+` or `&` was stored as its encoded text -- and then did not
  match when typed at the login form, which does decode.
* an alias was the highest in use plus one, read and used in two steps. Two
  registrations at once got the same alias, and with it each other's
  payments; and once the aliases ran out the next registration was an
  IndexError.
"""

import datetime as dt
from types import SimpleNamespace

import httpx2 as httpx
import pytest
from fastapi import HTTPException

from conftest import build_test_app
from ccdexplorer.ccdexplorer_api.app.models import User
from ccdexplorer.ccdexplorer_api.app.routers.auth import auth
from ccdexplorer.ccdexplorer_api.app.security import hash_password, verify_password
from ccdexplorer.ccdexplorer_api.app.state_getters import get_mongo_motor
from ccdexplorer.env import API_NET

NOW = dt.datetime.now().astimezone(dt.UTC)


class _Aliases:
    """api_aliases with the atomic claim a real collection offers."""

    def __init__(self, count=5):
        self.docs = [{"net": API_NET, "alias_id": i, "alias": f"alias-{i}"} for i in range(count)]

    async def find_one_and_update(self, query, update):
        for doc in self.docs:
            if all(
                (k not in doc) if v == {"$exists": False} else doc.get(k) == v
                for k, v in query.items()
            ):
                doc.update(update["$set"])
                return dict(doc)
        return None

    async def find_one(self, query):
        for doc in self.docs:
            if all(doc.get(k) == v for k, v in query.items()):
                return dict(doc)
        return None


class _Users:
    def __init__(self, docs=()):
        self.docs = list(docs)
        self.writes = []

    def find(self, query):
        hits = [d for d in self.docs if all(d.get(k) == v for k, v in query.items())]

        class R:
            async def to_list(self, length=None):
                return hits

            def __await__(self):
                async def me():
                    return self

                return me().__await__()

        return R()

    def aggregate(self, pipeline):
        top = max(self.docs, key=lambda d: d["alias_id"], default=None)

        class R:
            async def to_list(self, length=None):
                return [top] if top else []

            def __await__(self):
                async def me():
                    return self

                return me().__await__()

        return R()

    async def bulk_write(self, ops):
        self.writes.append(ops)


class _Db:
    """Stands in for MongoMotor, and for the factory that returns one."""

    def __init__(self, aliases=None, users=None):
        self.utilities_db = {
            "api_aliases": aliases or _Aliases(),
            "api_users": users or _Users(),
        }

    def __call__(self):
        return self


def _db(aliases=None, users=None):
    return _Db(aliases, users)


# --- aliases --------------------------------------------------------------


async def test_two_claims_never_get_the_same_alias():
    db = _db()
    first = await auth.claim_alias(db, "acct-1")
    second = await auth.claim_alias(db, "acct-2")
    assert first[0] != second[0]
    assert first[1] != second[1]


async def test_a_claim_on_an_alias_someone_just_took_moves_on():
    """Both registrants read the same highest alias; only one can claim it."""
    aliases = _Aliases()
    aliases.docs[0]["claimed_by"] = "someone-else"
    db = _db(aliases)
    alias_id, alias = await auth.claim_alias(db, "acct")
    assert alias_id == 1


async def test_a_claimed_alias_remembers_who_has_it():
    aliases = _Aliases()
    db = _db(aliases)
    alias_id, _ = await auth.claim_alias(db, "acct-7")
    assert aliases.docs[alias_id]["claimed_by"] == "acct-7"


async def test_running_out_of_aliases_is_a_clear_refusal_not_an_index_error():
    db = _db(_Aliases(count=0))
    with pytest.raises(HTTPException) as exc:
        await auth.claim_alias(db, "acct")
    assert exc.value.status_code == 503


async def test_the_search_starts_after_the_highest_alias_in_use():
    users = _Users(
        [{"alias_id": 3, "api_account_id": "x"}],
    )
    db = _db(_Aliases(), users)
    alias_id, _ = await auth.claim_alias(db, "acct")
    assert alias_id == 4


# --- forms ----------------------------------------------------------------


def _user(**extra):
    return User(
        scope="http://x",
        token="old-token",
        alias_id=0,
        alias_account_id="3alias",
        api_account_id="acct",
        email="a+b@example.org",
        password=hash_password("old-password"),
        plan_end_date=NOW,
        **extra,
    )


@pytest.fixture
def client(monkeypatch):
    app = build_test_app()
    sent = []
    app.tooter = SimpleNamespace(email_api=lambda **k: sent.append(k))
    user = _user(
        reset_password_token="rt", reset_password_token_expires=NOW + dt.timedelta(minutes=30)
    )
    users = _Users([user.model_dump(exclude_none=True)])

    async def db():
        return _Db(users=users)

    app.dependency_overrides[get_mongo_motor] = db
    http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver", follow_redirects=False
    )
    return SimpleNamespace(http=http, users=users, sent=sent)


async def test_a_password_with_form_characters_is_stored_as_typed(client):
    typed = "p&ss%40w+rd=1"
    async with client.http as c:
        r = await c.post(
            "/auth/set-password", data={"username": typed, "reset_password_token": "rt"}
        )
    assert r.status_code == 303
    stored = client.users.writes[0][0]._doc["password"]
    assert verify_password(typed, stored)


async def test_the_fields_may_arrive_in_either_order(client):
    async with client.http as c:
        r = await c.post(
            "/auth/set-password",
            content="reset_password_token=rt&username=another-one",
            headers={"content-type": "application/x-www-form-urlencoded"},
        )
    assert r.status_code == 303
    assert verify_password("another-one", client.users.writes[0][0]._doc["password"])


async def test_a_plus_in_the_address_still_finds_the_account(client):
    async with client.http as c:
        await c.post("/auth/reset-password", data={"username": "a+b@example.org"})
    assert client.sent and client.sent[0]["email_address"] == "a+b@example.org"


async def test_a_missing_field_is_not_a_server_error(client):
    async with client.http as c:
        r1 = await c.post("/auth/set-password", data={})
        r2 = await c.post("/auth/reset-password", data={})
    assert r1.status_code < 500 and r2.status_code < 500
