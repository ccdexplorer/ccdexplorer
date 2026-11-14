"""The api's side of one-time login and linking codes.

The site has no database, so it asks here: redeem this code, or mint one for
the account that is logged in. What the account token used to do on its own
-- log a browser in by being pasted into a url or a cookie -- is what these
pin shut.
"""

import datetime as dt
from types import SimpleNamespace

import mongomock
import pytest
from fastapi import HTTPException

from ccdexplorer.ccdexplorer_api.app.routers.site_auth import site_auth
from ccdexplorer.mongodb import CollectionsUtilities
from ccdexplorer.site_user.login_codes import (
    LOGIN_CODE_TTL,
    LoginCodePurpose,
    code_document,
    new_code,
    redeem_filter,
)

ACCOUNT_TOKEN = "3f0c1d6e-0000-4000-8000-000000000001"


class Async:
    """A mongomock collection behind the awaitable calls the router makes."""

    def __init__(self, collection):
        self.sync = collection

    async def find_one(self, *args, **kwargs):
        return self.sync.find_one(*args, **kwargs)

    async def find_one_and_delete(self, *args, **kwargs):
        return self.sync.find_one_and_delete(*args, **kwargs)

    async def insert_one(self, *args, **kwargs):
        return self.sync.insert_one(*args, **kwargs)

    async def delete_many(self, *args, **kwargs):
        return self.sync.delete_many(*args, **kwargs)


@pytest.fixture
def mongo():
    db = mongomock.MongoClient().concordium_utilities
    users = Async(db.users)
    users.sync.insert_one({"_id": "u1", "token": ACCOUNT_TOKEN, "email_address": "a@example.com"})
    return SimpleNamespace(
        utilities={
            CollectionsUtilities.users_v2_prod: users,
            CollectionsUtilities.users_v2_dev: users,
            CollectionsUtilities.user_sessions: Async(db.user_sessions),
            CollectionsUtilities.login_audit_log: Async(db.login_audit_log),
            CollectionsUtilities.login_codes: Async(db.login_codes),
        }
    )


def _codes(mongo):
    return mongo.utilities[CollectionsUtilities.login_codes].sync


def _issue(mongo, purpose=LoginCodePurpose.LOGIN, now=None, user_token=ACCOUNT_TOKEN):
    code = new_code()
    _codes(mongo).insert_one(code_document(code, user_token, purpose, now))
    return code


async def _redeem(mongo, code):
    return await site_auth.redeem_login_code(
        body=site_auth.RedeemLoginCodeRequest(code=code), mongomotor=mongo
    )


@pytest.mark.asyncio
async def test_a_code_the_bot_issued_names_its_account(mongo):
    assert await _redeem(mongo, _issue(mongo)) == {"token": ACCOUNT_TOKEN}


@pytest.mark.asyncio
async def test_the_same_code_does_not_work_twice(mongo):
    code = _issue(mongo)
    await _redeem(mongo, code)
    with pytest.raises(HTTPException) as exc:
        await _redeem(mongo, code)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_a_lapsed_code_is_refused(mongo):
    long_ago = dt.datetime.now(dt.timezone.utc) - LOGIN_CODE_TTL - dt.timedelta(minutes=1)
    code = _issue(mongo, now=long_ago)
    with pytest.raises(HTTPException) as exc:
        await _redeem(mongo, code)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_a_code_nobody_issued_is_refused(mongo):
    with pytest.raises(HTTPException) as exc:
        await _redeem(mongo, new_code())
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_the_account_token_is_not_a_code(mongo):
    """Every login link the bot ever sent holds this. None of them may work."""
    with pytest.raises(HTTPException) as exc:
        await _redeem(mongo, ACCOUNT_TOKEN)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_a_linking_code_does_not_log_in(mongo):
    code = _issue(mongo, purpose=LoginCodePurpose.LINK)
    with pytest.raises(HTTPException) as exc:
        await _redeem(mongo, code)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_a_code_for_an_account_that_is_gone_is_refused(mongo):
    code = _issue(mongo, user_token="deleted-account")
    with pytest.raises(HTTPException) as exc:
        await _redeem(mongo, code)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_the_site_can_ask_for_a_linking_code(mongo):
    answer = await site_auth.issue_telegram_link_code(token=ACCOUNT_TOKEN, mongomotor=mongo)

    stored = _codes(mongo).find_one(redeem_filter(answer["code"], LoginCodePurpose.LINK))
    assert stored["user_token"] == ACCOUNT_TOKEN


@pytest.mark.asyncio
async def test_the_linking_code_is_not_the_account_token(mongo):
    answer = await site_auth.issue_telegram_link_code(token=ACCOUNT_TOKEN, mongomotor=mongo)
    assert ACCOUNT_TOKEN not in answer["code"]


@pytest.mark.asyncio
async def test_no_linking_code_for_an_account_that_does_not_exist(mongo):
    with pytest.raises(HTTPException) as exc:
        await site_auth.issue_telegram_link_code(token="nobody", mongomotor=mongo)
    assert exc.value.status_code == 404
    assert _codes(mongo).count_documents({}) == 0


@pytest.mark.asyncio
async def test_issuing_sweeps_the_codes_that_lapsed(mongo):
    long_ago = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
    _issue(mongo, now=long_ago)
    await site_auth.issue_telegram_link_code(token=ACCOUNT_TOKEN, mongomotor=mongo)
    assert _codes(mongo).count_documents({}) == 1


@pytest.mark.asyncio
async def test_an_account_token_in_the_cookie_is_not_a_session(mongo):
    """The cookie once held the account token itself, and the resolver kept
    accepting one as a login long after sessions replaced it. No session that
    old is still alive, so all the fallback did was make the token a
    password."""
    result = await site_auth.resolve_session(ACCOUNT_TOKEN, mongo)

    assert result == {"ok": False, "reason": "invalid"}
    assert mongo.utilities[CollectionsUtilities.user_sessions].sync.count_documents({}) == 0
