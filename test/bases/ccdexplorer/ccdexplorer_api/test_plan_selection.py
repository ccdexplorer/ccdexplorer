"""Choosing a plan picks from the plans on offer, and from nothing else.

The handler stored whatever followed the first "=" in the body. The page only
ever sends the three names it draws a card for, but the route answers anyone
logged in, and the name is what the rate limiter groups a key by.
"""

import datetime as dt
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from ccdexplorer.mongodb import CollectionsUtilities


def _plans():
    """Resolved per call: a full-suite run re-imports the app package, and a
    module captured at collection time is one no monkeypatch reaches."""
    from ccdexplorer.ccdexplorer_api.app.routers.plans import plans

    return plans


def _models():
    from ccdexplorer.ccdexplorer_api.app import models

    return models


class _Setup:
    def __init__(self, monkeypatch, body: bytes, user="default"):
        from ccdexplorer.ccdexplorer_api.app.models import User

        self.user = (
            User(
                scope="http://localhost",
                token="t",
                alias_id=1,
                alias_account_id="alias",
                api_account_id="api-user-1",
                email="someone@example.com",
                password="hashed",
                plan_end_date=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
            )
            if user == "default"
            else user
        )
        self.bulk_write = AsyncMock(return_value=None)
        self.request = MagicMock()
        self.request.body = AsyncMock(return_value=body)
        self.request.app.motormongo.utilities = {
            CollectionsUtilities.api_users: MagicMock(bulk_write=self.bulk_write)
        }
        monkeypatch.setattr(_plans(), "get_user_details", lambda request: self.user)

    async def post(self):
        return await _plans().plans_set_plan(self.request)


@pytest.mark.parametrize("plan", ["free", "standard", "pro"])
async def test_a_plan_on_offer_is_stored(monkeypatch, plan):
    s = _Setup(monkeypatch, f"plan={plan}".encode())

    response = await s.post()

    assert s.user.plan == plan
    s.bulk_write.assert_awaited_once()
    assert response.headers["HX-Redirect"] == "/account"


@pytest.mark.parametrize(
    "plan",
    [
        "unrestricted",  # a rate limit group, never a card on the page
        "ccdexplorer.io",  # the site's own group
        "Pro",  # the names are exact
        "pro ",
        "enterprise",
        "",
    ],
)
async def test_any_other_name_is_refused_and_nothing_is_written(monkeypatch, plan):
    s = _Setup(monkeypatch, f"plan={plan}".encode())

    with pytest.raises(HTTPException) as refused:
        await s.post()

    assert refused.value.status_code == 400
    assert s.user.plan is None
    s.bulk_write.assert_not_awaited()


@pytest.mark.parametrize("body", [b"plan", b"=", b"other=pro", b"\xff\xfe"])
async def test_a_body_that_names_no_plan_is_refused_not_a_crash(monkeypatch, body):
    s = _Setup(monkeypatch, body)

    with pytest.raises(HTTPException) as refused:
        await s.post()

    assert refused.value.status_code == 400
    s.bulk_write.assert_not_awaited()


async def test_the_plan_is_read_as_a_form_field_not_by_position(monkeypatch):
    """Splitting on "=" took everything up to the next one, so a second field
    became part of the name."""
    s = _Setup(monkeypatch, b"plan=pro&source=card")

    await s.post()

    assert s.user.plan == "pro"


async def test_an_encoded_name_is_decoded_before_it_is_compared(monkeypatch):
    s = _Setup(monkeypatch, b"plan=%75nrestricted")

    with pytest.raises(HTTPException):
        await s.post()

    s.bulk_write.assert_not_awaited()


async def test_no_body_is_still_the_free_plan(monkeypatch):
    s = _Setup(monkeypatch, b"")

    await s.post()

    assert s.user.plan == "free"


async def test_logged_out_goes_to_login_before_the_body_is_read(monkeypatch):
    s = _Setup(monkeypatch, b"plan=unrestricted", user=None)

    response = await s.post()

    assert response.headers["HX-Redirect"] == "/auth/login"
    s.bulk_write.assert_not_awaited()


def test_what_can_be_chosen_is_what_the_page_draws():
    """One list, so a plan added to the page is selectable and a tier added
    for internal use is not, without anyone remembering to say so twice."""
    models = _models()

    assert models.SELECTABLE_PLANS == frozenset(models.plans_for_display)
    assert models.SELECTABLE_PLANS == {"free", "standard", "pro"}


# --- a key takes its rate limit group from the plan -----------------------
#
# Refusing the name on the way in does nothing for an account that stored one
# before the check existed. A new key copies the plan into api_group, so the
# same list is asked again there.


def _account():
    from ccdexplorer.ccdexplorer_api.app.routers.account import account

    return account


class _KeySetup:
    def __init__(self, monkeypatch, plan):
        from ccdexplorer.ccdexplorer_api.app.models import User

        self.user = User(
            scope="http://localhost",
            token="t",
            alias_id=1,
            alias_account_id="alias",
            api_account_id="api-user-1",
            email="someone@example.com",
            password="hashed",
            plan=plan,
            plan_end_date=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
        )
        self.bulk_write = AsyncMock(return_value=None)
        self.request = MagicMock()
        self.request.app.motormongo.utilities_db = {
            "api_api_keys": MagicMock(bulk_write=self.bulk_write)
        }
        account = _account()
        monkeypatch.setattr(account, "get_user_details", lambda request: self.user)
        monkeypatch.setattr(account, "get_api_keys", AsyncMock(return_value={}))

    async def post(self):
        return await _account().account_new_key(self.request, mongomotor=MagicMock())


@pytest.mark.parametrize("plan", ["free", "standard", "pro"])
async def test_a_key_is_made_for_a_plan_on_offer(monkeypatch, plan):
    s = _KeySetup(monkeypatch, plan)

    await s.post()

    s.bulk_write.assert_awaited_once()
    written = s.bulk_write.await_args.args[0][0]._doc
    assert written["api_group"] == plan


@pytest.mark.parametrize("plan", ["unrestricted", "enterprise", None])
async def test_no_key_is_made_for_any_other_plan(monkeypatch, plan):
    s = _KeySetup(monkeypatch, plan)

    with pytest.raises(HTTPException) as refused:
        await s.post()

    assert refused.value.status_code == 400
    s.bulk_write.assert_not_awaited()
