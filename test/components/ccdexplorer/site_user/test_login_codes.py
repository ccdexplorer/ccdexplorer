"""A link that logs someone in has to stop working.

The account token used to be the link: the bot sent /token/<account token>,
and the settings page put the same token in the Telegram deep link. It never
changed, so every copy of either link -- chat history, a group the command
was typed in, browser history, a proxy log -- stayed a way in for good.

A code stands in for it. It is random, works once, and lapses in minutes; the
store keeps only its hash, so reading the collection yields nothing usable.
"""

import datetime as dt
import re

import mongomock
import pytest

from ccdexplorer.site_user.login_codes import (
    LOGIN_CODE_TTL,
    LoginCodePurpose,
    code_document,
    expired_filter,
    is_live,
    new_code,
    redeem_filter,
)

NOW = dt.datetime(2026, 11, 13, 12, 0, tzinfo=dt.timezone.utc)


@pytest.fixture
def codes():
    return mongomock.MongoClient().concordium_utilities.login_codes


def test_a_code_fits_a_telegram_start_parameter():
    """t.me/<bot>?start= takes 64 characters of [A-Za-z0-9_-] and no more."""
    assert re.fullmatch(r"[A-Za-z0-9_-]{32,64}", new_code())


def test_no_two_codes_are_the_same():
    assert len({new_code() for _ in range(200)}) == 200


def test_the_code_itself_is_not_what_gets_stored():
    code = new_code()
    document = code_document(code, "account-token", LoginCodePurpose.LOGIN, NOW)
    assert code not in str(document)


def test_a_code_lapses_in_minutes_not_days():
    assert LOGIN_CODE_TTL <= dt.timedelta(minutes=15)


def test_a_fresh_code_is_live():
    document = code_document(new_code(), "t", LoginCodePurpose.LOGIN, NOW)
    assert is_live(document, NOW + dt.timedelta(minutes=1))


def test_a_code_past_its_lifetime_is_not():
    document = code_document(new_code(), "t", LoginCodePurpose.LOGIN, NOW)
    assert not is_live(document, NOW + LOGIN_CODE_TTL + dt.timedelta(seconds=1))


def test_a_document_with_no_expiry_is_not_live():
    assert not is_live({"_id": "x", "user_token": "t"}, NOW)


def test_nothing_is_not_live():
    assert not is_live(None, NOW)


def test_mongo_hands_back_naive_datetimes_and_they_are_utc(codes):
    code = new_code()
    codes.insert_one(code_document(code, "t", LoginCodePurpose.LOGIN, NOW))
    stored = codes.find_one(redeem_filter(code, LoginCodePurpose.LOGIN))
    assert stored["expires_at"].tzinfo is None
    assert is_live(stored, NOW + dt.timedelta(minutes=1))
    assert not is_live(stored, NOW + dt.timedelta(hours=1))


def test_a_code_is_found_by_the_code_it_was_made_from(codes):
    code = new_code()
    codes.insert_one(code_document(code, "account-token", LoginCodePurpose.LOGIN, NOW))
    found = codes.find_one_and_delete(redeem_filter(code, LoginCodePurpose.LOGIN))
    assert found["user_token"] == "account-token"


def test_a_code_works_once(codes):
    code = new_code()
    codes.insert_one(code_document(code, "t", LoginCodePurpose.LOGIN, NOW))
    codes.find_one_and_delete(redeem_filter(code, LoginCodePurpose.LOGIN))
    assert codes.find_one_and_delete(redeem_filter(code, LoginCodePurpose.LOGIN)) is None


def test_a_code_made_for_linking_does_not_log_anyone_in(codes):
    """The link code travels through Telegram to the bot. Pasted after
    /token/ instead, it must not be a login."""
    code = new_code()
    codes.insert_one(code_document(code, "t", LoginCodePurpose.LINK, NOW))
    assert codes.find_one(redeem_filter(code, LoginCodePurpose.LOGIN)) is None


def test_lapsed_codes_can_be_swept(codes):
    codes.insert_one(code_document(new_code(), "old", LoginCodePurpose.LOGIN, NOW))
    later = NOW + dt.timedelta(hours=1)
    codes.insert_one(code_document(new_code(), "new", LoginCodePurpose.LOGIN, later))
    codes.delete_many(expired_filter(later))
    assert [d["user_token"] for d in codes.find({})] == ["new"]
