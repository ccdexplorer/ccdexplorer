"""Blocked Telegram chats stop being sent to.

Telegram answers 424 on apprise's POST /notify when the recipient has blocked
the bot. Nothing acted on that, so ms_bot_sender logged a line and the whole
pipeline carried on building, enqueueing and posting messages those people
will never see -- on 2026-11-07 four chat ids produced eighteen 424s in forty
minutes, and had been doing it for weeks.

The count has to be consecutive and the bar high, because the cost of getting
this wrong is asymmetric: a wasted message is nothing, silently deleting a
real subscription over one bad minute at apprise is not. These tests pin both
halves -- that 24 failures change nothing, and that the 25th does.
"""

import datetime as dt
from types import SimpleNamespace

import mongomock
import pytest

from ccdexplorer.ccdexplorer_bot.telegram_delivery import (
    TELEGRAM_BLOCKED_STATUS,
    TELEGRAM_FAILURE_LIMIT,
    clear_telegram_block,
    record_telegram_failure,
    record_telegram_success,
    telegram_failures_so_far,
    users_collection_name,
    worth_retrying,
)
from ccdexplorer.site_user import SiteUser

CHAT = 7534089330  # one of the four from that log
OTHER_CHAT = 1146986275


@pytest.fixture
def users():
    """A users collection holding two chats, neither of them marked."""
    collection = mongomock.MongoClient().concordium_utilities.users_v2_dev
    collection.insert_many(
        [
            {"_id": str(CHAT), "telegram_chat_id": CHAT, "token": "t1"},
            {"_id": str(OTHER_CHAT), "telegram_chat_id": OTHER_CHAT, "token": "t2"},
        ]
    )
    return collection


def doc(users, chat=CHAT):
    return users.find_one({"telegram_chat_id": chat})


# --- the counter -----------------------------------------------------------


def test_the_limit_is_twenty_five():
    """Sander's number. A test rather than a comment because the whole design
    rests on it being high enough that an outage cannot unsubscribe anyone."""
    assert TELEGRAM_FAILURE_LIMIT == 25


def test_one_failure_counts_but_blocks_nobody(users):
    record_telegram_failure(users, CHAT)

    assert doc(users)["telegram_consecutive_failures"] == 1
    assert doc(users).get("telegram_blocked_since") is None


def test_one_short_of_the_limit_still_sends(users):
    """The expensive half of the bargain: 24 refusals and we keep trying."""
    for _ in range(TELEGRAM_FAILURE_LIMIT - 1):
        record_telegram_failure(users, CHAT)

    assert doc(users)["telegram_consecutive_failures"] == TELEGRAM_FAILURE_LIMIT - 1
    assert doc(users).get("telegram_blocked_since") is None


def test_the_limit_blocks_and_records_when(users):
    for _ in range(TELEGRAM_FAILURE_LIMIT):
        record_telegram_failure(users, CHAT)

    assert doc(users)["telegram_consecutive_failures"] == TELEGRAM_FAILURE_LIMIT
    assert isinstance(doc(users)["telegram_blocked_since"], dt.datetime)


def test_later_failures_do_not_move_the_timestamp(users):
    """It is the moment this chat went quiet, not the moment of the last
    attempt -- `$ifNull` in the second pipeline stage is what keeps it."""
    for _ in range(TELEGRAM_FAILURE_LIMIT):
        record_telegram_failure(users, CHAT)
    first = doc(users)["telegram_blocked_since"]

    for _ in range(5):
        record_telegram_failure(users, CHAT, now=dt.datetime(2030, 1, 1, tzinfo=dt.UTC))

    assert doc(users)["telegram_blocked_since"] == first
    assert doc(users)["telegram_consecutive_failures"] == TELEGRAM_FAILURE_LIMIT + 5


def test_only_the_named_chat_is_touched(users):
    for _ in range(TELEGRAM_FAILURE_LIMIT):
        record_telegram_failure(users, CHAT)

    assert doc(users, OTHER_CHAT).get("telegram_consecutive_failures") is None
    assert doc(users, OTHER_CHAT).get("telegram_blocked_since") is None


# --- clearing it -----------------------------------------------------------


def test_one_delivery_forgets_every_failure_before_it(users):
    """Consecutive, not cumulative. An apprise outage that straddles a working
    delivery must not accumulate towards the limit."""
    for _ in range(TELEGRAM_FAILURE_LIMIT - 1):
        record_telegram_failure(users, CHAT)

    record_telegram_success(users, CHAT)

    assert doc(users).get("telegram_consecutive_failures") is None
    for _ in range(TELEGRAM_FAILURE_LIMIT - 1):
        record_telegram_failure(users, CHAT)
    assert doc(users).get("telegram_blocked_since") is None


def test_a_healthy_chat_costs_no_write(users):
    """Every notification that works goes through here, so the filter has to
    demand there be something to clear before it writes anything."""
    before = doc(users)

    result = users.update_one(
        {
            "telegram_chat_id": CHAT,
            "$or": [
                {"telegram_consecutive_failures": {"$exists": True}},
                {"telegram_blocked_since": {"$exists": True}},
            ],
        },
        {"$unset": {"telegram_consecutive_failures": "", "telegram_blocked_since": ""}},
    )

    assert result.matched_count == 0
    assert doc(users) == before


def test_talking_to_the_bot_lifts_the_block(users):
    """Unblocking on Telegram is silent, and a blocked chat is never sent to,
    so without this path the block could never be cleared by anything."""
    for _ in range(TELEGRAM_FAILURE_LIMIT):
        record_telegram_failure(users, CHAT)
    assert doc(users)["telegram_blocked_since"]

    clear_telegram_block(users, CHAT)

    assert doc(users).get("telegram_blocked_since") is None
    assert doc(users).get("telegram_consecutive_failures") is None


# --- the parts either side of it ------------------------------------------


def test_the_user_model_carries_both_fields():
    """The bot reads the flag off a SiteUser, so pydantic has to keep it
    instead of dropping an unknown key from the document."""
    user = SiteUser(token="t", telegram_chat_id=CHAT, telegram_consecutive_failures=25)

    assert user.telegram_consecutive_failures == 25
    assert user.telegram_blocked_since is None


def test_a_fresh_user_is_not_blocked():
    assert SiteUser(token="t").telegram_blocked_since is None


def test_pytest_runs_against_the_dev_collection():
    """Both services resolve the collection through this one function, so a
    test run can never mark a production user."""
    assert users_collection_name() == "users_v2_dev"


def test_only_a_424_is_treated_as_a_block():
    """apprise's own failures are not the recipient's fault. A 500 must log
    and do nothing else."""
    assert TELEGRAM_BLOCKED_STATUS == 424


# --- the gate in the producer ---------------------------------------------


@pytest.fixture
def gate():
    """`Mixin.send_to_services` with celery replaced by a list.

    Bound to a stub rather than a Bot, because the rest of Bot wants gRPC,
    Mongo and a Telegram token to exist.
    """
    from ccdexplorer.ccdexplorer_bot.bot.messages_logic import Mixin

    published = []

    async def publish_to_celery(service, user, message_response):
        published.append(service.value)

    stub = SimpleNamespace(publish_to_celery=publish_to_celery)

    async def call(user):
        from ccdexplorer.site_user import NotificationServices

        await Mixin.send_to_services(
            stub,
            user=user,
            notification_services_to_send={
                NotificationServices.telegram: True,
                NotificationServices.email: True,
            },
            message_response=SimpleNamespace(),
        )
        return published

    return call


async def test_a_blocked_user_gets_no_task_at_all(gate):
    """The point of gating in the producer: no celery task, no apprise
    request, no log line for someone who will never see the message."""
    user = SiteUser(
        token="t",
        telegram_chat_id=CHAT,
        telegram_blocked_since=dt.datetime(2026, 11, 7, tzinfo=dt.UTC),
    )

    assert await gate(user) == []


async def test_a_blocked_user_still_gets_email(gate):
    """Blocking the bot on Telegram says nothing about their email."""
    user = SiteUser(
        token="t",
        telegram_chat_id=CHAT,
        email_address="a@b.c",
        email_verified=True,
        telegram_blocked_since=dt.datetime(2026, 11, 7, tzinfo=dt.UTC),
    )

    assert await gate(user) == ["email"]


async def test_an_ordinary_user_is_unaffected(gate):
    user = SiteUser(token="t", telegram_chat_id=CHAT)

    assert await gate(user) == ["telegram"]


async def test_failures_below_the_limit_do_not_gate(gate):
    """Only the timestamp gates. A user mid-count is still being tried."""
    user = SiteUser(token="t", telegram_chat_id=CHAT, telegram_consecutive_failures=24)

    assert await gate(user) == ["telegram"]


# --- was this chat working until now? --------------------------------------
#
# A 424 is Telegram refusing delivery, and it says nothing about why. "The
# recipient blocked the bot" and "not this message, not right now" arrive as
# the same number. On 2026-11-09 a payday pool reward to a working chat was
# refused 240 ms after that chat's payday account reward was accepted, and the
# message was lost: nothing resends a 424.
#
# The distinguishing fact is already on the user document. A chat with no
# consecutive failures was delivering as of its last send, so a refusal is
# worth one more try; a chat already collecting them is most likely blocked
# and must not cost a second send every time.


def test_a_chat_that_has_never_failed_reports_nothing(users):
    assert telegram_failures_so_far(users, CHAT) == 0


def test_a_chat_that_has_failed_reports_how_often(users):
    record_telegram_failure(users, CHAT)
    record_telegram_failure(users, CHAT)

    assert telegram_failures_so_far(users, CHAT) == 2


def test_a_chat_we_have_never_seen_reports_nothing(users):
    """`async_relay` is given a chat id, not a user, so the id may belong to
    no document at all -- as `...-backup`'s mangled id does."""
    assert telegram_failures_so_far(users, 404404404) == 0


def test_a_delivery_resets_what_is_reported(users):
    record_telegram_failure(users, CHAT)
    record_telegram_success(users, CHAT)

    assert telegram_failures_so_far(users, CHAT) == 0


def test_one_chat_failing_does_not_speak_for_another(users):
    for _ in range(3):
        record_telegram_failure(users, CHAT)

    assert telegram_failures_so_far(users, OTHER_CHAT) == 0


def test_a_working_chat_is_worth_another_try():
    assert worth_retrying(0) is True


def test_a_chat_already_failing_is_not_retried():
    """Twenty-five retries on top of twenty-five sends is the traffic this
    whole module exists to stop."""
    assert worth_retrying(1) is False
    assert worth_retrying(TELEGRAM_FAILURE_LIMIT - 1) is False
