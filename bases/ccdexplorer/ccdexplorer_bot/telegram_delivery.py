# ruff: noqa: E501
"""Whether a Telegram chat is still worth sending to.

Telegram answers `424` on apprise's `POST /notify` when the recipient has
blocked the bot: apprise accepted the payload and Telegram refused delivery.
Nothing in the pipeline noticed, so every subscription a blocked user ever
made kept being built, enqueued and posted forever -- on 2026-11-07 four chat
ids accounted for eighteen 424s in forty minutes, and they had been doing it
for weeks.

This module is the one place that decides it. It lives in the bot base
because both services that need it carry that brick and only those two do:
`ms_bot_sender` records each outcome, `ccdexplorer_bot` reads the flag before
enqueueing. Keeping the threshold and the queries together is the point --
split across the two projects they would drift.

A single 424 proves nothing. Telegram and apprise both have bad minutes, and
deleting a real subscription over one is worse than a wasted message, so the
count has to be consecutive and the bar high. Any `200` clears it.
"""

from __future__ import annotations

import datetime as dt
import sys

from pymongo.collection import Collection

from ccdexplorer.mongodb import CollectionsUtilities

# Consecutive 424s before this chat stops being sent to. At the observed rate
# -- a few an hour for a user following an active account -- a genuinely
# blocked chat trips this inside a day, while an apprise outage would have to
# last 25 of that user's notifications to cost them anything, and the first
# delivery that works puts the count back to zero.
TELEGRAM_FAILURE_LIMIT = 25

# Telegram's own code for "the recipient blocked me", as apprise passes it
# back. Only this one counts: a 500 from apprise is apprise's problem and
# must not unsubscribe anybody.
TELEGRAM_BLOCKED_STATUS = 424


def users_collection_name() -> str:
    """The users collection in use (dev under pytest, prod otherwise).

    Both services have to agree on this or the sender would mark a user in one
    collection while the bot reads the other, so it is resolved here rather
    than spelled out at each call site.
    """
    return (
        CollectionsUtilities.users_v2_dev.value
        if "pytest" in sys.modules
        else CollectionsUtilities.users_v2_prod.value
    )


def record_telegram_failure(
    users: Collection,
    telegram_chat_id: int,
    now: dt.datetime | None = None,
) -> None:
    """Count one 424 against this chat, and block it if that was the last one.

    An aggregation pipeline rather than `$inc` plus a read, because the
    decision depends on the value being written: the stages run in order, so
    the second sees the count the first just set, and the whole thing is one
    atomic update and one round trip. `$ifNull` on the timestamp keeps the
    moment it first tripped instead of moving it on every later failure.
    """
    now = now or dt.datetime.now().astimezone(dt.UTC)
    users.update_one(
        {"telegram_chat_id": telegram_chat_id},
        [
            {
                "$set": {
                    "telegram_consecutive_failures": {
                        "$add": [{"$ifNull": ["$telegram_consecutive_failures", 0]}, 1]
                    }
                }
            },
            {
                "$set": {
                    "telegram_blocked_since": {
                        "$cond": [
                            {"$gte": ["$telegram_consecutive_failures", TELEGRAM_FAILURE_LIMIT]},
                            {"$ifNull": ["$telegram_blocked_since", now]},
                            "$telegram_blocked_since",
                        ]
                    }
                }
            },
        ],
    )


def record_telegram_success(users: Collection, telegram_chat_id: int) -> None:
    """Forget any failures counted against this chat.

    The filter demands there be something to forget, so the overwhelming
    majority of messages -- the ones to users who never blocked anything --
    cost no write at all.
    """
    users.update_one(
        {
            "telegram_chat_id": telegram_chat_id,
            "$or": [
                {"telegram_consecutive_failures": {"$exists": True}},
                {"telegram_blocked_since": {"$exists": True}},
            ],
        },
        {"$unset": {"telegram_consecutive_failures": "", "telegram_blocked_since": ""}},
    )


def clear_telegram_block(users: Collection, telegram_chat_id: int) -> None:
    """Let a chat back in because its owner just talked to the bot.

    Without this the block is permanent: a blocked chat is never sent to, so
    it can never earn the `200` that would clear it. Unblocking on Telegram is
    silent -- there is no event -- but anyone who unblocks the bot and wants
    their notifications back will `/start` it, and that is this path.
    """
    record_telegram_success(users, telegram_chat_id)
