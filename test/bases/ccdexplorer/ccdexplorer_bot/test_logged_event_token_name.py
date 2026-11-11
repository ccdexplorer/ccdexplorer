"""A token the bot sees minted before the indexer has written it down.

On 2026-11-11 block 54154643 minted `<9882,0>-00002051`, and the bot read
the logged event before the tokens microservice had written that token
address to Mongo. The `else` branch for exactly that case only logs to the
console and never assigns `token_name`, so the next read of it raised
UnboundLocalError and the whole pass over the block's logged events died at
its first event.

Nothing recovers from that. `log_error` sets `exception_raised`, which only
withholds the `bot_last_processed_block` write for the failing block; the
next block in the batch succeeds and writes its own higher height, so the
block is never revisited and its notifications are simply gone.

`token_name` is also never reset between iterations, so an event whose token
is missing inherits the name of whichever event came before it. That one is
worse than the crash -- it mislabels a notification instead of dropping it,
and nobody would ever know. It raised here only because the mint happened to
be the block's first logged event.

These tests drive the method directly rather than through the Bot fixture,
which needs live Mongo and grpc to come up at all.
"""

import datetime as dt
from types import SimpleNamespace

import mongomock
import pytest

from ccdexplorer.ccdexplorer_bot.bot.blocks_logic import Mixin
from ccdexplorer.domain.mongo import MongoTypeLoggedEventV2
from ccdexplorer.mongodb import Collections

MINTER = "4MwARWeXdMs3YZ5MPPn2561ceani6AJAVTNPtwS6tceaG2qatK"
CONTRACT = "<9882,0>"
TOKEN = "<9882,0>-00002051"
OTHER_TOKEN = "<9882,0>-00002050"
TX = "f62b64e7390f1a0399140b2969a83a041ba2d518f8ab4efadc6d59a310a02628"


def mint_event(token_address: str, event_index: int = 0) -> MongoTypeLoggedEventV2:
    """A CIS-2 mint, as block 54154643's first logged event came in."""
    return MongoTypeLoggedEventV2(
        **{
            "_id": f"54154643-{TX}-contract_updated-0-updated-{event_index}",
            "event_info": {
                "contract": CONTRACT,
                "standard": "CIS-2",
                "logged_event": "fe04000020510100ba9f",
                "effect_index": 0,
                "event_index": event_index,
                "event_type": "CIS-2.mint_event",
                "token_address": token_address,
            },
            "tx_info": {
                "date": "2026-11-11",
                "tx_hash": TX,
                "tx_index": 0,
                "block_height": 54154643,
            },
            "recognized_event": {
                "tag": 254,
                "token_id": token_address.split("-")[1],
                "token_amount": 1,
                "to_address": MINTER,
            },
            "to_address_canonical": MINTER[:29],
        }
    )


def block(*logged_events) -> SimpleNamespace:
    return SimpleNamespace(
        logged_events=list(logged_events),
        block_info=SimpleNamespace(
            height=54154643,
            hash="62ac8e6297a4262a9c294cf353bc63cf715e49692da07b254d31ca8a501b23fb",
            slot_time=dt.datetime(2026, 11, 11, 13, 48, 9, 25000, tzinfo=dt.UTC),
        ),
    )


def stored(token_address: str, name: str | None) -> dict:
    """A token address as the tokens microservice writes it."""
    doc = {
        "_id": token_address,
        "contract": CONTRACT,
        "token_id": token_address.split("-")[1],
        "token_amount": "1",
        "last_height_processed": 54154643,
    }
    if name is not None:
        doc["token_metadata"] = {"name": name}
    return doc


@pytest.fixture
def bot():
    """The blocks mixin with only what find_events_in_logged_events touches.

    stable_address_info_by_account_id is pre-populated so complete_address
    resolves the minter from it rather than falling through to a grpc call.
    """
    token_addresses = mongomock.MongoClient().concordium_mainnet.tokens_token_addresses
    mixin = object.__new__(Mixin)
    mixin.connections = SimpleNamespace(
        mongodb=SimpleNamespace(mainnet={Collections.tokens_token_addresses: token_addresses}),
        grpcclient=None,
    )
    mixin.event_queue = []
    mixin.stable_address_info_by_account_id = {MINTER: {"account_index": 150322}}
    mixin.stable_address_info_by_account_index = {}
    mixin.token_addresses = token_addresses
    return mixin


def token_names(bot) -> list:
    return [event.event_type.account.token_event.token_name for event in bot.event_queue]


# --- the crash ------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_token_the_indexer_has_not_written_yet_still_notifies(bot):
    """The reproduction. An empty collection is what the bot actually saw."""
    await bot.find_events_in_logged_events(block(mint_event(TOKEN)))

    assert len(bot.event_queue) == 1
    assert bot.event_queue[0].event_type.account.token_event.token_address == TOKEN


@pytest.mark.asyncio
async def test_a_token_with_no_name_yet_is_reported_without_one(bot):
    """No name is the honest answer; the message template already handles it."""
    await bot.find_events_in_logged_events(block(mint_event(TOKEN)))

    assert token_names(bot) == [None]


@pytest.mark.asyncio
async def test_the_minter_is_still_the_impacted_account(bot):
    """What the lost notification was for."""
    await bot.find_events_in_logged_events(block(mint_event(TOKEN)))

    impacted = bot.event_queue[0].impacted_addresses
    assert impacted[0].address.account.id == MINTER
    assert impacted[0].address.account.index == 150322
    assert impacted[1].address.contract.index == 9882


# --- the borrowed name ----------------------------------------------------


@pytest.mark.asyncio
async def test_a_nameless_token_does_not_borrow_the_previous_name(bot):
    """The mislabelling bug. Two mints in one block, only the first indexed."""
    bot.token_addresses.insert_one(stored(OTHER_TOKEN, "Clark1406"))

    await bot.find_events_in_logged_events(block(mint_event(OTHER_TOKEN, 0), mint_event(TOKEN, 1)))

    assert token_names(bot) == ["Clark1406", None]


@pytest.mark.asyncio
async def test_a_nameless_token_does_not_borrow_the_placeholder_either(bot):
    """A stored token with no metadata hands on "Not yet available..." just
    as readily as a real name."""
    bot.token_addresses.insert_one(stored(OTHER_TOKEN, None))

    await bot.find_events_in_logged_events(block(mint_event(OTHER_TOKEN, 0), mint_event(TOKEN, 1)))

    assert token_names(bot) == ["Not yet available...", None]


# --- what must not change -------------------------------------------------


@pytest.mark.asyncio
async def test_an_indexed_token_keeps_its_name(bot):
    bot.token_addresses.insert_one(stored(TOKEN, "Clark1407"))

    await bot.find_events_in_logged_events(block(mint_event(TOKEN)))

    assert token_names(bot) == ["Clark1407"]


@pytest.mark.asyncio
async def test_an_indexed_token_without_metadata_says_so(bot):
    """The existing placeholder, which distinguishes "indexed but no
    metadata read yet" from "not indexed at all"."""
    bot.token_addresses.insert_one(stored(TOKEN, None))

    await bot.find_events_in_logged_events(block(mint_event(TOKEN)))

    assert token_names(bot) == ["Not yet available..."]


@pytest.mark.asyncio
async def test_a_block_with_no_logged_events_queues_nothing(bot):
    await bot.find_events_in_logged_events(block())

    assert bot.event_queue == []
