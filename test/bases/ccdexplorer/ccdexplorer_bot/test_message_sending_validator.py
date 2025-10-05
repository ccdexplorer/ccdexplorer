# ruff: noqa: F403, F405, E402, E501, F401

from unittest.mock import AsyncMock

import pytest
from ccdexplorer.ccdexplorer_bot.bot import Bot
from ccdexplorer.ccdexplorer_bot.notification_classes import *  # type: ignore
from ccdexplorer.domain.generic import NET
from ccdexplorer.domain.mongo import MongoTypeLoggedEvent
from ccdexplorer.env import BLOCK_COUNT_SPECIALS_CHECK
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import *  # type: ignore
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockComplete
from ccdexplorer.mongodb import (
    Collections,
    MongoDB,
)
from ccdexplorer.site_user import (
    NotificationServices,
)
from ccdexplorer.tooter import Tooter
from rich import print


def read_block_information_v3(
    block_height,
    tx_index: int,
    grpcclient: GRPCClient,
    mongodb: MongoDB,
    net: str = "mainnet",
):
    db_to_use = mongodb.mainnet if net == "mainnet" else mongodb.testnet
    block_info = grpcclient.get_finalized_block_at_height(block_height, NET(net))
    transaction_summaries = grpcclient.get_block_transaction_events(
        block_info.hash, NET(net)
    ).transaction_summaries
    if len(transaction_summaries) > 0:
        transaction_summaries = [transaction_summaries[tx_index]]
    special_events = grpcclient.get_block_special_events(block_info.hash, NET(net))

    result = db_to_use[Collections.tokens_logged_events].find({"block_height": block_height})

    ### Logged Events
    if result:
        logged_events_in_block = [MongoTypeLoggedEvent(**x) for x in result]
    else:
        logged_events_in_block = []

    ### Special Events
    special_events = grpcclient.get_block_special_events(block_info.hash, NET(net))

    block_complete = CCD_BlockComplete(
        **{
            "block_info": block_info,
            "transaction_summaries": transaction_summaries,
            "special_events": special_events,
            "logged_events": logged_events_in_block,
            "net": net,
        }
    )

    return block_complete


###################################
# TESTS
###################################

# Set this to have messages actually be sent
SEND_MESSAGES = True


@pytest.mark.asyncio
async def test_message_payday_pool_reward_72723(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    # block = read_block_information_v3(10471185, 0, grpcclient, mongodb)
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(22988438, 0, grpcclient, mongodb)
    await bot.find_events_in_block_special_events(block)

    for event in bot.event_queue:
        assert event.impacted_addresses is not None
        assert event.impacted_addresses[0].address is not None
        assert event.impacted_addresses[0].address.account is not None
        if event.impacted_addresses[0].address.account.index == 72723:
            break
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(bot.users["user_for_test"], event)  # type: ignore
    assert message_response is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


# @pytest.mark.asyncio
# async def test_baker_configured_test_1(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
#     bot.connections.tooter.async_relay = AsyncMock(return_value=None)
#     bot.connections.tooter.email = AsyncMock(return_value=None)
#     block = read_block_information_v3(4092803, 6, grpcclient, mongodb)
#     await bot.find_events_in_block_transactions(block)
#     (
#         message_response,
#         notification_services_to_send,
#     ) = await bot.determine_if_user_should_be_notified_of_event(
#         bot.users["user_for_test"], bot.event_queue[0]
#     )
#     assert message_response is not None

#     print(message_response)
#     print(notification_services_to_send)
#     await bot.send_notification_queue()
#     bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_message_block_validated(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(9106712, 0, grpcclient, mongodb)
    await bot.process_block_for_baker(block)

    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    assert message_response is not None

    assert bot.event_queue[0].event_type.validator is not None
    assert bot.event_queue[0].event_type.validator.block_validated is not None
    assert bot.event_queue[0].impacted_addresses is not None
    assert bot.event_queue[0].impacted_addresses[0].address is not None
    assert bot.event_queue[0].impacted_addresses[0].address.account is not None
    assert bot.event_queue[0].impacted_addresses[0].address.account.index == 72723
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_baker_removed(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(8481158, 0, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    assert message_response is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_baker_stake_increased(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(8752058, 1, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    assert message_response is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_baker_stake_decreased(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(8511339, 0, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    assert message_response is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_baker_restake_earnings_updated(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(8806445, 1, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    assert message_response is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_baker_set_open_status(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(4161221, 0, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    assert message_response is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_delegation_configured_message_for_validator(
    bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB
):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(9084129, 0, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    assert message_response is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_delegation_configured_message_for_validator_85223(
    bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB
):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(11483683, 0, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    assert message_response is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


# TODO fix this test
@pytest.mark.asyncio
async def test_delegation_removed_message(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(37181300, 0, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    # assert message_response is not None
    await bot.send_notification_queue()
    # bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_primed(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(28530504, 0, grpcclient, mongodb, "mainnet")
    await bot.find_events_in_block_special_events(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    assert bot.event_queue[0] is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_message_validator_validator_missed_rounds(
    bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB
):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    bot.block_count_for_specials = BLOCK_COUNT_SPECIALS_CHECK + 1
    bot.missed_rounds_by_id["72723"] = -1
    block = read_block_information_v3(29081740, 1, grpcclient, mongodb)
    await bot.check_for_specials(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    assert message_response is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()
