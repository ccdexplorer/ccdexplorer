# ruff: noqa: F403, F405, E402, E501, F401

from unittest.mock import AsyncMock
import pytest
from ccdexplorer.domain.mongo import MongoTypeLoggedEvent
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import *  # type: ignore
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockComplete
from ccdexplorer.mongodb import (
    Collections,
    MongoDB,
)
from rich import print

from ccdexplorer.ccdexplorer_bot.bot import Bot

from ccdexplorer.ccdexplorer_bot.notification_classes import *  # type: ignore


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
SEND_MESSAGES = False


@pytest.mark.asyncio
async def test_account_created_in_other(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(9330979, 1, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )

    assert bot.event_queue[0].event_type.other is not None
    assert bot.event_queue[0].impacted_addresses is not None
    assert bot.event_queue[0].impacted_addresses[0].address is not None
    assert bot.event_queue[0].impacted_addresses[0].address.account is not None
    assert bot.event_queue[0].impacted_addresses[0].address.account.index == 94210
    assert message_response is not None

    if SEND_MESSAGES:
        await bot.send_notification_queue()
    # bot.connections.tooter.async_relay.assert_awaited()


# @pytest.mark.asyncio
# async def test_module_deployed_in_other(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
#     bot.connections.tooter.async_relay = AsyncMock(return_value=None)
#     bot.connections.tooter.email = AsyncMock(return_value=None)
#     block = read_block_information_v3(8745518, 0, grpcclient, mongodb)
#     await bot.find_events_in_block_transactions(block)
#     (
#         message_response,
#         notification_services_to_send,
#     ) = await bot.determine_if_user_should_be_notified_of_event(
#         bot.users["user_for_test"], bot.event_queue[1]
#     )

#     assert bot.event_queue[1].event_type.other is not None
#     assert bot.event_queue[1].impacted_addresses is not None
#     assert bot.event_queue[1].impacted_addresses[0].address is not None
#     assert bot.event_queue[1].impacted_addresses[0].address.account is not None
#     assert bot.event_queue[1].impacted_addresses[0].address.account.index == 94088
#     assert message_response is not None
#     # test separately
#     # assert notification_services_to_send[NotificationServices.telegram] is True
#     # assert notification_services_to_send[NotificationServices.email] is False

#     print(message_response)
#     print(notification_services_to_send)
#     if SEND_MESSAGES:    await bot.send_notification_queue()
#     # bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_domain_minted_in_other(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(7137472, 3, grpcclient, mongodb)
    await bot.find_events_in_logged_events(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )

    assert bot.event_queue[0].event_type.other is not None
    assert message_response is not None

    print(message_response)
    print(notification_services_to_send)
    if SEND_MESSAGES:
        await bot.send_notification_queue()
    # bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_domain_minted_in_other_2(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(9985999, 5, grpcclient, mongodb)
    await bot.find_events_in_logged_events(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )

    assert bot.event_queue[0].event_type.other is not None
    assert message_response is not None
    # test separately
    # assert notification_services_to_send[NotificationServices.telegram] is True

    print(message_response)
    print(notification_services_to_send)
    if SEND_MESSAGES:
        await bot.send_notification_queue()
    # bot.connections.tooter.async_relay.assert_awaited()


# @pytest.mark.asyncio
# async def test_contract_initialized_in_other(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
#     bot.connections.tooter.async_relay = AsyncMock(return_value=None)
#     bot.connections.tooter.email = AsyncMock(return_value=None)
#     block = read_block_information_v3(8745565, 1, grpcclient, mongodb)
#     await bot.find_events_in_block_transactions(block)
#     (
#         message_response,
#         notification_services_to_send,
#     ) = await bot.determine_if_user_should_be_notified_of_event(
#         bot.users["user_for_test"], bot.event_queue[1]
#     )

#     assert bot.event_queue[1].event_type.other is not None
#     assert message_response is not None
#     # test separately
#     # assert notification_services_to_send[NotificationServices.telegram] is True

#     print(message_response)
#     print(notification_services_to_send)
#     if SEND_MESSAGES:    await bot.send_notification_queue()
#     # bot.connections.tooter.async_relay.assert_awaited()


# @pytest.mark.asyncio
# async def test_message_update_protocol_update_6(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
#     bot.connections.tooter.async_relay = AsyncMock(return_value=None)
#     bot.connections.tooter.email = AsyncMock(return_value=None)
#     block = read_block_information_v3(7045451, 4, grpcclient, mongodb)
#     await bot.find_events_in_block_transactions(block)

#     (
#         message_response,
#         notification_services_to_send,
#     ) = await bot.determine_if_user_should_be_notified_of_event(
#         bot.users["user_for_test"], bot.event_queue[0]
#     )
#     assert message_response is not None
#     # test separately
#     # assert notification_services_to_send[NotificationServices.telegram] is True

#     assert bot.event_queue[0].event_type.other is not None
#     assert bot.event_queue[0].event_type.other.protocol_update is not None
#     assert (
#         bot.event_queue[0].event_type.other.protocol_update.specificationHash
#         == "ede9cf0b2185e9e8657f5c3fd8b6f30cef2f1ef4d9692aa4f6ef6a9fb4a762cd"
#     )
#     if SEND_MESSAGES:    await bot.send_notification_queue()
#     # bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_message_account_transfer_with_schedule(
    bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB
):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(1830926, 0, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)

    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    assert message_response is not None
    # test separately
    # assert notification_services_to_send[NotificationServices.telegram] is True

    if SEND_MESSAGES:
        await bot.send_notification_queue()
    # bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_message_other_lowered_stake(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(8511324, 0, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[2]
    )
    assert message_response is not None
    # test separately
    # assert notification_services_to_send[NotificationServices.telegram] is True
    if SEND_MESSAGES:
        await bot.send_notification_queue()
    # bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_message_other_lowered_stake_remove_validator(
    bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB
):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(8877245, 1, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[2]
    )
    assert message_response is not None
    # test separately
    # assert notification_services_to_send[NotificationServices.telegram] is True
    if SEND_MESSAGES:
        await bot.send_notification_queue()
    # bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_message_add_identity_provider(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(1847943, 0, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    assert message_response is not None
    # test separately
    # assert notification_services_to_send[NotificationServices.telegram] is True
    # assert notification_services_to_send[NotificationServices.email] is False
    if SEND_MESSAGES:
        await bot.send_notification_queue()
    # bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_message_pool_commission_changed(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    validator_id = 72723
    baker_set_baking_reward_commission = CCD_BakerSetBakingRewardCommission(
        baker_id=validator_id, baking_reward_commission=0.15
    )
    baker_event = CCD_BakerEvent(
        baker_set_baking_reward_commission=baker_set_baking_reward_commission
    )
    commission_events = [baker_event]
    commission_changed_object = CCD_Pool_Commission_Changed(
        **{"events": commission_events, "validator_id": validator_id}
    )
    event_other = EventTypeOther(validator_commission_changed=commission_changed_object)
    account_info_parent_block = grpcclient.get_account_info(
        "33514ba0d37994457b7009e861a7dabcfac9eb5e4840bc4aa31bb5b130a383c4",
        account_index=72723,
    )
    assert account_info_parent_block is not None
    assert account_info_parent_block.stake is not None
    event_other.previous_block_validator_info = account_info_parent_block.stake.baker
    impacted_addresses = [
        ImpactedAddress(
            address=CCD_Address_Complete(
                account=CCD_AccountAddress_Complete(
                    id="3BFChzvx3783jGUKgHVCanFVxyDAn5xT3Y5NL5FKydVMuBa7Bm",
                    index=validator_id,
                )
            ),
            address_type=AddressType.validator,
        )
    ]
    # as we have already added a notification for commission_changed
    notification_event = NotificationEvent(
        **{
            "event_type": EventType(other=event_other),
            "impacted_addresses": impacted_addresses,
            "block_height": -1,
            "block_hash": "testtest",
            "block_slot_time": dt.datetime.now().astimezone(dt.UTC),
        }
    )
    bot.event_queue.append(notification_event)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    assert bot.event_queue[0].event_type.other is not None
    assert bot.event_queue[0].impacted_addresses is not None
    assert bot.event_queue[0].impacted_addresses[0].address is not None
    assert bot.event_queue[0].impacted_addresses[0].address.account is not None
    assert bot.event_queue[0].impacted_addresses[0].address.account.index == 72723
    assert message_response is not None
    # test separately
    # assert notification_services_to_send[NotificationServices.telegram] is True
    if SEND_MESSAGES:
        await bot.send_notification_queue()
    # bot.connections.tooter.async_relay.assert_awaited()


# @pytest.mark.asyncio
# async def test_message_pool_commission_changed_testnet(
#     bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB
# ):
#     bot.connections.tooter.async_relay = AsyncMock(return_value=None)
#     bot.connections.tooter.email = AsyncMock(return_value=None)
#     block = read_block_information_v3(7131699, 0, grpcclient, mongodb, net="testnet")
#     assert block is not None
#     assert len(block.transaction_summaries) > 0
#     assert block.transaction_summaries[0].account_transaction is not None
#     assert block.transaction_summaries[0].account_transaction.effects is not None
#     assert block.transaction_summaries[0].account_transaction.effects.baker_configured is not None
#     assert len(block.transaction_summaries[0].account_transaction.effects.baker_configured.events) >= 7
#     # we modify the block to have the validator we want
#     block.block_info.parent_block = (
#         "219156804cff97a681dae2ab13a8859074202b3c933f191c09f390da05171664"
#     )
#     block.transaction_summaries[
#         0
#     ].account_transaction.sender = "3BFChzvx3783jGUKgHVCanFVxyDAn5xT3Y5NL5FKydVMuBa7Bm"

#     block.transaction_summaries[0].account_transaction.effects.baker_configured.events[
#         4
#     ].baker_set_transaction_fee_commission.baker_id = 72723
#     block.transaction_summaries[0].account_transaction.effects.baker_configured.events[
#         5
#     ].baker_set_baking_reward_commission.baker_id = 72723
#     block.transaction_summaries[0].account_transaction.effects.baker_configured.events[
#         6
#     ].baker_set_finalization_reward_commission.baker_id = 72723
#     await bot.find_events_in_block_transactions(block)
#     (
#         message_response,
#         notification_services_to_send,
#     ) = await bot.determine_if_user_should_be_notified_of_event(
#         bot.users["user_for_test"], bot.event_queue[11]
#     )
#     # assert bot.event_queue[0].event_type.other is not None
#     # assert bot.event_queue[0].impacted_addresses[0].address.account.index == 72723
#     # assert message_response is not None
#     # assert notification_services_to_send[NotificationServices.telegram] is True
#     if SEND_MESSAGES:    await bot.send_notification_queue()
#     # bot.connections.tooter.async_relay.assert_awaited()


# @pytest.mark.asyncio
# async def test_message_other_primed_validator(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
#     bot.connections.tooter.async_relay = AsyncMock(return_value=None)
#     bot.connections.tooter.email = AsyncMock(return_value=None)
#     block = read_block_information_v3(29083480, 1, grpcclient, mongodb)
#     await bot.find_events_in_block_special_events(block)
#     (
#         message_response,
#         notification_services_to_send,
#     ) = await bot.determine_if_user_should_be_notified_of_event(
#         bot.users["user_for_test"], bot.event_queue[1]
#     )
#     assert message_response is not None
#     # test separately
#     # assert notification_services_to_send[NotificationServices.telegram] is True
#     if SEND_MESSAGES:    await bot.send_notification_queue()
#     # bot.connections.tooter.async_relay.assert_awaited()


# @pytest.mark.asyncio
# async def test_message_other_suspended_validator(
#     bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB
# ):
#     bot.connections.tooter.async_relay = AsyncMock(return_value=None)
#     bot.connections.tooter.email = AsyncMock(return_value=None)
#     block = read_block_information_v3(29081740, 1, grpcclient, mongodb)
#     await bot.find_events_in_block_special_events(block)
#     (
#         message_response,
#         notification_services_to_send,
#     ) = await bot.determine_if_user_should_be_notified_of_event(
#         bot.users["user_for_test"], bot.event_queue[1]
#     )
#     assert message_response is not None
#     # test separately
#     # assert notification_services_to_send[NotificationServices.telegram] is True
#     if SEND_MESSAGES:    await bot.send_notification_queue()
#     # bot.connections.tooter.async_relay.assert_awaited()


# @pytest.mark.asyncio
# async def test_message_other_token_creation(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
#     bot.connections.tooter.async_relay = AsyncMock(return_value=None)
#     bot.connections.tooter.email = AsyncMock(return_value=None)
#     block = read_block_information_v3(32754039, 0, grpcclient, mongodb, net="testnet")
#     await bot.find_events_in_block_transactions(block)

#     (
#         message_response,
#         notification_services_to_send,
#     ) = await bot.determine_if_user_should_be_notified_of_event(
#         bot.users["user_for_test"], bot.event_queue[0]
#     )
#     assert message_response is not None
#     # test separately
#     # assert notification_services_to_send[NotificationServices.telegram] is True

#     if SEND_MESSAGES:    await bot.send_notification_queue()
#     # bot.connections.tooter.async_relay.assert_awaited()
