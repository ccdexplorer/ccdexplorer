# ruff: noqa: F403, F405, E402, E501, F401

# from unittest.mock import Mock
from unittest.mock import patch
from unittest.mock import MagicMock
import pytest
from rich import print
from ccdexplorer.domain.mongo import MongoTypeLoggedEvent
from ccdexplorer.domain.generic import NET
from unittest.mock import AsyncMock
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import *  # type: ignore

from ccdexplorer.mongodb import (
    Collections,
    MongoDB,
)
from ccdexplorer.tooter import Tooter

from ccdexplorer.ccdexplorer_bot.bot import Bot, Connections

from ccdexplorer.ccdexplorer_bot.notification_classes import *  # type: ignore
from ccdexplorer.site_user import (
    AccountForUser,
    AccountNotificationPreferences,
    NotificationPreferences,
    NotificationService,
    NotificationServices,
    SiteUser,
)


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
async def test_logged_event_web23_domain(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    """ """
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(9122229, 0, grpcclient, mongodb)

    await bot.find_events_in_logged_events(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[1]
    )
    notification_services_to_send = MagicMock(
        telegram=MagicMock(enabled=True), email=MagicMock(enabled=False)
    )
    assert bot.event_queue[1].event_type.account is not None
    assert message_response is not None
    assert notification_services_to_send is not None

    print(message_response)
    print(notification_services_to_send)
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_logged_event_usdt_transfer(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    """ """
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(9115209, 0, grpcclient, mongodb)
    await bot.find_events_in_logged_events(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    notification_services_to_send = MagicMock(
        telegram=MagicMock(enabled=True), email=MagicMock(enabled=False)
    )
    assert bot.event_queue[0].event_type.account is not None
    assert message_response is not None
    assert notification_services_to_send is not None

    print(message_response)
    print(notification_services_to_send)
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_message_payday_account_reward(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    # NotificationEvent
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(8950655, 0, grpcclient, mongodb)
    await bot.find_events_in_block_special_events(block)
    for notification_event in bot.event_queue:
        assert notification_event.impacted_addresses is not None
        assert notification_event.impacted_addresses[0].address is not None
        assert notification_event.impacted_addresses[0].address.account is not None
        if notification_event.impacted_addresses[0].address.account.index == 72723:
            (
                message_response,
                notification_services_to_send,
            ) = await bot.determine_if_user_should_be_notified_of_event(
                bot.users["user_for_test"], notification_event
            )
    assert message_response is not None  # type: ignore

    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_message_transferred_with_schedule(
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
    notification_services_to_send = MagicMock(
        telegram=MagicMock(enabled=True), email=MagicMock(enabled=False)
    )
    assert message_response is not None
    assert notification_services_to_send is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_message_account_transfer(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    # NotificationEvent
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(8806471, 0, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)

    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    notification_services_to_send = MagicMock(
        telegram=MagicMock(enabled=True), email=MagicMock(enabled=False)
    )
    assert message_response is not None
    assert notification_services_to_send is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_message_data_registered(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(9154159, 0, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    notification_services_to_send = MagicMock(
        telegram=MagicMock(enabled=True), email=MagicMock(enabled=False)
    )
    assert message_response is not None
    assert notification_services_to_send is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_token_event_message(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    bot.read_users_from_collection()
    bot.read_contracts_with_tag_info()
    block = read_block_information_v3(6327195, 11, grpcclient, mongodb)
    await bot.find_events_in_logged_events(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    notification_services_to_send = MagicMock(
        telegram=MagicMock(enabled=True), email=MagicMock(enabled=False)
    )
    assert message_response is not None
    assert notification_services_to_send is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_delegation_configured_message_for_account(
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
    notification_services_to_send = MagicMock(
        telegram=MagicMock(enabled=True), email=MagicMock(enabled=False)
    )
    assert message_response is not None
    assert notification_services_to_send is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_delegation_configured_message2_for_account(
    bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB
):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(9214294, 0, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    notification_services_to_send = MagicMock(
        telegram=MagicMock(enabled=True), email=MagicMock(enabled=False)
    )
    assert message_response is not None
    assert notification_services_to_send is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_message_account_transfer_with_limit_should_not_send(
    bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB
):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(4832414, 16, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[1]
    )
    notification_services_to_send = MagicMock(
        telegram=MagicMock(enabled=True), email=MagicMock(enabled=False)
    )
    assert bot.event_queue[1].event_type.other is not None
    assert bot.event_queue[1].impacted_addresses is not None
    assert bot.event_queue[1].impacted_addresses[0].address is not None
    assert bot.event_queue[1].impacted_addresses[0].address.account is not None
    assert bot.event_queue[1].impacted_addresses[0].address.account.index == 83184
    assert message_response is not None
    assert notification_services_to_send is not None
    await bot.send_notification_queue()
    # bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_message_account_transfer_with_limit_should_send(
    bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB
):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(4832414, 16, grpcclient, mongodb)
    await bot.find_events_in_block_transactions(block)
    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    notification_services_to_send = MagicMock(
        telegram=MagicMock(enabled=True), email=MagicMock(enabled=False)
    )
    assert bot.event_queue[0].event_type.account is not None
    assert bot.event_queue[0].impacted_addresses is not None
    assert bot.event_queue[0].impacted_addresses[0].address is not None
    assert bot.event_queue[0].impacted_addresses[0].address.account is not None
    assert bot.event_queue[0].impacted_addresses[0].address.account.index == 72723
    assert message_response is not None
    assert notification_services_to_send is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


@pytest.mark.asyncio
async def test_message_target_pool_commission_changed(
    bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB
):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    validator_id = 72723
    delegator_id = 86662
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
    event_account = EventTypeAccount(validator_commission_changed=commission_changed_object)
    account_info_parent_block = grpcclient.get_account_info(
        "33514ba0d37994457b7009e861a7dabcfac9eb5e4840bc4aa31bb5b130a383c4",
        account_index=72723,
    )
    assert account_info_parent_block.stake is not None
    event_account.previous_block_validator_info = account_info_parent_block.stake.baker
    impacted_addresses = [
        ImpactedAddress(
            address=CCD_Address_Complete(
                account=CCD_AccountAddress_Complete(
                    id="3pGQi9V9j9YUWXZjdxwCJjnn2Jy76r16LAD9TUvirhv5Wbn6yy",
                    index=delegator_id,
                )
            ),
            address_type=AddressType.delegator,
        ),
        ImpactedAddress(
            address=CCD_Address_Complete(
                account=CCD_AccountAddress_Complete(
                    id="3BFChzvx3783jGUKgHVCanFVxyDAn5xT3Y5NL5FKydVMuBa7Bm",
                    index=validator_id,
                )
            ),
            address_type=AddressType.validator,
        ),
    ]
    # as we have already added a notification for commission_changed
    notification_event = NotificationEvent(
        **{
            "event_type": EventType(account=event_account),
            "impacted_addresses": impacted_addresses,
            "block_height": -1,
            "block_hash": "testtest",
            "block_slot_time": dt.datetime.now().astimezone(dt.UTC),
        }
    )
    bot.event_queue.append(notification_event)
    # set user
    user: SiteUser = bot.users["user_for_test"]
    user_account = AccountForUser(account_index=86662, delegation_target=72723)
    user_account.account_notification_preferences = AccountNotificationPreferences(
        validator_commission_changed=NotificationPreferences(
            telegram=NotificationService(enabled=True)
        )
    )
    assert user.accounts is not None
    user.accounts[str(86662)] = user_account

    (
        message_response,
        notification_services_to_send,
    ) = await bot.determine_if_user_should_be_notified_of_event(
        bot.users["user_for_test"], bot.event_queue[0]
    )
    notification_services_to_send = MagicMock(
        telegram=MagicMock(enabled=True), email=MagicMock(enabled=False)
    )
    assert bot.event_queue[0].event_type.account is not None
    assert bot.event_queue[0].impacted_addresses is not None
    assert bot.event_queue[0].impacted_addresses[0].address is not None
    assert bot.event_queue[0].impacted_addresses[0].address.account is not None
    assert bot.event_queue[0].impacted_addresses[0].address.account.index == 86662
    assert message_response is not None
    assert notification_services_to_send is not None
    await bot.send_notification_queue()
    bot.connections.tooter.async_relay.assert_awaited()


def fake_complete_address(impacted_address):
    return CCD_Address_Complete(
        account=CCD_AccountAddress_Complete(id=impacted_address, index=15573)
    )


@pytest.mark.asyncio
async def test_message_account_plt_event_mint(bot: Bot, grpcclient: GRPCClient, mongodb: MongoDB):
    bot.connections.tooter.async_relay = AsyncMock(return_value=None)
    bot.connections.tooter.email = AsyncMock(return_value=None)
    block = read_block_information_v3(32674681, 0, grpcclient, mongodb, net="testnet")
    with patch.object(Bot, "complete_address", side_effect=fake_complete_address) as mock_complete:
        await bot.find_events_in_block_transactions(block)
        mock_complete.assert_called()
        (
            message_response,
            notification_services_to_send,
        ) = await bot.determine_if_user_should_be_notified_of_event(
            bot.users["user_for_test"], bot.event_queue[0]
        )
        notification_services_to_send = MagicMock(
            telegram=MagicMock(enabled=True), email=MagicMock(enabled=False)
        )
        assert bot.event_queue[0].event_type.account is not None
        assert bot.event_queue[0].impacted_addresses is not None
        assert bot.event_queue[0].impacted_addresses[0].address is not None
        assert bot.event_queue[0].impacted_addresses[0].address.account is not None
        assert bot.event_queue[0].impacted_addresses[0].address.account.index == 15573
        assert message_response is not None
        assert notification_services_to_send is not None
        await bot.send_notification_queue()
        bot.connections.tooter.async_relay.assert_awaited()
