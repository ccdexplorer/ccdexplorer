# ruff: noqa: F403, F405, E402, E501, F401

from pydantic import BaseModel, ConfigDict
from rich import print
from rich.console import Console
import aiohttp
from ccdexplorer.domain.mongo import (
    MongoTypeLoggedEvent,
    MongoTypeTokenAddress,
    MongoTypeInvolvedAccount,
    MongoTypeInvolvedContract,
    MongoTypeTokensTag,
)
from ccdexplorer.domain.cis import (
    burnEvent,
    mintEvent,
    transferEvent,
)
from pymongo import ReplaceOne
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import *  # type: ignore
from ccdexplorer.mongodb import (
    Collections,
    MongoDB,
    MongoMotor,
)

from ccdexplorer.tooter import Tooter, TooterChannel, TooterType
from ccdexplorer.domain.node import ConcordiumNodeFromDashboard
from telegram import Update
from telegram.ext import ContextTypes

from ccdexplorer.env import *  # type: ignore
from ..notification_classes import *
from ccdexplorer.site_user import (
    AccountForUser,
    NotificationPreferences,
    Reward,
    SiteUser,
)

from .utils import Utils as Utils

console = Console()


class Mixin(Utils):
    def prepare_notification_event(
        self,
        event_type: EventType,
        tx_hash: str | None,
        block_info: CCD_BlockInfo | None,
        impacted_addresses: list[ImpactedAddress],
    ) -> NotificationEvent:
        return NotificationEvent(
            **{
                "event_type": event_type,
                "tx_hash": tx_hash,
                "block_height": block_info.height if block_info else None,
                "block_hash": block_info.hash if block_info else None,
                "block_slot_time": (
                    block_info.slot_time if block_info else dt.datetime.now().astimezone(dt.UTC)
                ),
                "impacted_addresses": impacted_addresses,
            }
        )

    def define_lowered_stake_amount(
        self, event_type_other: EventTypeOther
    ) -> CCD_LoweredStake | None:
        unstaked_amount = None
        assert event_type_other.baker_configured is not None
        assert event_type_other.previous_block_validator_info is not None
        for baker_config_event in event_type_other.baker_configured.events:
            if baker_config_event.baker_stake_decreased:
                previous_amount = event_type_other.previous_block_validator_info.staked_amount
                new_amount = baker_config_event.baker_stake_decreased.new_stake
                unstaked_amount = previous_amount - new_amount
                perc = (previous_amount - new_amount) / previous_amount
                return CCD_LoweredStake(
                    baker_stake_decreased=baker_config_event.baker_stake_decreased,
                    unstaked_amount=unstaked_amount,
                    new_stake=new_amount,
                    percentage_unstaked=perc,
                )

            elif baker_config_event.baker_removed:
                previous_amount = event_type_other.previous_block_validator_info.staked_amount
                new_amount = 0
                unstaked_amount = previous_amount - new_amount
                perc = (previous_amount - new_amount) / previous_amount
                return CCD_LoweredStake(
                    baker_removed=True,
                    unstaked_amount=unstaked_amount,
                    new_stake=new_amount,
                    percentage_unstaked=perc,
                )

        return None

    def find_commission_changed(
        self,
        event_type_other: EventTypeOther | EventTypeAccount,
        grpcclient: GRPCClient | None = None,
    ) -> CCD_Pool_Commission_Changed | None:
        commission_events = []
        validator_id = None
        assert event_type_other.baker_configured is not None
        for baker_config_event in event_type_other.baker_configured.events:
            if baker_config_event.baker_set_baking_reward_commission:
                commission_events.append(baker_config_event)
                validator_id = baker_config_event.baker_set_baking_reward_commission.baker_id
            if baker_config_event.baker_set_transaction_fee_commission:
                commission_events.append(baker_config_event)
                validator_id = baker_config_event.baker_set_transaction_fee_commission.baker_id
            if baker_config_event.baker_set_finalization_reward_commission:
                commission_events.append(baker_config_event)
                validator_id = baker_config_event.baker_set_finalization_reward_commission.baker_id

        if validator_id:
            assert grpcclient is not None
            delegators_info = grpcclient.get_delegators_for_pool(validator_id, "last_final")
            delegator_indices_list = [
                grpcclient.get_account_info("last_final", hex_address=x.account).index
                for x in delegators_info
            ]
        return (
            CCD_Pool_Commission_Changed(
                **{
                    "events": commission_events,
                    "validator_id": validator_id,
                    "delegators": delegator_indices_list,  # type: ignore
                }
            )
            if len(commission_events) > 0
            else None
        )

    def add_notification_event_to_queue(self, notification_event: NotificationEvent):
        self.event_queue.append(notification_event)  # type: ignore

    def add_plt_events_to_queue(
        self,
        token_effect: CCD_TokenEffect | None,
        tx: CCD_BlockItemSummary,
        block: CCD_BlockComplete,
    ):
        impacted_addresses = []
        if not token_effect:
            return

        for plt_event in token_effect.events:
            event_account = EventTypeAccount(
                **{"token_update_effect": plt_event}  # type: ignore
            )
            if plt_event.mint_event:
                impacted_addresses = [
                    ImpactedAddress(
                        address=self.complete_address(plt_event.mint_event.target.account),
                        address_type=AddressType.receiver,
                    )
                ]
                notification_event = self.prepare_notification_event(
                    EventType(account=event_account),
                    tx_hash=tx.hash,
                    block_info=block.block_info,
                    impacted_addresses=impacted_addresses,
                )
                self.add_notification_event_to_queue(notification_event)
            if plt_event.burn_event:
                impacted_addresses = [
                    ImpactedAddress(
                        address=self.complete_address(plt_event.burn_event.target.account),
                        address_type=AddressType.receiver,
                    )
                ]
                notification_event = self.prepare_notification_event(
                    EventType(account=event_account),
                    tx_hash=tx.hash,
                    block_info=block.block_info,
                    impacted_addresses=impacted_addresses,
                )
                self.add_notification_event_to_queue(notification_event)
            if plt_event.transfer_event:
                impacted_addresses = [
                    ImpactedAddress(
                        address=self.complete_address(plt_event.transfer_event.from_.account),
                        address_type=AddressType.sender,
                    ),
                    ImpactedAddress(
                        address=self.complete_address(plt_event.transfer_event.to.account),
                        address_type=AddressType.receiver,
                    ),
                ]
                notification_event = self.prepare_notification_event(
                    EventType(account=event_account),
                    tx_hash=tx.hash,
                    block_info=block.block_info,
                    impacted_addresses=impacted_addresses,
                )
                self.add_notification_event_to_queue(notification_event)
            if plt_event.module_event:
                continue

    async def get_new_blocks_from_mongo(self, context: ContextTypes.DEFAULT_TYPE):
        # print("get_new_blocks_from_mongo", end=" ")
        current_time = dt.datetime.now().astimezone(tz=dt.timezone.utc)
        if (current_time - self.internal_freqency_timer).total_seconds() > 5 * 60:
            self.connections.tooter.relay(
                channel=TooterChannel.NOTIFIER,
                title="",
                chat_id=ADMIN_CHAT_ID,
                body="Bot seems to not have processed a new block in 5 min? Exiting to restart.",
                notifier_type=TooterType.REQUESTS_ERROR,
            )
            exit()
        try:
            if not self.processing:
                net = NET(context.bot_data["net"]).value
                db_to_use = (
                    self.connections.mongodb.mainnet
                    if net == "mainnet"
                    else self.connections.mongodb.testnet
                )

                bot_last_processed_block = db_to_use[Collections.helpers].find_one(
                    {"_id": "bot_last_processed_block"}
                )
                assert bot_last_processed_block is not None
                bot_last_processed_block_height = bot_last_processed_block["height"]

                result = db_to_use[Collections.blocks].aggregate(
                    [{"$sort": {"height": -1}}, {"$limit": 1}]
                )

                if result:
                    last_block_info = CCD_BlockInfo(**list(result)[0])
                    if last_block_info.height > bot_last_processed_block_height:
                        max_steps_to_take = min(
                            10, last_block_info.height - bot_last_processed_block_height
                        )
                        for requested_height in range(
                            # bot_last_processed_block_height + 1, last_block_info.height
                            bot_last_processed_block_height + 1,
                            bot_last_processed_block_height + 1 + max_steps_to_take,
                        ):
                            result = db_to_use[Collections.blocks].find_one(
                                {"height": requested_height}
                            )
                            if not result:
                                _ = db_to_use[Collections.helpers].bulk_write(
                                    [
                                        ReplaceOne(
                                            {"_id": "special_purpose_block_request"},
                                            replacement={
                                                "_id": "special_purpose_block_request",
                                                "heights": [requested_height],
                                            },
                                            upsert=True,
                                        )
                                    ]
                                )
                                self.connections.tooter.send(
                                    channel=TooterChannel.NOTIFIER,
                                    message=f"BOT: Can't find {requested_height:,.0f} in collection blocks! BOT is stalling on this, have tried to add this as Special Purpose request.",
                                    notifier_type=TooterType.BOT_MAIN_LOOP_ERROR,
                                )
                                print(f"Can't find {requested_height} in collection blocks!")

                            ### Block Info
                            else:
                                block_info_at_height = CCD_BlockInfo(**result)

                                result = db_to_use[Collections.transactions].find(
                                    {"_id": {"$in": block_info_at_height.transaction_hashes}}
                                )
                                if not result:
                                    break

                                ### Transactions in block
                                txs_in_block = [CCD_BlockItemSummary(**x) for x in result]

                                result = db_to_use[Collections.tokens_logged_events].find(
                                    {"block_height": requested_height}
                                )

                                ### Logged Events
                                if result:
                                    logged_events_in_block = [
                                        MongoTypeLoggedEvent(**x) for x in result
                                    ]
                                else:
                                    logged_events_in_block = []

                                ### Special Events
                                result = db_to_use[Collections.special_events].find_one(
                                    {"_id": requested_height}
                                )

                                if result:
                                    special_events_in_block = [
                                        CCD_BlockSpecialEvent(**x) for x in result["special_events"]
                                    ]
                                else:
                                    special_events_in_block = (
                                        self.connections.grpcclient.get_block_special_events(  # type: ignore
                                            block_info_at_height.hash, NET(net)
                                        )
                                    )

                                block_complete = CCD_BlockComplete(
                                    **{
                                        "block_info": block_info_at_height,
                                        "transaction_summaries": txs_in_block,
                                        "special_events": special_events_in_block,
                                        "logged_events": logged_events_in_block,
                                        "net": net,
                                    }
                                )
                                console.log(
                                    f"Ret: {block_complete.block_info.height:,.0f}",
                                    end=" | ",
                                )
                                self.full_blocks_to_process.append(block_complete)
            else:
                console.log(f"{self.processing=}")
        except Exception as e:
            print(e)

    async def process_block_for_baker(self, block: CCD_BlockComplete):
        baker_id = block.block_info.baker
        if baker_id:
            if not block.block_info:
                self.connections.tooter.relay(
                    channel=TooterChannel.BOT,
                    title="Failed in process_block_for_baker",
                    chat_id=ADMIN_CHAT_ID,
                    body=f"process_block_for_baker has FAILED with: block.block_info == None. {block.model_dump(exclude_none=True)}",
                    notifier_type=TooterType.BOT_MAIN_LOOP_ERROR,
                )
            else:
                pi = self.connections.grpcclient.get_pool_info_for_pool(  # type: ignore
                    baker_id, block.block_info.hash, NET(block.net)
                )
                earliest_win_time = (
                    self.connections.grpcclient.get_baker_earliest_win_time(baker_id)  # type: ignore
                )

                notification_event = self.prepare_notification_event(
                    EventType(
                        validator=EventTypeValidator(
                            block_validated=True,
                            current_block_pool_info=pi,
                            earliest_win_time=earliest_win_time,
                        )
                    ),
                    tx_hash=None,
                    block_info=block.block_info,
                    impacted_addresses=[
                        ImpactedAddress(
                            address=self.complete_address(baker_id),
                            address_type=AddressType.validator,
                        )
                    ],
                )
                self.add_notification_event_to_queue(notification_event)  # type: ignore

    async def find_web23_domain_name(self, token_address: str):
        contract_str = token_address.split("-")[0]
        contract = CCD_ContractAddress.from_str(contract_str)
        token_id = token_address.split("-")[1]

        url_to_fetch_metadata = f"https://wallet-proxy.mainnet.concordium.software/v0/CIS2TokenMetadata/{contract.index}/0?tokenId={token_id}"
        metadata_url = None
        session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=2, sock_read=2)
        async with aiohttp.ClientSession(timeout=session_timeout) as session:
            async with session.get(url_to_fetch_metadata) as resp:
                token_metadata = await resp.json()
                if "metadata" in token_metadata:
                    if "metadataURL" in token_metadata["metadata"][0]:
                        metadata_url = token_metadata["metadata"][0]["metadataURL"]
                else:
                    return None

            if metadata_url:
                async with session.get(metadata_url) as resp:
                    token_metadata = await resp.json()
                    if resp.status == 200:
                        return token_metadata["name"]
            else:
                return None

    async def find_events_in_logged_events(self, block: CCD_BlockComplete):
        if block.logged_events:
            for logged_event in block.logged_events:
                logged_event: MongoTypeLoggedEvent
                stored_token_address = self.connections.mongodb.mainnet[
                    Collections.tokens_token_addresses
                ].find_one({"_id": logged_event.token_address})
                if stored_token_address:
                    stored_token_address = MongoTypeTokenAddress(**stored_token_address)
                    if stored_token_address.token_metadata:
                        token_name = stored_token_address.token_metadata.name
                    else:
                        token_name = "Not yet available..."
                else:
                    console.log(f"{logged_event.token_address=} not found in Mongo collection yet.")
                if (logged_event.contract == "<9377,0>") and (logged_event.tag == 254):
                    domain_name = await self.find_web23_domain_name(logged_event.token_address)
                    if domain_name:
                        event_other = EventTypeOther(
                            domain_name_minted=domain_name,
                        )
                        logged_event.result = mintEvent(**logged_event.result)
                        notification_event = self.prepare_notification_event(
                            EventType(other=event_other),
                            tx_hash=logged_event.tx_hash,
                            block_info=block.block_info,
                            impacted_addresses=[
                                ImpactedAddress(
                                    address=self.complete_address(logged_event.result.to_address),
                                    address_type=AddressType.account,
                                ),
                                ImpactedAddress(
                                    address=self.complete_address(logged_event.contract),
                                    address_type=AddressType.contract,
                                ),
                            ],
                        )

                        self.add_notification_event_to_queue(notification_event)
                    # self.event_queue.append(notification_event)

                if logged_event.tag in [255, 254, 253]:
                    event_account = EventTypeAccount(
                        token_event=TokenEvent(
                            result=logged_event.result,  # type: ignore
                            token_address=logged_event.token_address,
                            token_name=token_name if token_name else None,  # type: ignore
                        )
                    )
                    if logged_event.tag == 255:
                        if isinstance(logged_event.result, dict):
                            logged_event.result = transferEvent(**logged_event.result)
                        impacted_addresses = [
                            ImpactedAddress(
                                address=self.complete_address(logged_event.result.from_address),  # type: ignore
                                address_type=AddressType.sender,
                            ),
                            ImpactedAddress(
                                address=self.complete_address(logged_event.result.to_address),
                                address_type=AddressType.receiver,
                            ),
                        ]
                        # the FROM account
                        notification_event = self.prepare_notification_event(
                            EventType(account=event_account),
                            tx_hash=logged_event.tx_hash,
                            block_info=block.block_info,
                            impacted_addresses=impacted_addresses,
                        )
                        self.event_queue.append(notification_event)  # type: ignore

                        # the TO account
                        impacted_addresses = [
                            ImpactedAddress(
                                address=self.complete_address(logged_event.result.to_address),
                                address_type=AddressType.receiver,
                            ),
                            ImpactedAddress(
                                address=self.complete_address(logged_event.result.from_address),  # type: ignore
                                address_type=AddressType.sender,
                            ),
                        ]
                        notification_event = self.prepare_notification_event(
                            EventType(account=event_account),
                            tx_hash=logged_event.tx_hash,
                            block_info=block.block_info,
                            impacted_addresses=impacted_addresses,
                        )
                        self.event_queue.append(notification_event)  # type: ignore

                    elif logged_event.tag == 254:
                        if isinstance(logged_event.result, dict):
                            logged_event.result = mintEvent(**logged_event.result)
                        # the TO account
                        impacted_addresses = [
                            ImpactedAddress(
                                address=self.complete_address(logged_event.result.to_address),
                                address_type=AddressType.account,
                            ),
                            ImpactedAddress(
                                address=self.complete_address(logged_event.contract),
                                address_type=AddressType.contract,
                            ),
                        ]
                        notification_event = self.prepare_notification_event(
                            EventType(account=event_account),
                            tx_hash=logged_event.tx_hash,
                            block_info=block.block_info,
                            impacted_addresses=impacted_addresses,
                        )
                        self.event_queue.append(notification_event)  # type: ignore

                    elif logged_event.tag == 253:
                        if isinstance(logged_event.result, dict):
                            logged_event.result = burnEvent(**logged_event.result)

                        # the FROM account
                        impacted_addresses = [
                            ImpactedAddress(
                                address=self.complete_address(logged_event.result.from_address),  # type: ignore
                                address_type=AddressType.account,
                            ),
                            ImpactedAddress(
                                address=self.complete_address(logged_event.contract),
                                address_type=AddressType.contract,
                            ),
                        ]
                        notification_event = self.prepare_notification_event(
                            EventType(account=event_account),
                            tx_hash=logged_event.tx_hash,
                            block_info=block.block_info,
                            impacted_addresses=impacted_addresses,
                        )
                        self.event_queue.append(notification_event)  # type: ignore

    async def find_events_in_block_special_events(self, block: CCD_BlockComplete):
        if block.special_events:
            primed = [
                x.validator_primed_for_suspension
                for x in block.special_events
                if x.validator_primed_for_suspension is not None
            ]

            for prime in primed:
                # validator Event
                event_validator = EventTypeValidator(validator_primed_for_suspension=prime.baker_id)

                notification_event = self.prepare_notification_event(
                    EventType(validator=event_validator),
                    block_info=block.block_info,
                    tx_hash=None,
                    impacted_addresses=[
                        ImpactedAddress(
                            address=self.complete_address(prime.baker_id),
                            address_type=AddressType.validator,
                        )
                    ],
                )
                self.add_notification_event_to_queue(notification_event)

                # Other Event
                event_other = EventTypeOther(validator_primed_for_suspension=prime.baker_id)
                notification_event = self.prepare_notification_event(
                    EventType(other=event_other),
                    tx_hash=None,
                    block_info=block.block_info,
                    impacted_addresses=[
                        ImpactedAddress(
                            address=self.complete_address(prime.baker_id),
                            address_type=AddressType.validator,
                        )
                    ],
                )
                self.add_notification_event_to_queue(notification_event)

            suspended = [
                x.validator_suspended
                for x in block.special_events
                if x.validator_suspended is not None
            ]

            for sus in suspended:
                # Validator Event
                event_validator = EventTypeValidator(validator_suspended=sus.bakerId)

                notification_event = self.prepare_notification_event(
                    EventType(validator=event_validator),
                    block_info=block.block_info,
                    tx_hash=None,
                    impacted_addresses=[
                        ImpactedAddress(
                            address=self.complete_address(sus.bakerId),
                            address_type=AddressType.validator,
                        )
                    ],
                )
                self.add_notification_event_to_queue(notification_event)

                # Other Event
                event_other = EventTypeOther(validator_suspended=sus.bakerId)
                notification_event = self.prepare_notification_event(
                    EventType(other=event_other),
                    tx_hash=None,
                    block_info=block.block_info,
                    impacted_addresses=[
                        ImpactedAddress(
                            address=self.complete_address(sus.bakerId),
                            address_type=AddressType.validator,
                        )
                    ],
                )
                self.add_notification_event_to_queue(notification_event)

            last_block_of_payday_hash = self.connections.grpcclient.get_finalized_block_at_height(  # type: ignore
                block.block_info.height - 1
            ).hash

            # first fill the account reward dict for later lookup
            # in pool reward loop
            account_rewards_by_account_index = {}
            for reward in block.special_events:
                # Payday Account Rewards
                account_id = Reward(reward).account_reward()
                if account_id:
                    from_nightly = self.nightly_accounts_by_account_id.get(account_id)  # type: ignore
                    if from_nightly:
                        account_index = from_nightly["index"]
                        account_rewards_by_account_index[account_index] = (
                            reward.payday_account_reward
                        )

            for reward in block.special_events:
                # Payday Account Rewards
                account_id = Reward(reward).account_reward()
                if account_id:
                    assert reward.payday_account_reward is not None
                    event_account = EventTypeAccount(
                        payday_account_reward=CCD_BlockSpecialEvent_PaydayAccountReward(
                            account=account_id,
                            transaction_fees=reward.payday_account_reward.transaction_fees,
                            baker_reward=reward.payday_account_reward.baker_reward,
                            finalization_reward=reward.payday_account_reward.finalization_reward,
                        )
                    )

                    notification_event = self.prepare_notification_event(
                        EventType(account=event_account),
                        block_info=block.block_info,
                        tx_hash=None,
                        impacted_addresses=[
                            ImpactedAddress(
                                address=self.complete_address(account_id),
                                address_type=AddressType.account,
                            )
                        ],
                    )
                    self.add_notification_event_to_queue(notification_event)

                # Payday Pools Rewards
                pool_owner = Reward(reward).pool_reward()
                if pool_owner:
                    corresponding_account_reward = account_rewards_by_account_index.get(pool_owner)

                    pool_info = self.connections.grpcclient.get_pool_info_for_pool(  # type: ignore
                        pool_owner,
                        last_block_of_payday_hash,
                    )
                    assert reward.payday_pool_reward is not None
                    event_validator = EventTypeValidator(
                        pool_info=pool_info,
                        corresponding_account_reward=corresponding_account_reward,
                        payday_pool_reward=CCD_BlockSpecialEvent_PaydayPoolReward(
                            pool_owner=pool_owner,
                            transaction_fees=reward.payday_pool_reward.transaction_fees,
                            baker_reward=reward.payday_pool_reward.baker_reward,
                            finalization_reward=reward.payday_pool_reward.finalization_reward,
                        ),
                    )

                    notification_event = self.prepare_notification_event(
                        EventType(validator=event_validator),
                        block_info=block.block_info,
                        tx_hash=None,
                        impacted_addresses=[
                            ImpactedAddress(
                                address=self.complete_address(pool_owner),
                                address_type=AddressType.validator,
                            )
                        ],
                    )
                    self.add_notification_event_to_queue(notification_event)

    async def find_events_in_block_transactions(self, block: CCD_BlockComplete):
        self.connections: Connections
        # The first impacted address is the address that is used for
        # notification purposes, ie if the account_index of the first impacted address
        # is in user.accounts.kyes(), the user will be notified.
        # Hence, we need to make sure for some of the transaction types
        # that the correct impacted address is first in the list (with the correct
        # address type).

        for tx in block.transaction_summaries:
            if tx.token_creation:
                impacted_addresses = []

                event_other = EventTypeOther(
                    **{"token_creation": tx.token_creation.create_plt}  # type: ignore
                )
                notification_event = self.prepare_notification_event(
                    EventType(other=event_other),
                    tx_hash=tx.hash,
                    block_info=block.block_info,
                    impacted_addresses=impacted_addresses,
                )
                self.add_notification_event_to_queue(notification_event)

            if tx.account_transaction:
                effects = tx.account_transaction.effects

                field_set = list(effects.model_fields_set)[0]

                if EventTypeValidator.model_fields.get(field_set):
                    impacted_addresses = [
                        ImpactedAddress(
                            address=self.complete_address(tx.account_transaction.sender),
                            address_type=AddressType.sender,
                        )
                    ]
                    event_validator = EventTypeValidator(
                        **{field_set: effects.model_dump()[field_set]}
                    )
                    if (field_set == "baker_configured") or (field_set == "validator_configured"):
                        # We need to change the AddressType to Validator.
                        impacted_addresses[0].address_type = AddressType.validator
                        account_info_parent_block = self.connections.grpcclient.get_account_info(  # type: ignore
                            block.block_info.parent_block,
                            tx.account_transaction.sender,
                        )
                        assert account_info_parent_block.stake is not None
                        if account_info_parent_block.stake.baker:
                            event_validator.previous_block_validator_info = (
                                account_info_parent_block.stake.baker
                            )

                    # this should give a notification event for the target
                    # of this delegation action. The sender itself is notified
                    # in the same-named event in Account.
                    if field_set == "delegation_configured":
                        # this should lead to a notification to the target pool!
                        account_info_delegator = self.connections.grpcclient.get_account_info(  # type: ignore
                            block.block_info.hash, tx.account_transaction.sender
                        )
                        impacted_addresses[0].address_type = AddressType.delegator

                        target_pool = None
                        assert account_info_delegator.stake is not None
                        if account_info_delegator.stake.delegator is not None:
                            if account_info_delegator.stake.delegator.target:
                                if account_info_delegator.stake.delegator.target.baker:
                                    target_pool = (
                                        account_info_delegator.stake.delegator.target.baker
                                    )

                        account_info_parent_block = self.connections.grpcclient.get_account_info(  # type: ignore
                            block.block_info.parent_block,
                            tx.account_transaction.sender,
                        )
                        assert account_info_parent_block.stake is not None
                        if account_info_parent_block.stake.delegator:
                            event_validator.previous_block_account_info = (
                                account_info_parent_block.stake.delegator
                            )

                        if target_pool:
                            # for a delegation_configured event, we need to send
                            # seperately to the pool
                            # target_pool is None if its passive delegation

                            impacted_addresses.insert(
                                0,
                                (
                                    ImpactedAddress(
                                        address=self.complete_address(target_pool),
                                        address_type=AddressType.validator,
                                    )
                                ),
                            )

                    notification_event = self.prepare_notification_event(
                        EventType(validator=event_validator),
                        tx_hash=tx.hash,
                        block_info=block.block_info,
                        impacted_addresses=impacted_addresses,
                    )
                    self.add_notification_event_to_queue(notification_event)

                if EventTypeAccount.model_fields.get(field_set):
                    commission_changed_object = None
                    impacted_addresses = [
                        ImpactedAddress(
                            address=self.complete_address(tx.account_transaction.sender),
                            address_type=AddressType.sender,
                        )
                    ]
                    event_account = EventTypeAccount(**{field_set: effects.model_dump()[field_set]})
                    if field_set == "data_registered":
                        notification_event = self.prepare_notification_event(
                            EventType(account=event_account),
                            tx_hash=tx.hash,
                            block_info=block.block_info,
                            impacted_addresses=impacted_addresses,
                        )
                        self.add_notification_event_to_queue(notification_event)
                    # the notification for sender is added automatically below,
                    # hence we need to add the notification for receiver here seperately for both account_transfer
                    # as well as transferred_with_schedule.
                    # Note that we need to prepend the impacted address for receiver
                    # as this is the address used for notifications.
                    if field_set == "account_transfer":
                        assert effects.account_transfer is not None
                        impacted_addresses.insert(
                            0,
                            (
                                ImpactedAddress(
                                    address=self.complete_address(
                                        effects.account_transfer.receiver
                                    ),
                                    address_type=AddressType.receiver,
                                )
                            ),
                        )
                        notification_event = self.prepare_notification_event(
                            EventType(account=event_account),
                            tx_hash=tx.hash,
                            block_info=block.block_info,
                            impacted_addresses=impacted_addresses,
                        )
                        self.add_notification_event_to_queue(notification_event)

                        # we now have to set the impacted_addresses again in the correct order
                        # for the sender notification below, so sender, receiver.
                        impacted_addresses = [
                            ImpactedAddress(
                                address=self.complete_address(tx.account_transaction.sender),
                                address_type=AddressType.sender,
                            ),
                            ImpactedAddress(
                                address=self.complete_address(effects.account_transfer.receiver),
                                address_type=AddressType.receiver,
                            ),
                        ]

                    if field_set == "token_update_effect":
                        self.add_plt_events_to_queue(
                            tx.account_transaction.effects.token_update_effect,
                            tx,
                            block,
                        )

                    if field_set == "transferred_with_schedule":
                        assert effects.transferred_with_schedule is not None
                        impacted_addresses.insert(
                            0,
                            (
                                ImpactedAddress(
                                    address=self.complete_address(
                                        effects.transferred_with_schedule.receiver
                                    ),
                                    address_type=AddressType.receiver,
                                )
                            ),
                        )
                        notification_event = self.prepare_notification_event(
                            EventType(account=event_account),
                            tx_hash=tx.hash,
                            block_info=block.block_info,
                            impacted_addresses=impacted_addresses,
                        )
                        self.add_notification_event_to_queue(notification_event)

                        # we now have to set the impacted_addresses again in the correct order
                        # for the sender notification below, so sender, receiver.
                        impacted_addresses = [
                            ImpactedAddress(
                                address=self.complete_address(tx.account_transaction.sender),
                                address_type=AddressType.sender,
                            ),
                            ImpactedAddress(
                                address=self.complete_address(
                                    effects.transferred_with_schedule.receiver
                                ),
                                address_type=AddressType.receiver,
                            ),
                        ]

                    if field_set == "delegation_configured":
                        impacted_addresses[0].address_type = AddressType.delegator
                        account_info_delegator = self.connections.grpcclient.get_account_info(  # type: ignore
                            block.block_info.hash, tx.account_transaction.sender
                        )
                        account_info_parent_block = self.connections.grpcclient.get_account_info(  # type: ignore
                            block.block_info.parent_block,
                            tx.account_transaction.sender,
                        )
                        assert account_info_parent_block.stake is not None
                        if account_info_parent_block.stake.delegator:
                            event_account.previous_block_account_info = (
                                account_info_parent_block.stake.delegator
                            )

                        target_pool = None
                        assert account_info_delegator.stake is not None
                        if account_info_delegator.stake.delegator is not None:
                            if account_info_delegator.stake.delegator.target:
                                if account_info_delegator.stake.delegator.target.baker:
                                    target_pool = (
                                        account_info_delegator.stake.delegator.target.baker
                                    )

                        if target_pool:
                            impacted_addresses.insert(
                                0,
                                (
                                    ImpactedAddress(
                                        address=self.complete_address(target_pool),
                                        address_type=AddressType.validator,
                                    )
                                ),
                            )

                    if (field_set == "baker_configured") or (field_set == "validator_configured"):
                        impacted_addresses[0].address_type = AddressType.validator
                        account_info_parent_block = self.connections.grpcclient.get_account_info(  # type: ignore
                            block.block_info.parent_block,
                            tx.account_transaction.sender,
                        )
                        assert account_info_parent_block.stake is not None
                        if account_info_parent_block.stake.baker:
                            event_account.previous_block_validator_info = (
                                account_info_parent_block.stake.baker
                            )

                        commission_changed_object = self.find_commission_changed(
                            event_account,
                            self.connections.grpcclient,  # type: ignore
                        )

                        if commission_changed_object:
                            # event_account event_other to be a lowered_stake event
                            event_account = EventTypeAccount(
                                validator_commission_changed=commission_changed_object
                            )
                            # note this seems to be double, but remember we have
                            # changed the event_other to a different type,
                            # hence we need to add the parent block info again.
                            if account_info_parent_block.stake.baker:
                                event_account.previous_block_validator_info = (
                                    account_info_parent_block.stake.baker
                                )

                            # we need to notify for all delegators
                            assert commission_changed_object.delegators is not None
                            for delegator_id in commission_changed_object.delegators:
                                impacted_addresses = [
                                    ImpactedAddress(
                                        address=self.complete_address(delegator_id),
                                        address_type=AddressType.delegator,
                                    ),
                                    ImpactedAddress(
                                        address=self.complete_address(
                                            tx.account_transaction.sender
                                        ),
                                        address_type=AddressType.validator,
                                    ),
                                ]

                                notification_event = self.prepare_notification_event(
                                    EventType(account=event_account),
                                    tx_hash=tx.hash,
                                    block_info=block.block_info,
                                    impacted_addresses=impacted_addresses,
                                )
                                self.add_notification_event_to_queue(notification_event)
                        # as we have already added a notification for commission_changed
                        if not commission_changed_object:
                            notification_event = self.prepare_notification_event(
                                EventType(account=event_account),
                                tx_hash=tx.hash,
                                block_info=block.block_info,
                                impacted_addresses=impacted_addresses,
                            )
                            self.add_notification_event_to_queue(notification_event)

                if EventTypeOther.model_fields.get(field_set):
                    lowered_stake_object = None
                    commission_changed_object = None

                    impacted_addresses = [
                        ImpactedAddress(
                            address=self.complete_address(tx.account_transaction.sender),
                            address_type=AddressType.sender,
                        )
                    ]
                    event_other = EventTypeOther(**{field_set: effects.model_dump()[field_set]})

                    # only do this exception because we need to get the
                    # parent block account info.
                    if (field_set == "baker_configured") or (field_set == "validator_configured"):
                        impacted_addresses[0].address_type = AddressType.validator
                        account_info_parent_block = self.connections.grpcclient.get_account_info(  # type: ignore
                            block.block_info.parent_block,
                            tx.account_transaction.sender,
                        )
                        assert account_info_parent_block.stake is not None
                        if account_info_parent_block.stake.baker:
                            event_other.previous_block_validator_info = (
                                account_info_parent_block.stake.baker
                            )
                        # we have found a baker_configued event, however we
                        # notify for a lowered_stake event!

                        lowered_stake_object = self.define_lowered_stake_amount(event_other)
                        commission_changed_object = self.find_commission_changed(
                            event_other,
                            self.connections.grpcclient,  # type: ignore
                        )

                        if lowered_stake_object:
                            # update event_other to be a lowered_stake event
                            event_other = EventTypeOther(
                                validator_lowered_stake=lowered_stake_object
                            )
                            # note this seems to be double, but remember we have
                            # changed the event_other to a different type,
                            # hence we need to add the parent block info again.
                            if account_info_parent_block.stake.baker:
                                event_other.previous_block_validator_info = (
                                    account_info_parent_block.stake.baker
                                )

                            # we need to send an additional notification for this
                            notification_event = self.prepare_notification_event(
                                EventType(other=event_other),
                                tx_hash=tx.hash,
                                block_info=block.block_info,
                                impacted_addresses=impacted_addresses,
                            )
                            self.add_notification_event_to_queue(notification_event)

                        if commission_changed_object:
                            # update event_other to be a lowered_stake event
                            event_other = EventTypeOther(
                                validator_commission_changed=commission_changed_object
                            )
                            # note this seems to be double, but remember we have
                            # changed the event_other to a different type,
                            # hence we need to add the parent block info again.
                            if account_info_parent_block.stake.baker:
                                event_other.previous_block_validator_info = (
                                    account_info_parent_block.stake.baker
                                )

                            # we need to send an additional notification for this
                            notification_event = self.prepare_notification_event(
                                EventType(other=event_other),
                                tx_hash=tx.hash,
                                block_info=block.block_info,
                                impacted_addresses=impacted_addresses,
                            )
                            self.add_notification_event_to_queue(notification_event)

                    if field_set == "account_transfer":
                        assert effects.account_transfer is not None
                        self.append_impacted_address(
                            effects.account_transfer.receiver, impacted_addresses
                        )

                    if field_set == "transferred_with_schedule":
                        assert effects.transferred_with_schedule is not None
                        self.append_impacted_address(
                            effects.transferred_with_schedule.receiver,
                            impacted_addresses,
                        )
                    #
                    if not (commission_changed_object or lowered_stake_object):
                        notification_event = self.prepare_notification_event(
                            EventType(other=event_other),
                            tx_hash=tx.hash,
                            block_info=block.block_info,
                            impacted_addresses=impacted_addresses,
                        )
                        self.add_notification_event_to_queue(notification_event)

                if EventTypeContract.model_fields.get(field_set):
                    # impacted_addresses = [
                    #     ImpactedAddress(
                    #         address=self.complete_address(
                    #             tx.account_transaction.sender
                    #         ),
                    #         address_type=AddressType.sender,
                    #     )
                    # ]

                    address_receive_name_list = []
                    if field_set == "contract_update_issued":
                        assert effects.contract_update_issued is not None
                        for effect in effects.contract_update_issued.effects:
                            if effect.updated:
                                event_contract = EventTypeContract(
                                    **{field_set: effects.model_dump()[field_set]}
                                )
                                if "." in effect.updated.receive_name:
                                    receive_name = effect.updated.receive_name.split(".")[1]
                                else:
                                    receive_name = None
                                impacted_address_str = effect.updated.address.to_str()
                                impacted_address = ImpactedAddress(
                                    address=self.complete_address(effect.updated.address),
                                    address_type=AddressType.contract,
                                )

                                if (
                                    f"{impacted_address_str}-{receive_name}"
                                    not in address_receive_name_list
                                ):
                                    address_receive_name_list.append(
                                        f"{impacted_address_str}-{receive_name}"
                                    )
                                    event_contract.receive_name = receive_name

                                    # as it may be possible that multiple contracts are updated in a transaction
                                    # we need to send mulitple notifications, one for each unique contract.
                                    notification_event = self.prepare_notification_event(
                                        EventType(contract=event_contract),
                                        tx_hash=tx.hash,
                                        block_info=block.block_info,
                                        impacted_addresses=[impacted_address],
                                    )
                                    self.add_notification_event_to_queue(notification_event)

            elif tx.update:
                impacted_addresses = []
                field_set = list(tx.update.payload.model_fields_set)[0]
                if field_set in [
                    "protocol_update",
                    "add_anonymity_revoker_update",
                    "add_identity_provider_update",
                ]:
                    event_other = EventTypeOther(
                        **{field_set: tx.update.payload.model_dump()[field_set]}
                    )
                    notification_event = self.prepare_notification_event(
                        EventType(other=event_other),
                        tx_hash=tx.hash,
                        block_info=block.block_info,
                        impacted_addresses=impacted_addresses,
                    )
                    self.add_notification_event_to_queue(notification_event)

            elif tx.account_creation:
                impacted_addresses = [
                    ImpactedAddress(
                        address=self.complete_address(tx.account_creation.address),
                        address_type=AddressType.sender,
                    )
                ]
                field_set = list(tx.account_creation.model_fields_set)[0]

                event_other = EventTypeOther(account_created=tx.account_creation.address)
                notification_event = self.prepare_notification_event(
                    EventType(other=event_other),
                    tx_hash=tx.hash,
                    block_info=block.block_info,
                    impacted_addresses=impacted_addresses,
                )
                self.add_notification_event_to_queue(notification_event)

    async def check_for_specials(self, block: CCD_BlockComplete):
        if self.block_count_for_specials > BLOCK_COUNT_SPECIALS_CHECK:
            self.block_count_for_specials = 0

            # check for node version upgrade
            result = self.connections.mongodb.mainnet[Collections.dashboard_nodes].find({})

            running_version = None
            if result:
                for raw_node in list(result):
                    node = ConcordiumNodeFromDashboard(**raw_node)
                    if node.consensusBakerId == 0:
                        running_version = node.client
                    if running_version:
                        break
            if running_version:
                current_version = self.connections.mongodb.mainnet[Collections.helpers].find_one(
                    {"_id": "concordium-0-client-version"}
                )["version"]  # type: ignore

            if running_version != current_version:  # type: ignore
                event_other = EventTypeOther(validator_node_version_upgrade=running_version)
                notification_event = self.prepare_notification_event(
                    EventType(other=event_other),
                    tx_hash=None,
                    block_info=None,
                    impacted_addresses=[
                        ImpactedAddress(
                            # this is Concordium validator-0
                            address=self.complete_address(
                                "49SJ6R6T9zo1C5cLVyxbwAuZC3EcDB9a78vSQYm3ZLA2y2eojM"
                            ),
                            address_type=AddressType.validator,
                        ),
                    ],
                )

                self.add_notification_event_to_queue(notification_event)

            # check for missed rounds in validators
            missed_rounds_by_id = self.do_missed_rounds_read(initial=False)  # type: ignore
            for account_id in missed_rounds_by_id:
                missed_round_increase = missed_rounds_by_id[
                    account_id
                ] - self.missed_rounds_by_id.get(account_id, 0)
                if missed_round_increase > 0:
                    event_validator = EventTypeValidator(
                        validator_missed_rounds={
                            "current_missed_rounds": missed_rounds_by_id[account_id]
                        }
                    )
                    notification_event = self.prepare_notification_event(
                        EventType(validator=event_validator),
                        tx_hash=None,
                        block_info=None,
                        impacted_addresses=[
                            ImpactedAddress(
                                address=self.complete_address(int(account_id)),
                                address_type=AddressType.validator,
                            ),
                        ],
                    )

                    self.add_notification_event_to_queue(notification_event)
            # now set the missed rounds for the next check
            self.missed_rounds_by_id = missed_rounds_by_id

    def append_impacted_address(self, impacted_address, impacted_addresses: list):
        impacted_addresses.append(
            ImpactedAddress(
                address=self.complete_address(impacted_address),
                address_type=AddressType.receiver,
            )
        )

    def update_helper(self, id: str, replacement_value: dict, net: str):
        db_to_use = (
            self.connections.mongodb.mainnet
            if net == "mainnet"
            else self.connections.mongodb.testnet
        )
        query = {"_id": id}
        db_to_use[Collections.helpers].replace_one(
            query,
            replacement_value,
            upsert=True,
        )

        if id == "bot_last_processed_block":
            self.internal_freqency_timer = dt.datetime.now().astimezone(tz=dt.timezone.utc)

    async def log_error(self, error, block: CCD_BlockComplete, caller: str):
        console.log(f"{caller} has FAILED with {error}.")
        self.exception_raised = True
        self.connections.tooter.relay(
            channel=TooterChannel.BOT,
            title="",
            chat_id=ADMIN_CHAT_ID,
            body=f"{caller} has FAILED with {error}. {block.model_dump(exclude_none=True)}",
            notifier_type=TooterType.BOT_MAIN_LOOP_ERROR,
        )

    async def process_new_blocks(self, context: ContextTypes.DEFAULT_TYPE):
        self.full_blocks_to_process: list
        try:
            while len(self.full_blocks_to_process) > 0:
                self.processing = True
                self.exception_raised = False
                block: CCD_BlockComplete = self.full_blocks_to_process.pop(0)
                self.block_count_for_specials += 1
                if NET(block.net) == NET.MAINNET:
                    try:
                        await self.process_block_for_baker(block)
                    except Exception as ex:
                        await self.log_error(ex, block, "process_block_for_baker")
                    try:
                        await self.find_events_in_block_transactions(block)
                    except Exception as ex:
                        await self.log_error(ex, block, "find_events_in_block_transactions")
                    try:
                        await self.find_events_in_block_special_events(block)
                    except Exception as ex:
                        await self.log_error(ex, block, "find_events_in_block_special_events")
                    try:
                        await self.find_events_in_logged_events(block)
                    except Exception as ex:
                        await self.log_error(ex, block, "find_events_in_logged_events")
                    try:
                        await self.check_for_specials(block)
                    except Exception as ex:
                        await self.log_error(ex, block, "check_for_specials")

                if not self.exception_raised:
                    bot_last_processed_block = {
                        "_id": "bot_last_processed_block",
                        "height": block.block_info.height,
                    }
                    self.update_helper(
                        "bot_last_processed_block",
                        bot_last_processed_block,
                        block.net,  # type: ignore
                    )
                    console.log(
                        f"Pro: {block.block_info.height:,.0f} | Remaining: {len(self.full_blocks_to_process):4,.0f} block(s)",
                        end=" | ",
                    )

            self.processing = False
        except Exception as ex:
            await self.log_error(ex, block, "process_new_blocks")  # type: ignore
            # console.log(f"process_new_blocks has FAILED with {ex}.")
            # self.connections.tooter.relay(
            #     channel=TooterChannel.BOT,
            #     title="Failed in process new blocks",
            #     chat_id=ADMIN_CHAT_ID,
            #     body=f"process_new_blocks has FAILED with {ex}. {block.model_dump(exclude_none=True)}",
            #     notifier_type=TooterType.BOT_MAIN_LOOP_ERROR,
            # )
