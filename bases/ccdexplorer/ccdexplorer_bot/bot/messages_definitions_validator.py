# ruff: noqa: F403, F405, E402, E501, F401
# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
# pyright: reportOptionalOperand=false


from ccdexplorer.env import *  # type: ignore
from ccdexplorer.grpc_client.CCD_Types import *  # type: ignore
from ccdexplorer.site_user import (
    SiteUser,
)
from rich.console import Console

from ..notification_classes import *
from .utils import Utils as Utils

console = Console()


class MessageValidator(Utils):
    def define_delegation_configured_message(
        self,
        event_type: EventTypeValidator | EventTypeAccount,
        notification_event: NotificationEvent,
        user: SiteUser,
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        delegator: ImpactedAddress = self.return_specific_address_type(
            notification_event.impacted_addresses, AddressType.delegator
        )
        validator: ImpactedAddress = self.return_specific_address_type(
            notification_event.impacted_addresses, AddressType.validator
        )
        if validator:
            message = f'Delegator <a href="https://ccdexplorer.io/mainnet/account/{delegator.address.account.id}">{delegator.label}</a> performed the following delegation action(s) on validator <a href="https://ccdexplorer.io/mainnet/account/{validator.address.account.id}">{validator.label}</a>:<br/><br/>'
        else:
            message = f'Delegator <a href="https://ccdexplorer.io/mainnet/account/{delegator.address.account.id}">{delegator.label}</a> performed the following delegation action(s):<br/><br/>'

        if isinstance(event_type, EventTypeValidator):
            previous_block_info = (
                notification_event.event_type.validator.previous_block_account_info
            )
        else:
            previous_block_info = notification_event.event_type.account.previous_block_account_info

        for event in event_type.delegation_configured.events:
            if event.delegation_removed:
                message += f"Delegation removed. Delegation ID: <code>{event.delegation_removed}</code><br/><br/>"

            elif event.delegation_stake_increased:
                if previous_block_info:
                    previous_amount = previous_block_info.staked_amount
                    if previous_amount:
                        if previous_amount > 0:
                            perc = f" ({100 * ((event.delegation_stake_increased.new_stake - previous_amount) / previous_amount):,.2f}%)"
                            # unstaked_amount = (
                            #     previous_amount - event.delegation_stake_increased.new_stake
                            # ) / 1_000_000

                    if previous_amount:
                        message += f"Stake increased{perc} to {(event.delegation_stake_increased.new_stake / 1_000_000):,.0f} CCD.<br/><br/>"
                    else:
                        message += f"Stake set to {(event.delegation_stake_increased.new_stake / 1_000_000):,.0f} CCD.<br/><br/>"
                else:
                    message += f"Stake set to {(event.delegation_stake_increased.new_stake / 1_000_000):,.0f} CCD.<br/><br/>"

            elif event.delegation_stake_decreased:
                previous_amount = previous_block_info.staked_amount
                if previous_amount:
                    if previous_amount > 0:
                        perc = f" ({(100 * (previous_amount - event.delegation_stake_decreased.new_stake) / previous_amount):,.2f}%)"

                message += f"Stake decreased{perc} to {(event.delegation_stake_decreased.new_stake / 1_000_000):,.0f} CCD.<br/><br/>"

            elif event.delegation_set_delegation_target:
                if not event.delegation_set_delegation_target.delegation_target.passive_delegation:
                    target = event.delegation_set_delegation_target.delegation_target.baker
                    message += f"Delegation target set to {target:,.0f}. <br/><br/>"
                else:
                    message += "Delegation target set to passive delegation.<br/> <br/>"

        message += f"""

{self.footer(notification_event)}
"""
        return MessageResponse(
            **{
                # "delegator_index": delegator_index,
                "title_telegram": "",
                "title_email": f"CCDExplorer Notification - Delegation Configured for {delegator.label}",
                "message_telegram": message,
                "message_email": message,
            }
        )

    def define_baker_configured_message(
        self,
        effects: CCD_AccountTransactionEffects,
        notification_event: NotificationEvent,
        user: SiteUser,
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        # label = self.find_possible_label(user, notification_event)
        previous_block_info = notification_event.event_type.validator.previous_block_validator_info
        message_to_send = ""
        for event in effects.baker_configured.events:
            if event.baker_added:
                message_to_send += f"""

<i>Validator Added</i><br/>
Restake Earnings: <code>{event.baker_added.restake_earnings}</code><br/>
Staked amount: <code>{(event.baker_added.stake / 1_000_000):,.0f} CCD</code><br/>
 <br/> 
"""

            if event.baker_removed:
                message_to_send += f"""
<i>Validator Removed</i><br/>
Validator ID: <code>{event.baker_removed}</code><br/>
 <br/> 
"""

            if event.baker_stake_increased:
                if previous_block_info:
                    previous_amount = previous_block_info.staked_amount
                    if previous_amount:
                        if previous_amount > 0:
                            perc = f"{100 * ((event.baker_stake_increased.new_stake - previous_amount) / previous_amount):,.2f}%"
                message_to_send += f"""
<i>Validator Stake Increased</i><br/>
Increase: <code>{perc}</code><br/>
New staked amount: <code>{(event.baker_stake_increased.new_stake / 1_000_000):,.0f} CCD</code><br/>
 <br/> 
"""

            if event.baker_stake_decreased:
                if previous_block_info:
                    previous_amount = previous_block_info.staked_amount
                    if previous_amount:
                        if previous_amount > 0:
                            perc = f"{100 * ((previous_amount - event.baker_stake_decreased.new_stake) / previous_amount):,.2f}%"
                message_to_send += f"""
<i>Validator Stake Decreased</i><br/>
Decrease: <code>{perc}</code><br/>
New staked amount: <code>{(event.baker_stake_decreased.new_stake / 1_000_000):,.0f} CCD</code><br/>
 <br/> 
"""

            if event.baker_restake_earnings_updated:
                message_to_send += f"""
<i>Validator Restake Earnings Updated</i><br/>
Restake Earnings: <code>{event.baker_restake_earnings_updated.restake_earnings}</code><br/>
 <br/>
"""

            if event.baker_keys_updated:
                message_to_send += """
<i>Validator Keys Updated</i><br/>
 <br/>
"""

            if event.baker_set_open_status:
                if event.baker_set_open_status.open_status == 0:
                    status = "Open for All"
                elif event.baker_set_open_status.open_status == 1:
                    status = "Closed for New"
                elif event.baker_set_open_status.open_status == 2:
                    status = "Closed for All"

                message_to_send += f"""
<i>Set Open Status</i><br/>
Status: <code>{status}</code><br/>
 <br/> 
"""

            if event.baker_set_metadata_url:
                message_to_send += f"""
<i>Set MetaDataURL</i><br/>
URL: <a href='{event.baker_set_metadata_url.url}'>{event.baker_set_metadata_url.url}</a><br/>
 <br/>
"""
            previous_string = " <br/>"
            if event.baker_set_transaction_fee_commission:
                if previous_block_info:
                    if previous_block_info.pool_info:
                        previous_commission_rates = previous_block_info.pool_info.commission_rates
                        previous_string = f"Previous Commission: <code>{previous_commission_rates.transaction * 100:,.2f}%</code><br/>"
                message_to_send += f"""
<i>Set Transaction Commission</i><br/>
Transaction Commission: <code>{event.baker_set_transaction_fee_commission.transaction_fee_commission * 100:,.2f}%</code><br/>
{previous_string}
"""

            previous_string = " <br/>"
            if event.baker_set_baking_reward_commission:
                if previous_block_info:
                    if previous_block_info.pool_info:
                        previous_commission_rates = previous_block_info.pool_info.commission_rates
                        previous_string = f"Previous Commission: <code>{previous_commission_rates.baking * 100:,.2f}%</code><br/>"
                message_to_send += f"""
<i>Set Block Commission</i><br/>
Block Commission: <code>{event.baker_set_baking_reward_commission.baking_reward_commission * 100:,.2f}%</code><br/>
{previous_string}
"""

            previous_string = " <br/>"
            if event.baker_set_finalization_reward_commission:
                if previous_block_info:
                    if previous_block_info.pool_info:
                        previous_commission_rates = previous_block_info.pool_info.commission_rates
                        previous_string = f"Previous Commission: <code>{previous_commission_rates.finalization * 100:,.2f}%</code><br/>"
                message_to_send += f"""
                  
<i>Set Finalization Reward Commission</i><br/>
Finalization Reward Commission: <code>{event.baker_set_finalization_reward_commission.finalization_reward_commission * 100:,.2f}%</code><br/>
{previous_string}
"""

        message_to_send += f"""

{self.footer(notification_event)}
"""

        validator: ImpactedAddress = self.return_specific_address_type(
            notification_event.impacted_addresses, AddressType.validator
        )
        return MessageResponse(
            **{
                # "delegator_index": delegator_index,
                "title_telegram": f"Validator {validator.label} Configured",
                "title_email": f"CCDExplorer Notification - Validator {validator.label} Configured",
                "message_telegram": message_to_send,
                "message_email": message_to_send,
            }
        )

    def define_block_baked_by_baker_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        pi = notification_event.event_type.validator.current_block_pool_info
        expectation = pi.current_payday_info.lottery_power * self.payday_last_blocks_validated
        tally = pi.current_payday_info.blocks_baked
        validator: ImpactedAddress = self.return_specific_address_type(
            notification_event.impacted_addresses, AddressType.validator
        )
        telegram_message = f"""

Validator <code>{validator.label}</code> produced the <a href="https://ccdexplorer.io/mainnet/block/{notification_event.block_hash}">block</a> ({tally} / {expectation:,.0f}) at height: <code>{notification_event.block_height:,.0f}</code>.
 
Earliest possible next block production in {self.verbose_timedelta(notification_event.event_type.validator.earliest_win_time - dt.datetime.now().astimezone(dt.timezone.utc))}.<br/>
"""

        telegram_message += f"""

{self.footer(notification_event)}
"""
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": f"CCDExplorer Notification - Block produced by Validator {validator.label}",
                "message_telegram": telegram_message,
                "message_email": telegram_message,
            }
        )

    def define_validator_running_behind_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        validator: ImpactedAddress = self.return_specific_address_type(
            notification_event.impacted_addresses, AddressType.validator
        )
        telegram_message = f"""

Validator <code>{validator.label}</code> seems to be running behind <code>{notification_event.event_type.validator.validator_running_behind:,.0f} </code> blocks.<br/>

"""

        telegram_message += f"""

{self.footer(notification_event)}
"""
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": "CCDExplorer Notification - Validator running behind",
                "message_telegram": telegram_message,
                "message_email": telegram_message,
            }
        )

    def define_validator_primed_for_suspension_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        validator: ImpactedAddress = self.return_specific_address_type(
            notification_event.impacted_addresses, AddressType.validator
        )
        telegram_message = f"""

Validator <code>{validator.label}</code> is primed for suspension.<br/>

"""

        telegram_message += f"""

{self.footer(notification_event)}
"""
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": "CCDExplorer Notification - Validator is primed for suspension",
                "message_telegram": telegram_message,
                "message_email": telegram_message,
            }
        )

    def define_validator_suspended_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        validator: ImpactedAddress = self.return_specific_address_type(
            notification_event.impacted_addresses, AddressType.validator
        )
        telegram_message = f"""

Validator <code>{validator.label}</code> is suspended.<br/>

"""

        telegram_message += f"""

{self.footer(notification_event)}
"""
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": "CCDExplorer Notification - Validator is suspended",
                "message_telegram": telegram_message,
                "message_email": telegram_message,
            }
        )

    def define_validator_missed_rounds_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)

        validator: ImpactedAddress = self.return_specific_address_type(
            notification_event.impacted_addresses, AddressType.validator
        )
        message = f"Validator <a href='https://ccdexplorer.io/mainnet/account/{validator.address.account.id}'>{validator.label}</a> missed rounds now at <code>{notification_event.event_type.validator.validator_missed_rounds['current_missed_rounds']}</code>.<br/>"

        message += f"""

        {self.footer(notification_event)}
        """
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": ("CCDExplorer Notification - Validator missed rounds"),
                "message_telegram": message,
                "message_email": message,
            }
        )

    def define_payday_pool_reward_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        self.connections: Connections
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        impacted_address = notification_event.impacted_addresses[0]
        event_type = notification_event.event_type.validator
        payday_account_reward = event_type.corresponding_account_reward
        pool_info = event_type.pool_info

        total_amount = (
            event_type.payday_pool_reward.baker_reward
            + event_type.payday_pool_reward.transaction_fees
            + event_type.payday_pool_reward.finalization_reward
        )

        account_share_of_reward = (
            payday_account_reward.baker_reward
            + payday_account_reward.transaction_fees
            + payday_account_reward.finalization_reward
        )

        pool_message = ""
        pool_message += f"<code>{event_type.payday_pool_reward.baker_reward / 1_000_000:15,.6f} CCD</code> (Blocks)<br/>"
        pool_message += f"<code>{event_type.payday_pool_reward.transaction_fees / 1_000_000:15,.6f} CCD</code> (Tx fees)<br/>"
        # pool_message += f"<code>{event_type.payday_pool_reward.finalization_reward/1_000_000:15,.6f} CCD</code> (Finalization)<br/>"
        pool_message += "\nDistribution to validator and delegators:\n\n"
        pool_message += (
            f"<code>{((account_share_of_reward) / 1_000_000):15,.6f} CCD</code> (Validator)<br/>"
        )
        pool_message += f"<code>{((total_amount - account_share_of_reward) / 1_000_000):15,.6f} CCD</code> (Delegators)<br/>"
        pool_message += f"<code>{(total_amount / 1_000_000):15,.6f} CCD</code> (Total)\n"

        ###
        table_message = """
        <table border="0">
        <tr>
            <th></th>
            <th style="text-align:right;">Amount (CCD)</th>
        </tr>
        <tr>
            <td>Blocks</td>
            <td style="text-align:right;"><code>{:,.6f}</code></td>
        </tr>
        <tr>
            <td>Tx fees</td>
            <td style="text-align:right;"><code>{:,.6f}</code></td>
        </tr>
        
        <tr>
            <td colspan="2"><i>Distribution to validator and delegators:</i></td>
        </tr>
        <tr>
            <td>Validator</td>
            <td style="text-align:right;"><code>{:,.6f}</code></td>
        </tr>
        <tr>
            <td>Delegators</td>
            <td style="text-align:right;"><code>{:,.6f}</code></td>
        </tr>
        <tr>
            <td>Total</td>
            <td style="text-align:right;"><code>{:,.6f}</code></td>
        </tr>
        </table>
        """

        # Format the values into the table
        html_table = table_message.format(
            event_type.payday_pool_reward.baker_reward / 1_000_000,
            event_type.payday_pool_reward.transaction_fees / 1_000_000,
            # event_type.payday_pool_reward.finalization_reward / 1_000_000,
            account_share_of_reward / 1_000_000,
            (total_amount - account_share_of_reward) / 1_000_000,
            total_amount / 1_000_000,
        )

        # Now `html_table` contains the HTML code for the table with the values formatted
        # You can use `html_table` wherever you need in your HTML document.

        ###
        ending = "" if pool_info.current_payday_info.blocks_baked == 1 else "s"

        message = (
            f"Payday: <code>{notification_event.block_slot_time:%Y-%m-%d}</code> (<code>{pool_info.current_payday_info.blocks_baked}</code> block{ending})\n\nReward payout of <code>{(total_amount / 1_000_000):,.6f} CCD</code> for validator <a href='https://ccdexplorer.io/mainnet/account/{impacted_address.address.account.id}'>{impacted_address.label}</a>: <br/><br/>"
            + pool_message
        )
        message += f"""

{self.footer(notification_event)}
"""
        message_email = (
            f"Payday: <code>{notification_event.block_slot_time:%Y-%m-%d}</code> (<code>{pool_info.current_payday_info.blocks_baked}</code> block{ending})<br/>Reward payout of <code>{(total_amount / 1_000_000):,.6f} CCD</code> for validator <a href='https://ccdexplorer.io/mainnet/account/{impacted_address.address.account.id}'>{impacted_address.label}</a>: <br/><br/>"
            + html_table
        )
        message_email += f"""

{self.footer(notification_event)}
"""

        validator: ImpactedAddress = self.return_specific_address_type(
            notification_event.impacted_addresses, AddressType.validator
        )
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": f"CCDExplorer Notification - Payday {notification_event.block_slot_time:%Y-%m-%d} Pool Reward for Validator {validator.label}",
                "message_telegram": message,
                "message_email": message_email,  # f"""In the block at height {event.block_height:,.0f} account <a href='https://ccdexplorer.io/mainnet/account/{event.account_id}'>{label}</a> was affected by a <a href='https://ccdexplorer.io/mainnet/transaction/{event.tx_hash}'>transaction</a>.""",
            }
        )
