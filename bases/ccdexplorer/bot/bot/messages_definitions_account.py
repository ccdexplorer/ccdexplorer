# ruff: noqa: F403, F405, E402, E501, F401
# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false

import math

from ccdexplorer.domain.mongo import MongoTypeTokensTag
from ccdexplorer.env import *  # type: ignore
from ccdexplorer.grpc_client.CCD_Types import *  # type: ignore
from ccdexplorer.site_user import (
    AccountForUser,
    SiteUser,
)
from rich.console import Console

from ..notification_classes import *
from .utils import Utils as Utils

console = Console()


class MessageAccount(Utils):
    # this is located in the validator section
    # def define_delegation_configured_message(
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

            elif event.delegation_set_restake_earnings:
                message += f"Restake earnings: <code>{event.delegation_set_restake_earnings.restake_earnings}</code>.<br/> <br/>"
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

    def define_module_deployed_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        event_type = (
            notification_event.event_type.validator
            if notification_event.event_type.validator is not None
            else notification_event.event_type.other
        )
        message = f"""
             
Module: <a href="/mainnet/module/{event_type.module_deployed}">{event_type.module_deployed}</a><br/>

"""
        message += f"""

{self.footer(notification_event)}
"""
        impacted_address = notification_event.impacted_addresses[0]
        return MessageResponse(
            **{
                "title_telegram": "Module Deployed",
                "title_email": f"CCDExplorer Notification - Account {impacted_address.label} Deployed a new Module",
                "message_telegram": message,
                "message_email": message,
            }
        )

    def define_contract_initialized_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        event_type = (
            notification_event.event_type.account
            if notification_event.event_type.account is not None
            else notification_event.event_type.other
        )
        message_telegram = f"""
              
Contract: <code>{event_type.contract_initialized.address.index}</code><br/>
Initializer: {event_type.contract_initialized.init_name}<br/>
Module: <a href="/mainnet/module/{event_type.contract_initialized.origin_ref}">{event_type.contract_initialized.origin_ref[:10]}</a><br/>
"""
        message_telegram += f"""

{self.footer(notification_event)}
"""

        message_email = message_telegram
        impacted_address = notification_event.impacted_addresses[0]
        return MessageResponse(
            **{
                "title_telegram": "Contract Initialized",
                "title_email": f"CCDExplorer Notification - Account {impacted_address.label} Initialized a Smart Contract",
                "message_telegram": message_telegram,
                "message_email": message_email,
            }
        )

    def define_data_registered_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        e = notification_event.event_type.account
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        message_telegram = f"""
Data registered: <code>{e.data_registered}</code>
{self.footer(notification_event)}


"""
        message_email = message_telegram
        impacted_address = notification_event.impacted_addresses[0]
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": f"CCDExplorer Notification - Account {impacted_address.label} Registered Data",
                "message_telegram": message_telegram,
                "message_email": message_email,
            }
        )

    def define_account_transfer_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        impacted_address = notification_event.impacted_addresses[0]
        amount = notification_event.event_type.account.account_transfer.amount / 1_000_000
        message_telegram = f"Account <a href='https://ccdexplorer.io/mainnet/account/{impacted_address.address.account.id}'>{impacted_address.label}</a> was affected by a transfer <a href='https://ccdexplorer.io/mainnet/transaction/{notification_event.tx_hash}'>transaction</a> with amount: {amount:,.0f} CCD.<br/>"
        message_telegram += f"""

{self.footer(notification_event)}
"""
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": f"CCDExplorer Notification - Account Transfer for {impacted_address.label}",
                "message_telegram": message_telegram,
                "message_email": message_telegram,
            }
        )

    def define_token_event_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        result = notification_event.event_type.account.token_event.result
        token_name = notification_event.event_type.account.token_event.token_name
        event_type = ""
        if result.tag == 255:
            event_type = "transfer"
        elif result.tag == 254:
            event_type = "mint"
        elif result.tag == 253:
            event_type = "burn"

        contract_address = notification_event.event_type.account.token_event.token_address.split(
            "-"
        )[0]
        contract_info = self.contracts_with_tag_info.get(contract_address)

        if contract_info:
            decimals = contract_info.decimals
            contract_info: MongoTypeTokensTag
            if not decimals:
                amount = result.token_amount
            else:
                amount = self.token_amount_using_decimals(result.token_amount, decimals)

            amount_string = (
                f"Amount: <code>{amount}</code> {contract_info.id}"
                if contract_info.token_type == "fungible"
                else ""
            )
            token_name_string = f"Name: {token_name}" if token_name else ""
            token_string = f"""
Token: {contract_info.display_name}<br/>
{amount_string}<br/>
{token_name_string}<br/>
"""
        message_telegram = f"""Account {notification_event.impacted_addresses[0].label} was affected by a CIS token <code>{event_type}</code> event:<br/>
{token_string}<br/>
"""
        message_telegram += f"""

{self.footer(notification_event)}
"""
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": f"CCDExplorer Notification - Account {notification_event.impacted_addresses[0].label} was affected by a CIS token {event_type} event",
                "message_telegram": message_telegram,
                "message_email": message_telegram,
            }
        )

    def define_transferred_with_schedule_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        e = notification_event.event_type.account

        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        impacted_address = notification_event.impacted_addresses[0]

        message_telegram = """
<i>Scheduled Transfer</i><br/>
"""
        for new_release in e.transferred_with_schedule.amount:
            message_telegram += f"<code>{new_release.timestamp:%Y-%m-%d %H:%M}  {(new_release.amount / 1_000_000):8,.0f} CCD </code>\n"
        message_telegram += f"""

{self.footer(notification_event)}
"""
        ###
        release_table_message = """
        <table border="0">
        <tr>
            <th>Date</th>
            <th style="text-align:right;">Amount (CCD)</th>
        </tr>
        {}
        </table>
        """

        # Format individual release rows
        release_rows = ""
        for new_release in e.transferred_with_schedule.amount:
            release_rows += (
                "<tr>"
                f"<td><code>{new_release.timestamp:%Y-%m-%d %H:%M}</code></td>"
                f"<td style='text-align:right;'><code>{new_release.amount / 1_000_000:,.0f} CCD</code></td>"
                "</tr>"
            )

        # Construct the full HTML message
        html_release_table = """
<i>Scheduled Transfer</i><br/>
"""
        html_release_table += release_table_message.format(release_rows)
        html_release_table += f"""

{self.footer(notification_event)}
"""
        ###
        return MessageResponse(
            **{
                "title_telegram": f"Scheduled Account Transfer for {impacted_address.label}",
                "title_email": f"CCDExplorer Notification - Scheduled Account Transfer for {impacted_address.label}",
                "message_telegram": message_telegram,
                "message_email": html_release_table,
            }
        )

    def define_payday_account_reward_message(
        self,
        event_type: EventTypeAccount,
        notification_event: NotificationEvent,
        user: SiteUser,
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        impacted_address = notification_event.impacted_addresses[0]
        account_message = ""
        account_message += f"<code>{event_type.payday_account_reward.baker_reward / 1_000_000:15,.6f} CCD</code> (Blocks)<br/>"

        account_message += f"<code>{event_type.payday_account_reward.transaction_fees / 1_000_000:15,.6f} CCD</code> (Tx fees)<br/>"
        # account_message += f"<code>{event_type.payday_account_reward.finalization_reward/1_000_000:15,.6f} CCD</code> (Finalization)<br/>"
        total_amount = (
            event_type.payday_account_reward.baker_reward / 1_000_000
            + event_type.payday_account_reward.transaction_fees / 1_000_000
            + event_type.payday_account_reward.finalization_reward / 1_000_000
        )
        message = (
            f"Payday: <code>{notification_event.block_slot_time:%Y-%m-%d}</code>\n\nReward payout of <code>{total_amount:,.6f} CCD</code> for account <a href='https://ccdexplorer.io/mainnet/account/{impacted_address.address.account.id}'>{impacted_address.label}</a>: \n\n"
            + account_message
        )
        message += f"""

{self.footer(notification_event)}
"""
        ###
        account_table_message = """
        <table border="0">
        <tr>
            <th>Category</th>
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
        
        </table>
        """

        # Calculate the total amount
        total_amount = (
            event_type.payday_account_reward.baker_reward / 1_000_000
            + event_type.payday_account_reward.transaction_fees / 1_000_000
            + event_type.payday_account_reward.finalization_reward / 1_000_000
        )

        # Format the values into the table
        html_account_table = account_table_message.format(
            event_type.payday_account_reward.baker_reward / 1_000_000,
            event_type.payday_account_reward.transaction_fees / 1_000_000,
            # event_type.payday_account_reward.finalization_reward / 1_000_000,
        )

        # Construct the full HTML message
        html_message = (
            f"Payday: <code>{notification_event.block_slot_time:%Y-%m-%d}</code><br/>"
            f"Reward payout of <code>{total_amount:,.6f} CCD</code> for account "
            f"<a href='https://ccdexplorer.io/mainnet/account/{impacted_address.address.account.id}'>{impacted_address.label}</a>: \n\n"
            + html_account_table
        )

        ###
        html_message += f"""

{self.footer(notification_event)}
"""

        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": f"CCDExplorer Notification - Payday {notification_event.block_slot_time:%Y-%m-%d} Account Reward for {impacted_address.label}",
                "message_telegram": message,
                "message_email": html_message,  # f"""In the block at height {event.block_height:,.0f} account <a href='https://ccdexplorer.io/mainnet/account/{event.account_id}'>{label}</a> was affected by a <a href='https://ccdexplorer.io/mainnet/transaction/{event.tx_hash}'>transaction</a>.""",
            }
        )

    def define_validator_target_commission_changed_message(
        self,
        events: list[CCD_BakerEvent],
        notification_event: NotificationEvent,
        user: SiteUser,
        user_account: AccountForUser,
    ) -> MessageResponse:
        # Add the user account as impacted address as delegator
        # notification_event.impacted_addresses.append(
        #     ImpactedAddress(
        #         address=self.complete_address(user_account.account_index),
        #         address_type=AddressType.delegator,
        #     )
        # )
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        # label = self.find_possible_label(user, notification_event)
        previous_block_info = notification_event.event_type.account.previous_block_validator_info
        message_to_send = ""
        for event in events:
            previous_string = "<br/>"
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

            previous_string = "<br/>"
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

            previous_string = "<br/>"
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
                "title_telegram": f"Validator {validator.label} (delegation target) Set Commission Rates",
                "title_email": f"CCDExplorer Notification - Validator {validator.label} (delegation target) Set Commission Rates",
                "message_telegram": message_to_send,
                "message_email": message_to_send,
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

    def define_token_update_effect_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        token_event = notification_event.event_type.account.token_update_effect
        assert token_event is not None
        amount_string = ""
        formatted_amount = ""
        event_type = ""
        if token_event.mint_event:
            event_type = "mint"
            amount = int(token_event.mint_event.amount.value) * (
                math.pow(10, -token_event.mint_event.amount.decimals)
            )
            formatted_amount = f"{amount:,.{token_event.mint_event.amount.decimals}f}"
        elif token_event.burn_event:
            event_type = "burn"
            amount = int(token_event.burn_event.amount.value) * (
                math.pow(10, -token_event.burn_event.amount.decimals)
            )
            formatted_amount = f"{amount:,.{token_event.burn_event.amount.decimals}f}"
        elif token_event.transfer_event:
            event_type = "transfer"
            amount = int(token_event.transfer_event.amount.value) * (
                math.pow(10, -token_event.transfer_event.amount.decimals)
            )
            formatted_amount = f"{amount:,.{token_event.transfer_event.amount.decimals}f}"

        amount_string = f"Amount: <code>{formatted_amount}</code> {token_event.token_id}"

        message_telegram = f"""Account {notification_event.impacted_addresses[0].label} was affected in a PLT transaction<br/><br/>Token: <a href='https://ccdexplorer.io/mainnet/tokens/{token_event.token_id}'>{token_event.token_id}</a><br/>Event: <code>{event_type}</code><br/>
{amount_string}<br/>
"""
        message_telegram += f"""

{self.footer(notification_event)}
"""
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": f"CCDExplorer Notification - Account {notification_event.impacted_addresses[0].label} was affected by a Protocol-Level token {event_type} event",
                "message_telegram": message_telegram,
                "message_email": message_telegram,
            }
        )
