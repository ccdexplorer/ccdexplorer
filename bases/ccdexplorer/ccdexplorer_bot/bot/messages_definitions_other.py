# ruff: noqa: F403, F405, E402, E501, F401
# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false


from ccdexplorer.env import *  # type: ignore
from ccdexplorer.grpc_client.CCD_Types import *  # type: ignore
from ccdexplorer.site_user import (
    SiteUser,
)
from rich.console import Console

from ..notification_classes import *
from .utils import Utils as Utils

console = Console()


class MessageOther(Utils):
    def define_protocol_update_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        event_type = notification_event.event_type.other
        message_telegram = f"""
<i>{event_type.protocol_update.message_}</i>
Further info <a href='{event_type.protocol_update.specification_url}'>here</a>.
"""
        message_telegram += f"""

{self.footer(notification_event)}
"""
        return MessageResponse(
            **{
                "title_telegram": "Protocol Update",
                "title_email": "CCDExplorer Notification - Protocol Update",
                "message_telegram": message_telegram,
                "message_email": f"""In the block at height {notification_event.block_height:,.0f} a protocol update was issued with the following message:

            {event_type.protocol_update.message_} 

            Further info can be found at the following URL: 
            {event_type.protocol_update.specification_url}.
                            """,
            }
        )

    def define_add_anonymity_revoker_update_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        e = notification_event.event_type.other
        message_telegram = f"""
Name: {e.add_anonymity_revoker_update.description.name}<br/>
URL: <a href='{e.add_anonymity_revoker_update.description.url}'>{e.add_anonymity_revoker_update.description.url}</a><br/>
Description: {e.add_anonymity_revoker_update.description.description}<br/>
"""
        message_telegram += f"""

{self.footer(notification_event)}
"""
        return MessageResponse(
            **{
                "title_telegram": "Anonymity Revoker Added",
                "title_email": "CCDExplorer Notification - Anonymity Revoker Added",
                "message_telegram": message_telegram,
                "message_email": message_telegram,
            }
        )

    def define_add_identity_provider_update_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        e = notification_event.event_type.other
        message_telegram = f"""
Name: {e.add_identity_provider_update.description.name}<br/>
URL: <a href='{e.add_identity_provider_update.description.url}'>{e.add_identity_provider_update.description.url}</a><br/>
Description: {e.add_identity_provider_update.description.description}<br/>
"""
        message_telegram += f"""

{self.footer(notification_event)}
"""
        return MessageResponse(
            **{
                "title_telegram": "Identity Provider Added",
                "title_email": "CCDExplorer Notification - Identity Provider Added",
                "message_telegram": message_telegram,
                "message_email": message_telegram,
            }
        )

    def define_commission_changed_message(
        self,
        events: list[CCD_BakerEvent],
        notification_event: NotificationEvent,
        user: SiteUser,
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        # label = self.find_possible_label(user, notification_event)
        previous_block_info = notification_event.event_type.other.previous_block_validator_info
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
                "title_telegram": f"Validator {validator.label} Set Commission Rates",
                "title_email": f"CCDExplorer Notification - Validator {validator.label} Set Commission Rates",
                "message_telegram": message_to_send,
                "message_email": message_to_send,
            }
        )

    def define_other_lowered_stake_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        lowered_stake_object = notification_event.event_type.other.validator_lowered_stake
        validator: ImpactedAddress = self.return_specific_address_type(
            notification_event.impacted_addresses, AddressType.validator
        )
        message = ""

        if lowered_stake_object.baker_removed:
            message = f"Validator Account <a href='https://ccdexplorer.io/mainnet/account/{validator.address.account.id}'>{validator.label}</a> removed its validator and unstaked <code>{(lowered_stake_object.unstaked_amount) / 1_000_000:,.0f} CCD</code>.<br/>"
        else:
            message = f"Validator Account <a href='https://ccdexplorer.io/mainnet/account/{validator.address.account.id}'>{validator.label}</a> decreased its stake with <code>{(lowered_stake_object.unstaked_amount) / 1_000_000:,.0f} CCD ({(100 * lowered_stake_object.percentage_unstaked):,.2f}%)</code> to <code>{(lowered_stake_object.new_stake / 1_000_000):,.0f} CCD</code>.<br/>"

        message += f"""

        {self.footer(notification_event)}
        """
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": (
                    "CCDExplorer Notification - Validator Unstaked"
                    if not lowered_stake_object.baker_removed
                    else "CCDExplorer Notification - Validator Removed"
                ),
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
        return MessageResponse(
            **{
                "title_telegram": "Module Deployed",
                "title_email": "CCDExplorer Notification - Module Deployed",
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
        return MessageResponse(
            **{
                "title_telegram": "Contract Initialized",
                "title_email": "CCDExplorer Notification - Contract Initialized",
                "message_telegram": message_telegram,
                "message_email": message_email,
            }
        )

    def define_account_transfer_message_for_other(
        self,
        event_type: EventTypeOther,
        notification_event: NotificationEvent,
        user: SiteUser,
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        if user.other_notification_preferences.account_transfer.telegram:
            telegram_notification_limit = (
                user.other_notification_preferences.account_transfer.telegram.limit / 1_000_000
                if user.other_notification_preferences.account_transfer.telegram.limit
                else 0
            )
        else:
            telegram_notification_limit = 0

        if user.other_notification_preferences.account_transfer.email:
            email_notification_limit = (
                user.other_notification_preferences.account_transfer.email.limit / 1_000_000
                if user.other_notification_preferences.account_transfer.email.limit
                else 0
            )
        else:
            email_notification_limit = 0

        message_telegram = f'An account transfer <a href="https://ccdexplorer.io/mainnet/transaction/{notification_event.tx_hash}">transaction</a> was done with amount: {(event_type.account_transfer.amount) / 1_000_000:,.0f} CCD<br/>'
        message_telegram += f"""

{self.footer(notification_event, telegram_notification_limit)}
"""

        message_email = f'An account transfer <a href="https://ccdexplorer.io/mainnet/transaction/{notification_event.tx_hash}">transaction</a> was done with amount: {(event_type.account_transfer.amount) / 1_000_000:,.0f} CCD<br/>'
        message_email += f"""

{self.footer(notification_event, email_notification_limit)}
"""
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": "CCDExplorer Notification - Account Transfer",
                "message_telegram": message_telegram,
                "message_email": message_email,
            }
        )

    def define_transferred_with_schedule_message_for_other(
        self,
        event_type: EventTypeOther,
        notification_event: NotificationEvent,
        user: SiteUser,
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        if user.other_notification_preferences.transferred_with_schedule.telegram:
            telegram_notification_limit = (
                user.other_notification_preferences.transferred_with_schedule.telegram.limit
                / 1_000_000
                if user.other_notification_preferences.transferred_with_schedule.telegram.limit
                else 0
            )
        else:
            telegram_notification_limit = 0

        sum_amount = 0
        message_schedule = "<br/>"
        for new_release in event_type.transferred_with_schedule.amount:
            message_schedule += f"<code>{new_release.timestamp:%Y-%m-%d %H:%M}  {(new_release.amount / 1_000_000):8,.0f} CCD </code>\n"
            sum_amount += new_release.amount

        message_telegram = f'A scheduled account transfer <a href="https://ccdexplorer.io/mainnet/transaction/{notification_event.tx_hash}">transaction</a> was done with amount: {(sum_amount) / 1_000_000:,.0f} CCD\n'
        message_telegram += message_schedule
        message_telegram += f"""

{self.footer(notification_event, telegram_notification_limit)}
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
        for new_release in event_type.transferred_with_schedule.amount:
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
                "title_telegram": "",
                "title_email": "CCDExplorer Notification - Scheduled Account Transfer",
                "message_telegram": message_telegram,
                "message_email": html_release_table,
            }
        )

    def define_domain_name_minted_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        message_telegram = f"The CCD domain <code>{notification_event.event_type.other.domain_name_minted}</code> was minted."
        message_telegram += f"""

{self.footer(notification_event)}
"""
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": "CCDExplorer Notification - CCD Domain minted",
                "message_telegram": message_telegram,
                "message_email": message_telegram,
            }
        )

    def define_account_created_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        message_telegram = "A new Concordium account is created. <br/>"
        message_telegram += (
            f"Address: <code>{notification_event.event_type.other.account_created}</code><br/>"
        )
        message_telegram += f"""

    {self.footer(notification_event)}
    """
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": "CCDExplorer Notification - Account Created",
                "message_telegram": message_telegram,
                "message_email": message_telegram,
            }
        )

    def define_validator_primed_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)

        validator: ImpactedAddress = self.return_specific_address_type(
            notification_event.impacted_addresses, AddressType.validator
        )
        message = f"Validator <a href='https://ccdexplorer.io/mainnet/account/{validator.address.account.id}'>{validator.label}</a> is primed for suspension.<br/>"

        message += f"""

        {self.footer(notification_event)}
        """
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": ("CCDExplorer Notification - Validator Primed for Suspension"),
                "message_telegram": message,
                "message_email": message,
            }
        )

    def define_validator_suspended_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)

        validator: ImpactedAddress = self.return_specific_address_type(
            notification_event.impacted_addresses, AddressType.validator
        )
        message = f"Validator <a href='https://ccdexplorer.io/mainnet/account/{validator.address.account.id}'>{validator.label}</a> is suspended.<br/>"

        message += f"""

        {self.footer(notification_event)}
        """
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": ("CCDExplorer Notification - Validator Suspended"),
                "message_telegram": message,
                "message_email": message,
            }
        )

    def define_validator_node_version_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)

        validator: ImpactedAddress = self.return_specific_address_type(
            notification_event.impacted_addresses, AddressType.validator
        )
        message = f"Concordium Validator <a href='https://ccdexplorer.io/mainnet/account/{validator.address.account.id}'>{validator.label}</a> has just upgraded its node to version {notification_event.event_type.other.validator_node_version_upgrade}.<br/>"

        message += f"""

        {self.footer(notification_event)}
        """
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": (
                    "CCDExplorer Notification - Concordium Validator node version upgrade"
                ),
                "message_telegram": message,
                "message_email": message,
            }
        )

    def define_token_creation_message(
        self, notification_event: NotificationEvent, user: SiteUser
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)

        message = f"""
        A new Protocol-Level Token (PLT) was created.<br/><br/>
        Name: {notification_event.event_type.other.token_creation.initialization_parameters.name}<br/>
        Symbol: {notification_event.event_type.other.token_creation.token_id}<br/>
        """  # type: ignore

        message += f"""

        {self.footer(notification_event)}
        """
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": ("CCDExplorer Notification - PLT Token Creation"),
                "message_telegram": message,
                "message_email": message,
            }
        )
