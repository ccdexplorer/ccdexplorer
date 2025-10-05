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
from ccdexplorer.site_user import SiteUser, AccountForUser
from rich.console import Console

from ..notification_classes import *
from .utils import Utils as Utils

from .messages_definitions_account import MessageAccount as MessageAccount

console = Console()


class ProcessAccount(MessageAccount, Utils):
    def process_event_type_account(self, user: SiteUser, notification_event: NotificationEvent):
        message_response = None
        notification_services_to_send = None
        event_type = EventTypeAccount(
            **notification_event.event_type.model_dump()[
                list(notification_event.event_type.model_fields_set)[0]
            ]
        )
        account_index = (
            notification_event.impacted_addresses[0].address.account.index
            if notification_event.impacted_addresses[0].address.account
            else None
        )
        if user.accounts.get(str(account_index)):
            user_account: AccountForUser = user.accounts[str(account_index)]

        else:
            return None, None

        if not user_account.account_notification_preferences:
            return None, None

        if event_type.module_deployed:
            notification_services_to_send = self.set_notification_service(
                user_account.account_notification_preferences.module_deployed
            )
            if any(notification_services_to_send.values()):
                message_response = self.define_module_deployed_message(notification_event, user)

        if event_type.contract_initialized:
            notification_services_to_send = self.set_notification_service(
                user_account.account_notification_preferences.contract_initialized
            )
            if any(notification_services_to_send.values()):
                message_response = self.define_contract_initialized_message(
                    notification_event, user
                )

        if event_type.account_transfer:
            notification_services_to_send = self.set_notification_service(
                user_account.account_notification_preferences.account_transfer,
                event_type.account_transfer.amount,
            )

            if any(notification_services_to_send.values()):
                message_response = self.define_account_transfer_message(notification_event, user)

        if event_type.transferred_with_schedule:
            scheduled_send_amount = sum(
                [int(x.amount) for x in event_type.transferred_with_schedule.amount]
            )
            notification_services_to_send = self.set_notification_service(
                user_account.account_notification_preferences.transferred_with_schedule,
                scheduled_send_amount,
            )

            if any(notification_services_to_send.values()):
                message_response = self.define_transferred_with_schedule_message(
                    notification_event, user
                )

        if event_type.delegation_configured:
            notification_services_to_send = self.set_notification_service(
                user_account.account_notification_preferences.delegation_configured
            )
            if any(notification_services_to_send.values()):
                message_response = self.define_delegation_configured_message(
                    event_type, notification_event, user
                )

        if event_type.data_registered:
            notification_services_to_send = self.set_notification_service(
                user_account.account_notification_preferences.data_registered
            )
            if any(notification_services_to_send.values()):
                message_response = self.define_data_registered_message(notification_event, user)

        if event_type.payday_account_reward:
            notification_services_to_send = self.set_notification_service(
                user_account.account_notification_preferences.payday_account_reward
            )
            if any(notification_services_to_send.values()):
                message_response = self.define_payday_account_reward_message(
                    event_type, notification_event, user
                )

        if event_type.token_event:
            notification_services_to_send = self.set_notification_service(
                user_account.account_notification_preferences.token_event
            )
            if any(notification_services_to_send.values()):
                message_response = self.define_token_event_message(notification_event, user)

        if event_type.validator_commission_changed:
            if (
                user_account.delegation_target
                == event_type.validator_commission_changed.validator_id
            ):
                notification_services_to_send = self.set_notification_service(
                    user_account.account_notification_preferences.validator_commission_changed
                )
                if any(notification_services_to_send.values()):
                    message_response = self.define_validator_target_commission_changed_message(
                        event_type.validator_commission_changed.events,
                        notification_event,
                        user,
                        user_account,
                    )

        if event_type.validator_primed_for_suspension:
            notification_services_to_send = self.set_notification_service(
                user_account.account_notification_preferences.validator_primed_for_suspension
            )

            if any(notification_services_to_send.values()):
                message_response = self.define_validator_primed_for_suspension_message(
                    notification_event, user
                )

        if event_type.validator_suspended:
            notification_services_to_send = self.set_notification_service(
                user_account.account_notification_preferences.validator_suspended
            )

            if any(notification_services_to_send.values()):
                message_response = self.define_validator_suspended_message(notification_event, user)

        if event_type.token_update_effect:
            notification_services_to_send = self.set_notification_service(
                user_account.account_notification_preferences.token_update_effect
            )

            if any(notification_services_to_send.values()):
                message_response = self.define_token_update_effect_message(notification_event, user)

        return message_response, notification_services_to_send
