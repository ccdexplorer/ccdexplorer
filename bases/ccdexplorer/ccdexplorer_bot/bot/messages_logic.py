# ruff: noqa: F403, F405, E402, E501, F401
# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
# pyright: reportOptionalOperand=false
# pyright: reportOptionalIterable=false


import asyncio
import math
import sys

from ccdexplorer.celery_app import app as celery_app
from ccdexplorer.env import *  # type: ignore
from ccdexplorer.grpc_client.CCD_Types import *  # type: ignore
from ccdexplorer.mongodb import CollectionsUtilities
from ccdexplorer.site_user import (
    NotificationServices,
    SiteUser,
)
from ccdexplorer.tooter import TooterChannel, TooterType
from pymongo import ReplaceOne
from rich.console import Console
from telegram.ext import ContextTypes

from ..notification_classes import *
from .messages_definitions_validator import MessageValidator as MessageValidator
from .messages_logic_account import ProcessAccount as ProcessAccount
from .messages_logic_contract import ProcessContract as ProcessContract
from .messages_logic_other import ProcessOther as ProcessOther
from .messages_logic_validator import ProcessValidator as ProcessValidator
from .utils import Utils as Utils

console = Console()


class Mixin(ProcessContract, ProcessAccount, ProcessValidator, ProcessOther, Utils):
    # TODO make lines the same
    line_top = "___________________________________"
    line_bottom = "\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e\u203e"

    def token_amount_using_decimals(self, value: int, decimals: int = None):
        if not decimals:
            return f"{value}"

        return f"{(value * (math.pow(10, -decimals))):,.6f}"

    def footer(
        self,
        notification_event: NotificationEvent,
        notification_limit: int = None,
    ):
        if notification_event.event_type.account:
            pre_event = "Account - "
            event_type: str = list(
                EventTypeAccount(
                    **notification_event.event_type.model_dump(exclude_none=True)[
                        list(notification_event.event_type.model_fields_set)[0]
                    ]
                ).model_fields_set
            )[0]
            if event_type == "previous_block_validator_info":
                event_type = "validator_commission_changed"
            if event_type == "previous_block_account_info":
                event_type = "delegation_configured"
        if notification_event.event_type.validator:
            pre_event = "Validator - "
            event_type: str = list(
                EventTypeValidator(
                    **notification_event.event_type.model_dump(exclude_none=True)[
                        list(notification_event.event_type.model_fields_set)[0]
                    ]
                ).model_fields_set
            )[0]
            if event_type in [
                "earliest_win_time",
                "current_block_pool_info",
                "block_baked_by_baker",
            ]:
                event_type = "block_validated"
            if event_type == "previous_block_account_info":
                event_type = "delegation_configured"
            if event_type == "previous_block_validator_info":
                event_type = "validator_configured"
            if event_type == "baker_configured":
                event_type = "validator_configured"
            if event_type == "corresponding_account_reward":
                event_type = "payday_pool_reward"
            if event_type == "pool_info":
                event_type = "payday_pool_reward"
        if notification_event.event_type.other:
            pre_event = "General - "
            event_type_list = list(
                EventTypeOther(
                    **notification_event.event_type.model_dump(exclude_none=True)[
                        list(notification_event.event_type.model_fields_set)[0]
                    ]
                ).model_fields_set
            )
            event_type = event_type_list[0]
            if "validator_commission_changed" in event_type_list:
                event_type = "validator_commission_changed"
            if "validator_lowered_stake" in event_type_list:
                event_type = "validator_lowered_stake"
        if notification_event.event_type.contract:
            pre_event = "Contract - "
            event_type: str = list(
                EventTypeContract(
                    **notification_event.event_type.model_dump(exclude_none=True)[
                        list(notification_event.event_type.model_fields_set)[0]
                    ]
                ).model_fields_set
            )[0]
            if event_type == "receive_name":
                event_type = "contract_update_issued"
        footer_event_type = (
            f"Notification type: {pre_event}{event_type.capitalize().replace('_', ' ')}<br/>"
        )
        footer_tx = (
            f"Transaction: <a href='https://ccdexplorer.io/mainnet/transaction/{notification_event.tx_hash}'>{notification_event.tx_hash[:8]}</a><br/>"
            if notification_event.tx_hash
            else ""
        )
        footer_time = (
            f"Timestamp: <code>{notification_event.block_slot_time:%Y-%m-%d %H:%M:%S}</code><br/>"
        )
        footer_block = (
            f"Block: <a href='https://ccdexplorer.io/mainnet/block/{notification_event.block_hash}'>{notification_event.block_height:,.0f}</a><br/>"
            if notification_event.block_height
            else ""
        )
        if notification_limit:
            footer_notification_limit = f"Notification limit: {notification_limit:,.0f} CCD<br/>"
        else:
            footer_notification_limit = ""
        footer_impacted_addresses = ""
        for impacted_address in notification_event.impacted_addresses:
            if impacted_address.address.contract:
                footer_impacted_addresses += f"Contract: <a href='https://ccdexplorer.io/mainnet/instance/{impacted_address.address.contract.index}/{impacted_address.address.contract.subindex}'>{impacted_address.label}</a><br/>"
            else:
                address_type = "Account"
                tab = ""
                if impacted_address.address_type == AddressType.validator:
                    address_type = "Validator"
                    tab = "?tab=validator"
                if impacted_address.address_type == AddressType.delegator:
                    address_type = "Delegator"
                    tab = "?tab=delegation"
                if impacted_address.address_type == AddressType.sender:
                    address_type = "Sender"
                if impacted_address.address_type == AddressType.receiver:
                    address_type = "Receiver"
                footer_impacted_addresses += f"{address_type}: <a href='https://ccdexplorer.io/mainnet/account/{impacted_address.address.account.index}{tab}'>{impacted_address.label}</a><br/>"
        footer = f"""
{self.line_top}<br/>
{footer_time}
{footer_block}
{footer_tx}
{footer_impacted_addresses}
{footer_notification_limit}
{footer_event_type}
{self.line_bottom}<br/>
"""
        return footer

    def footer_email(
        self,
        notification_event: NotificationEvent,
        notification_limit: int = None,
    ):
        footer_tx = (
            f"Transaction: https://ccdexplorer.io/mainnet/transaction/{notification_event.tx_hash}"
            if notification_event.tx_hash
            else ""
        )
        footer_time = f"Timestamp: {notification_event.block_slot_time:%Y-%m-%d %H:%M:%S}"
        footer_block = (
            f"Block: https://ccdexplorer.io/mainnet/block/{notification_event.block_hash}"
        )
        if notification_limit:
            footer_notification_limit = f"Notification limit: {notification_limit:,.0f} CCD"
        else:
            footer_notification_limit = ""
        footer_impacted_addresses = ""
        for impacted_address in notification_event.impacted_addresses:
            if impacted_address.address.contract:
                footer_impacted_addresses += f"Contract: https://ccdexplorer.io/mainnet/instance/{impacted_address.address.contract.index}/{impacted_address.address.contract.subindex}\n"
            else:
                address_type = "Account"
                if impacted_address.address_type == AddressType.validator:
                    address_type = "Validator"
                if impacted_address.address_type == AddressType.delegator:
                    address_type = "Delegator"
                if impacted_address.address_type == AddressType.sender:
                    address_type = "Sender"
                if impacted_address.address_type == AddressType.receiver:
                    address_type = "Receiver"
                footer_impacted_addresses += f"{address_type}: https://ccdexplorer.io/mainnet/account/{impacted_address.address.account.id}\n"
        footer = f"""
{footer_time}
{footer_block}
{footer_tx}
{footer_impacted_addresses}
{footer_notification_limit}
"""
        return footer

    def verbose_timedelta(self, delta: dt.timedelta, days_only=False):
        if delta.seconds <= 0:
            return "0 sec"
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        dstr = "%s day%s" % (delta.days, "s"[delta.days == 1 :])
        hstr = "%s hr%s" % (hours, "s"[hours == 1 :])
        mstr = "%s min%s" % (minutes, "s"[minutes == 1 :])
        sstr = "%s sec%s" % (seconds, ""[seconds == 1 :])
        total_minutes = delta.days * 24 * 60 + hours * 60 + minutes
        if total_minutes < 30:
            dhms = [dstr, hstr, mstr, sstr] if total_minutes < 30 else [dstr, hstr, mstr]
        elif total_minutes < 720:
            dhms = [dstr, hstr, mstr] if total_minutes < 720 else [dstr, hstr]
        else:
            dhms = [dstr, hstr] if total_minutes < 1440 else [dstr]

        dhms = [dstr] if days_only else dhms

        for x in range(len(dhms)):
            if not dhms[x].startswith("0"):
                dhms = dhms[x:]
                break
        dhms.reverse()
        for x in range(len(dhms)):
            if not dhms[x].startswith("0"):
                dhms = dhms[x:]
                break
        dhms.reverse()
        return " ".join(dhms)

    async def send_notification_queue(self, _: ContextTypes.DEFAULT_TYPE = None):
        """
        This methods loops through all current NotificaionEvents in the `event_queue`
        and determines for all users whether we should be notifiying them of these
        events. If so, we will send the notification to the selected service(s).
        """
        self.event_queue: list[NotificationEvent]
        self.users: dict[str, SiteUser]
        while len(self.event_queue) > 0:
            notification_event: NotificationEvent = self.event_queue.pop(0)
            notification_to_be_sent = False
            message_response_to_be_sent = None
            for user in self.users.values():
                user: SiteUser
                (
                    message_response,
                    notification_services_to_send,
                ) = await self.determine_if_user_should_be_notified_of_event(
                    user, notification_event
                )
                if message_response:
                    notification_to_be_sent = True
                    message_response_to_be_sent = message_response
                    console.log(f"send notification to {user.token}")
                    await self.send_to_services(
                        user, notification_services_to_send, message_response
                    )
            if notification_to_be_sent and message_response_to_be_sent:
                if "pytest" not in sys.modules:
                    if ENVIRONMENT != "dev":
                        try:
                            self.send_to_collection(
                                user,
                                notification_event,
                                message_response_to_be_sent,
                            )
                        except Exception as e:
                            console.log(e)

    def send_to_collection(
        self,
        user: SiteUser,
        notification_event: NotificationEvent,
        message_response: MessageResponse,
    ):
        """
        Sends a log to the collection.
        """
        event_dump = notification_event.model_dump(exclude_none=True)
        if message_response:
            message_response_dump = message_response.model_dump(exclude_none=True)
        else:
            message_response_dump = ""
        event_dump["message_response"] = message_response_dump
        event_dump["user_token"] = user.token
        _id = f"{notification_event.block_hash}-{user.token}"
        self.connections.mongodb.utilities[CollectionsUtilities.message_log].bulk_write(
            [
                ReplaceOne(
                    {"_id": _id},
                    event_dump,
                    upsert=True,
                )
            ]
        )

    async def determine_if_user_should_be_notified_of_event(
        self, user: SiteUser, event: NotificationEvent
    ):
        """
        The goal of this method is to determine for a particular user
        and notification if we need to send this notification and to which
        service(s).

        In an EventType, we set only one of the three main properties
        account, validator, other. The variable field_set contains the property
        that we have set.
        """
        self.connections: Connections
        message_response: MessageResponse | None = None
        notification_services_to_send = None

        field_set = list(event.event_type.model_fields_set)[0]

        if field_set == "other" and user.other_notification_preferences:
            (
                message_response,
                notification_services_to_send,
            ) = self.process_event_type_other(user, event)

        if field_set == "account":
            (
                message_response,
                notification_services_to_send,
            ) = self.process_event_type_account(user, event)

        if field_set == "validator":
            (
                message_response,
                notification_services_to_send,
            ) = self.process_event_type_validator(user, event)

        if field_set == "contract":
            (
                message_response,
                notification_services_to_send,
            ) = self.process_event_type_contract(user, event)

        # Finally, we have determined if we need to send to which
        # services...
        return message_response, notification_services_to_send

    async def publish_to_celery(
        self, service: NotificationServices, user: SiteUser, message_response: MessageResponse
    ) -> None:
        """
        Publish a message to the specified service.
        """
        task_name = "bot_message"
        payload = {
            "service": service.value,
            "telegram_chat_id": user.telegram_chat_id,
            "email_address": user.email_address,
            "message_response": message_response.model_dump(exclude_none=True),
        }
        await asyncio.to_thread(
            celery_app.send_task,
            task_name,
            args=[],
            kwargs={"payload": payload},
            queue="bot_sender",
        )

    async def send_to_services(
        self,
        user: SiteUser,
        notification_services_to_send: dict[NotificationServices, bool],
        message_response: MessageResponse,
    ):
        if notification_services_to_send[NotificationServices.telegram] and user.telegram_chat_id:
            await self.publish_to_celery(
                service=NotificationServices.telegram,
                user=user,
                message_response=message_response,
            )

        if notification_services_to_send[NotificationServices.email] and user.email_address:
            await self.publish_to_celery(
                service=NotificationServices.email,
                user=user,
                message_response=message_response,
            )
