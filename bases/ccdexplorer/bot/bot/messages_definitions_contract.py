# ruff: noqa: F403, F405, E402, E501, F401
# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false

from typing import TYPE_CHECKING

from ccdexplorer.domain.mongo import MongoTypeTokensTag
from ccdexplorer.env import *  # type: ignore
from ccdexplorer.grpc_client.CCD_Types import *  # type: ignore
from ccdexplorer.site_user import (
    SiteUser,
)
from rich.console import Console

from ..notification_classes import *
from .utils import Utils as Utils

console = Console()


class MessageContract(Utils):
    def define_contract_update_issued_message(
        self,
        event_type: EventTypeValidator | EventTypeAccount,
        notification_event: NotificationEvent,
        user: SiteUser,
    ) -> MessageResponse:
        notification_event = self.add_labels_to_notitication_event(user, notification_event)
        receive_name = notification_event.event_type.contract.receive_name
        contract_address: ImpactedAddress = self.return_specific_address_type(
            notification_event.impacted_addresses, AddressType.contract
        )
        contract_with_info: MongoTypeTokensTag | None = self.contracts_with_tag_info.get(
            contract_address.address.contract.to_str()
        )
        module_name = (
            (
                contract_with_info.display_name
                if contract_with_info.display_name
                else contract_address.label
            )
            if contract_with_info
            else contract_address.label
        )
        message = f'On smart contract <a href="https://ccdexplorer.io/mainnet/instance/{contract_address.address.contract.index}/{contract_address.address.contract.subindex}">{module_name}</a> the method "{receive_name}" was called.<br/><br/>'

        message += f"""

{self.footer(notification_event)}
"""
        return MessageResponse(
            **{
                "title_telegram": "",
                "title_email": "CCDExplorer Notification - Method called on smart contract",
                "message_telegram": message,
                "message_email": message,
            }
        )
