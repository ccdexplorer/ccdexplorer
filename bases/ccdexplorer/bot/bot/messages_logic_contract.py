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
from ccdexplorer.site_user import ContractForUser, SiteUser
from rich.console import Console

from ..notification_classes import *
from .messages_definitions_contract import MessageContract as MessageContract
from .utils import Utils as Utils

console = Console()


class ProcessContract(MessageContract, Utils):
    def process_event_type_contract(self, user: SiteUser, notification_event: NotificationEvent):
        message_response = None
        notification_services_to_send = None
        event_type = EventTypeContract(
            **notification_event.event_type.model_dump()[
                list(notification_event.event_type.model_fields_set)[0]
            ]
        )
        ia = notification_event.impacted_addresses[0]
        if ia.address_type == AddressType.contract:
            contract_index = ia.address.contract.index if ia.address.contract.index else None
            if user.contracts.get(str(contract_index)):
                contract: ContractForUser = user.contracts[str(contract_index)]

            else:
                return None, None

            if not contract.contract_notification_preferences:
                return None, None

            if event_type.contract_update_issued:
                if event_type.contract_update_issued and (
                    event_type.receive_name
                    in contract.contract_notification_preferences.contract_update_issued.keys()
                ):
                    notification_services_to_send = self.set_notification_service(
                        contract.contract_notification_preferences.contract_update_issued[
                            event_type.receive_name
                        ]
                    )
                    if any(notification_services_to_send.values()):
                        message_response = self.define_contract_update_issued_message(
                            event_type, notification_event, user
                        )
        return message_response, notification_services_to_send
