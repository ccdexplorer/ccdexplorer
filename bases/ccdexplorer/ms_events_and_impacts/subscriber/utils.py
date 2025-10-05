from enum import Enum
from ccdexplorer.grpc_client.CCD_Types import CCD_Address, CCD_NewRelease
from ccdexplorer.tooter import Tooter, TooterChannel, TooterType
from rich.console import Console

from ccdexplorer.env import ADMIN_CHAT_ID

console = Console()


class SubscriberType(Enum):
    """ """

    modules_and_instances = "modules_and_instances"


class Utils:
    def address_to_str(self, address: CCD_Address) -> str:
        if address.contract:
            return address.contract.to_str()
        elif address.account:
            return address.account
        else:
            return "<unknown>"

    def get_sum_amount_from_scheduled_transfer(self, schedule: list[CCD_NewRelease]):
        sum = 0
        for release in schedule:
            sum += release.amount
        return sum

    def send_to_tooter(self, msg: str):
        self.tooter: Tooter
        self.tooter.relay(
            channel=TooterChannel.NOTIFIER,
            title="",
            chat_id=ADMIN_CHAT_ID,
            body=msg,
            notifier_type=TooterType.INFO,
        )
