from enum import Enum

from ccdexplorer.tooter import Tooter, TooterChannel, TooterType
from rich.console import Console

from ccdexplorer.env import ADMIN_CHAT_ID

console = Console()


class SubscriberType(Enum):
    """ """

    modules_and_instances = "modules_and_instances"


class Utils:
    def send_to_tooter(self, msg: str):
        self.tooter: Tooter
        self.tooter.relay(
            channel=TooterChannel.NOTIFIER,
            title="",
            chat_id=ADMIN_CHAT_ID,
            body=msg,
            notifier_type=TooterType.INFO,
        )
