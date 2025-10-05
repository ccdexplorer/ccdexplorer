from enum import Enum
from ccdexplorer.grpc_client.CCD_Types import CCD_NewRelease
from ccdexplorer.tooter import Tooter, TooterChannel, TooterType
from rich.console import Console

from ccdexplorer.env import ADMIN_CHAT_ID

console = Console()


class Queue(Enum):
    """
    Type of queue to store messages in to send to MongoDB.
    Names correspond to the collection names.
    """

    block_per_day = -2
    block_heights = -1
    blocks = 0
    transactions = 1
    involved_all = 2
    involved_transfer = 3
    involved_contract = 4
    instances = 5
    modules = 6
    updated_modules = 7
    logged_events = 8
    token_addresses_to_redo_accounting = 9
    provenance_contracts_to_add = 10
    impacted_addresses = 11
    special_events = 12
    token_accounts = 13
    token_addresses = 14
    token_links = 15
    queue_todo = 16
    blocks_log = 17
    events_and_impacts = 18


class SubscriberType(Enum):
    """ """

    modules_and_instances = "modules_and_instances"


class Utils:
    def send_to_tooter(self, msg: str):
        self.tooter: Tooter
        self.tooter.relay(
            channel=TooterChannel.NOTIFIER,
            title="",
            chat_id=int(ADMIN_CHAT_ID),  # type: ignore
            body=msg,
            notifier_type=TooterType.INFO,
        )

    def get_sum_amount_from_scheduled_transfer(self, schedule: list[CCD_NewRelease]):
        sum = 0
        for release in schedule:
            sum += release.amount
        return sum
