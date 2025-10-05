import datetime as dt
from enum import Enum

from ccdexplorer.grpc_client.CCD_Types import CCD_BlockInfo
from ccdexplorer.mongodb import Collections
from pymongo.collection import Collection

from ccdexplorer.env import HEARTBEAT_PROGRESS_DOCUMENT_ID


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


class ProvenanceMintAddress(Enum):
    mainnet = ["3suZfxcME62akyyss72hjNhkzXeZuyhoyQz1tvNSXY2yxvwo53"]
    testnet = [
        "4AuT5RRmBwcdkLMA6iVjxTDb1FQmxwAh3wHBS22mggWL8xH6s3",
        "4s3QS7Vdp7b6yrLngKQwCQcexKVQLKifcGKgmVXoH6wffZMQhM",
    ]


class Utils:
    async def get_project_addresses(self):
        self.db: dict[Collections, Collection]
        self.project_addresses = {
            x["account_address"]: x["project_id"]
            for x in self.db[Collections.projects].find({"type": "account_address"})
        }
        pass

    def log_error_in_mongo(self, e, current_block_to_process: CCD_BlockInfo):
        query = {"_id": f"block_failure_{current_block_to_process.height}"}
        self.db[Collections.helpers].replace_one(
            query,
            {
                "_id": f"block_failure_{current_block_to_process.height}",
                "height": current_block_to_process.height,
                "Exception": e,
            },
            upsert=True,
        )

    def log_last_processed_message_in_mongo(self, current_block_to_process: CCD_BlockInfo):
        query = {"_id": HEARTBEAT_PROGRESS_DOCUMENT_ID}
        self.db[Collections.helpers].replace_one(
            query,
            {
                "_id": HEARTBEAT_PROGRESS_DOCUMENT_ID,
                "height": current_block_to_process.height,
            },
            upsert=True,
        )
        self.internal_freqency_timer = dt.datetime.now().astimezone(tz=dt.timezone.utc)

    def log_last_heartbeat_memo_to_hashes_in_mongo(self, height: int):
        query = {"_id": "heartbeat_memos_last_processed_block"}
        self.db[Collections.helpers].replace_one(
            query,
            {
                "_id": "heartbeat_memos_last_processed_block",
                "height": height,
            },
            upsert=True,
        )
