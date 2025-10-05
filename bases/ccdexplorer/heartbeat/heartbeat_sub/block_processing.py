import datetime as dt
import json
from enum import Enum

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_Block,
    CCD_BlockInfo,
)
from ccdexplorer.mongodb import Collection, Collections
from ccdexplorer.mongodb.core import build_collection_identifier
from pymongo import ReplaceOne
from rich.console import Console

from .utils import Queue

console = Console()


class AccountTransactionOutcome(Enum):
    Success = "success"
    Failure = "failure"


class BlockProcessing:
    def lookout_for_end_of_day(self, current_block_to_process: CCD_BlockInfo):
        self.grpc_client: GRPCClient
        self.db: dict[Collections, Collection]
        end_of_day_timeframe_start = dt.time(0, 0, 0)
        end_of_day_timeframe_end = dt.time(0, 2, 0)

        if (current_block_to_process.slot_time.time() >= end_of_day_timeframe_start) and (
            current_block_to_process.slot_time.time() <= end_of_day_timeframe_end
        ):
            previous_block_height = current_block_to_process.height - 1
            previous_block_info = self.grpc_client.get_finalized_block_at_height(
                previous_block_height, NET(self.net)
            )

            if current_block_to_process.slot_time.day != previous_block_info.slot_time.day:
                start_of_day0 = previous_block_info.slot_time.replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                start_of_day1 = previous_block_info.slot_time.replace(
                    hour=0, minute=1, second=59, microsecond=999999
                )

                start_of_day_blocks = list(
                    self.db[Collections.blocks].find(
                        filter={
                            "$and": [
                                {"slot_time": {"$gte": start_of_day0}},
                                {"slot_time": {"$lte": start_of_day1}},
                            ]
                        }
                    )
                )

                if len(start_of_day_blocks) == 0:
                    start_of_day_blocks = [
                        self.grpc_client.get_finalized_block_at_height(0, NET(self.net))
                    ]
                    self.add_end_of_day_to_queue(  # type: ignore
                        f"{previous_block_info.slot_time:%Y-%m-%d}",
                        start_of_day_blocks[0],
                        previous_block_info,
                    )
                else:
                    self.add_end_of_day_to_queue(  # type: ignore
                        f"{previous_block_info.slot_time:%Y-%m-%d}",
                        CCD_BlockInfo(**start_of_day_blocks[0]),
                        previous_block_info,
                    )
                console.log(f"End of day found for {previous_block_info.slot_time:%Y-%m-%d}")

    def lookout_for_payday(self, current_block_to_process: CCD_BlockInfo):
        special_events = self.grpc_client.get_block_special_events(
            current_block_to_process.hash, NET(self.net)
        )
        found = False
        for se in special_events:
            if se.payday_account_reward or se.payday_pool_reward:
                found = True
                break

        if found:
            new_payday_date_string = f"{current_block_to_process.slot_time:%Y-%m-%d}"

            query = {"_id": "last_known_payday"}
            dd = {
                "_id": "last_known_payday",
                "date": new_payday_date_string,
                "hash": current_block_to_process.hash,
                "height": current_block_to_process.height,
            }
            self.db[Collections.helpers].replace_one(query, dd, upsert=True)

            console.log(f"Payday {current_block_to_process.slot_time:%Y-%m-%d} found!")
            payday_information_entry = {
                "_id": current_block_to_process.hash,
                "date": new_payday_date_string,
            }
            query = {"_id": current_block_to_process.hash}

            _ = self.db[Collections.paydays_v2].replace_one(
                query, payday_information_entry, upsert=True
            )

    def add_block_and_txs_to_queue(
        self,
        block_info: CCD_BlockInfo,
    ):
        self.net: str
        self.queues: dict[Queue, list]
        self.namespace: str
        try:
            json_block_info: dict = json.loads(block_info.model_dump_json(exclude_none=True))
        except Exception as e:
            exit(f"Error in block_info to json: {e}")

        json_block_info.update({"_id": block_info.hash})
        json_block_info.update({"slot_time": block_info.slot_time})
        del json_block_info["arrive_time"]
        del json_block_info["receive_time"]
        json_block_info.update({"transaction_hashes": []})

        if block_info.transaction_count > 0:
            block: CCD_Block = self.grpc_client.get_block_transaction_events(
                block_info.hash, NET(self.net)
            )

            json_block_info.update(
                {"transaction_hashes": [x.hash for x in block.transaction_summaries]}
            )

            # adding transactions if any
            for tx in block.transaction_summaries:
                # add recognized_sender_id from projects if we can find it
                if tx.account_transaction:
                    if self.project_addresses.get(tx.account_transaction.sender):  # type: ignore
                        tx.recognized_sender_id = self.project_addresses[  # type: ignore
                            tx.account_transaction.sender
                        ]

                json_tx: dict = json.loads(tx.model_dump_json(exclude_none=True))

                json_tx.update({"_id": tx.hash})
                json_tx.update(
                    {
                        "block_info": {
                            "height": block_info.height,
                            "hash": block_info.hash,
                            "slot_time": block_info.slot_time,
                        }
                    }
                )
                self.queues[Queue.transactions].append(
                    ReplaceOne(
                        {"_id": tx.hash},
                        replacement=json_tx,
                        upsert=True,
                        namespace=build_collection_identifier(
                            self.namespace, Collections.transactions
                        ),
                    )
                )

        self.queues[Queue.blocks].append(
            ReplaceOne(
                {"_id": block_info.hash},
                replacement=json_block_info,
                upsert=True,
                namespace=build_collection_identifier(self.namespace, Collections.blocks),
            )
        )
        self.queues[Queue.block_heights].append(block_info.height)
