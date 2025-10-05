# ruff: noqa: F403, F405, E402, E501, E722
from .utils import Utils, Queue
from ccdexplorer.mongodb import Collections
from ccdexplorer.grpc_client.CCD_Types import *
from pymongo import ASCENDING, DESCENDING, ReplaceOne
from ccdexplorer.env import *
import datetime as dt
from datetime import timedelta
from rich.console import Console

console = Console()


class StartOver(Utils):
    def add_end_of_day_to_queue(
        self, date_string: str, start_block: CCD_BlockInfo, end_block: CCD_BlockInfo
    ):
        """
        This method adds an end_of_day document to the queue.
        """
        dd = {}
        dd["_id"] = date_string
        dd["date"] = date_string
        dd["height_for_first_block"] = start_block.height
        dd["height_for_last_block"] = end_block.height
        dd["slot_time_for_first_block"] = start_block.slot_time
        dd["slot_time_for_last_block"] = end_block.slot_time
        dd["hash_for_first_block"] = start_block.hash
        dd["hash_for_last_block"] = end_block.hash

        self.queues[Queue.block_per_day].append(  # type: ignore
            ReplaceOne({"_id": date_string}, dd, upsert=True)
        )

    def date_range_generator(self, start, end):
        """
        Range generator to generate all days.
        Used only if we need to re-create the block_per_day collection.
        """
        delta = end - start
        days: list[dt.datetime] = [start + timedelta(days=i) for i in range(delta.days + 1)]
        return days

    def create_block_per_day(self):
        """
        Method to re-create the blocks_per_day collections
        for testnet and mainnet.
        """
        if self.net == "testnet":  # type: ignore
            start_date = dt.datetime(2022, 6, 13)
        else:
            start_date = dt.datetime(2021, 6, 9)

        end_date = dt.datetime.now()

        date_range = self.date_range_generator(start_date, end_date)

        for index, date in enumerate(date_range):
            date_string = f"{date:%Y-%m-%d}"
            if index == 0:
                if self.net == "testnet":  # type: ignore
                    start_of_day0 = date.replace(hour=10, minute=0, second=0, microsecond=0)
                    start_of_day1 = date.replace(hour=10, minute=1, second=59, microsecond=999999)
                else:
                    start_of_day0 = date.replace(hour=6, minute=0, second=0, microsecond=0)
                    start_of_day1 = date.replace(hour=6, minute=1, second=59, microsecond=999999)

            else:
                start_of_day0 = date.replace(hour=0, minute=0, second=0, microsecond=0)
                start_of_day1 = date.replace(hour=0, minute=1, second=59, microsecond=999999)

            end_of_day0 = date.replace(hour=23, minute=58, second=0, microsecond=0)
            end_of_day1 = date.replace(hour=23, minute=59, second=59, microsecond=999999)

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
            end_of_day_blocks = list(
                self.db[Collections.blocks].find(
                    filter={
                        "$and": [
                            {"slot_time": {"$gte": end_of_day0}},
                            {"slot_time": {"$lte": end_of_day1}},
                        ]
                    }
                )
            )

            # if this isn't true, the day hasn't ended yet.
            if len(end_of_day_blocks) > 0:
                console.log(date_string)

                self.add_end_of_day_to_queue(
                    date_string,
                    CCD_BlockInfo(**start_of_day_blocks[0]),
                    CCD_BlockInfo(**end_of_day_blocks[-1]),
                )

        _ = self.db[Collections.blocks_per_day].bulk_write(
            self.queues[Queue.block_per_day]  # type: ignore
        )

    def create_index(self, collection: Collections, key: str, direction, sparse=False):
        response = self.db[collection].create_index([(key, direction)], sparse=sparse)
        print(
            f"Reponse for index creation on collection '{collection.value}' for key '{key}': {response}."
        )

    def create_compound_index(self, collection: Collections, index_tuple: tuple, sparse=False):
        response = self.db[collection].create_index([index_tuple], sparse=sparse)
        print(
            f"Reponse for index creation on collection '{collection.value}' for '{index_tuple}': {response}."
        )

    def create_mongodb_indices(self):
        """
        If needed, we can use this method to re-create
        all indices on all collections (TODO: first delete
        existing indices?)
        """
        for collection in Collections:
            if collection == Collections.blocks:
                self.create_index(collection, "baker", ASCENDING)
                self.create_index(collection, "slot_time", ASCENDING)
                self.create_index(collection, "height", DESCENDING)
                self.create_index(collection, "transaction_count", ASCENDING)

            if collection == Collections.blocks_per_day:
                self.create_index(collection, "date", ASCENDING)

            if collection == Collections.instances:
                self.create_index(collection, "source_module", ASCENDING)
                self.create_index(collection, "owner", ASCENDING)

            if collection in [
                Collections.involved_accounts_transfer,
            ]:
                self.create_index(collection, "block_height", ASCENDING)
                self.create_index(collection, "sender_canonical", ASCENDING)
                self.create_index(collection, "receiver_canonical", ASCENDING)
                self.create_index(collection, "type.type", ASCENDING)
                self.create_index(collection, "type.contents", ASCENDING)

            if collection == Collections.modules:
                self.create_index(collection, "module_name", ASCENDING)

            if collection == Collections.dashboard_nodes:
                self.create_index(collection, "consensusBakerId", ASCENDING)

            if collection == Collections.cns_domains:
                self.create_index(collection, "domain_name", ASCENDING)

            if collection == Collections.nightly_accounts:
                self.create_index(collection, "index", ASCENDING)

            if collection == Collections.transactions:
                self.create_index(collection, "type.type", ASCENDING)
                self.create_index(collection, "type.contents", ASCENDING)
                self.create_index(collection, "block_info.height", ASCENDING)
                self.create_index(collection, "block_info.slot_time", ASCENDING)
                self.create_index(collection, "account_creation", ASCENDING, sparse=True)
                self.create_index(
                    collection,
                    "account_transaction.effects.account_transfer",
                    ASCENDING,
                    sparse=True,
                )
                self.create_index(
                    collection,
                    "account_transaction.effects.baker_configured",
                    ASCENDING,
                    sparse=True,
                )
                self.create_index(
                    collection,
                    "account_transaction.effects.delegation_configured",
                    ASCENDING,
                    sparse=True,
                )

            if collection == Collections.tokens_token_addresses:
                self.create_index(collection, "contract", ASCENDING)

            if collection == Collections.tokens_logged_events:
                self.create_index(collection, "block_height", ASCENDING)
                self.create_index(collection, "token_address", ASCENDING)
                self.create_index(collection, "contract", ASCENDING)
                self.create_compound_index(
                    collection,
                    [
                        ("block_height", ASCENDING),
                        ("tx_index", ASCENDING),
                        ("ordering", ASCENDING),
                    ],  # type: ignore
                )

            if self.net == "mainnet":  # type: ignore
                if collection == Collections.paydays:
                    self.create_index(collection, "date", ASCENDING)
                    self.create_index(collection, "height_for_first_block", ASCENDING)
                    self.create_index(collection, "height_for_last_block", ASCENDING)

                if collection == Collections.paydays_performance:
                    self.create_index(collection, "date", ASCENDING)
                    self.create_index(collection, "baker_id", ASCENDING)

                if collection == Collections.paydays_rewards:
                    self.create_index(collection, "date", ASCENDING)
                    self.create_index(collection, "pool_owner", ASCENDING)
