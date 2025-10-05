from ccdexplorer.mongodb.core import build_collection_identifier
from .utils import Queue
from .block_processing import BlockProcessing as _block_processing
from ccdexplorer.tooter import TooterChannel, TooterType, Tooter
from ccdexplorer.mongodb import Collections
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockInfo
from pymongo import ReplaceOne
from pymongo.collection import Collection
from ccdexplorer.domain.generic import NET

from ccdexplorer.env import (
    HEARTBEAT_SPECIAL_PURPOSE,
    MAX_BLOCKS_PER_RUN,
    DEBUG,
    ADMIN_CHAT_ID,
    HEARTBEAT_PROGRESS_DOCUMENT_ID,
)
import datetime as dt
import grpc

from copy import copy


from rich.console import Console

console = Console()


class BlockLoop(_block_processing):
    def process_list_of_blocks(self, block_list: list, special_purpose: bool = False):
        self.db: dict[Collections, Collection]

        start = dt.datetime.now()
        while len(block_list) > 0:
            current_block_to_process: CCD_BlockInfo = block_list.pop(0)

            try:
                self.add_block_and_txs_to_queue(current_block_to_process)

                self.lookout_for_payday(current_block_to_process)
                self.lookout_for_end_of_day(current_block_to_process)

                self.special_purpose_remove_from_helper(special_purpose, current_block_to_process)

            except Exception as e:
                self.log_error_in_mongo(e, current_block_to_process)  # type: ignore
        duration = dt.datetime.now() - start
        console.log(
            f"Spent {duration.total_seconds():,.0f} sec on {len(self.queues[Queue.transactions]):,.0f} txs."
        )

        return current_block_to_process  # type: ignore

    def special_purpose_remove_from_helper(self, special_purpose, current_block_to_process):
        if special_purpose:
            # if it's a special purpose block, we need to remove it from the helper
            result: dict = self.db[Collections.helpers].find_one({"_id": HEARTBEAT_SPECIAL_PURPOSE})  # type: ignore
            if result is not None:
                result: dict
                heights: list = result["heights"]
                try:
                    heights.remove(current_block_to_process.height)
                except ValueError:
                    pass
                result.update({"heights": heights})
            _ = self.db[Collections.helpers].bulk_write(
                [
                    ReplaceOne(
                        {"_id": HEARTBEAT_SPECIAL_PURPOSE},
                        replacement=result,
                        upsert=True,
                        namespace=build_collection_identifier(self.namespace, Collections.helpers),
                    )
                ]
            )

    async def process_blocks(self):
        """
        This method takes the queue `finalized_block_infos_to_process` and processes
        each block.
        """
        self.finalized_block_infos_to_process: list[CCD_BlockInfo]
        # while True:
        if len(self.finalized_block_infos_to_process) > 0:
            pp = copy(self.finalized_block_infos_to_process)
            # this is the last block that was processed
            current_block_to_process = self.process_list_of_blocks(
                self.finalized_block_infos_to_process
            )

            self.log_last_processed_message_in_mongo(current_block_to_process)  # type: ignore
            if len(pp) == 1:
                console.log(f"Block processed: {pp[0].height:,.0f}")
            else:
                console.log(f"Blocks processed: {pp[0].height:,.0f} - {pp[-1].height:,.0f}")
            # await asyncio.sleep(1)

    async def process_special_purpose_blocks(self):
        """
        This method takes the queue `special_purpose_block_infos_to_process` and processes
        each block.
        """
        self.special_purpose_block_infos_to_process: list[CCD_BlockInfo]
        # while True:
        if len(self.special_purpose_block_infos_to_process) > 0:
            pp = copy(self.special_purpose_block_infos_to_process)
            # this is the last block that was processed

            _ = self.process_list_of_blocks(
                self.special_purpose_block_infos_to_process, special_purpose=True
            )

            if len(pp) == 1:
                console.log(f"SP Block processed: {pp[0].height:,.0f}")
            else:
                console.log(f"SP Blocks processed: {pp[0].height:,.0f} - {pp[-1].height:,.0f}")
            # await asyncio.sleep(5)

    async def get_special_purpose_blocks(self):
        """
        This methods gets special purpose blocks from the chosen net.
        It batches blocks up to MAX_BLOCKS_PER_RUN and stores blocks to be
        processed in the queue `finalized_block_infos_to_process`.
        """
        # while True:
        result = self.db[Collections.helpers].find_one({"_id": HEARTBEAT_SPECIAL_PURPOSE})
        if result:
            for height in result["heights"]:
                try:
                    self.special_purpose_block_infos_to_process.append(
                        self.grpc_client.get_finalized_block_at_height(int(height), NET(self.net))
                    )

                except grpc.RpcError as rpc_error:
                    self.tooter.relay(
                        channel=TooterChannel.NOTIFIER,
                        title="",
                        chat_id=int(ADMIN_CHAT_ID),  # type: ignore
                        body=f"Heartbeat on {self.net} received GRPC Error {rpc_error}. Exiting to restart.",
                        notifier_type=TooterType.REQUESTS_ERROR,
                    )
                    exit()

            # await asyncio.sleep(10)

    async def get_finalized_blocks(self):
        """
        This methods gets finalized blocks from the chosen net.
        It batches blocks up to MAX_BLOCKS_PER_RUN and stores blocks to be
        processed in the queue `finalized_block_infos_to_process`.
        """
        self.tooter: Tooter
        self.grpc_client: GRPCClient
        self.net: str
        self.internal_freqency_timer: dt.datetime
        if DEBUG:
            console.log("get_finalized_blocks")
        # while True:
        # this comparison makes sure that if we haven't logged a new block in 2 min
        # something somewhere has gone wrong. Hoping that a restart will fix things...
        current_time = dt.datetime.now().astimezone(tz=dt.timezone.utc)
        if (current_time - self.internal_freqency_timer).total_seconds() > 2 * 60:
            self.tooter.relay(
                channel=TooterChannel.NOTIFIER,
                title="",
                chat_id=int(ADMIN_CHAT_ID),  # type: ignore
                body=f"Heartbeat on {self.net} seems to not have processed a new block in 2 min? Exiting to restart.",
                notifier_type=TooterType.REQUESTS_ERROR,
            )
            exit()

        request_counter = 0
        result = self.db[Collections.helpers].find_one({"_id": HEARTBEAT_PROGRESS_DOCUMENT_ID})
        heartbeat_last_processed_block_height = result["height"]  # type: ignore
        if DEBUG:
            console.log(f"{heartbeat_last_processed_block_height=}")
        last_requested_block_not_finalized = False
        block_to_request_in_queue = False

        while (
            not (last_requested_block_not_finalized)
            and (request_counter < MAX_BLOCKS_PER_RUN)
            and not block_to_request_in_queue
        ):
            request_counter += 1
            if DEBUG:
                console.log(
                    f"{last_requested_block_not_finalized=}, {request_counter=}, {block_to_request_in_queue=}"
                )
            # increment the block height to request
            heartbeat_last_processed_block_height += 1

            # check to see if we haven't finished processing the queue
            # If so, no need to request and add the same block again.
            block_to_request_in_queue = heartbeat_last_processed_block_height in [
                x.height for x in self.finalized_block_infos_to_process
            ]
            if DEBUG:
                console.log(f"{self.finalized_block_infos_to_process=}")
            # we haven't previously requested this block
            if not block_to_request_in_queue:
                try:
                    finalized_block_info_at_height = self.grpc_client.get_finalized_block_at_height(
                        heartbeat_last_processed_block_height, NET(self.net)
                    )
                except grpc._channel._InactiveRpcError | ValueError as rpc_error:  # type: ignore
                    self.tooter.relay(
                        channel=TooterChannel.NOTIFIER,
                        title="",
                        chat_id=int(ADMIN_CHAT_ID),  # type: ignore
                        body=f"Heartbeat on {self.net} received GRPC Error {rpc_error}. Exiting to restart.",
                        notifier_type=TooterType.REQUESTS_ERROR,
                    )
                    exit()

                if finalized_block_info_at_height:
                    self.finalized_block_infos_to_process.append(finalized_block_info_at_height)
                else:
                    last_requested_block_not_finalized = True
            else:
                if DEBUG:
                    console.log(f"Block already in queue: {heartbeat_last_processed_block_height}")
        if DEBUG:
            console.log(f"{len(self.finalized_block_infos_to_process)=}")
        if len(self.finalized_block_infos_to_process) > 0:
            if len(self.finalized_block_infos_to_process) == 1:
                console.log(
                    f"Block retrieved: {self.finalized_block_infos_to_process[0].height:,.0f}"
                )
            else:
                console.log(
                    f"Blocks retrieved: {self.finalized_block_infos_to_process[0].height:,.0f} - {self.finalized_block_infos_to_process[-1].height:,.0f}"
                )
                if (
                    self.finalized_block_infos_to_process[0].height
                    > self.finalized_block_infos_to_process[-1].height
                ):
                    self.tooter.relay(
                        channel=TooterChannel.NOTIFIER,
                        title="",
                        chat_id=int(ADMIN_CHAT_ID),  # type: ignore
                        body=f"Heartbeat on {self.net} seems to be in a loop? Exiting to restart.",
                        notifier_type=TooterType.REQUESTS_ERROR,
                    )
                    exit()
