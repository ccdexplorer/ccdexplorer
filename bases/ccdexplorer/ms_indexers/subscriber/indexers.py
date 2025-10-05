from typing import Optional

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_ContractAddress,
    CCD_BlockInfo,
    CCD_BlockItemSummary,
    CCD_ShortBlockInfo,
    CCD_Block,
    CCD_TransactionType,
)
from ccdexplorer.mongodb import Collections, MongoDB, build_collection_identifier
from pymongo.collection import Collection
from pymongo import ReplaceOne
from pymongo.results import ClientBulkWriteResult

from rich.console import Console
from enum import Enum
from .utils import Utils, Queue

console = Console()


class ClassificationResult:
    """
    This is a result type to store the classification of a transaction,
    used to determine which indices, if any, need to be created for this
    transaction.
    """

    def __init__(self):
        self.sender: Optional[str] = None
        self.receiver: Optional[str] = None
        self.tx_hash: Optional[str] = None
        self.memo: Optional[str] = None
        self.contract: Optional[str] = None
        self.amount: Optional[int] = None
        self.type: Optional[CCD_TransactionType | None] = None
        self.contents: Optional[str] = None
        self.accounts_involved_all: bool = False
        self.accounts_involved_transfer: bool = False
        self.contracts_involved: bool = False
        self.module_involved: bool = False
        self.actual_module_involved: Optional[str] = None
        self.list_of_contracts_involved: list[dict] = []
        self.contract_initialized: Optional[str] = None


class AccountTransactionOutcome(Enum):
    Success = "success"
    Failure = "failure"


class Indexers(Utils):
    def index_transfer_and_all(
        self,
        tx: CCD_BlockItemSummary,
        result: ClassificationResult,
        block_info: CCD_BlockInfo,
    ):
        dct_transfer_and_all = {
            "_id": tx.hash,
            "sender": result.sender,
            "receiver": result.receiver,
            "sender_canonical": result.sender[:29] if result.sender else None,
            "receiver_canonical": result.receiver[:29] if result.receiver else None,
            "amount": result.amount,
            "type": result.type.model_dump(),  # type: ignore
            "block_height": block_info.height,
        }
        if result.memo:
            dct_transfer_and_all.update({"memo": result.memo})

        return dct_transfer_and_all

    def classify_transaction(self, tx: CCD_BlockItemSummary) -> ClassificationResult:
        """
        This classifies a transaction (based on GRPCv2 output)
        for inclusion in the index tables.
        """
        result = ClassificationResult()
        result.type = tx.type
        self.namespace: str = (
            "concordium_mainnet" if self.net == "mainnet" else "concordium_testnet"
        )
        assert tx.block_info is not None
        if tx.update:
            pass

        if tx.account_creation:
            result.sender = tx.account_creation.address

        if tx.account_transaction:
            result.sender = tx.account_transaction.sender

            if tx.account_transaction.outcome == AccountTransactionOutcome.Success.value:
                effects = tx.account_transaction.effects

                if effects.account_transfer:
                    result.accounts_involved_transfer = True

                    ac = effects.account_transfer
                    result.amount = ac.amount
                    result.receiver = ac.receiver
                    if ac.memo:
                        result.memo = ac.memo

                elif effects.transferred_with_schedule:
                    result.accounts_involved_transfer = True

                    ts = effects.transferred_with_schedule
                    result.amount = self.get_sum_amount_from_scheduled_transfer(ts.amount)
                    result.receiver = ts.receiver
                    if ts.memo:
                        result.memo = ts.memo

                elif effects.contract_initialized:
                    result.contracts_involved = True
                    result.contract_initialized = effects.contract_initialized.address.to_str()

                    ci = effects.contract_initialized
                    result.list_of_contracts_involved.append({"address": ci.address})

                elif effects.module_deployed:
                    result.module_involved = True
                    result.actual_module_involved = effects.module_deployed

                elif effects.contract_update_issued:
                    result.contracts_involved = True

                    update_effects = effects.contract_update_issued.effects

                    for effect in update_effects:
                        if effect.interrupted:
                            result.list_of_contracts_involved.append(
                                {"address": effect.interrupted.address}
                            )

                        elif effect.resumed:
                            result.list_of_contracts_involved.append(
                                {"address": effect.resumed.address}
                            )

                        elif effect.updated:
                            result.list_of_contracts_involved.append(
                                {
                                    "address": effect.updated.address,
                                    "receive_name": effect.updated.receive_name,
                                }
                            )

                        elif effect.transferred:
                            if type(effect.transferred.sender) is CCD_ContractAddress:
                                result.list_of_contracts_involved.append(
                                    {"address": effect.transferred.sender}
                                )

                            if type(effect.transferred.receiver) is CCD_ContractAddress:
                                result.list_of_contracts_involved.append(
                                    {"address": effect.transferred.receiver}
                                )

            else:
                pass

        return result

    def generate_indices_based_on_transactions(self, block_height: int):
        """
        Given a list of transactions, apply rules to determine which index needs to be updated.
        Add this to a to_be_sent_to_mongo list and do insert_many.
        """
        self.queues: dict[Enum, list]
        self.db: dict[Collections, Collection]
        self.mongodb: MongoDB
        self.net: str
        self.grpc_client: GRPCClient
        self.queues[Queue.involved_transfer] = []
        block_txs: CCD_Block = self.grpc_client.get_block_transaction_events(
            block_height, NET(self.net)
        )
        block_info: CCD_BlockInfo = self.grpc_client.get_block_info(block_height, NET(self.net))

        for tx in block_txs.transaction_summaries:
            tx.block_info = CCD_ShortBlockInfo(
                height=block_info.height,
                hash=block_info.hash,
                slot_time=block_info.slot_time,
            )

            result = self.classify_transaction(tx)

            dct_transfer_and_all = self.index_transfer_and_all(tx, result, block_info)

            # only store tx in this collection if it's a transfer
            if result.accounts_involved_transfer:
                self.queues[Queue.involved_transfer].append(
                    ReplaceOne(
                        {"_id": dct_transfer_and_all["_id"]},
                        dct_transfer_and_all,
                        upsert=True,
                        namespace=build_collection_identifier(
                            self.namespace, Collections.involved_accounts_transfer
                        ),
                    )
                )
        if len(self.queues[Queue.involved_transfer]) > 0:
            write_result: ClientBulkWriteResult = self.mongodb.connection.bulk_write(
                self.queues[Queue.involved_transfer]
            )

            console.log(
                f"{block_height:,.0f}: {len(self.queues[Queue.involved_transfer]):,.0f} involved_transfer transaction(s)"
            )
            self.queues[Queue.involved_transfer] = []
            if write_result.upserted_count > 0:
                console.log(f"Upserted: {write_result.upserted_count:,.0f}")
            if write_result.matched_count > 0:
                console.log(f"Matched:  {write_result.matched_count:,.0f}")
            if write_result.modified_count > 0:
                console.log(f"Modified: {write_result.modified_count:,.0f}")

    def do_specials(self, block_height: int):
        self.queues: dict[Enum, list]
        self.db: dict[Collections, Collection]
        self.mongodb: MongoDB
        self.net: str
        self.grpc_client: GRPCClient

        block_info: CCD_BlockInfo = self.grpc_client.get_block_info(block_height, NET(self.net))

        # add special events
        se = self.grpc_client.get_block_special_events(block_info.height, NET(self.net))

        if self.net == "mainnet":
            suspended = any([x.validator_suspended is not None for x in se])
            primed = any([x.validator_primed_for_suspension for x in se])

            validator_logs_queue = []
            if suspended:
                for x in se:
                    if x.validator_suspended:
                        _id = f"{block_info.slot_time:%Y-%m-%d}-{block_info.height}-{x.validator_suspended.baker_id}-suspended"
                        print("SUSPENDED", _id)
                        validator_logs_queue.append(
                            ReplaceOne(
                                {"_id": _id},
                                replacement={
                                    "_id": _id,
                                    "height": block_info.height,
                                    "baker_id": x.validator_suspended.baker_id,
                                    "date": f"{block_info.slot_time:%Y-%m-%d}",
                                    "action": "suspended",
                                },
                                upsert=True,
                                namespace=build_collection_identifier(
                                    self.namespace, Collections.validator_logs
                                ),
                            )
                        )

            if primed:
                for x in se:
                    if x.validator_primed_for_suspension:
                        _id = f"{block_info.slot_time:%Y-%m-%d}-{block_info.height}-{x.validator_primed_for_suspension.baker_id}-primed"
                        print("PRIMED", _id)
                        validator_logs_queue.append(
                            ReplaceOne(
                                {"_id": _id},
                                replacement={
                                    "_id": _id,
                                    "height": block_info.height,
                                    "baker_id": x.validator_primed_for_suspension.baker_id,
                                    "date": f"{block_info.slot_time:%Y-%m-%d}",
                                    "action": "primed",
                                },
                                upsert=True,
                                namespace=build_collection_identifier(
                                    self.namespace, Collections.validator_logs
                                ),
                            )
                        )
            if len(validator_logs_queue) > 0:
                write_result: ClientBulkWriteResult = self.mongodb.connection.bulk_write(
                    validator_logs_queue
                )

        se_list = [x.model_dump(exclude_none=True) for x in se]

        self.queues[Queue.special_events] = []
        d = {"_id": block_info.height, "special_events": se_list}
        self.queues[Queue.special_events].append(
            ReplaceOne(
                {"_id": block_info.height},
                replacement=d,
                upsert=True,
                namespace=build_collection_identifier(self.namespace, Collections.special_events),
            )
        )
        if len(self.queues[Queue.special_events]) > 0:
            write_result: ClientBulkWriteResult = self.mongodb.connection.bulk_write(
                self.queues[Queue.special_events]
            )

            console.log(
                f"{block_height:,.0f}: {len(self.queues[Queue.special_events]):,.0f} special_events"
            )
            self.queues[Queue.special_events] = []
            if write_result.upserted_count > 0:
                console.log(f"Upserted: {write_result.upserted_count:,.0f}")
            if write_result.matched_count > 0:
                console.log(f"Matched:  {write_result.matched_count:,.0f}")
            if write_result.modified_count > 0:
                console.log(f"Modified: {write_result.modified_count:,.0f}")
