from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockItemStatus,
    CCD_BlockItemSummary,
    CCD_BlockItemSummaryInBlock,
)
from ccdexplorer.grpc_client.protocol_level_tokens_pb2 import TokenCreationDetails
from ccdexplorer.grpc_client.queries._SharedConverters import Mixin as _SharedConverters
from ccdexplorer.grpc_client.types_pb2 import (
    AccountCreationDetails,
    AccountTransactionDetails,
    TransactionHash,
    UpdateDetails,
)

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


class Mixin(_SharedConverters):
    def convertBlockItemSummaryInBlock(self, message) -> CCD_BlockItemSummaryInBlock:
        block_hash = self.convertType(message.block_hash)

        result = {}
        for field, value in message.outcome.ListFields():
            key = field.name
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            if type(value) is TokenCreationDetails:
                result[key], result["type"] = self.convertTokenCreationDetails(value)

            if type(value) is UpdateDetails:
                result[key], result["type"] = self.convertUpdateDetails(value)

            if type(value) is AccountCreationDetails:
                result[key], result["type"] = self.convertAccountCreationDetails(value)

            if type(value) is AccountTransactionDetails:
                result[key], result["type"] = self.convertAccountTransactionDetails(value)

        return CCD_BlockItemSummaryInBlock(
            block_hash=block_hash, outcome=CCD_BlockItemSummary(**result)
        )

    def get_block_item_status(
        self: GRPCClient,
        tx_hash: str,
        net: Enum = NET.MAINNET,
    ) -> CCD_BlockItemStatus:
        """Look up a transaction's status and outcome by its hash alone (no block hash needed).

        This is the node's `GetBlockItemStatus` call - useful when you have a transaction hash
        but don't yet know which block it landed in.
        """
        request = TransactionHash(value=bytes.fromhex(tx_hash))
        grpc_return_value = self.stub_on_net(net, "GetBlockItemStatus", request)

        which = grpc_return_value.WhichOneof("status")
        if which == "received":
            return CCD_BlockItemStatus(received=True)
        elif which == "committed":
            outcomes = [
                self.convertBlockItemSummaryInBlock(o)
                for o in grpc_return_value.committed.outcomes
            ]
            return CCD_BlockItemStatus(committed=outcomes)
        else:
            outcome = self.convertBlockItemSummaryInBlock(grpc_return_value.finalized.outcome)
            return CCD_BlockItemStatus(finalized=outcome)
