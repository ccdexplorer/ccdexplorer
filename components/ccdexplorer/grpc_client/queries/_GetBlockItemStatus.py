from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockItemStatus,
    CCD_BlockItemSummary,
    CCD_BlockItemSummaryInBlock,
)
from ccdexplorer.grpc_client.queries._GetBlockTransactionEvents import (
    _BLOCK_ITEM_DETAILS_CONVERTERS,
)
from ccdexplorer.grpc_client.queries._SharedConverters import Mixin as _SharedConverters
from ccdexplorer.grpc_client.types_pb2 import (
    TransactionHash,
)

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


class Mixin(_SharedConverters):
    def convertBlockItemSummaryInBlock(self, message) -> CCD_BlockItemSummaryInBlock:
        block_hash = self.convertType(message.block_hash)

        summary = message.outcome
        result = {
            "index": self.convertType(summary.index),
            "energy_cost": self.convertType(summary.energy_cost),
            "hash": self.convertType(summary.hash),
        }
        key = summary.WhichOneof("details")
        if key is not None:
            converter = getattr(self, _BLOCK_ITEM_DETAILS_CONVERTERS[key])
            result[key], result["type"] = converter(getattr(summary, key))

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
                self.convertBlockItemSummaryInBlock(o) for o in grpc_return_value.committed.outcomes
            ]
            return CCD_BlockItemStatus(committed=outcomes)
        else:
            outcome = self.convertBlockItemSummaryInBlock(grpc_return_value.finalized.outcome)
            return CCD_BlockItemStatus(finalized=outcome)
