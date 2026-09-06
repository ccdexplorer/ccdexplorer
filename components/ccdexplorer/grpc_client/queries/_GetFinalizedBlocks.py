from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.types_pb2 import Empty

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_FinalizedBlockInfo


class Mixin(_SharedConverters):
    def convertFinalizedBlock(self, block) -> CCD_FinalizedBlockInfo:
        result = {}

        for descriptor in block.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, block)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_FinalizedBlockInfo(**result)

    def get_finalized_blocks(
        self: GRPCClient,
        net: Enum = NET.MAINNET,
    ) -> CCD_FinalizedBlockInfo | None:
        """Read the first block off the node's `GetFinalizedBlocks` stream.

        Every net configured on the client is addressable here; picking the stub
        by hand previously sent anything that was not mainnet to testnet, devnet
        included.
        """
        stub = getattr(self, f"stub_{NET(net).value}")
        if stub is None:
            return None

        for block in stub.GetFinalizedBlocks(request=Empty()):
            return self.convertFinalizedBlock(block)

        return None
