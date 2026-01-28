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
from ccdexplorer.grpc_client.CCD_Types import CCD_ArrivedBlockInfo


class Mixin(_SharedConverters):
    def convertBlock(self, block) -> CCD_ArrivedBlockInfo:
        result = {}

        for descriptor in block.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, block)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_ArrivedBlockInfo(**result)

    def get_blocks(
        self: GRPCClient,
        net: Enum = NET.MAINNET,
    ) -> CCD_ArrivedBlockInfo | None:
        if net == NET.MAINNET:
            for block in self.stub_mainnet.GetBlocks(request=Empty()):
                return self.convertBlock(block)
        else:
            for block in self.stub_testnet.GetBlocks(request=Empty()):
                return self.convertBlock(block)
