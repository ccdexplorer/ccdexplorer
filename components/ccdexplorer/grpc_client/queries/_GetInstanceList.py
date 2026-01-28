from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.types_pb2 import ContractAddress

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient

from enum import Enum

from ccdexplorer.grpc_client.CCD_Types import CCD_ContractAddress


class Mixin(_SharedConverters):
    def get_instance_list(
        self: GRPCClient,
        block_hash: str,
        net: Enum = NET.MAINNET,
    ) -> list[CCD_ContractAddress]:
        result = []
        blockHashInput = self.generate_block_hash_input_from(block_hash)

        grpc_return_value: Iterator[ContractAddress] = self.stub_on_net(
            net, "GetInstanceList", blockHashInput, streaming=True
        )
        for instance in list(grpc_return_value):
            result.append(self.convertType(instance))

        return result
