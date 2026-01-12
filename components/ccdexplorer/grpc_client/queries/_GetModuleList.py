from __future__ import annotations

from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.types_pb2 import ModuleRef
from enum import Enum

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_ModuleRef


class Mixin(_SharedConverters):
    def get_module_list(
        self: GRPCClient,  # type: ignore
        block_hash: str,
        net: Enum = NET.MAINNET,
    ) -> list[CCD_ModuleRef]:
        result = []
        blockHashInput = self.generate_block_hash_input_from(block_hash)

        grpc_return_value: list[ModuleRef] = self.stub_on_net(
            net, "GetModuleList", blockHashInput, streaming=True
        )  # type: ignore

        for module in list(grpc_return_value):
            result.append(self.convertType(module))

        return result
