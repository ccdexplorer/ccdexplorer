# ruff: noqa: F403, F405, E402
from __future__ import annotations
from ccdexplorer.grpc_client.types_pb2 import *
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)

from ccdexplorer.domain.generic import NET
from typing import TYPE_CHECKING
from ccdexplorer.grpc_client.CCD_Types import *

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


class Mixin(_SharedConverters):
    def get_account_list(
        self: GRPCClient,
        block_hash: str,
        net: Enum = NET.MAINNET,
    ) -> list[CCD_AccountAddress]:
        result = []
        blockHashInput = self.generate_block_hash_input_from(block_hash)

        grpc_return_value: list[AccountInfo] = self.stub_on_net(
            net, "GetAccountList", blockHashInput, streaming=True
        )

        for account in list(grpc_return_value):
            result.append(self.convertType(account))

        return result
