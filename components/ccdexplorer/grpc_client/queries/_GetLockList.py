from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.CCD_Types import CCD_LockId
from ccdexplorer.grpc_client.queries._SharedConverters import Mixin as _SharedConverters

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


class Mixin(_SharedConverters):
    def get_lock_list(
        self: GRPCClient,
        block_hash: str,
        net: Enum = NET.MAINNET,
    ) -> list[CCD_LockId]:
        blockHashInput = self.generate_block_hash_input_from(block_hash)
        grpc_return_value = self.stub_on_net(
            net, "GetLockList", blockHashInput, streaming=True
        )
        return [self.convertLockId(lock) for lock in grpc_return_value]
