from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.CCD_Types import CCD_LockId, CCD_LockInfo
from ccdexplorer.grpc_client.protocol_level_tokens_pb2 import LockId
from ccdexplorer.grpc_client.queries._SharedConverters import Mixin as _SharedConverters
from ccdexplorer.grpc_client.types_pb2 import LockInfoRequest

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


class Mixin(_SharedConverters):
    def get_lock_info(
        self: GRPCClient,
        block_hash: str,
        lock_id: CCD_LockId,
        net: Enum = NET.MAINNET,
    ) -> CCD_LockInfo:
        blockHashInput = self.generate_block_hash_input_from(block_hash)
        request = LockInfoRequest(
            block_hash=blockHashInput,
            lock_id=LockId(
                account_index=lock_id.account_index,
                sequence_number=lock_id.sequence_number,
                creation_order=lock_id.creation_order,
            ),
        )
        grpc_return_value = self.stub_on_net(net, "GetLockInfo", request)

        result = {}
        for descriptor in grpc_return_value.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, grpc_return_value)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_LockInfo(**result)
