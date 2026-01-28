from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.CCD_Types import CCD_PoolInfo
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.types_pb2 import (
    BakerId,
    BakerPoolInfo,
    PoolCurrentPaydayInfo,
    PoolInfoRequest,
    PoolInfoResponse,
    PoolPendingChange,
)

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


class Mixin(_SharedConverters):
    def get_pool_info_for_pool(
        self: GRPCClient,
        pool_id: int,
        block_hash: str,
        net: Enum = NET.MAINNET,
    ) -> CCD_PoolInfo:
        prefix = ""
        result = {}
        blockHashInput = self.generate_block_hash_input_from(block_hash)
        baker_id = BakerId(value=pool_id)
        poolInfoRequest = PoolInfoRequest(baker=baker_id, block_hash=blockHashInput)

        grpc_return_value: PoolInfoResponse = self.stub_on_net(net, "GetPoolInfo", poolInfoRequest)

        for descriptor in grpc_return_value.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, grpc_return_value)
            key_to_store = f"{prefix}{key}"
            if type(value) in self.simple_types:
                result[key_to_store] = self.convertType(value)

            elif type(value) is BakerPoolInfo:
                result[key_to_store] = self.convertBakerPoolInfo(value)

            elif type(value) is PoolCurrentPaydayInfo:
                if self.valueIsEmpty(value):
                    result[key_to_store] = None
                else:
                    result[key_to_store] = self.convertPoolCurrentPaydayInfo(value)

            elif type(value) is PoolPendingChange:
                result[key_to_store] = self.convertPoolPendingChange(value)

        return CCD_PoolInfo(**result)
