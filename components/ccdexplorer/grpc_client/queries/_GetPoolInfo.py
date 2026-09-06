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
        result = {}
        blockHashInput = self.generate_block_hash_input_from(block_hash)
        baker_id = BakerId(value=pool_id)
        poolInfoRequest = PoolInfoRequest(baker=baker_id, block_hash=blockHashInput)

        grpc_return_value: PoolInfoResponse = self.stub_on_net(net, "GetPoolInfo", poolInfoRequest)

        for descriptor in grpc_return_value.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, grpc_return_value)

            # From protocol version 7 a removed pool is still reported for the
            # current reward period, but with `equity_capital`,
            # `delegated_capital`, `delegated_capital_cap`, `pool_info` and
            # `is_suspended` all absent. Converting them regardless described a
            # removed pool as one holding no capital and open to all delegators.
            if descriptor.has_presence and not grpc_return_value.HasField(key):
                result[key] = None

            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) is BakerPoolInfo:
                result[key] = self.convertBakerPoolInfo(value)

            elif type(value) is PoolCurrentPaydayInfo:
                result[key] = self.convertPoolCurrentPaydayInfo(value)

            elif type(value) is PoolPendingChange:
                result[key] = self.convertPoolPendingChange(value)

        return CCD_PoolInfo(**result)
