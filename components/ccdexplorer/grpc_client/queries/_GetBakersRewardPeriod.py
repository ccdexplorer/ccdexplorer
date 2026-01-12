from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.CCD_Types import CCD_BakerRewardPeriodInfo
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.types_pb2 import BakerRewardPeriodInfo

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


class Mixin(_SharedConverters):
    def get_bakers_reward_period(
        self: GRPCClient,
        block_hash: str,
        net: Enum = NET.MAINNET,
    ) -> list[CCD_BakerRewardPeriodInfo]:
        result = {}
        blockHashInput = self.generate_block_hash_input_from(block_hash)

        grpc_return_value: BakerRewardPeriodInfo = self.stub_on_net(
            net, "GetBakersRewardPeriod", blockHashInput, streaming=True
        )
        result: list[CCD_BakerRewardPeriodInfo] = []
        for baker_reward_info in list(grpc_return_value):
            result.append(self.convertBakerRewardPeriodInfo(baker_reward_info))

        return result
