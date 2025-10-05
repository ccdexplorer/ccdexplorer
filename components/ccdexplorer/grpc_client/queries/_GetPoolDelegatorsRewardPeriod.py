# ruff: noqa: F403, F405, E402
from __future__ import annotations
from ccdexplorer.grpc_client.types_pb2 import *
from ccdexplorer.domain.generic import NET
from enum import Enum
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from typing import Iterator
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_DelegatorRewardPeriodInfo
from ccdexplorer.grpc_client.CCD_Types import *


class Mixin(_SharedConverters):
    def get_delegators_for_pool_in_reward_period(
        self: GRPCClient,
        pool_id: int,
        block_hash: str,
        net: Enum = NET.MAINNET,
    ) -> list[CCD_DelegatorRewardPeriodInfo]:
        result = []
        blockHashInput = self.generate_block_hash_input_from(block_hash)
        baker_id = BakerId(value=pool_id)
        delegatorsRequest = GetPoolDelegatorsRequest(baker=baker_id, block_hash=blockHashInput)

        grpc_return_value: Iterator[DelegatorRewardPeriodInfo] = self.stub_on_net(
            net, "GetPoolDelegatorsRewardPeriod", delegatorsRequest
        )

        for delegator in list(grpc_return_value):
            result.append(
                CCD_DelegatorRewardPeriodInfo(
                    **{
                        "account": self.convertAccountAddress(delegator.account),
                        "stake": self.convertAmount(delegator.stake),
                    }
                )
            )

        return result
