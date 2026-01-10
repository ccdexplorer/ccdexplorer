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
from ccdexplorer.grpc_client.CCD_Types import CCD_DelegatorInfo
from ccdexplorer.grpc_client.CCD_Types import *


class Mixin(_SharedConverters):
    def get_delegators_for_passive_delegation(
        self: GRPCClient,
        block_hash: str,
        net: Enum = NET.MAINNET,
    ) -> list[CCD_DelegatorInfo]:
        result: list[CCD_DelegatorInfo] = []
        blockHashInput = self.generate_block_hash_input_from(block_hash)

        grpc_return_value: Iterator[DelegatorInfo] = self.stub_on_net(
            net, "GetPassiveDelegators", blockHashInput, streaming=True
        )
        for delegator in list(grpc_return_value):
            delegator_dict = {
                "account": self.convertAccountAddress(delegator.account),
                "stake": self.convertAmount(delegator.stake),
            }
            if delegator.pending_change:
                if self.valueIsEmpty(delegator.pending_change):
                    pass
                else:
                    delegator_dict.update(
                        {"pending_change": self.convertPendingChange(delegator.pending_change)}
                    )

            result.append(CCD_DelegatorInfo(**delegator_dict))

        return result
