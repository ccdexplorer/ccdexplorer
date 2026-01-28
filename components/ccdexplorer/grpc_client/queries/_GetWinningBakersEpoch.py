from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_WinningBaker
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)


class Mixin(_SharedConverters):
    def get_winning_bakers_epoch(
        self: GRPCClient,
        genesis_index: int,
        epoch: int,
        net: Enum = NET.MAINNET,
    ) -> list[CCD_WinningBaker]:
        epoch_request = self.generate_epoch_request_from_genesis(
            genesis_index=genesis_index, epoch=epoch
        )

        grpc_return_value: list[CCD_WinningBaker] | None = self.stub_on_net(
            net, "GetWinningBakersEpoch", epoch_request, streaming=True
        )

        if grpc_return_value is None:
            return []

        result = []
        for winner in list(grpc_return_value):
            result.append(self.convertWinningBaker(winner))

        return result
