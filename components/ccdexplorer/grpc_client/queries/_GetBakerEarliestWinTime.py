from __future__ import annotations

import datetime as dt
from datetime import timezone
from enum import Enum
from typing import TYPE_CHECKING

import grpc
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.types_pb2 import BakerId

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_TimeStamp


class Mixin(_SharedConverters):
    def get_baker_earliest_win_time(
        self: GRPCClient,
        baker_id: int,
        net: Enum = NET.MAINNET,
    ) -> CCD_TimeStamp | None:
        try:
            grpc_return_value = self.stub_on_net(
                net, "GetBakerEarliestWinTime", BakerId(value=baker_id)
            )
        except grpc.RpcError as rpc_error:
            if rpc_error.code() == grpc.StatusCode.UNIMPLEMENTED:  # ty:ignore[unresolved-attribute]
                print("Not implemented on mainnet")
            return None

        win_time = None

        for descriptor in grpc_return_value.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, grpc_return_value)

            if key == "value":
                win_time = dt.datetime.fromtimestamp(value / 1_000).astimezone(tz=timezone.utc)

        return win_time
