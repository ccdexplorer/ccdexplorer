from __future__ import annotations
from ccdexplorer.grpc_client.health_pb2 import NodeHealthRequest, NodeHealthResponse
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)


class Mixin(_SharedConverters):
    def check_health(self: GRPCClient):
        grpc_return_value: NodeHealthResponse = (
            self.health.Check(  # ty:ignore[unresolved-attribute]
                request=NodeHealthRequest()
            )
        )
        return grpc_return_value
