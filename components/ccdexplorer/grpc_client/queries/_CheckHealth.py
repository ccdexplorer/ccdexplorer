from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.health_pb2 import NodeHealthRequest, NodeHealthResponse
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from rich.console import Console

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient

console = Console()


class Mixin(_SharedConverters):
    def check_health(
        self: GRPCClient,
        net: Enum = NET.MAINNET,
    ) -> bool:
        """Whether the node currently serving `net` reports itself healthy.

        The node answers `concordium.health.Health/Check` with an empty message
        on success and signals problems -- most commonly "not caught up to the
        head of the chain" -- through the gRPC status code, so there is nothing
        to read off the response itself.
        """
        health = getattr(self, f"health_{NET(net).value}")
        if health is None:
            return False

        try:
            response: NodeHealthResponse = health.Check(NodeHealthRequest(), timeout=10.0)
        except Exception as error:
            console.log(f"Health check failed for {NET(net).value}: {error}")
            return False

        return response is not None
