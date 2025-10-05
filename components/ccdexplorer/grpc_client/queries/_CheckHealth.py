from __future__ import annotations
from ccdexplorer.grpc_client.types_pb2 import *
from ccdexplorer.grpc_client.health_pb2 import *
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
import sys


class Mixin(_SharedConverters):
    def check_health(self: GRPCClient):
        result = {}

        self.check_connection(sys._getframe().f_code.co_name)
        grpc_return_value: NodeHealthResponse = self.health.Check(request=NodeHealthRequest())

        return grpc_return_value
