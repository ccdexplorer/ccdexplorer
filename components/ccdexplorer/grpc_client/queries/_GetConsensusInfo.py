from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.types_pb2 import ConsensusInfo, Empty

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_ConsensusInfo, ProtocolVersions


class Mixin(_SharedConverters):
    def get_consensus_info(
        self: GRPCClient,
        net: Enum = NET.MAINNET,
    ) -> CCD_ConsensusInfo:
        grpc_return_value: ConsensusInfo = self.stub_on_net(net, "GetConsensusInfo", Empty())

        result = {}

        for descriptor in grpc_return_value.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, grpc_return_value)

            if key == "protocol_version":
                result[key] = ProtocolVersions(value).name

            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_ConsensusInfo(**result)
