from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.types_pb2 import ElectionInfo

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_ElectionInfo, CCD_ElectionInfo_Baker
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)


class Mixin(_SharedConverters):
    def convertElectionInfoBaker(self, message) -> list[CCD_ElectionInfo_Baker]:
        bakers = []

        for list_entry in message:
            bakers.append(CCD_ElectionInfo_Baker(**self.convertTypeWithSingleValues(list_entry)))

        return bakers

    def get_election_info(
        self: GRPCClient,
        block_hash: str,
        net: Enum = NET.MAINNET,
    ) -> CCD_ElectionInfo:
        result = {}
        blockHashInput = self.generate_block_hash_input_from(block_hash)

        grpc_return_value: ElectionInfo = self.stub_on_net(net, "GetElectionInfo", blockHashInput)

        for descriptor in grpc_return_value.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, grpc_return_value)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif key == "baker_election_info":
                result[key] = self.convertElectionInfoBaker(value)

        return CCD_ElectionInfo(**result)
