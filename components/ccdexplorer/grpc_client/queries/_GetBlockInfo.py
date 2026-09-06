from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Union

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.types_pb2 import BlockInfo

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockInfo,
    ProtocolVersions,
)
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)


class Mixin(_SharedConverters):
    def get_block_info(
        self: GRPCClient,
        block_input: Union[str, int],
        net: Enum = NET.MAINNET,
    ) -> CCD_BlockInfo:
        result = {}
        blockHashInput = self.generate_block_hash_input_from(block_input)

        grpc_return_value: BlockInfo = self.stub_on_net(net, "GetBlockInfo", blockHashInput)

        for descriptor in grpc_return_value.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, grpc_return_value)

            # `baker` is absent on a (re)genesis block, and `slot_number`,
            # `round` and `epoch` only exist on some protocol versions --
            # slot_number up to 5, round and epoch from 6. Converting them
            # regardless reported every one of those as 0, which is a valid
            # round and a valid epoch.
            if descriptor.has_presence and not grpc_return_value.HasField(key):
                result[key] = None

            elif key == "protocol_version":
                result[key] = ProtocolVersions(value).name

            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BlockInfo(**result)
