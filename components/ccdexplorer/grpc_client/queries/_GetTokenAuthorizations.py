from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.CCD_Types import CCD_TokenAuthorizations
from ccdexplorer.grpc_client.protocol_level_tokens_pb2 import TokenId
from ccdexplorer.grpc_client.queries._SharedConverters import Mixin as _SharedConverters
from ccdexplorer.grpc_client.types_pb2 import TokenAuthorizationsRequest

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


class Mixin(_SharedConverters):
    def get_token_authorizations(
        self: GRPCClient,
        block_hash: str,
        token_id: str,
        net: Enum = NET.MAINNET,
    ) -> CCD_TokenAuthorizations:
        blockHashInput = self.generate_block_hash_input_from(block_hash)
        request = TokenAuthorizationsRequest(
            block_hash=blockHashInput,
            token_id=TokenId(value=token_id),
        )
        grpc_return_value = self.stub_on_net(net, "GetTokenAuthorizations", request)

        result = {}
        for descriptor in grpc_return_value.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, grpc_return_value)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_TokenAuthorizations(**result)
