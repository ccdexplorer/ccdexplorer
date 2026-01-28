from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

import cbor2
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_ModuleState,
    CCD_TokenAmount,
    CCD_TokenInfo,
    CCD_TokenState,
)
from ccdexplorer.grpc_client.protocol_level_tokens_pb2 import (
    TokenAmount,
    TokenId,
    TokenState,
)
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.types_pb2 import TokenInfo, TokenInfoRequest
from google.protobuf import message

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


class Mixin(_SharedConverters):
    def convertTokenState(self, message: message.Message) -> CCD_TokenState:
        keys = {}

        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if key == "module_state":
                cbor_decoded = cbor2.loads(value.value)
                if "governanceAccount" in cbor_decoded:
                    cbor_decoded["governanceAccount"] = self.convert_GovernanceAccount(cbor_decoded)
                if "metadata" in cbor_decoded:
                    # the value of key metadata is again a dict.
                    # we need to make sure all values in that dict are converted to str (can be bytes)
                    cbor_decoded["metadata"] = {
                        str(k): (
                            v
                            if isinstance(v, str)
                            else (
                                v.hex() if isinstance(v, (bytes, bytearray, memoryview)) else str(v)
                            )
                        )
                        for k, v in cbor_decoded.get("metadata", {}).items()
                    }
                keys[key] = CCD_ModuleState(**cbor_decoded)

            elif type(value) in self.simple_types:
                keys[key] = self.convertType(value)
            elif type(value) is TokenAmount:
                keys[key] = CCD_TokenAmount(
                    **{"value": str(value.value), "decimals": value.decimals}
                )

        return CCD_TokenState(**keys)

    def get_token_info(
        self: GRPCClient,
        block_hash: str,
        token_id: str,
        net: Enum = NET.MAINNET,
    ) -> CCD_TokenInfo:
        blockHashInput = self.generate_block_hash_input_from(block_hash)

        token_info_request = TokenInfoRequest(
            block_hash=blockHashInput, token_id=TokenId(value=token_id)
        )

        grpc_return_value: TokenInfo = self.stub_on_net(net, "GetTokenInfo", token_info_request)

        result = {}
        for descriptor in grpc_return_value.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, grpc_return_value)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif isinstance(value, TokenState):
                result[key] = self.convertTokenState(value)

        return CCD_TokenInfo(**result)
