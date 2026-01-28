from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.protocol_level_tokens_pb2 import (
    TokenId,
)
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_TokenId


class Mixin(_SharedConverters):
    def get_token_list(
        self: GRPCClient,
        block_hash: str,
        net: Enum = NET.MAINNET,
    ) -> list[CCD_TokenId]:
        result = []
        blockHashInput = self.generate_block_hash_input_from(block_hash)

        grpc_return_value: list[TokenId] = self.stub_on_net(
            net, "GetTokenList", blockHashInput, streaming=True
        )

        for token in list(grpc_return_value):
            result.append(self.convertType(token))

        return result
