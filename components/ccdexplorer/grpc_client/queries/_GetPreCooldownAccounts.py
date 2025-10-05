# ruff: noqa: F403, F405, E402
from __future__ import annotations
from ccdexplorer.grpc_client.types_pb2 import *
from ccdexplorer.domain.generic import NET
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.CCD_Types import *


class Mixin(_SharedConverters):
    def get_pre_cooldown_accounts(
        self: GRPCClient,
        block_hash_or_height: str | int,
        net: Enum = NET.MAINNET,
    ) -> list[CCD_AccountIndex]:
        result = {}
        result = []
        blockHashInput = self.generate_block_hash_input_from(block_hash_or_height)

        grpc_return_value: list[CCD_AccountIndex] = self.stub_on_net(
            net, "GetPreCooldownAccounts", blockHashInput
        )

        for account_index in list(grpc_return_value):
            result.append(account_index.value)

        return result
