from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_AccountPending
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)


class Mixin(_SharedConverters):
    def get_cooldown_accounts(
        self: GRPCClient,
        block_hash_or_height: str | int,
        net: Enum = NET.MAINNET,
    ) -> list[CCD_AccountPending]:
        result = {}
        result = []
        blockHashInput = self.generate_block_hash_input_from(block_hash_or_height)

        grpc_return_value: list[CCD_AccountPending] = self.stub_on_net(
            net, "GetCooldownAccounts", blockHashInput, streaming=True
        )

        for account_pending in list(grpc_return_value):
            result.append(self.convertAccountPending(account_pending))

        return result
