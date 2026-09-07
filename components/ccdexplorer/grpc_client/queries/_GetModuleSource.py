from __future__ import annotations

from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from enum import Enum

from ccdexplorer.grpc_client.CCD_Types import (
    CCD_ModuleRef,
    CCD_VersionedModuleSource,
)
from ccdexplorer.grpc_client.types_pb2 import VersionedModuleSource


class Mixin(_SharedConverters):
    def get_module_source(
        self: GRPCClient,
        module_ref: CCD_ModuleRef,
        block_hash: str,
        net: Enum = NET.MAINNET,
    ) -> CCD_VersionedModuleSource:
        result = {}
        moduleSourceRequest = self.generate_module_source_request_from(module_ref, block_hash)

        grpc_return_value: VersionedModuleSource = self.stub_on_net(
            net, "GetModuleSource", moduleSourceRequest
        )

        key = grpc_return_value.WhichOneof("module")
        if key is not None:
            result[key] = self.convertType(getattr(grpc_return_value, key))

        return CCD_VersionedModuleSource(**result)

    def get_module_source_original_classes(
        self: GRPCClient,
        module_ref: CCD_ModuleRef,
        block_hash: str,
        net: Enum = NET.MAINNET,
    ) -> VersionedModuleSource:
        result = {}
        moduleSourceRequest = self.generate_module_source_request_from(module_ref, block_hash)

        grpc_return_value: VersionedModuleSource = self.stub_on_net(
            net, "GetModuleSource", moduleSourceRequest
        )

        key = grpc_return_value.WhichOneof("module")
        if key is not None:
            result[key] = getattr(grpc_return_value, key)

        return VersionedModuleSource(**result)
