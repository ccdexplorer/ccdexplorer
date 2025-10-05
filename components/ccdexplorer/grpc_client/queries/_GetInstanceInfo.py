# ruff: noqa: F403, F405, E402
from __future__ import annotations
from ccdexplorer.grpc_client.types_pb2 import *
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import *


class Mixin(_SharedConverters):
    def convertMethods(self, message) -> list[CCD_ReceiveName]:
        methods = []
        for method in message:
            for descriptor in method.DESCRIPTOR.fields:
                _, value = self.get_key_value_from_descriptor(descriptor, method)

                methods.append(self.convertType(value))

        return methods

    def convertInstanceInfo_V0(self, message) -> CCD_InstanceInfo_V0:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif key == "methods":
                result[key] = self.convertMethods(value)

        return CCD_InstanceInfo_V0(**result)

    def convertInstanceInfo_V1(self, message) -> CCD_InstanceInfo_V1:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif key == "methods":
                result[key] = self.convertMethods(value)

        return CCD_InstanceInfo_V1(**result)

    def get_instance_info(
        self: GRPCClient,
        contract_index: int,
        contract_sub_index: int,
        block_hash: str,
        net: Enum = NET.MAINNET,
    ) -> CCD_InstanceInfo:
        result = {}
        instanceInfoRequest = self.generate_instance_info_request_from(
            contract_index, contract_sub_index, block_hash
        )

        grpc_return_value: InstanceInfo = self.stub_on_net(
            net, "GetInstanceInfo", instanceInfoRequest
        )

        for descriptor in grpc_return_value.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, grpc_return_value)

            if type(value) is InstanceInfo.V0:
                result[f"{key}"] = self.convertInstanceInfo_V0(value)

            elif type(value) is InstanceInfo.V1:
                result[f"{key}"] = self.convertInstanceInfo_V1(value)

        return CCD_InstanceInfo(**result)
