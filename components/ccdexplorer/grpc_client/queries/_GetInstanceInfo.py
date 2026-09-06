from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.types_pb2 import InstanceInfo

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_InstanceInfo,
    CCD_InstanceInfo_V0,
    CCD_InstanceInfo_V1,
    CCD_ReceiveName,
)


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

        # `version` is a oneof: an instance is either V0 or V1. Converting both
        # arms unconditionally produced a fully-formed but fictional record for
        # whichever one the node had not sent -- an owner derived from 32 zero
        # bytes, a zero balance and an empty source module -- which callers then
        # had to tell apart by testing source_module against "".
        key = grpc_return_value.WhichOneof("version")
        if key is not None:
            value = getattr(grpc_return_value, key)
            if type(value) is InstanceInfo.V0:
                result[key] = self.convertInstanceInfo_V0(value)
            elif type(value) is InstanceInfo.V1:
                result[key] = self.convertInstanceInfo_V1(value)

        return CCD_InstanceInfo(**result)
