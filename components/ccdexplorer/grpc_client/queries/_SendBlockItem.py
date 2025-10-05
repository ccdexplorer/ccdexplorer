# # ruff: noqa: F403, F405, E402
# from __future__ import annotations
# from ccdexplorer.grpc_client.types_pb2 import *
# from ccdexplorer.domain.net import NET
# from enum import Enum
# from typing import TYPE_CHECKING

# if TYPE_CHECKING:
#     from ccdexplorer.grpc_client import GRPCClient
# from ccdexplorer.grpc_client.queries._SharedConverters import (
#     Mixin as _SharedConverters,
# )
# import os
# import sys

#
#


# class Mixin(_SharedConverters):
#     def convertSuccess(self, message) -> CCD_InvokeInstanceResponse_Success:
#         result = {}
#         for descriptor in message.DESCRIPTOR.fields:
#             key, value = self.get_key_value_from_descriptor(descriptor, message)
#             if key == "effects":
#                 result[key] = self.convertUpdateEvents(value)

#             elif type(value) in self.simple_types:
#                 result[key] = self.convertType(value)

#         return CCD_InvokeInstanceResponse_Success(**result)

#     def convertFailure(self, message) -> CCD_InvokeInstanceResponse_Failure:
#         result = {}
#         for descriptor in message.DESCRIPTOR.fields:
#             key, value = self.get_key_value_from_descriptor(descriptor, message)
#             if type(value) is RejectReason:
#                 result[key], _ = self.convertRejectReason(value)

#             elif type(value) in self.simple_types:
#                 result[key] = self.convertType(value)

#         return CCD_InvokeInstanceResponse_Failure(**result)

#     def send_account_transaction(
#         self: GRPCClient,
#         block_hash: str,
#         contract_name: str,
#         instance_index: int,
#         instance_subindex: int,
#         entrypoint: str,
#         parameter_bytes: bytes,
#         net: Enum = NET.MAINNET,
#     ) -> TransactionHash:
#         result = {}
#         pass
#         # signature = AccountTransactionSignature()
#         # update_header = AccountTransactionHeader(
#         #     expiry: TransactionExpiry.futureMinutes(60),
#         #     nonce: (await client.getNextAccountNonce(sender)).nonce,
#         #     sender,
#         #     )

#         # update_params = serializeUpdateContractParameters(
#         #     contract_name,
#         #     entrypoint,
#         #     rainyWeather,
#         #     schema
#         # )
#         # update_payload: UpdateContractPayload = {
#         #     amount: CcdAmount.zero(),
#         #     address: unwrap(contractAddress),
#         #     receiveName,
#         #     message: update_params,
#         #     maxContractExecutionEnergy: maxCost,
#         # }
#         # payload = AccountTransactionPayload()
#         # update_signature =  signTransaction(updateTransaction, signer);
#         # update_transaction= AccountTransaction(
#         #     signature=update_signature,
#         #     header= update_header,
#         #     payload= update_payload
#         # )

#         # updateTrxHash = sendAccountTransaction(
#         #     updateTransaction,
#         #     updateSignature
#         # );

#         # blockHashInput = self.generate_block_hash_input_from(block_hash)
#         # invokeInstanceRequest = self.generate_invoke_instance_request_from(
#         #     instance_index,
#         #     instance_subindex,
#         #     blockHashInput,
#         #     entrypoint,
#         #     parameter_bytes,
#         # )

#         # grpc_return_value: InvokeInstanceResponse = self.stub_on_net(
#         #     net, "InvokeInstance", invokeInstanceRequest
#         # )

#         # for descriptor in grpc_return_value.DESCRIPTOR.fields:
#         #     key, value = self.get_key_value_from_descriptor(
#         #         descriptor, grpc_return_value
#         #     )

#         #     if type(value) is InvokeInstanceResponse.Success:
#         #         result[key] = self.convertSuccess(value)

#         #     elif type(value) is InvokeInstanceResponse.Failure:
#         #         result[key] = self.convertFailure(value)

#         # return CCD_InvokeInstanceResponse(**result)
