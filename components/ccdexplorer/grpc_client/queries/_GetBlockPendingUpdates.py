from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.types_pb2 import (
    ArInfo,
    BakerStakeThreshold,
    CooldownParametersCpv1,
    ElectionDifficulty,
    ExchangeRate,
    GasRewards,
    IpInfo,
    Level1Update,
    MintDistributionCpv0,
    MintDistributionCpv1,
    PoolParametersCpv1,
    ProtocolUpdate,
    RootUpdate,
    TimeParametersCpv1,
    TransactionFeeDistribution,
    UpdatePayload,
)

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


from ccdexplorer.grpc_client.CCD_Types import (
    CCD_ExchangeRate,
    CCD_PendingUpdate,
    CCD_UpdateDetails,
    CCD_UpdatePayload,
)
from google.protobuf.json_format import MessageToDict


class Mixin(_SharedConverters):
    def convertPendingUpdatePayload(self, message) -> tuple[CCD_UpdatePayload, dict] | None:
        if MessageToDict(message) == {}:
            return None
        else:
            result = {}
            _type = {"type": "update"}
            for descriptor in message.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, message)

                if self.valueIsEmpty(value):
                    pass
                else:
                    _type.update({"contents": key})

            return CCD_UpdatePayload(**result), _type

    def convertPendingUpdateDetails(self, message) -> tuple[CCD_UpdateDetails, dict] | None:
        if MessageToDict(message) == {}:
            return None
        else:
            result = {}
            _type = {"type": "update"}
            for descriptor in message.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, message)

                if self.valueIsEmpty(value):
                    pass
                else:
                    if type(value) is UpdatePayload:
                        result[key], _type = self.convertPendingUpdatePayload(value)

                    elif type(value) in self.simple_types:
                        result[key] = self.convertType(value)

            return CCD_UpdateDetails(**result), _type

    def get_block_pending_updates(
        self: GRPCClient,
        block_hash: str,
        net: Enum = NET.MAINNET,
    ) -> list[CCD_PendingUpdate]:
        blockHashInput = self.generate_block_hash_input_from(block_hash)

        grpc_return_value = self.stub_on_net(
            net, "GetBlockPendingUpdates", blockHashInput, streaming=True
        )

        events = []
        for tx in list(grpc_return_value):
            result = {}
            for descriptor in tx.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, tx)
                if self.valueIsEmpty(value):
                    pass
                else:
                    if type(value) is ExchangeRate:
                        value_as_dict = MessageToDict(value)
                        result[key] = CCD_ExchangeRate(
                            **{
                                "numerator": value_as_dict["value"]["numerator"],
                                "denominator": value_as_dict["value"]["denominator"],
                            }
                        )

                    elif type(value) in [BakerStakeThreshold, ProtocolUpdate]:
                        result[key] = self.convertTypeWithSingleValues(value)

                    elif type(value) is Level1Update:
                        result[key] = self.convertLevel1Update(value)

                    elif type(value) is IpInfo:
                        result[key] = self.convertIpInfo(value)

                    elif type(value) in self.simple_types:
                        result[key] = self.convertType(value)

                    # TODO: no test available
                    elif type(value) is ElectionDifficulty:
                        result[key] = self.convertElectionDifficulty(value)

                    # TODO: no test available
                    elif type(value) is MintDistributionCpv0:
                        result[key] = self.convertMintDistributionCpv0(value)

                    # TODO: no test available
                    elif type(value) is TransactionFeeDistribution:
                        result[key] = self.convertTransactionFeeDistribution(value)

                    # TODO: no test available
                    elif type(value) is GasRewards:
                        result[key] = self.convertGasRewards(value)

                    # TODO: no test available
                    elif type(value) is RootUpdate:
                        result[key] = self.convertRootUpdate(value)

                    # TODO: no test available
                    elif type(value) is ArInfo:
                        result[key] = self.convertArInfo(value)

                    # TODO: no test available
                    elif type(value) is CooldownParametersCpv1:
                        result[key] = self.convertCooldownParametersCpv1(value)

                    # TODO: no test available
                    elif type(value) is PoolParametersCpv1:
                        result[key] = self.convertPoolParametersCpv1(value)

                    # TODO: no test available
                    elif type(value) is TimeParametersCpv1:
                        result[key] = self.convertTimeParametersCpv1(value)

                    # TODO: no test available
                    elif type(value) is MintDistributionCpv1:
                        result[key] = self.convertMintDistributionCpv1(value)
            events.append(CCD_PendingUpdate(**result))

        return events
