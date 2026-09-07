from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Union

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.types_pb2 import (
    AuthorizationsV0,
    AuthorizationsV1,
    ChainParameters,
    ConsensusParametersV1,
    CooldownParametersCpv1,
    ExchangeRate,
    FinalizationCommitteeParameters,
    GasRewards,
    GasRewardsCpv2,
    HigherLevelKeys,
    MintDistributionCpv0,
    MintDistributionCpv1,
    PoolParametersCpv1,
    TimeParametersCpv1,
    TransactionFeeDistribution,
    ValidatorScoreParameters,
)

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


from ccdexplorer.grpc_client.CCD_Types import (
    CCD_ChainParameters,
    CCD_ChainParametersV0,
    CCD_ChainParametersV1,
    CCD_ChainParametersV2,
    CCD_ChainParametersV3,
)


class Mixin(_SharedConverters):
    def convertv0(self, message) -> CCD_ChainParametersV0:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) is ExchangeRate:
                result[key] = self.convertExchangeRateValue(value)

            elif type(value) is MintDistributionCpv0:
                result[key] = self.convertMintDistributionCpv0(value)

            elif type(value) is TransactionFeeDistribution:
                result[key] = self.convertTransactionFeeDistribution(value)

            elif type(value) is GasRewards:
                result[key] = self.convertGasRewards(value)

            elif type(value) is HigherLevelKeys:
                result[key] = self.convertHigherLevelKeys(value)

            elif type(value) is AuthorizationsV0:
                result[key] = self.convertAuthorizationsV0(value)

        return CCD_ChainParametersV0(**result)

    def convertv1(self, message) -> CCD_ChainParametersV1:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) is CooldownParametersCpv1:
                result[key] = self.convertCooldownParametersCpv1(value)

            elif type(value) is TimeParametersCpv1:
                result[key] = self.convertTimeParametersCpv1(value)

            elif type(value) is ExchangeRate:
                result[key] = self.convertExchangeRateValue(value)

            elif type(value) is MintDistributionCpv1:
                result[key] = self.convertMintDistributionCpv1(value)

            elif type(value) is TransactionFeeDistribution:
                result[key] = self.convertTransactionFeeDistribution(value)

            elif type(value) is GasRewards:
                result[key] = self.convertGasRewards(value)

            elif type(value) is PoolParametersCpv1:
                result[key] = self.convertPoolParametersCpv1(value)

            elif type(value) is HigherLevelKeys:
                result[key] = self.convertHigherLevelKeys(value)

            elif type(value) is AuthorizationsV1:
                result[key] = self.convertAuthorizationsV1(value)

        return CCD_ChainParametersV1(**result)

    def convertv2(self, message) -> CCD_ChainParametersV2:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) is ConsensusParametersV1:
                result[key] = self.convertConsensusParametersV1(value)

            elif type(value) is CooldownParametersCpv1:
                result[key] = self.convertCooldownParametersCpv1(value)

            elif type(value) is TimeParametersCpv1:
                result[key] = self.convertTimeParametersCpv1(value)

            elif type(value) is ExchangeRate:
                result[key] = self.convertExchangeRateValue(value)

            elif type(value) is MintDistributionCpv1:
                result[key] = self.convertMintDistributionCpv1(value)

            elif type(value) is TransactionFeeDistribution:
                result[key] = self.convertTransactionFeeDistribution(value)

            elif type(value) is GasRewardsCpv2:
                result[key] = self.convertGasRewardsV2(value)

            elif type(value) is PoolParametersCpv1:
                result[key] = self.convertPoolParametersCpv1(value)

            elif type(value) is HigherLevelKeys:
                result[key] = self.convertHigherLevelKeys(value)

            elif type(value) is AuthorizationsV1:
                result[key] = self.convertAuthorizationsV1(value)

            elif type(value) is FinalizationCommitteeParameters:
                result[key] = self.convertFinalizationCommitteeParameters(value)

        return CCD_ChainParametersV2(**result)

    def convertv3(self, message) -> CCD_ChainParametersV3:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) is ConsensusParametersV1:
                result[key] = self.convertConsensusParametersV1(value)

            elif type(value) is CooldownParametersCpv1:
                result[key] = self.convertCooldownParametersCpv1(value)

            elif type(value) is TimeParametersCpv1:
                result[key] = self.convertTimeParametersCpv1(value)

            elif type(value) is ExchangeRate:
                result[key] = self.convertExchangeRateValue(value)

            elif type(value) is MintDistributionCpv1:
                result[key] = self.convertMintDistributionCpv1(value)

            elif type(value) is TransactionFeeDistribution:
                result[key] = self.convertTransactionFeeDistribution(value)

            elif type(value) is GasRewardsCpv2:
                result[key] = self.convertGasRewardsV2(value)

            elif type(value) is PoolParametersCpv1:
                result[key] = self.convertPoolParametersCpv1(value)

            elif type(value) is HigherLevelKeys:
                result[key] = self.convertHigherLevelKeys(value)

            elif type(value) is AuthorizationsV1:
                result[key] = self.convertAuthorizationsV1(value)

            elif type(value) is FinalizationCommitteeParameters:
                result[key] = self.convertFinalizationCommitteeParameters(value)

            elif type(value) is ValidatorScoreParameters:
                result[key] = self.convertValidatorScoreParameters(value)

        return CCD_ChainParametersV3(**result)

    def get_block_chain_parameters(
        self: GRPCClient,
        block_input: Union[str, int],
        net: Enum = NET.MAINNET,
    ) -> CCD_ChainParameters:
        result = {}

        blockHashInput = self.generate_block_hash_input_from(block_input)

        grpc_return_value: ChainParameters = self.stub_on_net(
            net, "GetBlockChainParameters", blockHashInput
        )

        # `parameters` is a oneof, one arm per chain-parameters version.
        key = grpc_return_value.WhichOneof("parameters")
        if key is not None:
            converter = getattr(self, f"convert{key}")
            result[key] = converter(getattr(grpc_return_value, key))

        return CCD_ChainParameters(**result)
