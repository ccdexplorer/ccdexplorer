# ruff: noqa: F403, F405, E402
from __future__ import annotations
from ccdexplorer.grpc_client.types_pb2 import *
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


from google.protobuf.json_format import MessageToDict
from ccdexplorer.grpc_client.CCD_Types import *


class Mixin(_SharedConverters):
    def convertAccountAmountsEntries(
        self, message
    ) -> list[CCD_BlockSpecialEvent_AccountAmounts_Entry]:
        entries = []

        for list_entry in message:
            entries.append(
                CCD_BlockSpecialEvent_AccountAmounts_Entry(
                    **self.convertTypeWithSingleValues(list_entry)
                )
            )

        return entries

    def convertAccountAmountsBakingRewards(self, message) -> CCD_BlockSpecialEvent_BakingRewards:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            result[key] = self.convertAccountAmountsEntries(value)

        return CCD_BlockSpecialEvent_AccountAmounts(**result)

    def convertAccountAmountsFinalizationRewards(
        self, message
    ) -> CCD_BlockSpecialEvent_FinalizationRewards:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            result[key] = self.convertAccountAmountsEntries(value)

        return CCD_BlockSpecialEvent_AccountAmounts(**result)

    def convertBakingRewards(self, message) -> CCD_BlockSpecialEvent_BakingRewards:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) is BlockSpecialEvent.AccountAmounts:
                result[key] = self.convertAccountAmountsBakingRewards(value)

            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BlockSpecialEvent_BakingRewards(**result)

    def convertFinalizationRewards(self, message) -> CCD_BlockSpecialEvent_FinalizationRewards:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) is BlockSpecialEvent.AccountAmounts:
                result[key] = self.convertAccountAmountsFinalizationRewards(value)

            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BlockSpecialEvent_FinalizationRewards(**result)

    def convertv0(self, message) -> CCD_ChainParametersV0:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) is ExchangeRate:
                value_as_dict = MessageToDict(value)
                result[key] = CCD_ExchangeRate(
                    **{
                        "numerator": value_as_dict["value"]["numerator"],
                        "denominator": value_as_dict["value"]["denominator"],
                    }
                )

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
                value_as_dict = MessageToDict(value)
                result[key] = CCD_ExchangeRate(
                    **{
                        "numerator": value_as_dict["value"]["numerator"],
                        "denominator": value_as_dict["value"]["denominator"],
                    }
                )

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
                value_as_dict = MessageToDict(value)
                result[key] = CCD_ExchangeRate(
                    **{
                        "numerator": value_as_dict["value"]["numerator"],
                        "denominator": value_as_dict["value"]["denominator"],
                    }
                )

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
                value_as_dict = MessageToDict(value)
                result[key] = CCD_ExchangeRate(
                    **{
                        "numerator": value_as_dict["value"]["numerator"],
                        "denominator": value_as_dict["value"]["denominator"],
                    }
                )

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

        for descriptor in grpc_return_value.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, grpc_return_value)

            if key == "v0" and not self.valueIsEmpty(value):
                # result_type = "v0"
                result[key] = self.convertv0(value)

            elif key == "v1" and not self.valueIsEmpty(value):
                # result_type = "v1"
                result[key] = self.convertv1(value)

            elif key == "v2" and not self.valueIsEmpty(value):
                # result_type = "v2"
                result[key] = self.convertv2(value)

            elif key == "v3" and not self.valueIsEmpty(value):
                # result_type = "v3"
                result[key] = self.convertv3(value)

        return CCD_ChainParameters(**result)
