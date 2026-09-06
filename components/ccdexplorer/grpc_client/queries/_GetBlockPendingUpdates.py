from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.types_pb2 import (
    ArInfo,
    AuthorizationsV0,
    AuthorizationsV1,
    BakerStakeThreshold,
    CooldownParametersCpv1,
    ElectionDifficulty,
    ExchangeRate,
    FinalizationCommitteeParameters,
    GasRewards,
    GasRewardsCpv2,
    HigherLevelKeys,
    IpInfo,
    MintDistributionCpv0,
    MintDistributionCpv1,
    PoolParametersCpv1,
    ProtocolUpdate,
    TimeoutParameters,
    TimeParametersCpv1,
    TransactionFeeDistribution,
    ValidatorScoreParameters,
)

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


from ccdexplorer.grpc_client.CCD_Types import (
    CCD_ExchangeRate,
    CCD_PendingUpdate,
)
from google.protobuf.json_format import MessageToDict


class Mixin(_SharedConverters):
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

                    # `root_keys` and `level1_keys` are HigherLevelKeys, and
                    # `level2_keys_cpv_0`/`_1` are AuthorizationsV0/V1 -- these are
                    # not the RootUpdate/Level1Update wrappers that UpdatePayload
                    # carries, so they need their own branches here.
                    elif type(value) is HigherLevelKeys:
                        result[key] = self.convertHigherLevelKeys(value)

                    elif type(value) is AuthorizationsV0:
                        result[key] = self.convertAuthorizationsV0(value)

                    elif type(value) is AuthorizationsV1:
                        result[key] = self.convertAuthorizationsV1(value)

                    elif type(value) is IpInfo:
                        result[key] = self.convertIpInfo(value)

                    elif type(value) is ElectionDifficulty:
                        result[key] = self.convertElectionDifficulty(value)

                    elif type(value) is MintDistributionCpv0:
                        result[key] = self.convertMintDistributionCpv0(value)

                    elif type(value) is TransactionFeeDistribution:
                        result[key] = self.convertTransactionFeeDistribution(value)

                    elif type(value) is GasRewards:
                        result[key] = self.convertGasRewards(value)

                    elif type(value) is GasRewardsCpv2:
                        result[key] = self.convertGasRewardsV2(value)

                    elif type(value) is ArInfo:
                        result[key] = self.convertArInfo(value)

                    elif type(value) is CooldownParametersCpv1:
                        result[key] = self.convertCooldownParametersCpv1(value)

                    elif type(value) is PoolParametersCpv1:
                        result[key] = self.convertPoolParametersCpv1(value)

                    elif type(value) is TimeParametersCpv1:
                        result[key] = self.convertTimeParametersCpv1(value)

                    elif type(value) is MintDistributionCpv1:
                        result[key] = self.convertMintDistributionCpv1(value)

                    elif type(value) is TimeoutParameters:
                        result[key] = self.convertTypeWithSingleValues(value)

                    elif type(value) is FinalizationCommitteeParameters:
                        result[key] = self.convertFinalizationCommitteeParameters(value)

                    elif type(value) is ValidatorScoreParameters:
                        result[key] = self.convertValidatorScoreParameters(value)

                    # Catch-all last: `simple_types` includes wrappers such as
                    # ElectionDifficulty, so testing it earlier would shadow the
                    # specific branches above.
                    elif type(value) in self.simple_types:
                        result[key] = self.convertType(value)
            events.append(CCD_PendingUpdate(**result))

        return events
