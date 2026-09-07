from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
    assert_oneof_covered,
)
from ccdexplorer.grpc_client.types_pb2 import PendingUpdate

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


from ccdexplorer.grpc_client.CCD_Types import (
    CCD_PendingUpdate,
)


# Every arm of PendingUpdate's `effect` oneof. Note these are not the same types
# as the equivalent UpdatePayload arms: `root_keys`/`level1_keys` are
# HigherLevelKeys here, not the RootUpdate/Level1Update wrappers.
_PENDING_UPDATE_CONVERTERS = {
    "root_keys": "convertHigherLevelKeys",
    "level1_keys": "convertHigherLevelKeys",
    "level2_keys_cpv_0": "convertAuthorizationsV0",
    "level2_keys_cpv_1": "convertAuthorizationsV1",
    "protocol": "convertTypeWithSingleValues",
    "election_difficulty": "convertElectionDifficulty",
    "euro_per_energy": "convertExchangeRateValue",
    "micro_ccd_per_euro": "convertExchangeRateValue",
    "foundation_account": "convertType",
    "mint_distribution_cpv_0": "convertMintDistributionCpv0",
    "mint_distribution_cpv_1": "convertMintDistributionCpv1",
    "transaction_fee_distribution": "convertTransactionFeeDistribution",
    "gas_rewards": "convertGasRewards",
    "gas_rewards_cpv_2": "convertGasRewardsV2",
    "pool_parameters_cpv_0": "convertTypeWithSingleValues",
    "pool_parameters_cpv_1": "convertPoolParametersCpv1",
    "add_anonymity_revoker": "convertArInfo",
    "add_identity_provider": "convertIpInfo",
    "cooldown_parameters": "convertCooldownParametersCpv1",
    "time_parameters": "convertTimeParametersCpv1",
    "timeout_parameters": "convertTypeWithSingleValues",
    "min_block_time": "convertType",
    "block_energy_limit": "convertType",
    "finalization_committee_parameters": "convertFinalizationCommitteeParameters",
    "validator_score_parameters": "convertValidatorScoreParameters",
}
assert_oneof_covered(PendingUpdate.DESCRIPTOR, "effect", _PENDING_UPDATE_CONVERTERS)


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
        for pending_update in list(grpc_return_value):
            result = {"effective_time": self.convertType(pending_update.effective_time)}

            key = pending_update.WhichOneof("effect")
            if key is not None:
                converter = getattr(self, _PENDING_UPDATE_CONVERTERS[key])
                result[key] = converter(getattr(pending_update, key))

            events.append(CCD_PendingUpdate(**result))

        return events
