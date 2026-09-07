from __future__ import annotations

import re
from enum import Enum
from typing import TYPE_CHECKING, Union

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.protocol_level_tokens_pb2 import (
    MetaEffect,
    TokenEffect,
)
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
    assert_oneof_covered,
)
from ccdexplorer.grpc_client.types_pb2 import (
    BlockItemSummary,
    AccountTransactionEffects,
    BakerEvent,
    BakerId,
    BakerKeysEvent,
    BakerStakeUpdatedData,
    ContractInitializedEvent,
    DelegationEvent,
    DelegationTarget,
    EncryptedAmountRemovedEvent,
    EncryptedSelfAmountAddedEvent,
    NewEncryptedAmountEvent,
    RegisteredData,
    SponsorDetails,
    UpdatePayload,
)

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


from ccdexplorer.grpc_client.CCD_Types import (
    CCD_AccountCreationDetails,
    CCD_AccountTransactionDetails,
    CCD_AccountTransactionEffects,
    CCD_AccountTransactionEffects_CredentialsUpdated,
    CCD_AccountTransactionEffects_EncryptedAmountTransferred,
    CCD_AccountTransactionEffects_TransferredToPublic,
    CCD_AccountTransfer,
    CCD_BakerAdded,
    CCD_BakerConfigured,
    CCD_BakerKeysEvent,
    CCD_BakerRestakeEarningsUpdated,
    CCD_BakerSetBakingRewardCommission,
    CCD_BakerSetFinalizationRewardCommission,
    CCD_BakerSetMetadataUrl,
    CCD_BakerSetOpenStatus,
    CCD_BakerSetTransactionFeeCommission,
    CCD_BakerStakeDecreased,
    CCD_BakerStakeIncreased,
    CCD_BakerStakeUpdated,
    CCD_BakerStakeUpdatedData,
    CCD_Block,
    CCD_BlockItemSummary,
    CCD_ContractInitializedEvent,
    CCD_ContractUpdateIssued,
    CCD_CredentialType,
    CCD_DelegationConfigured,
    CCD_DelegationSetDelegationTarget,
    CCD_DelegationSetRestakeEarnings,
    CCD_DelegationStakeDecreased,
    CCD_DelegationStakeIncreased,
    CCD_EncryptedSelfAmountAddedEvent,
    CCD_NewRelease,
    CCD_SponsorDetails,
    CCD_MetaEffect,
    CCD_TokenCreationDetails,
    CCD_TokenEffect,
    CCD_TokenEvent,
    CCD_TransactionType,
    CCD_TransferredWithSchedule,
    CCD_UpdateDetails,
    CCD_UpdatePayload,
)


# Every arm of UpdatePayload's `payload` oneof, mapped to the method that
# converts it. assert_oneof_covered pins this against the proto at import.
_UPDATE_PAYLOAD_CONVERTERS = {
    "protocol_update": "convertTypeWithSingleValues",
    "election_difficulty_update": "convertElectionDifficulty",
    "euro_per_energy_update": "convertExchangeRateValue",
    "micro_ccd_per_euro_update": "convertExchangeRateValue",
    "foundation_account_update": "convertType",
    "mint_distribution_update": "convertMintDistributionCpv0",
    "transaction_fee_distribution_update": "convertTransactionFeeDistribution",
    "gas_rewards_update": "convertGasRewards",
    "baker_stake_threshold_update": "convertTypeWithSingleValues",
    "root_update": "convertRootUpdate",
    "level_1_update": "convertLevel1Update",
    "add_anonymity_revoker_update": "convertArInfo",
    "add_identity_provider_update": "convertIpInfo",
    "cooldown_parameters_cpv_1_update": "convertCooldownParametersCpv1",
    "pool_parameters_cpv_1_update": "convertPoolParametersCpv1",
    "time_parameters_cpv_1_update": "convertTimeParametersCpv1",
    "mint_distribution_cpv_1_update": "convertMintDistributionCpv1",
    "gas_rewards_cpv_2_update": "convertGasRewardsV2",
    "timeout_parameters_update": "convertTypeWithSingleValues",
    "min_block_time_update": "convertType",
    "block_energy_limit_update": "convertType",
    "finalization_committee_parameters_update": "convertFinalizationCommitteeParameters",
    "validator_score_parameters_update": "convertValidatorScoreParameters",
    "create_plt_update": "convertCreatePLT",
}
assert_oneof_covered(UpdatePayload.DESCRIPTOR, "payload", _UPDATE_PAYLOAD_CONVERTERS)


# Every arm of BlockItemSummary's `details` oneof.
_BLOCK_ITEM_DETAILS_CONVERTERS = {
    "account_transaction": "convertAccountTransactionDetails",
    "account_creation": "convertAccountCreationDetails",
    "update": "convertUpdateDetails",
    "token_creation": "convertTokenCreationDetails",
}
assert_oneof_covered(BlockItemSummary.DESCRIPTOR, "details", _BLOCK_ITEM_DETAILS_CONVERTERS)


class Mixin(_SharedConverters):
    def convertNewRelease(self, message) -> list:
        schedule = []
        for entry in message:
            entry_dict = {}
            for descriptor in entry.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, entry)
                if type(value) in self.simple_types:
                    converted_value = self.convertType(value)
                    if converted_value:
                        entry_dict[key] = converted_value
            schedule.append(CCD_NewRelease(**entry_dict))

        return schedule

    def convertBakerKeysEvent(self, message) -> CCD_BakerKeysEvent:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BakerKeysEvent(**result)

    def convertEffectBakerAdded(self, message) -> CCD_BakerAdded:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            if type(value) is BakerKeysEvent:
                result[key] = self.convertBakerKeysEvent(value)

        return CCD_BakerAdded(**result)

    def convertBakerStakeUpdatedData(self, message) -> CCD_BakerStakeUpdatedData:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BakerStakeUpdatedData(**result)

    def convertEffectBakerStakeUpdated(self, message) -> CCD_BakerStakeUpdated:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            if type(value) is BakerStakeUpdatedData:
                result[key] = self.convertBakerStakeUpdatedData(value)

        return CCD_BakerStakeUpdated(**result)

    def convertBakerStakeIncreased(self, message) -> CCD_BakerStakeIncreased:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BakerStakeIncreased(**result)

    def convertBakerStakeDecreased(self, message) -> CCD_BakerStakeDecreased:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BakerStakeDecreased(**result)

    def convertDelegationStakeIncreased(self, message) -> CCD_DelegationStakeIncreased:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_DelegationStakeIncreased(**result)

    def convertDelegationStakeDecreased(self, message) -> CCD_DelegationStakeDecreased:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_DelegationStakeDecreased(**result)

    def convertBakerSetOpenStatus(self, message) -> CCD_BakerSetOpenStatus:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BakerSetOpenStatus(**result)

    def convertBakerSetMetadataUrl(self, message) -> CCD_BakerSetMetadataUrl:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BakerSetMetadataUrl(**result)

    def convertDelegationSetSetRestakeEarnings(self, message) -> CCD_DelegationSetRestakeEarnings:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_DelegationSetRestakeEarnings(**result)

    def convertBakerRestakeEarningsUpdated(self, message) -> CCD_BakerRestakeEarningsUpdated:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BakerRestakeEarningsUpdated(**result)

    def convertBakerSetBakingRewardCommission(self, message) -> CCD_BakerSetBakingRewardCommission:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)
            if key == "baking_reward_commission":
                result[key] = self.convertType(value)
        return CCD_BakerSetBakingRewardCommission(**result)

    def convertBakerSetFinalizationRewardCommission(
        self, message
    ) -> CCD_BakerSetFinalizationRewardCommission:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)
            if key == "finalization_reward_commission":
                result[key] = self.convertType(value)
        return CCD_BakerSetFinalizationRewardCommission(**result)

    def convertBakerSetTransactionFeeCommission(
        self, message
    ) -> CCD_BakerSetTransactionFeeCommission:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)
            if key == "transaction_fee_commission":
                result[key] = self.convertType(value)
        return CCD_BakerSetTransactionFeeCommission(**result)

    def convertBakerBakerAdded(self, message) -> CCD_BakerAdded:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) is BakerKeysEvent:
                result[key] = self.convertBakerKeysEvent(value)

            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BakerAdded(**result)

    def convertDelegationSetDelegationTarget(self, message) -> CCD_DelegationSetDelegationTarget:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) is DelegationTarget:
                result[key] = self.convertDelegationTarget(value)

        return CCD_DelegationSetDelegationTarget(**result)

    def convertBakerConfiguredEvents(self, message) -> list:
        events = []
        for entry in message:
            # `event` is a oneof, so exactly one field is set per entry. Picking
            # it by name rather than scanning every field for a non-empty value
            # keeps events whose whole payload is a zero-valued id (validator 0,
            # say), which an emptiness test on the wrapper would have discarded.
            key = entry.WhichOneof("event")
            if key is None:
                continue
            value = getattr(entry, key)

            if type(value) is BakerEvent.BakerStakeIncreased:
                converted = self.convertBakerStakeIncreased(value)

            elif type(value) is BakerEvent.BakerStakeDecreased:
                converted = self.convertBakerStakeDecreased(value)

            elif type(value) is BakerEvent.BakerSetMetadataUrl:
                converted = self.convertBakerSetMetadataUrl(value)

            elif type(value) is BakerEvent.BakerSetOpenStatus:
                converted = self.convertBakerSetOpenStatus(value)

            elif type(value) is BakerEvent.BakerRestakeEarningsUpdated:
                converted = self.convertBakerRestakeEarningsUpdated(value)

            elif type(value) is BakerEvent.BakerSetBakingRewardCommission:
                converted = self.convertBakerSetBakingRewardCommission(value)

            elif type(value) is BakerEvent.BakerSetTransactionFeeCommission:
                converted = self.convertBakerSetTransactionFeeCommission(value)

            elif type(value) is BakerEvent.BakerSetFinalizationRewardCommission:
                converted = self.convertBakerSetFinalizationRewardCommission(value)

            elif type(value) is BakerKeysEvent:
                converted = self.convertBakerKeysEvent(value)

            elif type(value) is BakerEvent.BakerAdded:
                converted = self.convertBakerBakerAdded(value)

            # baker_removed (BakerId), baker_suspended, baker_resumed and
            # delegation_removed all reduce to a single id.
            else:
                converted = self.convertType(value)

            events.append({key: converted})

        return events

    def convertDelegationConfiguredEvents(self, message) -> list:
        events = []
        for entry in message:
            # See convertBakerConfiguredEvents: `event` is a oneof, selected by
            # name so a zero-valued delegator or validator id survives.
            key = entry.WhichOneof("event")
            if key is None:
                continue
            value = getattr(entry, key)

            if type(value) is DelegationEvent.DelegationStakeIncreased:
                converted = self.convertDelegationStakeIncreased(value)

            elif type(value) is DelegationEvent.DelegationStakeDecreased:
                converted = self.convertDelegationStakeDecreased(value)

            elif type(value) is DelegationEvent.DelegationSetDelegationTarget:
                converted = self.convertDelegationSetDelegationTarget(value)

            elif type(value) is DelegationEvent.DelegationSetRestakeEarnings:
                converted = self.convertDelegationSetSetRestakeEarnings(value)

            # delegation_added, delegation_removed (both DelegatorId) and
            # baker_removed all reduce to a single id.
            else:
                converted = self.convertType(value)

            events.append({key: converted})

        return events

    def convertEffectBakerConfigured(self, message) -> CCD_BakerConfigured:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if key == "events":
                result[key] = self.convertBakerConfiguredEvents(value)

        return CCD_BakerConfigured(**result)

    def convertEffectDelegationConfigured(self, message) -> CCD_DelegationConfigured:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if key == "events":
                result[key] = self.convertDelegationConfiguredEvents(value)

        return CCD_DelegationConfigured(**result)

    def convertEffectAccountTransfer(self, message) -> CCD_AccountTransfer:
        result = {}
        for key, value in self.iter_set_fields(message):
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_AccountTransfer(**result)

    def convertEffectAccountTransferWithSchedule(self, message) -> CCD_TransferredWithSchedule:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if key == "amount":
                result[key] = self.convertNewRelease(value)

            elif type(value) in self.simple_types:
                converted_value = self.convertType(value)
                if converted_value:
                    result[key] = converted_value

        return CCD_TransferredWithSchedule(**result)

    def convertEffectTokenUpdate(self, message) -> CCD_TokenEffect:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if key == "events":
                result[key] = self.convertTokenEvents(value)

            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_TokenEffect(**result)

    def convertEffectMetaUpdate(self, message) -> CCD_MetaEffect:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if key == "events":
                result[key] = self.convertMetaEvents(value)

        return CCD_MetaEffect(**result)

    def convertEffectContractInitializedEvent(self, message) -> CCD_ContractInitializedEvent:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if key == "events":
                result[key] = self.convertEvents(value)

            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_ContractInitializedEvent(**result)

    def convertEffectContractUpdateIssued(self, message) -> CCD_ContractUpdateIssued:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if key == "effects":
                result[key] = self.convertUpdateEvents(value)

        return CCD_ContractUpdateIssued(**result)

    def convertCredentialsUpdated(
        self, message
    ) -> CCD_AccountTransactionEffects_CredentialsUpdated:
        result = {}

        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if key in ["new_cred_ids", "removed_cred_ids"]:
                result[key] = self.convertCredentialRegistrationIdEntries(value)

            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_AccountTransactionEffects_CredentialsUpdated(**result)

    def convertEffectAccountEncryptedAmountTransferred(
        self, message
    ) -> CCD_AccountTransactionEffects_EncryptedAmountTransferred:
        result = {}
        _type = None
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) in [EncryptedAmountRemovedEvent, NewEncryptedAmountEvent]:
                result[key] = self.convertTypeWithSingleValues(value)

        return CCD_AccountTransactionEffects_EncryptedAmountTransferred(**result)

    def convertEffectAccountTransferredToPublic(
        self, message
    ) -> CCD_AccountTransactionEffects_TransferredToPublic:
        result = {}
        _type = None
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) in [EncryptedAmountRemovedEvent]:
                result[key] = self.convertTypeWithSingleValues(value)

        return CCD_AccountTransactionEffects_TransferredToPublic(**result)

    def get_plt_token_events_type(self, effect: CCD_TokenEffect) -> str | None:
        if len(effect.events) == 0:
            return None
        event: CCD_TokenEvent = effect.events[0]
        if event.transfer_event:
            return "transfer"
        elif event.mint_event:
            return "mint"
        elif event.burn_event:
            return "burn"
        elif event.module_event:
            return re.sub(r"(?<!^)(?=[A-Z])", " ", event.module_event.type).lower()
        else:
            return None

    def convertAccountTransactionEffects(
        self, message
    ) -> tuple[CCD_AccountTransactionEffects, dict, str]:
        result = {}
        _type: dict = {"type": "account_transaction"}
        _outcome = "success"

        # `effect` is a oneof, so exactly one field is set. Selecting it by name
        # keeps a genuine conversion error visible: dispatching on whether
        # `HasField("reject_reason")` raised meant any exception raised while
        # converting a reject reason was swallowed and returned as an empty
        # effect that still claimed to be a rejection.
        key = message.WhichOneof("effect")
        if key is None:
            return CCD_AccountTransactionEffects(**result), _type, _outcome

        value = getattr(message, key)
        _type.update({"contents": key})

        if key == "none":
            _outcome = "reject"
            result[key], type_contents = self.convertRejectReasonNone(value)
            _type.update({"contents": type_contents})

        elif type(value) is ContractInitializedEvent:
            result[key] = self.convertEffectContractInitializedEvent(value)

        elif type(value) is AccountTransactionEffects.ContractUpdateIssued:
            result[key] = self.convertEffectContractUpdateIssued(value)

        elif type(value) is AccountTransactionEffects.AccountTransfer:
            result[key] = self.convertEffectAccountTransfer(value)

        elif type(value) is BakerEvent.BakerAdded:
            result[key] = self.convertEffectBakerAdded(value)

        elif type(value) is BakerId:
            result[key] = self.convertType(value)

        elif type(value) is AccountTransactionEffects.BakerStakeUpdated:
            result[key] = self.convertEffectBakerStakeUpdated(value)

        elif type(value) is BakerEvent.BakerRestakeEarningsUpdated:
            result[key] = self.convertTypeWithSingleValues(value)

        elif type(value) is BakerKeysEvent:
            result[key] = self.convertBakerKeysEvent(value)

        elif type(value) is AccountTransactionEffects.EncryptedAmountTransferred:
            result[key] = self.convertEffectAccountEncryptedAmountTransferred(value)

        elif type(value) is EncryptedSelfAmountAddedEvent:
            result[key] = CCD_EncryptedSelfAmountAddedEvent(
                **self.convertTypeWithSingleValues(value)
            )

        elif type(value) is AccountTransactionEffects.TransferredToPublic:
            result[key] = self.convertEffectAccountTransferredToPublic(value)

        elif type(value) is AccountTransactionEffects.TransferredWithSchedule:
            result[key] = self.convertEffectAccountTransferWithSchedule(value)

        elif type(value) is AccountTransactionEffects.CredentialsUpdated:
            result[key] = self.convertCredentialsUpdated(value)

        elif type(value) is RegisteredData:
            result[key] = self.convertType(value)

        elif type(value) is AccountTransactionEffects.BakerConfigured:
            result[key] = self.convertEffectBakerConfigured(value)

        elif type(value) is AccountTransactionEffects.DelegationConfigured:
            result[key] = self.convertEffectDelegationConfigured(value)

        elif type(value) is TokenEffect:
            result[key] = self.convertEffectTokenUpdate(value)
            _type.update({"additional_data": self.get_plt_token_events_type(result[key])})

        elif type(value) is MetaEffect:
            result[key] = self.convertEffectMetaUpdate(value)

        elif type(value) in self.simple_types:
            result[key] = self.convertType(value)

        return CCD_AccountTransactionEffects(**result), _type, _outcome

    def convertAccountTransactionDetails(
        self, message
    ) -> tuple[CCD_AccountTransactionDetails, CCD_TransactionType]:
        result = {}
        _type = {"type": "account_transaction"}
        for field, value in message.ListFields():
            key = field.name
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)
            if type(value) is SponsorDetails:
                result[key] = CCD_SponsorDetails(**self.convertTypeWithSingleValues(value))
            if type(value) in [AccountTransactionEffects, TokenEffect, MetaEffect]:
                (
                    result[key],
                    _type,
                    result["outcome"],
                ) = self.convertAccountTransactionEffects(value)

        return CCD_AccountTransactionDetails(**result), CCD_TransactionType(**_type)

    def convertAccountCreationDetails(
        self, message
    ) -> tuple[CCD_AccountCreationDetails, CCD_TransactionType]:
        result = {}
        _type = {"type": "account_creation"}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if key == "credential_type":
                result[key] = value
                _type.update({"contents": CCD_CredentialType(value).name})

            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_AccountCreationDetails(**result), CCD_TransactionType(**_type)

    def convertUpdatePayload(self, message) -> tuple[CCD_UpdatePayload, dict[str, str]]:
        _type = {"type": "update"}
        key = message.WhichOneof("payload")
        if key is None:
            return CCD_UpdatePayload(), _type

        _type["contents"] = key
        converter = getattr(self, _UPDATE_PAYLOAD_CONVERTERS[key])
        return CCD_UpdatePayload(**{key: converter(getattr(message, key))}), _type

    def convertTokenCreationDetails(
        self, message
    ) -> tuple[CCD_TokenCreationDetails, CCD_TransactionType]:
        result = {}
        _type = {"type": "token_creation"}
        for key, value in self.iter_set_fields(message):
            if key == "events":
                result[key] = self.convertTokenEvents(value)

            elif key == "create_plt":
                _type.update({"contents": key})
                result[key] = self.convertCreatePLT(value)

        return CCD_TokenCreationDetails(**result), CCD_TransactionType(**_type)

    def convertUpdateDetails(self, message) -> tuple[CCD_UpdateDetails, CCD_TransactionType]:
        result = {}
        _type = {"type": "update"}
        for key, value in self.iter_set_fields(message):
            if type(value) is UpdatePayload:
                result[key], _type = self.convertUpdatePayload(value)

            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_UpdateDetails(**result), CCD_TransactionType(**_type)

    def get_block_transaction_events(
        self: GRPCClient,
        block_input: Union[str, int],
        net: Enum = NET.MAINNET,
    ) -> CCD_Block:
        blockHashInput = self.generate_block_hash_input_from(block_input)

        grpc_return_value = self.stub_on_net(
            net, "GetBlockTransactionEvents", blockHashInput, streaming=True
        )

        tx_list = []
        if not grpc_return_value:
            return CCD_Block(**{"transaction_summaries": []})

        for summary in list(grpc_return_value):
            result = {
                "index": self.convertType(summary.index),
                "energy_cost": self.convertType(summary.energy_cost),
                "hash": self.convertType(summary.hash),
            }
            key = summary.WhichOneof("details")
            if key is not None:
                converter = getattr(self, _BLOCK_ITEM_DETAILS_CONVERTERS[key])
                result[key], result["type"] = converter(getattr(summary, key))
            tx_list.append(CCD_BlockItemSummary(**result))

        return CCD_Block(**{"transaction_summaries": tx_list})
