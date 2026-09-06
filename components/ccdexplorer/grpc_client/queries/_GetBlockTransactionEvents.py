from __future__ import annotations

import re
from enum import Enum
from typing import TYPE_CHECKING, Union

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.protocol_level_tokens_pb2 import (
    CreatePLT,
    MetaEffect,
    TokenCreationDetails,
    TokenEffect,
)
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.types_pb2 import (
    AccountCreationDetails,
    AccountTransactionDetails,
    AccountTransactionEffects,
    ArInfo,
    BakerEvent,
    BakerId,
    BakerKeysEvent,
    BakerStakeThreshold,
    BakerStakeUpdatedData,
    ContractInitializedEvent,
    CooldownParametersCpv1,
    DelegationEvent,
    DelegationTarget,
    ElectionDifficulty,
    EncryptedAmountRemovedEvent,
    EncryptedSelfAmountAddedEvent,
    ExchangeRate,
    FinalizationCommitteeParameters,
    GasRewards,
    GasRewardsCpv2,
    IpInfo,
    Level1Update,
    MintDistributionCpv0,
    MintDistributionCpv1,
    NewEncryptedAmountEvent,
    PoolParametersCpv1,
    ProtocolUpdate,
    RegisteredData,
    RootUpdate,
    SponsorDetails,
    TimeoutParameters,
    TimeParametersCpv1,
    TransactionFeeDistribution,
    UpdateDetails,
    UpdatePayload,
    ValidatorScoreParameters,
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
    CCD_ExchangeRate,
    CCD_NewRelease,
    CCD_SponsorDetails,
    CCD_LockCreateEvent,
    CCD_LockDestroyEvent,
    CCD_MetaEffect,
    CCD_TokenCreationDetails,
    CCD_TokenEffect,
    CCD_TokenEvent,
    CCD_TransactionType,
    CCD_TransferredWithSchedule,
    CCD_UpdateDetails,
    CCD_UpdatePayload,
)
from google.protobuf.json_format import MessageToDict


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
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if MessageToDict(value) == {}:
                pass
            else:
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
        for field, value in message.ListFields():
            key = field.name
            _outcome = "success"
            try:
                if value.HasField("reject_reason"):
                    _outcome = "reject"
                    result[key], type_contents = self.convertRejectReasonNone(value)
                    _type.update({"contents": type_contents})
            except:  # noqa F403
                if _outcome == "success":
                    if type(value) is ContractInitializedEvent:
                        _type.update({"contents": key})
                        result[key] = self.convertEffectContractInitializedEvent(value)

                    elif type(value) is AccountTransactionEffects.ContractUpdateIssued:
                        _type.update({"contents": key})
                        result[key] = self.convertEffectContractUpdateIssued(value)

                    elif type(value) is AccountTransactionEffects.AccountTransfer:
                        _type.update({"contents": key})
                        result[key] = self.convertEffectAccountTransfer(value)

                    elif type(value) is BakerEvent.BakerAdded:
                        _type.update({"contents": key})
                        result[key] = self.convertEffectBakerAdded(value)

                    elif type(value) is BakerId:
                        _type.update({"contents": key})
                        result[key] = self.convertType(value)

                    elif type(value) is AccountTransactionEffects.BakerStakeUpdated:
                        _type.update({"contents": key})
                        result[key] = self.convertEffectBakerStakeUpdated(value)

                    elif type(value) is BakerEvent.BakerRestakeEarningsUpdated:
                        _type.update({"contents": key})
                        result[key] = self.convertTypeWithSingleValues(value)

                    elif type(value) is BakerKeysEvent:
                        _type.update({"contents": key})
                        result[key] = self.convertBakerKeysEvent(value)

                    elif type(value) is AccountTransactionEffects.EncryptedAmountTransferred:
                        _type.update({"contents": key})
                        result[key] = self.convertEffectAccountEncryptedAmountTransferred(value)

                    elif type(value) is EncryptedSelfAmountAddedEvent:
                        _type.update({"contents": key})
                        result[key] = CCD_EncryptedSelfAmountAddedEvent(
                            **self.convertTypeWithSingleValues(value)
                        )

                    elif type(value) is AccountTransactionEffects.TransferredToPublic:
                        _type.update({"contents": key})
                        result[key] = self.convertEffectAccountTransferredToPublic(value)

                    elif type(value) is AccountTransactionEffects.TransferredWithSchedule:
                        _type.update({"contents": key})
                        result[key] = self.convertEffectAccountTransferWithSchedule(value)

                    elif type(value) is AccountTransactionEffects.CredentialsUpdated:
                        _type.update({"contents": key})
                        result[key] = self.convertCredentialsUpdated(value)

                    elif type(value) is RegisteredData:
                        _type.update({"contents": key})
                        result[key] = self.convertType(value)

                    elif type(value) is AccountTransactionEffects.BakerConfigured:
                        _type.update({"contents": key})
                        result[key] = self.convertEffectBakerConfigured(value)

                    elif type(value) is AccountTransactionEffects.DelegationConfigured:
                        _type.update({"contents": key})
                        result[key] = self.convertEffectDelegationConfigured(value)

                    elif type(value) is TokenEffect:
                        _type.update({"contents": key})
                        result[key] = self.convertEffectTokenUpdate(value)
                        _type.update(
                            {"additional_data": self.get_plt_token_events_type(result[key])}
                        )

                    elif type(value) is MetaEffect:
                        _type.update({"contents": key})
                        result[key] = self.convertEffectMetaUpdate(value)

                    elif type(value) in self.simple_types:
                        _type.update({"contents": key})
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
        result = {}
        _type = {"type": "update"}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if self.valueIsEmpty(value):
                pass
            else:
                _type.update({"contents": key})

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

                elif type(value) is RootUpdate:
                    result[key] = self.convertRootUpdate(value)

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

                elif type(value) is CreatePLT:
                    result[key] = self.convertCreatePLT(value)

                # Catch-all last: `simple_types` includes wrappers such as
                # ElectionDifficulty, so testing it earlier would shadow the
                # specific branches above.
                elif type(value) in self.simple_types:
                    result[key] = self.convertType(value)

        return CCD_UpdatePayload(**result), _type

    def convertTokenCreationDetails(
        self, message
    ) -> tuple[CCD_TokenCreationDetails, CCD_TransactionType]:
        result = {}
        _type = {"type": "token_creation"}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if self.valueIsEmpty(value):
                pass
            else:
                if key == "events":
                    result[key] = self.convertTokenEvents(value)

                elif key == "create_plt":
                    _type.update({"contents": key})
                    result[key] = self.convertCreatePLT(value)

        return CCD_TokenCreationDetails(**result), CCD_TransactionType(**_type)

    def convertUpdateDetails(self, message) -> tuple[CCD_UpdateDetails, CCD_TransactionType]:
        result = {}
        _type = {"type": "update"}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if self.valueIsEmpty(value):
                pass
            else:
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

        for tx in list(grpc_return_value):
            result = {}
            for field, value in tx.ListFields():
                key = field.name
                if type(value) in self.simple_types:
                    result[key] = self.convertType(value)

                if type(value) is TokenCreationDetails:
                    result[key], result["type"] = self.convertTokenCreationDetails(value)

                if type(value) is UpdateDetails:
                    result[key], result["type"] = self.convertUpdateDetails(value)

                if type(value) is AccountCreationDetails:
                    result[key], result["type"] = self.convertAccountCreationDetails(value)

                if type(value) is AccountTransactionDetails:
                    result[key], result["type"] = self.convertAccountTransactionDetails(value)

            tx_list.append(CCD_BlockItemSummary(**result))

        return CCD_Block(**{"transaction_summaries": tx_list})
