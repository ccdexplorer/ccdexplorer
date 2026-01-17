# ruff: noqa: F403, F405, E402
from __future__ import annotations

import datetime as dt
from datetime import timezone
from enum import Enum
from typing import Any, Protocol, Union
from decimal import Decimal
import base58
import cbor2
from google._upb._message import RepeatedCompositeContainer
from google.protobuf.json_format import MessageToDict


from ccdexplorer.grpc_client.kernel_pb2 import *
from ccdexplorer.grpc_client.protocol_level_tokens_pb2 import *
from ccdexplorer.grpc_client.types_pb2 import *
from ccdexplorer.grpc_client.CCD_Types import *


class OpenStatusEnum(Enum):
    openForAll = 0
    closedForNew = 1
    closedForAll = 2


class TransactionType(Enum):
    DEPLOY_MODULE = 0
    INIT_CONTRACT = 1
    UPDATE = 2
    TRANSFER = 3
    ADD_BAKER = 4
    REMOVE_BAKER = 5
    UPDATE_BAKER_STAKE = 6
    UPDATE_BAKER_RESTAKE_EARNINGS = 7
    UPDATE_BAKER_KEYS = 8
    UPDATE_CREDENTIAL_KEYS = 9
    ENCRYPTED_AMOUNT_TRANSFER = 10
    TRANSFER_TO_ENCRYPTED = 11
    TRANSFER_TO_PUBLIC = 12
    TRANSFER_WITH_SCHEDULE = 13
    UPDATE_CREDENTIALS = 14
    REGISTER_DATA = 15
    TRANSFER_WITH_MEMO = 16
    ENCRYPTED_AMOUNT_TRANSFER_WITH_MEMO = 17
    TRANSFER_WITH_SCHEDULE_AND_MEMO = 18
    CONFIGURE_BAKER = 19
    CONFIGURE_DELEGATION = 20


class Mixin(Protocol):
    # These types should be encoded to HEX
    bytes_to_hex_types = [
        bytes,
        BlockHash,
        TransactionHash,
        CredentialRegistrationId,
        StateHash,
        Memo,
        ModuleRef,
        Commitment,
        EncryptionKey,
        EncryptedAmount,
        BakerElectionVerifyKey,
        BakerSignatureVerifyKey,
        BakerAggregationVerifyKey,
        RegisteredData,
        IpInfo.IpVerifyKey,
        IpInfo.IpCdiVerifyKey,
        ArInfo.ArPublicKey,
        LeadershipElectionNonce,
        Parameter,
        Sha256Hash,
        ContractStateV0,
        VersionedModuleSource.ModuleSourceV0,
        VersionedModuleSource.ModuleSourceV1,
        QuorumSignature,
        TimeoutSignature,
        BlockSignature,
        SuccessorProof,
        FinalizationCommitteeHash,
        Cbor,
        TokenModuleRef,
    ]
    # These types should be shown as is (from the .value property)
    value_property_types = [
        AbsoluteBlockHeight,
        Amount,
        AccountThreshold,
        AccountIndex,
        ArInfo.ArIdentity,
        ArThreshold,
        CredentialsPerBlockLimit,
        BlockHeight,
        BlockItemSummary.TransactionIndex,
        DurationSeconds,
        Duration,
        Energy,
        RewardPeriodLength,
        Epoch,
        InitName,
        IpIdentity,
        IdentityProviderIdentity,
        GenesisIndex,
        ReceiveName,
        SequenceNumber,
        SignatureThreshold,
        Slot,
        TransactionTime,
        UpdateKeysThreshold,
        Round,
        FinalizerIndex,
        TokenId,
    ]
    # These types need special attention for conversion
    remaining_types = [
        AccountAddress,
        Address,
        AccountVerifyKey,
        BakerId,
        ChainArData,
        ContractAddress,
        DelegatorId,
        Empty,
        Timestamp,
        ElectionDifficulty,
        str,
        int,
        bool,
        float,
        TokenHolder,
    ]

    # all types that are covered by the `convertType` method.
    simple_types = bytes_to_hex_types + value_property_types + remaining_types

    def get_key_value_from_descriptor(self, descriptor, the_list):
        return descriptor.name, getattr(the_list, descriptor.name)

    def generate_account_identifier_input_from(self, hex_address: str):
        try:
            bin_value = base58.b58decode_check(hex_address)[1:]
            address = AccountAddress(value=bin_value)
            account = AccountIdentifierInput(address=address)
            return account
        except:  # noqa : F403, E722
            return None

    def generate_account_identifier_input_from_account_index(self, account_index: CCD_AccountIndex):
        try:
            account = AccountIdentifierInput(account_index=AccountIndex(value=account_index))
            return account
        except:  # noqa: E722
            return None

    def generate_epoch_request_from_genesis(self, genesis_index: int, epoch: int) -> EpochRequest:
        relative_epoch = EpochRequest.RelativeEpoch(
            genesis_index=GenesisIndex(value=genesis_index),
            epoch=Epoch(value=epoch),
        )
        return EpochRequest(relative_epoch=relative_epoch)

    def generate_consensus_detailed_status_query(self):
        return ConsensusDetailedStatusQuery()

    def valueIsEmpty(self, value, key=None, message=None):
        if isinstance(value, int):
            return value is None
        else:
            if type(value) is RepeatedCompositeContainer:
                return False
            else:
                if hasattr(value, "DESCRIPTOR"):
                    if message:
                        lll = list(message)
                        key_in_message = key in lll[0].__str__()
                        if key_in_message:
                            # special case for baker_id = 0
                            # (doesn't get transmitted in a message)
                            if key in ["baker_removed"]:
                                return False
                            else:
                                if MessageToDict(value) != {}:
                                    return False
                                else:
                                    return True
                        return MessageToDict(value) == {}
                    else:
                        return MessageToDict(value) == {}
                else:  # pragma: no cover
                    return False

    def generate_block_hash_input_from(self, block_input: Union[str, int]) -> BlockHashInput:
        if isinstance(block_input, str):
            if block_input == "last_final":
                return BlockHashInput(last_final={})
            else:
                return BlockHashInput(given=BlockHash(value=bytes.fromhex(block_input)))
        elif isinstance(block_input, int):
            absoluteBlockHeight = AbsoluteBlockHeight(value=block_input)
            return BlockHashInput(absolute_height=absoluteBlockHeight)

    def generate_invoke_instance_request_from(
        self,
        contract_index: int,
        contract_sub_index: int,
        block_hash_input: str,
        receive_name: str,
        parameter_bytes: bytes,
    ):
        # parameter_bytes = self.get_bytes_parameter_from_hex(parameter_hex)
        # if we include a zero subindex, grpc will complain about
        # LengthDelimited vs VarInt.
        if contract_sub_index == 0:
            instance = ContractAddress(index=contract_index)
        else:
            instance = ContractAddress(
                index=contract_index,
                subindex=contract_sub_index,
            )

        return InvokeInstanceRequest(
            block_hash=block_hash_input,
            amount=Amount(value=0),
            entrypoint=ReceiveName(value=receive_name),
            parameter=Parameter(value=parameter_bytes),
            energy=Energy(value=100_000_000_000),
            instance=instance,
        )

    def generate_instance_info_request_from(
        self, contract_index: int, contract_sub_index: int, hex_block_hash: str
    ):
        return InstanceInfoRequest(
            block_hash=self.generate_block_hash_input_from(hex_block_hash),
            address=ContractAddress(**{"index": contract_index, "subindex": contract_sub_index}),
        )

    def generate_module_source_request_from(self, module_ref: CCD_ModuleRef, hex_block_hash: str):
        return ModuleSourceRequest(
            block_hash=self.generate_block_hash_input_from(hex_block_hash),
            module_ref=ModuleRef(value=bytes.fromhex(module_ref)),
        )

    def convertList(self, message):
        entries = []

        for list_entry in message:
            entries.append(self.convertType(list_entry))

        return entries

    def convertType(self, value) -> Any:
        # these types have a property `value` that we need to return unmodified
        if type(value) in self.value_property_types:
            return value.value

        if isinstance(value, bytes):
            return value.hex()
        # these types have a property `value` that we need to encode as HEX>
        elif type(value) in self.bytes_to_hex_types:
            return value.value.hex()

        elif type(value) in [int, bool, str, float]:
            return value

        elif type(value) is ElectionDifficulty:
            return value.value.parts_per_hundred_thousand / 100_000

        elif type(value) is AmountFraction:
            return value.parts_per_hundred_thousand / 100_000

        elif type(value) is Empty:
            return None  # pragma: no cover
        elif type(value) is BakerEvent.BakerResumed:
            return value.baker_id.value

        elif type(value) is Address:
            if MessageToDict(value.account) == {}:
                return CCD_Address(**{"contract": self.convertContractAddress(value.contract)})
            else:
                return CCD_Address(**{"account": self.convertAccountAddress(value.account)})

        elif type(value) is AccountAddress:
            return self.convertAccountAddress(value)

        elif type(value) is BakerId:
            return value.value
            # else:
            #     return None

        elif type(value) is ContractAddress:
            return self.convertContractAddress(value)

        elif type(value) is AccountVerifyKey:
            return value.ed25519_key.hex()

        elif type(value) is ChainArData:
            return value.enc_id_cred_pub_share.hex()

        elif type(value) is Timestamp:
            if MessageToDict(value) == {}:
                pass
            else:
                return dt.datetime.fromtimestamp(
                    int(MessageToDict(value)["value"]) / 1_000, tz=timezone.utc
                )

        elif type(value) is DelegatorId:
            return value.id.value

        elif type(value) is TokenHolder:
            return CCD_TokenHolder(account=self.convertAccountAddress(value.account))
        # elif type(value) is TokenModuleRef:
        #     return CCD_TokenModuleRef(value=value.value)
        elif type(value) is TokenId:
            return value.value  # CCD_TokenId(value=value.value)

    def convertContractAddress(self, value: ContractAddress) -> CCD_ContractAddress:
        return CCD_ContractAddress(**{"index": value.index, "subindex": value.subindex})

    def convertAccountAddress(self, value: AccountAddress) -> CCD_AccountAddress:
        return base58.b58encode_check(b"\x01" + value.value).decode()

    def convertAmount(self, value: Amount) -> microCCD:
        return value.value

    def convertDelegationTarget(self, message) -> CCD_DelegationTarget:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) is Empty:
                pass
            if type(value) is BakerId:
                if self.valueIsEmpty(value):
                    result["passive_delegation"] = True
                else:
                    # result['passive_delegation'] = False
                    result["baker"] = self.convertType(value)

        return CCD_DelegationTarget(**result)

    def convertCommissionRates(self, value) -> CCD_CommissionRates:
        result = {}
        for descriptor in value.DESCRIPTOR.fields:
            key, val = self.get_key_value_from_descriptor(descriptor, value)
            result[key] = val.parts_per_hundred_thousand / 100_000
        return CCD_CommissionRates(**result)

    def convertValidatorScoreParameters(self, message) -> CCD_ValidatorScoreParameters:
        return CCD_ValidatorScoreParameters(
            **{"maximum_missed_rounds": message.maximum_missed_rounds}
        )

    def convertRelease(self, message) -> CCD_ReleaseSchedule:
        resulting_dict = {}

        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if key == "schedules":
                schedule = []
                for entry in value:
                    entry_dict = {}
                    for descriptor in entry.DESCRIPTOR.fields:
                        key, value = self.get_key_value_from_descriptor(descriptor, entry)

                        if key == "transactions":
                            entry_dict[key] = self.convertList(value)

                        elif type(value) is Timestamp:
                            entry_dict[key] = self.convertType(value)

                        elif type(value) is Amount:
                            entry_dict[key] = self.convertType(value)

                    schedule.append(entry_dict)
                resulting_dict["schedules"] = schedule

            elif type(value) is Amount:
                resulting_dict[key] = self.convertType(value)

        return CCD_ReleaseSchedule(**resulting_dict)

    def convertCoolDowns(self, message) -> list[CCD_Cooldown]:
        resulting_dict = {}

        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            # if key == "schedules":
            #     schedule = []
            #     for entry in value:
            #         entry_dict = {}
            #         for descriptor in entry.DESCRIPTOR.fields:
            #             key, value = self.get_key_value_from_descriptor(
            #                 descriptor, entry
            #             )

            #             if key == "transactions":
            #                 entry_dict[key] = self.convertList(value)

            #             elif type(value) is Timestamp:
            #                 entry_dict[key] = self.convertType(value)

            #             elif type(value) is Amount:
            #                 entry_dict[key] = self.convertType(value)

            #         schedule.append(entry_dict)
            #     resulting_dict["schedules"] = schedule

            # elif type(value) is Amount:
            #     resulting_dict[key] = self.convertType(value)

        return CCD_Cooldown(**resulting_dict)

    def convertAccountIndex(self, message) -> CCD_AccountIndex:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            result[key] = self.convertType(value)

        return CCD_AccountIndex(**result)

    def convertAccountPending(self, message) -> CCD_AccountPending:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in [AccountIndex, Timestamp]:
                result[key] = self.convertType(value)

        return CCD_AccountPending(**result)

    def convertWinningBaker(self, message) -> CCD_WinningBaker:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            result[key] = self.convertType(value)

        return CCD_WinningBaker(**result)

    def convertArInfo(self, message) -> CCD_ArInfo:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in [ArInfo.ArIdentity, ArInfo.ArPublicKey]:
                result[key] = self.convertType(value)

            elif type(value) is Description:
                result[key] = self.convertTypeWithSingleValues(value)

        return CCD_ArInfo(**result)

    def convertIpInfo(self, message) -> CCD_IpInfo:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in [IpIdentity, IpInfo.IpVerifyKey, IpInfo.IpCdiVerifyKey]:
                result[key] = self.convertType(value)

            elif type(value) is Description:
                result[key] = self.convertTypeWithSingleValues(value)

        return CCD_IpInfo(**result)

    def convertBakerPoolInfo(self, message) -> CCD_BakerPoolInfo:
        result = {}

        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            if key == "open_status":
                result[key] = CCD_OpenStatusTranslation(value).name

            elif type(value) is CommissionRates:
                result[key] = self.convertCommissionRates(value)

        return CCD_BakerPoolInfo(**result)

    def convertBakerRewardPeriodInfo(self, message) -> CCD_BakerRewardPeriodInfo:
        result = {}

        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if key == "is_finalizer":
                result[key] = value
            elif key == "baker":
                result[key] = self.convertTypeWithSingleValues(value)
            else:
                if type(value) in self.simple_types:
                    result[key] = self.convertType(value)

                elif type(value) is CommissionRates:
                    result[key] = self.convertCommissionRates(value)

        return CCD_BakerRewardPeriodInfo(**result)

    def convertPoolCurrentPaydayInfo(self, message) -> CCD_CurrentPaydayStatus:
        result = {}

        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            result[key] = self.convertType(value)

        return CCD_CurrentPaydayStatus(**result)

    def convertInclusiveRange(self, message) -> CCD_InclusiveRangeAmountFraction:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) is AmountFraction:
                result[key] = self.convertType(value)

        return CCD_InclusiveRangeAmountFraction(**result)

    def convertTypeWithSingleValues(self, message):
        result = {}

        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) is InclusiveRangeAmountFraction:
                result[key] = self.convertTypeWithSingleValues(value)

            elif type(value) is Ratio:
                result[key] = CCD_Ratio(
                    **{
                        "numerator": str(value.numerator),
                        "denominator": str(value.denominator),
                    }
                )

            elif type(value) is RewardPeriodLength:
                result[key] = self.convertType(value.value)

            elif type(value) is MintRate:
                result[key] = CCD_MintRate(
                    **{"mantissa": value.mantissa, "exponent": value.exponent}
                )

            elif type(value) is TokenId:
                result[key] = value.value
            elif type(value) is BakerId:
                if descriptor.json_name in MessageToDict(message):
                    result[key] = self.convertType(value)
            else:
                result[key] = self.convertType(value)

        return result

    def convertPendingChange_Reduce_Remove(self, message) -> CCD_StakePendingChange_Remove:
        return self.convertType(message)

    def convertPendingChange(self, message) -> CCD_StakePendingChange:
        result = {}

        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if message.WhichOneof("change") == "reduce" and key == "reduce":
                result[key] = self.convertPendingChange_Reduce_Remove(value)
            elif message.WhichOneof("change") == "remove" and key == "remove":
                result[key] = self.convertPendingChange_Reduce_Remove(value)

        return CCD_StakePendingChange(**result)

    def convertPoolPendingChange_Reduce_Remove(self, message) -> CCD_BakerStakePendingChange_Remove:
        return self.convertTypeWithSingleValues(message)

    def convertPoolPendingChange(self, message) -> CCD_BakerStakePendingChange:
        result = {}

        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if message.WhichOneof("change") == "reduce" and key == "reduce":
                result[key] = self.convertPoolPendingChange_Reduce_Remove(value)
            elif message.WhichOneof("change") == "remove" and key == "remove":
                result[key] = self.convertPoolPendingChange_Reduce_Remove(value)

        return CCD_BakerStakePendingChange(**result)

    def convertExchangeRate(self, message) -> CCD_ExchangeRate:
        if MessageToDict(message) == {}:
            return None
        else:
            result = {}
            for descriptor in message.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, message)
                if MessageToDict(value) == {}:
                    pass
                else:
                    if type(value) is Ratio:
                        result[key] = CCD_Ratio(
                            **{
                                "numerator": value.numerator,
                                "denominator": value.denominator,
                            }
                        )

        return CCD_ExchangeRate(**result)

    def convertUpdatePublicKeys(self, message) -> list[CCD_UpdatePublicKey]:
        keys = []

        for entry in message:
            for descriptor in entry.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, entry)

                # TODO: this needs to be decoded to hex still
                if type(value) in self.simple_types:
                    keys.append(self.convertType(value))

        return keys

    def convertHigherLevelKeys(self, message) -> CCD_HigherLevelKeys:
        if MessageToDict(message) == {}:
            return None
        else:
            result = {}
            for descriptor in message.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, message)
                if key == "keys":
                    result[key] = self.convertUpdatePublicKeys(value)

                elif type(value) in self.simple_types:
                    result[key] = self.convertType(value)

        return CCD_HigherLevelKeys(**result)

    def convertAccessPublicKeys(self, message) -> list[CCD_UpdatePublicKey]:
        keys = []

        for entry in message:
            for descriptor in entry.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, entry)

                if type(value) in self.simple_types:
                    keys.append(self.convertType(value))

        return keys

    def convertAccessStructure(self, message) -> CCD_AccessStructure:
        if MessageToDict(message) == {}:
            return None
        else:
            result = {}
            for descriptor in message.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, message)
                if self.valueIsEmpty(value):
                    pass
                else:
                    if key == "access_public_keys":
                        result[key] = self.convertAccessPublicKeys(value)

                    elif type(value) in self.simple_types:
                        result[key] = self.convertType(value)

        return CCD_AccessStructure(**result)

    def convertAuthorizationsV0(self, message) -> CCD_AuthorizationsV0:
        if MessageToDict(message) == {}:
            return None
        else:
            result = {}
            for descriptor in message.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, message)
                if self.valueIsEmpty(value):
                    pass
                else:
                    if key == "keys":
                        result[key] = self.convertUpdatePublicKeys(value)

                    elif type(value) is AccessStructure:
                        result[key] = self.convertAccessStructure(value)

                    elif type(value) in self.simple_types:
                        result[key] = self.convertType(value)

        return CCD_AuthorizationsV0(**result)

    def convertAuthorizationsV1(self, message) -> CCD_AuthorizationsV1:
        if MessageToDict(message) == {}:
            return None
        else:
            result = {}
            for descriptor in message.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, message)
                if self.valueIsEmpty(value):
                    pass
                else:
                    if type(value) is AuthorizationsV0:
                        result[key] = self.convertAuthorizationsV0(value)

                    elif type(value) is AccessStructure:
                        result[key] = self.convertAccessStructure(value)

                    elif type(value) in self.simple_types:
                        result[key] = self.convertType(value)

        return CCD_AuthorizationsV1(**result)

    def convertLevel1Update(self, message) -> CCD_Level1Update:
        if MessageToDict(message) == {}:
            return None
        else:
            result = {}
            for descriptor in message.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, message)
                if MessageToDict(value) == {}:
                    pass
                else:
                    if type(value) is HigherLevelKeys:
                        result[key] = self.convertHigherLevelKeys(value)

                    elif type(value) is AuthorizationsV0:
                        result[key] = self.convertAuthorizationsV0(value)

                    elif type(value) is AuthorizationsV1:
                        result[key] = self.convertAuthorizationsV1(value)

        return CCD_Level1Update(**result)

    def convertRootUpdate(self, message) -> CCD_RootUpdate:
        if MessageToDict(message) == {}:
            return None
        else:
            result = {}
            for descriptor in message.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, message)
                if MessageToDict(value) == {}:
                    pass
                else:
                    if type(value) is HigherLevelKeys:
                        result[key] = self.convertHigherLevelKeys(value)

                    elif type(value) is AuthorizationsV0:
                        result[key] = self.convertAuthorizationsV0(value)

                    elif type(value) is AuthorizationsV1:
                        result[key] = self.convertAuthorizationsV1(value)

        return CCD_RootUpdate(**result)

    def converCommissionRanges(self, message) -> CCD_CommissionRanges:
        # TODO: no test available
        result = self.convertTypeWithSingleValues(message)

        return CCD_CommissionRanges(**result)

    def convertPoolParametersCpv1(self, message) -> CCD_PoolParametersCpv1:
        if MessageToDict(message) == {}:
            return None
        else:
            result = {}
            for descriptor in message.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, message)
                if MessageToDict(value) == {}:
                    pass
                else:
                    if type(value) is CommissionRanges:
                        result[key] = self.converCommissionRanges(value)

                    # elif type(value) in [BakerStakeThreshold, ProtocolUpdate]:
                    #         result[key] = self.convertTypeWithSingleValues(value)

                    elif type(value) in self.simple_types:
                        result[key] = self.convertType(value)

                    elif type(value) in [CapitalBound, LeverageFactor]:
                        result[key] = self.convertTypeWithSingleValues(value)

                    elif type(value) is AmountFraction:
                        result[key] = self.convertType(value)

        return CCD_PoolParametersCpv1(**result)

    def convertElectionDifficulty(self, message) -> CCD_ElectionDifficulty:
        # TODO: no test available
        return message.value.parts_per_hundred_thousand / 100_000

    def convertMintRate(self, message) -> CCD_MintRate:
        # TODO: no test available
        result = self.convertTypeWithSingleValues(message)

        return CCD_MintRate(**result)

    def convertTimeParametersCpv1(self, message) -> CCD_TimeParametersCpv1:
        # TODO: no test available
        result = self.convertTypeWithSingleValues(message)

        return CCD_TimeParametersCpv1(**result)

    def convertCooldownParametersCpv1(self, message) -> CCD_CooldownParametersCpv1:
        # TODO: no test available
        result = self.convertTypeWithSingleValues(message)

        return CCD_CooldownParametersCpv1(**result)

    def convertConsensusParametersV1(self, message) -> CCD_ConsensusParametersV1:
        # TODO: no test available
        if MessageToDict(message) == {}:
            return None
        else:
            result = {}
            for descriptor in message.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, message)
                if self.valueIsEmpty(value):
                    pass
                else:
                    if type(value) is TimeoutParameters:
                        result[key] = self.convertTypeWithSingleValues(value)

                    elif type(value) in self.simple_types:
                        result[key] = self.convertType(value)
        # result = self.convertTypeWithSingleValues(message)

        return CCD_ConsensusParametersV1(**result)

    def convertFinalizationCommitteeParameters(
        self, message
    ) -> CCD_FinalizationCommitteeParameters:
        # TODO: no test available
        result = self.convertTypeWithSingleValues(message)

        return CCD_FinalizationCommitteeParameters(**result)

    def convertTransactionFeeDistribution(self, message) -> CCD_TransactionFeeDistribution:
        # TODO: no test available
        result = self.convertTypeWithSingleValues(message)

        return CCD_TransactionFeeDistribution(**result)

    def convertGasRewards(self, message) -> CCD_GasRewards:
        # TODO: no test available
        result = self.convertTypeWithSingleValues(message)

        return CCD_GasRewards(**result)

    def convertGasRewardsV2(self, message) -> CCD_GasRewardsV2:
        # TODO: no test available
        result = self.convertTypeWithSingleValues(message)

        return CCD_GasRewardsV2(**result)

    def convertMintDistributionCpv1(self, message) -> CCD_MintDistributionCpv1:
        # TODO: no test available
        result = self.convertTypeWithSingleValues(message)

        return CCD_MintDistributionCpv1(**result)

    def convertMintDistributionCpv0(self, message) -> CCD_MintDistributionCpv0:
        # TODO: no test available
        if self.valueIsEmpty(message):
            return None
        else:
            result = {}
            for descriptor in message.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, message)
                if self.valueIsEmpty(value):
                    pass
                else:
                    if type(value) is MintRate:
                        result[key] = self.convertMintRate(value)

                    elif type(value) is AmountFraction:
                        result[key] = self.convertType(value)

        return CCD_MintDistributionCpv0(**result)

    def convertTokenModuleEvent(self, message) -> CCD_TokenModuleEvent:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if value == "":
                pass
            else:
                if type(value) in self.simple_types:
                    result[key] = self.convertType(value)

        return CCD_TokenModuleEvent(**result)

    def convertTokenTransferEvent(self, message) -> CCD_TokenTransferEvent:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)
            elif key == "amount":
                result[key] = CCD_TokenAmount(
                    **{"value": str(value.value), "decimals": value.decimals}
                )
        return CCD_TokenTransferEvent(**result)

    def convertTokenSupplyUpdateEvent(self, message) -> CCD_TokenSupplyUpdateEvent:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)
            elif key == "amount":
                result[key] = CCD_TokenAmount(
                    **{"value": str(value.value), "decimals": value.decimals}
                )
        return CCD_TokenSupplyUpdateEvent(**result)

    def convertDecimal(self, value: Decimal) -> CCD_TokenAmount:
        return CCD_TokenAmount(
            **{
                "value": "".join(str(digit) for digit in value.as_tuple().digits),
                "decimals": -1 * value.as_tuple().exponent,
            }
        )

    def convert_GovernanceAccount(self, cbor_decoded: dict) -> CCD_TokenHolder:
        return CCD_TokenHolder(
            account=self.convertAccountAddress(
                AccountAddress(value=cbor_decoded.get("governanceAccount").value[3])  # type: ignore
            )
        )

    def convertCreatePLT(self, message) -> CCD_CreatePLT:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if key == "initialization_parameters":
                cbor_decoded = cbor2.loads(value.value)
                if "initialSupply" in cbor_decoded:
                    token_amount = self.convertDecimal(cbor_decoded.get("initialSupply"))
                    cbor_decoded["initialSupply"] = token_amount
                cbor_decoded["governanceAccount"] = self.convert_GovernanceAccount(cbor_decoded)
                if "metadata" in cbor_decoded:
                    # the value of key metadata is again a dict.
                    # we need to make sure all values in that dict are converted to str (can be bytes)
                    cbor_decoded["metadata"] = {
                        str(k): (
                            v
                            if isinstance(v, str)
                            else (
                                v.hex() if isinstance(v, (bytes, bytearray, memoryview)) else str(v)
                            )
                        )
                        for k, v in cbor_decoded.get("metadata", {}).items()
                    }
                result[key] = CCD_InitializationParameters(**cbor_decoded)
            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_CreatePLT(**result)

    def convertTokenEvents(self, message) -> list:
        events = []

        for entry in message:
            entry_dict: dict = {}
            for descriptor in entry.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, entry)
                if type(value) in self.simple_types:
                    converted_value = self.convertType(value)
                    if converted_value:
                        entry_dict[key] = converted_value
                elif MessageToDict(value) == {}:
                    pass
                elif type(value) is TokenModuleEvent:
                    entry_dict[key] = self.convertTokenModuleEvent(value)
                elif type(value) is TokenTransferEvent:
                    entry_dict[key] = self.convertTokenTransferEvent(value)
                elif type(value) is TokenSupplyUpdateEvent:
                    entry_dict[key] = self.convertTokenSupplyUpdateEvent(value)

            if entry_dict == {}:
                pass
            else:
                events.append(entry_dict)

        return events

    def convertEvents(self, message) -> list:
        events = []
        for entry in message:
            for descriptor in entry.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, entry)
                if type(value) in self.simple_types:
                    converted_value = self.convertType(value)
                    if converted_value:
                        # entry_dict[key] = converted_value
                        events.append(converted_value)
            # if entry_dict == {}:
            #         pass
            # else:
            #     events.append(entry_dict)

        return events

    def convertInstanceInterruptedEvent(self, message) -> CCD_ContractTraceElement_Interrupted:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            if key == "events":
                result[key] = self.convertEvents(value)

        return CCD_ContractTraceElement_Interrupted(**result)

    def convertInstanceUpdatedEvent(self, message) -> CCD_InstanceUpdatedEvent:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            if key == "events":
                result[key] = self.convertEvents(value)

        return CCD_InstanceUpdatedEvent(**result)

    def convertInstanceResumedEvent(self, message) -> CCD_ContractTraceElement_Resumed:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_ContractTraceElement_Resumed(**result)

    def convertInstanceTransferredEvent(self, message) -> CCD_ContractTraceElement_Transferred:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_ContractTraceElement_Transferred(**result)

    def convertInstanceUpgradedEvent(self, message) -> CCD_ContractTraceElement_Upgraded:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_ContractTraceElement_Upgraded(**result)

    def convertUpdateEvents(self, message) -> list:
        events = []
        for entry in message:
            for descriptor in entry.DESCRIPTOR.fields:
                entry_dict = {}
                key, value = self.get_key_value_from_descriptor(descriptor, entry)
                if MessageToDict(value) == {}:
                    pass
                else:
                    if type(value) is InstanceUpdatedEvent:
                        entry_dict[key] = self.convertInstanceUpdatedEvent(value)

                    if type(value) is ContractTraceElement.Interrupted:
                        entry_dict[key] = self.convertInstanceInterruptedEvent(value)

                    if type(value) is ContractTraceElement.Resumed:
                        entry_dict[key] = self.convertInstanceResumedEvent(value)

                    if type(value) is ContractTraceElement.Transferred:
                        entry_dict[key] = self.convertInstanceTransferredEvent(value)

                    if type(value) is ContractTraceElement.Upgraded:
                        entry_dict[key] = self.convertInstanceUpgradedEvent(value)
                if entry_dict == {}:
                    pass
                else:
                    events.append(entry_dict)

        return events

    def convertCredentialRegistrationIdEntries(self, message):
        entries = []

        for list_entry in message:
            entries.append(self.convertType(list_entry))

        return entries

    def convertDuplicateCredIds(self, message) -> CCD_RejectReason_DuplicateCredIds:
        result = {}

        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            result[key] = self.convertCredentialRegistrationIdEntries(value)

        return CCD_RejectReason_DuplicateCredIds(**result)

    def convertTokenModuleRejectReason(self, message) -> CCD_TokenModuleRejectReason:
        result = {}

        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if key == "details":
                cbor_decoded = cbor2.loads(value.value)
                tmp_result = {}
                if "requiredBalance" in cbor_decoded:
                    token_amount = self.convertDecimal(cbor_decoded.get("requiredBalance"))
                    tmp_result["requiredBalance"] = token_amount
                if "availableBalance" in cbor_decoded:
                    token_amount = self.convertDecimal(cbor_decoded.get("availableBalance"))
                    tmp_result["availableBalance"] = token_amount

                result[key] = CCD_TokenModuleRejectReasonDetails(**tmp_result)
            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_TokenModuleRejectReason(**result)

    def convertRejectReason(self, message) -> CCD_RejectReason:
        result = {}
        _type = None
        for field, value in message.ListFields():
            key = field.name

            # Note this next section is purely to have Coverage
            # show us that we have not covered all possible reject
            # reasons with adequate tests...
            if key == "module_not_wf":
                test_me_please = True
            if key == "module_hash_already_exists":
                test_me_please = True
            if key == "invalid_account_reference":
                test_me_please = True
            if key == "invalid_init_method":
                test_me_please = True
            if key == "invalid_receive_method":
                test_me_please = True
            if key == "invalid_module_reference":
                test_me_please = True
            if key == "invalid_contract_address":
                test_me_please = True
            if key == "runtime_failure":
                test_me_please = True
            if key == "amount_too_large":
                test_me_please = True
            if key == "serialization_failure":
                test_me_please = True
            if key == "out_of_energy":
                test_me_please = True
            if key == "rejected_init":
                test_me_please = True
            if key == "rejected_receive":
                test_me_please = True
            if key == "invalid_proof":
                test_me_please = True
            if key == "already_a_baker: ":
                test_me_please = True
            if key == "not_a_baker":
                test_me_please = True
            if key == "insufficient_balance_for_baker_stake":
                test_me_please = True
            if key == "stake_under_minimum_threshold_for_baking":
                test_me_please = True
            if key == "baker_in_cooldown":
                test_me_please = True
            if key == "duplicate_aggregation_key":
                test_me_please = True
            if key == "non_existent_credential_id":
                test_me_please = True
            if key == "key_index_already_in_use":
                test_me_please = True
            if key == "invalid_account_threshold":
                test_me_please = True
            if key == "invalid_credential_key_sign_threshold":
                test_me_please = True
            if key == "invalid_encrypted_amount_transfer_proof":
                test_me_please = True
            if key == "invalid_transfer_to_public_proof":
                test_me_please = True
            if key == "encrypted_amount_self_transfer":
                test_me_please = True
            if key == "invalid_index_on_encrypted_transfer":
                test_me_please = True
            if key == "zero_scheduledAmount":
                test_me_please = True
            if key == "non_increasing_schedule":
                test_me_please = True
            if key == "first_scheduled_release_expired":
                test_me_please = True
            if key == "scheduled_self_transfer":
                test_me_please = True
            if key == "invalid_credentials":
                test_me_please = True
            if key == "duplicate_cred_ids":
                test_me_please = True
            if key == "non_existent_cred_ids":
                test_me_please = True
            if key == "remove_first_credential":
                test_me_please = True
            if key == "credential_holder_did_not_sign":
                test_me_please = True
            if key == "not_allowed_multiple_credentials":
                test_me_please = True
            if key == "not_allowed_to_receive_encrypted":
                test_me_please = True
            if key == "not_allowed_to_handle_encrypted":
                test_me_please = True
            if key == "missing_baker_add_parameters":
                test_me_please = True
            if key == "finalization_reward_commission_not_in_range":
                test_me_please = True
            if key == "baking_reward_commission_not_in_range":
                test_me_please = True
            if key == "transaction_fee_commission_not_in_range":
                test_me_please = True
            if key == "already_a_delegator":
                test_me_please = True
            if key == "insufficient_balance_for_delegation_stake":
                test_me_please = True
            if key == "missing_delegation_add_parameters":
                test_me_please = True
            if key == "insufficient_delegation_stak":
                test_me_please = True
            if key == "delegator_in_cooldown":
                test_me_please = True
            if key == "not_a_delegator":
                test_me_please = True
            if key == "delegation_target_not_a_baker: ":
                test_me_please = True
            if key == "stake_over_maximum_threshold_for_pool":
                test_me_please = True
            if key == "pool_would_become_over_delegated":
                test_me_please = True
            if key == "pool_closed":
                test_me_please = True  # noqa: F841

            _type = key
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) in [
                RejectReason.InvalidInitMethod,
                RejectReason.InvalidReceiveMethod,
                RejectReason.AmountTooLarge,
                RejectReason.RejectedInit,
                RejectReason.RejectedReceive,
            ]:
                result[key] = self.convertTypeWithSingleValues(value)

            elif type(value) is TokenModuleRejectReason:
                result[key] = self.convertTokenModuleRejectReason(value)

            elif type(value) is RejectReason.DuplicateCredIds:
                result[key] = self.convertDuplicateCredIds(value)
        return result, _type

    def convertRejectReasonNone(self, message) -> CCD_AccountTransactionEffects_None:
        result = {}
        _type = None
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) is RejectReason:
                result[key], _type = self.convertRejectReason(value)

        return CCD_AccountTransactionEffects_None(**result), _type
