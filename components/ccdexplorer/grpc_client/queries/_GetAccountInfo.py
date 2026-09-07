from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

import cbor2
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_AccountCredential,
    CCD_AccountInfo,
    CCD_AccountStakingInfo,
    CCD_AccountVerifyKey,
    CCD_ChainArData,
    CCD_Commitment,
    CCD_Cooldown,
    CCD_CredentialCommitments,
    CCD_CredentialPublicKeys,
    CCD_EncryptedBalance,
    CCD_InitialCredentialValues,
    CCD_ModuleAccountState,
    CCD_NormalCredentialValues,
    CCD_Policy,
    CCD_Token,
    CCD_TokenAccountState,
    CCD_TokenAmount,
    CCD_YearMonth,
    CCD_Policy_Attributes,
    CredentialDocType,
    CredentialElement,
    CoolDownStatus,
)
from ccdexplorer.grpc_client.kernel_pb2 import AccountAddress
from ccdexplorer.grpc_client.protocol_level_tokens_pb2 import TokenAmount
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.types_pb2 import (
    AccountInfo,
    AccountInfoRequest,
    AccountStakingInfo,
    Amount,
    BakerId,
    BakerInfo,
    BakerPoolInfo,
    CredentialCommitments,
    CredentialPublicKeys,
    DelegationTarget,
    EncryptedBalance,
    Policy,
    ReleaseSchedule,
    StakePendingChange,
    YearMonth,
)
from google.protobuf import message

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


class Mixin(_SharedConverters):
    def convertKeysEntry(self, message) -> dict[str, CCD_AccountVerifyKey]:
        entry_dict = {}
        for index, entry in message.items():
            converted_value = self.convertType(entry)
            entry_dict[str(index)] = CCD_AccountVerifyKey(**{"ed25519_key": converted_value})

        return entry_dict

    def convertArData(self, message) -> dict[str, CCD_ChainArData]:
        entry_dict = {}
        for index, entry in message.items():
            converted_value = self.convertType(entry)
            entry_dict[str(index)] = CCD_ChainArData(**{"enc_id_cred_pub_share": converted_value})

        return entry_dict

    def convertAttributesEntry(self, message) -> dict[str, CCD_Commitment]:
        attributes = {}
        for element, value in message.items():
            converted_value = self.convertType(value)
            attributes[CredentialElement(element).name] = converted_value

        return attributes

    def convertPolicy_AttributesEntry(self, message) -> dict[str, CCD_Policy_Attributes]:
        attributes = {}
        for element, value in message.items():
            if CredentialElement(element).name == "idDocType":
                value = CredentialDocType(bytes.decode(value)).name.replace("_", " ")
                attributes[CredentialElement(element).name] = value

            else:
                attributes[CredentialElement(element).name] = bytes.decode(value)

        return attributes

    def convertCredentialPublicKeys(self, message) -> CCD_CredentialPublicKeys:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif key == "keys":
                result[key] = self.convertKeysEntry(value)

        return CCD_CredentialPublicKeys(**result)

    def convertYearMonth(self, message) -> CCD_YearMonth:
        result = self.convertTypeWithSingleValues(message)

        return CCD_YearMonth(**result)

    def convertPolicy(self, message) -> CCD_Policy:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if key == "attributes":
                result[key] = self.convertPolicy_AttributesEntry(value)

            elif type(value) is YearMonth:
                result[key] = self.convertYearMonth(value)

        return CCD_Policy(**result)

    def convertListOfCommitments(self, message) -> list[CCD_Commitment]:
        entries = []

        for list_entry in message:
            entries.append(self.convertType(list_entry))

        return entries

    def convertCredentialCommitments(self, message) -> CCD_CredentialCommitments:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif key == "attributes":
                result[key] = self.convertAttributesEntry(value)

            elif key == "id_cred_sec_sharing_coeff":
                result[key] = self.convertListOfCommitments(value)

        return CCD_CredentialCommitments(**result)

    def convertInitialCredentialValues(self, message) -> CCD_InitialCredentialValues:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) is CredentialPublicKeys:
                result["credential_public_keys"] = self.convertCredentialPublicKeys(value)

            elif type(value) is Policy:
                result[key] = self.convertPolicy(value)

        return CCD_InitialCredentialValues(**result)

    def convertNormalCredentialValues(self, message) -> CCD_NormalCredentialValues:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) is CredentialPublicKeys:
                result["credential_public_keys"] = self.convertCredentialPublicKeys(value)

            elif key == "ar_data":
                result[key] = self.convertArData(value)

            elif type(value) is Policy:
                result[key] = self.convertPolicy(value)

            elif type(value) is CredentialCommitments:
                result[key] = self.convertCredentialCommitments(value)

        return CCD_NormalCredentialValues(**result)

    def convertAccountCredential(self, message) -> CCD_AccountCredential:
        result = {}
        key = message.WhichOneof("credential_values")
        if key == "initial":
            result[key] = self.convertInitialCredentialValues(message.initial)
        elif key == "normal":
            result[key] = self.convertNormalCredentialValues(message.normal)

        return CCD_AccountCredential(**result)

    def convertTokenAccountState(self, message: message.Message) -> CCD_TokenAccountState:
        keys = {}

        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if key == "module_state":
                # `module_state` is optional: a token module that keeps no
                # per-account state for this account (no allow/deny list entry,
                # say) leaves it unset, and cbor2 raises on the empty payload
                # the default instance hands back.
                if message.HasField("module_state"):
                    cbor_decoded = cbor2.loads(value.value)
                    keys[key] = CCD_ModuleAccountState(**cbor_decoded)

            elif type(value) in self.simple_types:
                keys[key] = self.convertType(value)
            elif type(value) is TokenAmount:
                keys[key] = CCD_TokenAmount(
                    **{"value": str(value.value), "decimals": value.decimals}
                )

        return CCD_TokenAccountState(**keys)

    def convertTokens(self, message) -> list[CCD_Token]:
        tokens = []

        for entry in message:
            result = {}
            for descriptor in entry.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, entry)
                if type(value) in self.simple_types:
                    result[key] = self.convertType(value)
                if key == "token_account_state":
                    result[key] = self.convertTokenAccountState(value)

            tokens.append(CCD_Token(**result))
        return tokens

    def convertCredentials(self, message) -> dict[str, CCD_AccountCredential]:
        cred_dict = {}
        for index, credential in message.items():
            cred_dict[str(index)] = self.convertAccountCredential(credential)

        return cred_dict

    def convertCooldowns(self, message) -> list[CCD_Cooldown]:
        cooldowns = []

        for entry in message:
            result = {}
            for descriptor in entry.DESCRIPTOR.fields:
                key, value = self.get_key_value_from_descriptor(descriptor, entry)
                if type(value) in self.simple_types:
                    result[key] = self.convertType(value)
                if key == "status":
                    result[key] = CoolDownStatus(value)

            cooldowns.append(CCD_Cooldown(**result))
        return cooldowns

    def convertEncryptedBalance(self, message) -> CCD_EncryptedBalance:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif key == "incoming_amounts":
                result[key] = self.convertList(value)

        return CCD_EncryptedBalance(**result)

    def convertBakerInfo(self, message):
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return result

    def convertAccountStakingInfo(self, message) -> CCD_AccountStakingInfo | None:
        result = {}
        which_one = message.WhichOneof("staking_info")
        if not which_one:
            return None

        staking_info = getattr(message, which_one)
        for descriptor in staking_info.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, staking_info)

            # `pending_change` never occurs from protocol version 7, and a
            # validator's `pool_info` is present only while it sits in the
            # current epoch's committee. Both are optional in the proto.
            if descriptor.has_presence and not staking_info.HasField(key):
                result[key] = None

            elif which_one == "delegator" and type(value) not in [
                BakerId,
                AccountAddress,
                Amount,
                str,
                int,
                bool,
                float,
                DelegationTarget,
                StakePendingChange,
            ]:
                continue

            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) is BakerPoolInfo:
                result[key] = self.convertBakerPoolInfo(value)

            elif type(value) is BakerInfo:
                result[key] = self.convertBakerInfo(value)

            elif type(value) is DelegationTarget:
                result[key] = self.convertDelegationTarget(value)

            elif type(value) is StakePendingChange:
                result[key] = self.convertPendingChange(value)

        return CCD_AccountStakingInfo(**{which_one: result})

    def get_account_info(
        self: GRPCClient,
        block_hash: str,
        hex_address: str | None = None,
        account_index: int | None = None,
        net: Enum = NET.MAINNET,
    ) -> CCD_AccountInfo:
        blockHashInput = self.generate_block_hash_input_from(block_hash)
        if account_index is not None:
            accountIdentifierInput = self.generate_account_identifier_input_from_account_index(
                account_index
            )
        elif hex_address:
            accountIdentifierInput = self.generate_account_identifier_input_from(hex_address)
        else:
            # Falling through left accountIdentifierInput unbound (UnboundLocalError
            # from deep inside the client); say what is actually wrong instead.
            raise ValueError("get_account_info: pass either hex_address or account_index")

        if accountIdentifierInput is None:
            # generate_account_identifier_input_from swallows a malformed address
            # and returns None, which the node would reject as an opaque
            # INVALID_ARGUMENT.
            raise ValueError(
                f"get_account_info: could not build an account identifier from "
                f"{hex_address!r} / account_index={account_index!r}"
            )

        account_info = AccountInfoRequest(
            block_hash=blockHashInput, account_identifier=accountIdentifierInput
        )

        grpc_return_value: AccountInfo = self.stub_on_net(net, "GetAccountInfo", account_info)

        result = {}
        for descriptor in grpc_return_value.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, grpc_return_value)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

            elif type(value) is ReleaseSchedule:
                result[key] = self.convertRelease(value)

            elif type(value) is EncryptedBalance:
                result[key] = self.convertEncryptedBalance(value)

            # TODO
            # type EncryptedBalance
            # type EncryptionKey

            elif key == "creds":
                result["credentials"] = self.convertCredentials(value)

            elif key == "tokens":
                result["tokens"] = self.convertTokens(value)

            elif type(value) is AccountStakingInfo:
                result[key] = (
                    self.convertAccountStakingInfo(value)
                    if grpc_return_value.HasField("stake")
                    else None
                )

            elif key == "cooldowns":
                result[key] = self.convertCooldowns(value)

        return CCD_AccountInfo(**result)
