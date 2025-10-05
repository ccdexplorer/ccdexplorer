# ruff: noqa: F403, F405, E402
# pyright: reportInvalidTypeForm=false
from pydantic import BaseModel, Field, ConfigDict
import datetime as dt
from enum import Enum
from typing import Optional

# this leads to circluar import!
# from ccdexplorer.cis import MongoTypeLoggedEvent


################
#
# CCD_ is the prefix for all Pydantic classes
# to prevent namespace collision with the
# protobuf generated classes.
#
################


class CredentialElement(Enum):
    """
    Enum class representing different types of credential elements.

    Attributes:
        firstName (int): Represents the first name of an individual.
        lastName (int): Represents the last name of an individual.
        sex (int): Represents the sex of an individual.
        dob (int): Represents the date of birth of an individual.
        countryOfResidence (int): Represents the country of residence of an individual.
        nationality (int): Represents the nationality of an individual.
        idDocType (int): Represents the type of identification document.
        idDocNo (int): Represents the identification document number.
        idDocIssuer (int): Represents the issuer of the identification document.
        idDocIssuedAt (int): Represents the issuance date of the identification document.
        idDocExpiresAt (int): Represents the expiration date of the identification document.
        nationalIdNo (int): Represents the national identification number.
        taxIdNo (int): Represents the tax identification number.
        lei (int): Represents the Legal Entity Identifier.
        legalName (int): Represents the legal name of a business entity.
        legalJurisdictionCountry (int): Represents the legal jurisdiction country of a business entity.
        businessNumber (int): Represents the business number of a business entity.
        registrationAuthority (int): Represents the registration authority of a business entity.
    """

    firstName = 0
    lastName = 1
    sex = 2
    dob = 3
    countryOfResidence = 4
    nationality = 5
    idDocType = 6
    idDocNo = 7
    idDocIssuer = 8
    idDocIssuedAt = 9
    idDocExpiresAt = 10
    nationalIdNo = 11
    taxIdNo = 12
    lei = 13
    legalName = 14
    legalJurisdictionCountry = 15
    businessNumber = 16
    registrationAuthority = 17


class CredentialDocType(Enum):
    """
    Enum class representing different types of credential documents.

    Attributes:
        na (str): Represents an unspecified or unknown document type.
        Passport (str): Represents a passport document type.
        National_ID_Card (str): Represents a national ID card document type.
        Driving_License (str): Represents a driving license document type.
        Immigration_Card (str): Represents an immigration card document type.
    """

    na = "0"
    Passport = "1"
    National_ID_Card = "2"
    Driving_License = "3"
    Immigration_Card = "4"


class ProtocolVersions(Enum):
    """
    Enum representing different protocol versions.

    Attributes:
        PROTOCOL_VERSION_1 (int): Protocol version 1.
        PROTOCOL_VERSION_2 (int): Protocol version 2.
        PROTOCOL_VERSION_3 (int): Protocol version 3.
        PROTOCOL_VERSION_4 (int): Protocol version 4.
        PROTOCOL_VERSION_5 (int): Protocol version 5.
        PROTOCOL_VERSION_6 (int): Protocol version 6.
        PROTOCOL_VERSION_7 (int): Protocol version 7.
        PROTOCOL_VERSION_8 (int): Protocol version 8.
    """

    PROTOCOL_VERSION_1 = 0
    PROTOCOL_VERSION_2 = 1
    PROTOCOL_VERSION_3 = 2
    PROTOCOL_VERSION_4 = 3
    PROTOCOL_VERSION_5 = 4
    PROTOCOL_VERSION_6 = 5
    PROTOCOL_VERSION_7 = 6
    PROTOCOL_VERSION_8 = 7
    PROTOCOL_VERSION_9 = 8


class CCD_OpenStatusTranslation(Enum):
    """
    Enum representing the open status of a resource.

    Attributes:
        open_for_all (int): The resource is open for all users.
        closed_for_new (int): The resource is closed for new users but open for existing users.
        closed_for_all (int): The resource is closed for all users.
    """

    open_for_all = 0
    closed_for_new = 1
    closed_for_all = 2


class CoolDownStatus(Enum):
    """
    Enum representing the different stages of cooldown status.

    Attributes:
        COOLDOWN (int): Represents the cooldown stage with a value of 0.
        PRE_COOLDOWN (int): Represents the pre-cooldown stage with a value of 1.
        PRE_PRE_COOLDOWN (int): Represents the pre-pre-cooldown stage with a value of 2.
    """

    COOLDOWN = 0
    PRE_COOLDOWN = 1
    PRE_PRE_COOLDOWN = 2


# Type aliases
CCD_ArIdentity = int
"""Represents an anonymity revoker identity."""

CCD_IpIdentity = int
"""Represents an identity provider identity."""

CCD_ArPublicKey = str
"""Public key of an anonymity revoker."""

CCD_IpVerifyKey = str
"""Verification key of an identity provider."""

CCD_IpCdiVerifyKey = str
"""Credential deployment information verification key of an identity provider."""

CCD_BlockHash = str
"""Hash representing a block in the blockchain."""

CCD_TransactionHash = str
"""Hash representing a transaction."""

CCD_AccountAddress = str
"""String representation of an account address."""

CCD_AccountIndex = int
"""Index representing an account."""

CCD_DelegatorId = CCD_AccountIndex
"""Delegator ID, which is the same as an account index."""

microCCD = int
"""Smallest unit of CCD (1 microCCD = 10^-6 CCD)."""

CCD_BakerId = int
"""The ID of a validator, which is the index of its account."""

CCD_FinalizerIndex = int
"""Unique identifier for a Finalizer."""

CCD_QuorumSignature = str
"""The bytes representing the raw aggregate signature. The bytes have a fixed length of 48 bytes."""

CCD_SuccessorProof = str
"""A cryptographic proof showing that one block is a valid successor of another."""

CCD_FinalizationCommitteeHash = str
"""A hash of the finalization committee members and their stakes."""

CCD_BlockSignature = str
"""A signature on a block."""

CCD_TimeoutSignature = str
"""A signature on a timeout message."""

CCD_Epoch = int
"""Represents an epoch number in the blockchain."""

CCD_Energy = int
"""Represents the amount of energy consumed or available."""

CCD_DurationSeconds = int
"""Duration represented in seconds."""

CCD_Duration = int
"""General representation of a duration (may be in different units)."""

CCD_ModuleRef = str
"""Reference to a smart contract module."""

CCD_ContractEvent = str
"""Event related to a smart contract execution."""

CCD_Memo = str
"""Memo field for transactions or messages."""

CCD_RegisteredData = str
"""Registered on-chain data associated with an account."""

CCD_BakerSignatureVerifyKey = str
"""Public key used to verify validator signatures."""

CCD_OpenStatus = int
"""Status indicator for openness (e.g., open, closed, restricted)."""

CCD_BakerElectionVerifyKey = str
"""Election verification key for a validator."""

CCD_BakerAggregationVerifyKey = str
"""Aggregation verification key for a validator."""

CCD_Parameter = str
"""Parameters passed to a smart contract function."""

CCD_AmountFraction = float
"""Fractional representation of an amount."""

CCD_ReceiveName = str
"""Name of a smart contract receive function."""

CCD_CredentialsPerBlockLimit = int
"""Maximum number of credentials allowed per block."""

CCD_LeadershipElectionNonce = str
"""Nonce used for leadership election."""

CCD_CredentialRegistrationId = str
"""Unique identifier for credential registration."""

CCD_Sha256Hash = str
"""SHA-256 hash representation."""

CCD_ElectionDifficulty = float
"""Represents the difficulty level in the election process."""

CCD_UpdatePublicKey = str
"""Public key used for blockchain updates."""

CCD_UpdateKeysIndex = int
"""Index of the update key."""

CCD_UpdateKeysThreshold = int
"""Threshold for update key signatures."""

CCD_TransactionTime = int
"""Timestamp representing the time of a transaction."""

CCD_SignatureThreshold = int
"""Threshold for signature verification."""

CCD_IdentityProviderIdentity = int
"""Identity of an identity provider."""

CCD_ArThreshold = int
"""Threshold for anonymity revoker operations."""

CCD_Commitment = str
"""Commitment value used in cryptographic operations."""

CCD_VersionedModuleSource_ModuleSourceV0 = str
"""Versioned module source, version 0."""

CCD_VersionedModuleSource_ModuleSourceV1 = str
"""Versioned module source, version 1."""

CCD_ProtocolVersion = int
"""Protocol version of the blockchain."""

CCD_StateHash = str
"""State hash representing a snapshot of blockchain state."""

CCD_TimeStamp = dt.datetime
"""Timestamp representing a point in time."""

CCD_SequenceNumber = int
"""Sequence number used for ordering transactions or events."""

CCD_StakePendingChange_Remove = CCD_TimeStamp
"""Timestamp representing when a stake pending change will be removed."""

CCD_ContractStateV0 = str
"""Version 0 representation of contract state."""

CCD_InitName = str
"""Name of the initialization function for a smart contract."""

CCD_EncryptedAmount = str
"""Encrypted representation of an amount."""

CCD_Empty = None
"""Represents an empty or null value."""

CCD_AccountThreshold = int
"""Threshold of signatures required for account operations."""

CCD_Policy_Attributes = str
"""Attributes defining a policy."""

CCD_Round = int
"""Round number in consensus or other processes."""


class CCD_ContractAddress(BaseModel):
    """Address of a smart contract instance.

    GRPC documentation: [concordium.v2.ContractAddress](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ContractAddress)

    Attributes:
        index (uint64): The index of the smart contract.
        subindex (uint64): The subindex of the smart contract instance. Currently not used, so it is always 0.
    """

    index: int
    subindex: int

    def to_str(self):
        return f"<{self.index},{self.subindex}>"

    @classmethod
    def from_str(cls, str_repr: str):
        """
        Parses a string representation of a CCD contract address and returns a CCD_ContractAddress object.
        Args:
            str_repr (str): The string representation of the CCD contract address in the format "<index,subindex>".
        Returns:
            CCD_ContractAddress: An object representing the parsed contract address.
        Raises:
            ValueError: If the string representation is not in the expected format.
        """

        contract_string = str_repr.split("-")[0]
        try:
            c = CCD_ContractAddress(
                **{
                    "index": int(contract_string.split(",")[0][1:]),
                    "subindex": int(contract_string.split(",")[1][:-1]),
                }
            )
        except:  # noqa: E722
            c = CCD_ContractAddress(
                **{
                    "index": contract_string.split(",")[0][1:],
                    "subindex": 0,
                }
            )
        return c

    @classmethod
    def from_index(cls, index: int, subindex: int):
        """
        Create a CCD_ContractAddress instance from the given index and subindex.

        Args:
            index (int): The index value for the contract address.
            subindex (int): The subindex value for the contract address.

        Returns:
            CCD_ContractAddress (CCD_ContractAddress): An instance of CCD_ContractAddress initialized with the provided index and subindex.
        """
        s = CCD_ContractAddress(**{"index": index, "subindex": subindex})
        return s


class CCD_Address(BaseModel):
    """An address of either a contract or an account.

    GRPC documentation: [concordium.v2.Address](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.Address)

    Attributes:
        account (Optional[CCD_AccountAddress]): The account address if this is an account address.
        contract (Optional[CCD_ContractAddress]): The contract address if this is a contract address.
    """

    account: Optional[CCD_AccountAddress] = None
    contract: Optional[CCD_ContractAddress] = None


###########
### PLT ###
###########


class CCD_TokenModuleRejectReasonType(Enum):
    address_not_found = "address_not_found"
    token_balance_insufficient = "token_balance_insufficient"
    deserialization_failure = "deserialization_failure"
    unsupported_operation = "unsupported_operation"
    operation_not_permitted = "operation_not_permitted"
    mint_would_overflow = "mint_would_overflow"
    unknown = "unknown"


CCD_Cbor = str
"""A CBOR encoded bytestring"""

CCD_TokenId = str
"""Token ID: a unique symbol and identifier of a protocol level token."""

CCD_TokenModuleRef = str
"""A token module reference. This is always 32 bytes long."""


class CCD_TokenAmount(BaseModel):
    value: str  # is int, but for BSON serialization we use str
    decimals: int


class CCD_GovernanceAccount(BaseModel):
    """NOT mirrored to an official class.Represents a governance account in the blockchain."""

    address: Optional[CCD_AccountAddress | CCD_ContractAddress] = None
    type: Optional[str] = None


class CCD_TokenHolder(BaseModel):
    account: CCD_AccountAddress


# Cbor deccoding classes
class CCD_InitializationParameters(BaseModel):
    """NOT mirrored to an official class. Represents the state of a module in the blockchain."""

    name: str
    metadata: dict
    governance_account: CCD_TokenHolder = Field(..., alias="governanceAccount")
    allow_list: Optional[bool] = Field(default=None, alias="allowList")
    deny_list: Optional[bool] = Field(default=None, alias="denyList")
    mintable: Optional[bool] = None
    burnable: Optional[bool] = None
    paused: Optional[bool] = None
    initial_supply: Optional[CCD_TokenAmount] = Field(default=None, alias="initialSupply")
    model_config = {"populate_by_name": True}


class CCD_ModuleState(BaseModel):
    """NOT mirrored to an official class. Represents the state of a module in the blockchain."""

    model_config = ConfigDict(extra="allow")
    name: Optional[str] = None
    metadata: dict
    governance_account: CCD_TokenHolder = Field(..., alias="governanceAccount")
    allow_list: Optional[bool] = Field(default=None, alias="allowList")
    deny_list: Optional[bool] = Field(default=None, alias="denyList")
    mintable: Optional[bool] = None
    burnable: Optional[bool] = None
    paused: Optional[bool] = None
    model_config = {"populate_by_name": True}


class CCD_ModuleAccountState(BaseModel):
    """NOT mirrored to an official class. Represents the state of a module in the blockchain."""

    model_config = ConfigDict(extra="allow")
    allow_list: Optional[bool] = Field(default=None, alias="allowList")
    deny_list: Optional[bool] = Field(default=None, alias="denyList")
    model_config = {"populate_by_name": True}


class CCD_TokenModuleRejectReasonDetails(BaseModel):
    """NOT mirrored to an official class. Represents the state of a module in the blockchain."""


# Cbor deccoding classes


class CCD_TokenState(BaseModel):
    token_module_ref: CCD_TokenModuleRef
    decimals: int
    total_supply: CCD_TokenAmount
    module_state: CCD_ModuleState


class CCD_TokenAccountState(BaseModel):
    balance: CCD_TokenAmount
    module_state: Optional[CCD_ModuleAccountState] = None


class CCD_TokenModuleEvent(BaseModel):
    type: str
    details: CCD_Cbor


class CCD_TokenTransferEvent(BaseModel):
    from_: CCD_TokenHolder = Field(..., alias="from")
    to: CCD_TokenHolder
    amount: CCD_TokenAmount
    memo: Optional[CCD_Memo] = None

    model_config = {"populate_by_name": True}


class CCD_TokenSupplyUpdateEvent(BaseModel):
    target: CCD_TokenHolder
    amount: CCD_TokenAmount


class CCD_TokenEvent(BaseModel):
    token_id: Optional[CCD_TokenId] = None
    module_event: Optional[CCD_TokenModuleEvent] = None
    transfer_event: Optional[CCD_TokenTransferEvent] = None
    mint_event: Optional[CCD_TokenSupplyUpdateEvent] = None
    burn_event: Optional[CCD_TokenSupplyUpdateEvent] = None


class CCD_TokenEffect(BaseModel):
    events: list[CCD_TokenEvent]


class CCD_TokenModuleRejectReason(BaseModel):
    token_id: CCD_TokenId
    reason_type: str = Field(..., alias="type")
    details: Optional[CCD_TokenModuleRejectReasonDetails] = None
    model_config = {"populate_by_name": True}


class CCD_CreatePLT(BaseModel):
    token_id: CCD_TokenId
    token_module: CCD_TokenModuleRef
    decimals: int
    initialization_parameters: CCD_InitializationParameters


class CCD_TokenCreationDetails(BaseModel):
    create_plt: CCD_CreatePLT
    events: list[CCD_TokenEvent]


class CCD_TokenInfo(BaseModel):
    token_id: CCD_TokenId
    token_state: CCD_TokenState
    tag_information: Optional[dict] = None


class CCD_Token(BaseModel):
    token_id: CCD_TokenId
    token_account_state: CCD_TokenAccountState


#### PLT END ###


class CCD_RejectReason_InvalidInitMethod(BaseModel):
    """Reference to a non-existing contract init method.

    GRPC documentation: [concordium.v2.RejectReason.InvalidInitMethod](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RejectReason.InvalidInitMethod)

    Attributes:
        module_ref (CCD_ModuleRef): Reference to the module.
        init_name (CCD_InitName): Name of the initialization method.
    """

    module_ref: CCD_ModuleRef
    init_name: CCD_InitName


class CCD_RejectReason_InvalidReceiveMethod(BaseModel):
    """Reference to a non-existing contract receive method.

    GRPC documentation: [concordium.v2.RejectReason.InvalidReceiveMethod](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RejectReason.InvalidReceiveMethod)

    Attributes:
        module_ref (CCD_ModuleRef): Reference to the module.
        receive_name (CCD_ReceiveName): Name of the receive method.
    """

    module_ref: CCD_ModuleRef
    receive_name: CCD_ReceiveName


class CCD_RejectReason_AmountTooLarge(BaseModel):
    """When one wishes to transfer an amount from A to B but there are not enough funds on account/contract A to make this possible. The data are the from address and the amount to transfer.

    GRPC documentation: [concordium.v2.RejectReason.AmountTooLarge](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RejectReason.AmountTooLarge)

    Attributes:
        address (CCD_Address): The from address.
        amount (microCCD): The amount to transfer.
    """

    address: CCD_Address
    amount: microCCD


class CCD_RejectReason_RejectedInit(BaseModel):
    """Rejected due to contract logic in init function of a contract.

    GRPC documentation: [concordium.v2.RejectReason.RejectedInit](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RejectReason.RejectedInit)

    Attributes:
        reject_reason (int): The rejection reason code.
    """

    reject_reason: int


class CCD_RejectReason_RejectedReceive(BaseModel):
    """Rejected due to contract logic in receive function of a contract.

    GRPC documentation: [concordium.v2.RejectReason.RejectedReceive](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RejectReason.RejectedReceive)

    Attributes:
        reject_reason (int): The rejection reason code.
        contract_address (CCD_ContractAddress): The contract address.
        receive_name (CCD_ReceiveName): Name of the receive method.
        parameter (CCD_Parameter): The parameter passed to the method.
    """

    reject_reason: int
    contract_address: CCD_ContractAddress
    receive_name: CCD_ReceiveName
    parameter: CCD_Parameter


class CCD_RejectReason_DuplicateCredIds(BaseModel):
    """Some of the credential IDs already exist or are duplicated in the transaction.

    GRPC documentation: [concordium.v2.RejectReason.DuplicateCredIds](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RejectReason.DuplicateCredIds)

    Attributes:
        ids (list[CCD_CredentialRegistrationId]): The credential IDs that were duplicated.
    """

    ids: list[CCD_CredentialRegistrationId]


class CCD_RejectReason_NonExistentCredIds(BaseModel):
    """A credential id that was to be removed is not part of the account.

    GRPC documentation: [concordium.v2.RejectReason.NonExistentCredIds](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RejectReason.NonExistentCredIds)

    Attributes:
        ids (list[CCD_CredentialRegistrationId]): The credential IDs that were not found.
    """

    ids: list[CCD_CredentialRegistrationId]


class CCD_RejectReason(BaseModel):
    """A reason for why a transaction was rejected. Rejected means included in a block, but the desired action was not achieved. The only effect of a rejected transaction is payment.

    GRPC documentation: [concordium.v2.RejectReason](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RejectReason)

    Attributes:
        module_not_wf (Optional[CCD_Empty]): Raised while validating a Wasm module that is not well formed.
        module_hash_already_exists (Optional[CCD_ModuleRef]): The smart contract module hash already exists.
        invalid_account_reference (Optional[CCD_AccountAddress]): Account does not exist.
        invalid_init_method (Optional[CCD_RejectReason_InvalidInitMethod]): Reference to a non-existing contract init method.
        invalid_receive_method (Optional[CCD_RejectReason_InvalidReceiveMethod]): Reference to a non-existing contract receive method.
        invalid_module_reference (Optional[CCD_ModuleRef]): Reference to a non-existing smart contract module.
        invalid_contract_address (Optional[CCD_ContractAddress]): Contract instance does not exist.
        runtime_failure (Optional[CCD_Empty]): Runtime exception occurred when running either the init or receive method.
        amount_too_large (Optional[CCD_RejectReason_AmountTooLarge]): When one wishes to transfer an amount from A to B but there are not enough funds on account/contract A to make this possible.
        serialization_failure (Optional[CCD_Empty]): Serialization of the body failed.
        out_of_energy (Optional[CCD_Empty]): We ran of out energy to process this transaction.
        rejected_init (Optional[CCD_RejectReason_RejectedInit]): Rejected due to contract logic in init function of a contract.
        rejected_receive (Optional[CCD_RejectReason_RejectedReceive]): Rejected due to contract logic in receive function of a contract.
        invalid_proof (Optional[CCD_Empty]): Proof that the validator owns relevant private keys is not valid.
        already_a_baker (Optional[CCD_BakerId]): Tried to add validator for an account that already has a validator.
        not_a_baker (Optional[CCD_AccountAddress]): Tried to remove a validator for an account that has no validator.
        insufficient_balance_for_baker_stake (Optional[CCD_Empty]): The amount on the account was insufficient to cover the proposed stake.
        stake_under_minimum_threshold_for_baking (Optional[CCD_Empty]): The amount provided is under the threshold required for becoming a validator.
        baker_in_cooldown (Optional[CCD_Empty]): The change could not be made because the validator is in cooldown for another change.
        duplicate_aggregation_key (Optional[CCD_BakerAggregationVerifyKey]): A validator with the given aggregation key already exists.
        non_existent_credential_id (Optional[CCD_Empty]): Encountered credential ID that does not exist.
        key_index_already_in_use (Optional[CCD_Empty]): Attempted to add an account key to a key index already in use.
        invalid_account_threshold (Optional[CCD_Empty]): When the account threshold is updated, it must not exceed the amount of existing keys.
        invalid_credential_key_sign_threshold (Optional[CCD_Empty]): When the credential key threshold is updated, it must not exceed the amount of existing keys.
        invalid_encrypted_amount_transfer_proof (Optional[CCD_Empty]): Proof for an encrypted amount transfer did not validate.
        invalid_transfer_to_public_proof (Optional[CCD_Empty]): Proof for a secret to public transfer did not validate.
        encrypted_amount_self_transfer (Optional[CCD_AccountAddress]): Account tried to transfer an encrypted amount to itself, that's not allowed.
        invalid_index_on_encrypted_transfer (Optional[CCD_Empty]): The provided index is below the start index or above `startIndex + length incomingAmounts`.
        zero_scheduledAmount (Optional[CCD_Empty]): The transfer with schedule is going to send 0 tokens.
        non_increasing_schedule (Optional[CCD_Empty]): The transfer with schedule has a non strictly increasing schedule.
        first_scheduled_release_expired (Optional[CCD_Empty]): The first scheduled release in a transfer with schedule has already expired.
        scheduled_self_transfer (Optional[CCD_AccountAddress]): Account tried to transfer with schedule to itself, that's not allowed.
        invalid_credentials (Optional[CCD_Empty]): At least one of the credentials was either malformed or its proof was incorrect.
        duplicate_cred_ids (Optional[CCD_RejectReason_DuplicateCredIds]): Some of the credential IDs already exist or are duplicated in the transaction.
        non_existent_cred_ids (Optional[CCD_RejectReason_NonExistentCredIds]): A credential id that was to be removed is not part of the account.
        remove_first_credential (Optional[CCD_Empty]): Attempt to remove the first credential.
        credential_holder_did_not_sign (Optional[CCD_Empty]): The credential holder of the keys to be updated did not sign the transaction.
        not_allowed_multiple_credentials (Optional[CCD_Empty]): Account is not allowed to have multiple credentials because it contains a non-zero encrypted transfer.
        not_allowed_to_receive_encrypted (Optional[CCD_Empty]): The account is not allowed to receive encrypted transfers because it has multiple credentials.
        not_allowed_to_handle_encrypted (Optional[CCD_Empty]): The account is not allowed to send encrypted transfers (or transfer from/to public to/from encrypted).
        missing_baker_add_parameters (Optional[CCD_Empty]): A configure validator transaction is missing one or more arguments in order to add a validator.
        finalization_reward_commission_not_in_range (Optional[CCD_Empty]): Finalization reward commission is not in the valid range for a validator.
        baking_reward_commission_not_in_range (Optional[CCD_Empty]): Baking reward commission is not in the valid range for a validator.
        transaction_fee_commission_not_in_range (Optional[CCD_Empty]): Transaction fee commission is not in the valid range for a validator.
        already_a_delegator (Optional[CCD_Empty]): Tried to add validator for an account that already has a delegator.
        insufficient_balance_for_delegation_stake (Optional[CCD_Empty]): The amount on the account was insufficient to cover the proposed stake.
        missing_delegation_add_parameters (Optional[CCD_Empty]): A configure delegation transaction is missing one or more arguments in order to add a delegator.
        insufficient_delegation_stake (Optional[CCD_Empty]): Delegation stake when adding a delegator was 0.
        delegator_in_cooldown (Optional[CCD_Empty]): Account is not a delegation account.
        not_a_delegator (Optional[CCD_AccountAddress]): Account is not a delegation account.
        delegation_target_not_a_baker (Optional[CCD_BakerId]): Delegation target is not a validator.
        stake_over_maximum_threshold_for_pool (Optional[CCD_Empty]): The amount would result in pool capital higher than the maximum threshold.
        pool_would_become_over_delegated (Optional[CCD_Empty]): The amount would result in pool with a too high fraction of delegated capital.
        pool_closed (Optional[CCD_Empty]): The pool is not open to delegators.
    """

    module_not_wf: Optional[CCD_Empty] = None
    module_hash_already_exists: Optional[CCD_ModuleRef] = None
    invalid_account_reference: Optional[CCD_AccountAddress] = None
    invalid_init_method: Optional[CCD_RejectReason_InvalidInitMethod] = None
    invalid_receive_method: Optional[CCD_RejectReason_InvalidReceiveMethod] = None
    invalid_module_reference: Optional[CCD_ModuleRef] = None
    invalid_contract_address: Optional[CCD_ContractAddress] = None
    runtime_failure: Optional[CCD_Empty] = None
    amount_too_large: Optional[CCD_RejectReason_AmountTooLarge] = None
    serialization_failure: Optional[CCD_Empty] = None
    out_of_energy: Optional[CCD_Empty] = None
    rejected_init: Optional[CCD_RejectReason_RejectedInit] = None
    rejected_receive: Optional[CCD_RejectReason_RejectedReceive] = None
    invalid_proof: Optional[CCD_Empty] = None
    already_a_baker: Optional[CCD_BakerId] = None
    not_a_baker: Optional[CCD_AccountAddress] = None
    insufficient_balance_for_baker_stake: Optional[CCD_Empty] = None
    stake_under_minimum_threshold_for_baking: Optional[CCD_Empty] = None
    baker_in_cooldown: Optional[CCD_Empty] = None
    duplicate_aggregation_key: Optional[CCD_BakerAggregationVerifyKey] = None
    non_existent_credential_id: Optional[CCD_Empty] = None
    key_index_already_in_use: Optional[CCD_Empty] = None
    invalid_account_threshold: Optional[CCD_Empty] = None
    invalid_credential_key_sign_threshold: Optional[CCD_Empty] = None
    invalid_encrypted_amount_transfer_proof: Optional[CCD_Empty] = None
    invalid_transfer_to_public_proof: Optional[CCD_Empty] = None
    encrypted_amount_self_transfer: Optional[CCD_AccountAddress] = None
    invalid_index_on_encrypted_transfer: Optional[CCD_Empty] = None
    zero_scheduledAmount: Optional[CCD_Empty] = None
    non_increasing_schedule: Optional[CCD_Empty] = None
    first_scheduled_release_expired: Optional[CCD_Empty] = None
    scheduled_self_transfer: Optional[CCD_AccountAddress] = None
    invalid_credentials: Optional[CCD_Empty] = None
    duplicate_cred_ids: Optional[CCD_RejectReason_DuplicateCredIds] = None
    non_existent_cred_ids: Optional[CCD_RejectReason_NonExistentCredIds] = None
    remove_first_credential: Optional[CCD_Empty] = None
    credential_holder_did_not_sign: Optional[CCD_Empty] = None
    not_allowed_multiple_credentials: Optional[CCD_Empty] = None
    not_allowed_to_receive_encrypted: Optional[CCD_Empty] = None
    not_allowed_to_handle_encrypted: Optional[CCD_Empty] = None
    missing_baker_add_parameters: Optional[CCD_Empty] = None
    finalization_reward_commission_not_in_range: Optional[CCD_Empty] = None
    baking_reward_commission_not_in_range: Optional[CCD_Empty] = None
    transaction_fee_commission_not_in_range: Optional[CCD_Empty] = None
    already_a_delegator: Optional[CCD_Empty] = None
    insufficient_balance_for_delegation_stake: Optional[CCD_Empty] = None
    missing_delegation_add_parameters: Optional[CCD_Empty] = None
    insufficient_delegation_stake: Optional[CCD_Empty] = None
    delegator_in_cooldown: Optional[CCD_Empty] = None
    not_a_delegator: Optional[CCD_AccountAddress] = None
    delegation_target_not_a_baker: Optional[CCD_BakerId] = None
    stake_over_maximum_threshold_for_pool: Optional[CCD_Empty] = None
    pool_would_become_over_delegated: Optional[CCD_Empty] = None
    pool_closed: Optional[CCD_Empty] = None
    non_existent_token_id: Optional[CCD_TokenId] = None
    token_update_transaction_failed: Optional[CCD_TokenModuleRejectReason] = None


class CCD_CredentialType(Enum):
    """The type of credential. Initial credentials have a special status, while normal credentials are subject to additional verification.

    GRPC documentation: [concordium.v2.CredentialType](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.CredentialType)

    Attributes:
        initial (int): Initial credentials have a special status.
        normal (int): Normal credentials are subject to additional verification.
    """

    initial = 0
    normal = 1


class CCD_StakePendingChange_Reduce(BaseModel):
    """The validator's stake will be reduced at the specified time.

    GRPC documentation: [concordium.v2.StakePendingChange.Reduce](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.StakePendingChange.Reduce)

    Attributes:
        new_stake (microCCD): The reduced stake amount.
        effective_time (CCD_TimeStamp): The time at which the reduction takes effect.
    """

    new_stake: microCCD
    effective_time: CCD_TimeStamp


class CCD_StakePendingChange(BaseModel):
    """A pending change to an account's stake. Either a reduction in stake amount, or complete removal of stake.

    GRPC documentation: [concordium.v2.StakePendingChange](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.StakePendingChange)

    Attributes:
        reduce (Optional[CCD_StakePendingChange_Reduce]): Details of a pending reduction in stake.
        remove (Optional[CCD_StakePendingChange_Remove]): Details of a pending removal of stake.
    """

    reduce: Optional[CCD_StakePendingChange_Reduce] = None
    remove: Optional[CCD_StakePendingChange_Remove] = None


class CCD_BakerStakePendingChange_Reduce(BaseModel):
    """The validator's equity capital will be reduced at the specified time.

    GRPC documentation: [concordium.v2.BakerStakePendingChange.Reduce](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerStakePendingChange.Reduce)

    Attributes:
        reduced_equity_capital (microCCD): The reduced equity capital amount.
        effective_time (CCD_TimeStamp): The time at which the reduction takes effect.
    """

    reduced_equity_capital: microCCD
    effective_time: CCD_TimeStamp


class CCD_BakerStakePendingChange_Remove(BaseModel):
    """The validator's stake will be removed at the specified time.

    GRPC documentation: [concordium.v2.BakerStakePendingChange.Remove](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerStakePendingChange.Remove)

    Attributes:
        effective_time (CCD_TimeStamp): The time at which the removal takes effect.
    """

    effective_time: CCD_TimeStamp


class CCD_BakerStakePendingChange(BaseModel):
    """A pending change to a validator's staking. Either a reduction in stake amount, or complete removal of stake.

    GRPC documentation: [concordium.v2.BakerStakePendingChange](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerStakePendingChange)

    Attributes:
        reduce (Optional[CCD_BakerStakePendingChange_Reduce]): Details of a pending reduction in validator's stake.
        remove (Optional[CCD_BakerStakePendingChange_Remove]): Details of a pending removal of validator's stake.
    """

    reduce: Optional[CCD_BakerStakePendingChange_Reduce] = None
    remove: Optional[CCD_BakerStakePendingChange_Remove] = None


class CCD_DelegatorInfo(BaseModel):
    """Information about a delegator for the current reward period.

    GRPC documentation: [concordium.v2.DelegatorInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.DelegatorInfo)

    Attributes:
        account (CCD_AccountAddress): The delegator account address.
        stake (microCCD): The amount of stake currently staked to the pool.
        pending_change (Optional[CCD_StakePendingChange]): Any pending changes to the stake.
    """

    account: CCD_AccountAddress
    stake: microCCD
    pending_change: Optional[CCD_StakePendingChange] = None


class CCD_DelegatorRewardPeriodInfo(BaseModel):
    """Information about a delegator for the current reward period.

    GRPC documentation: [concordium.v2.DelegatorRewardPeriodInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.DelegatorRewardPeriodInfo)

    Attributes:
        account (CCD_AccountAddress): The delegator account address.
        stake (microCCD): The amount of stake currently staked to the pool.
    """

    account: CCD_AccountAddress
    stake: microCCD


class CCD_CurrentPaydayStatus(BaseModel):
    """Information about a validator's stake, rewards and status for the current reward period.

    GRPC documentation: [concordium.v2.CurrentPaydayStatus](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.CurrentPaydayStatus)

    Attributes:
        baker_equity_capital (microCCD): The equity capital provided by the pool owner.
        blocks_baked (int): The number of blocks baked in the current reward period.
        delegated_capital (microCCD): The capital delegated to the pool by other accounts.
        effective_stake (microCCD): The effective stake of the pool, computed according to the leverage bound.
        finalization_live (bool): Whether the validator participates in finalization.
        lottery_power (float): The pool's relative probability of being selected as validator.
        transaction_fees_earned (microCCD): Transaction fees earned in the current reward period.
        is_primed_for_suspension (Optional[bool]): Whether the validator is primed for suspension.
        missed_rounds (Optional[int]): Number of rounds missed by the validator.
    """

    baker_equity_capital: microCCD
    blocks_baked: int
    delegated_capital: microCCD
    effective_stake: microCCD
    finalization_live: bool
    lottery_power: float
    transaction_fees_earned: microCCD
    is_primed_for_suspension: Optional[bool] = None
    missed_rounds: Optional[int] = None


class CCD_CommissionRates(BaseModel):
    """Distribution of rewards for a particular pool.

    GRPC documentation: [concordium.v2.CommissionRates](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.CommissionRates)

    Attributes:
        baking (float): Fraction of block production rewards charged by the pool owner.
        finalization (float): Fraction of finalization rewards charged by the pool owner.
        transaction (float): Fraction of transaction rewards charged by the pool owner.
    """

    baking: float
    finalization: float
    transaction: float


class CCD_BakerPoolInfo(BaseModel):
    """Additional information about a validator pool.

    GRPC documentation: [concordium.v2.BakerPoolInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerPoolInfo)

    Attributes:
        commission_rates (CCD_CommissionRates): The commission rates charged by the pool owner.
        url (str): The URL that links to the metadata about the pool.
        open_status (OpenStatus): Whether the pool allows delegators.
    """

    commission_rates: CCD_CommissionRates
    url: str
    open_status: str


class CCD_PoolInfo(BaseModel):
    """Type for the response of GetPoolInfo. Contains information about a given pool at the end of a given block.

    GRPC documentation: [concordium.v2.PoolInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.PoolInfo)

    Attributes:
        all_pool_total_capital (microCCD): Total capital staked across all pools, including passive delegation.
        address (CCD_AccountAddress): The account address of the pool owner.
        equity_capital (Optional[microCCD]): The equity capital provided by the pool owner. Absent if the pool is removed.
        validator (CCD_BakerId): The validator ID of the pool owner.
        equity_pending_change (Optional[CCD_BakerStakePendingChange]): Any pending changes to the equity capital of the pool.
        current_payday_info (Optional[CCD_CurrentPaydayStatus]): Information of the pool in the current reward period.
        delegated_capital (Optional[microCCD]): The capital delegated to the pool by other accounts. Absent if the pool is removed.
        delegated_capital_cap (Optional[microCCD]): The maximum amount that may be delegated to the pool. Absent if the pool is removed.
        pool_info (Optional[CCD_BakerPoolInfo]): The pool info associated with the pool. Absent if the pool is removed.
        is_suspended (Optional[bool]): Whether the pool is suspended.
    """

    all_pool_total_capital: microCCD
    address: CCD_AccountAddress
    equity_capital: Optional[microCCD] = None
    baker: int
    equity_pending_change: Optional[CCD_BakerStakePendingChange] = None
    current_payday_info: Optional[CCD_CurrentPaydayStatus] = None
    delegated_capital: Optional[microCCD] = None
    delegated_capital_cap: Optional[microCCD] = None
    pool_info: Optional[CCD_BakerPoolInfo] = None
    is_suspended: Optional[bool] = None
    # poolType: str = None


class CCD_PassiveDelegationInfo(BaseModel):
    """Information about the passive delegation pool.

    GRPC documentation: [concordium.v2.PassiveDelegationInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.PassiveDelegationInfo)

    Attributes:
        all_pool_total_capital (microCCD): Total capital staked across all pools, including passive delegation.
        delegated_capital (microCCD): The total amount delegated to passive delegation.
        current_payday_transaction_fees_earned (microCCD): Transaction fees earned in the current reward period.
        current_payday_delegated_capital (microCCD): The delegated capital in the current reward period.
        commission_rates (CCD_CommissionRates): The commission rates charged by the passive delegation pool.
    """

    all_pool_total_capital: microCCD
    delegated_capital: microCCD
    current_payday_transaction_fees_earned: microCCD
    current_payday_delegated_capital: microCCD
    commission_rates: CCD_CommissionRates


class CCD_ArrivedBlockInfo(BaseModel):
    """Information about an arrived block that is part of the streaming response.

    GRPC documentation: [concordium.v2.ArrivedBlockInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ArrivedBlockInfo)

    Attributes:
        hash (CCD_BlockHash): Hash of the block.
        height (int): Absolute height of the block, height 0 is the genesis block.
    """

    hash: CCD_BlockHash
    height: int


class CCD_FinalizedBlockInfo(BaseModel):
    """Information about a finalized block that is part of the streaming response.

    GRPC documentation: [concordium.v2.FinalizedBlockInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.FinalizedBlockInfo)

    Attributes:
        hash (CCD_BlockHash): Hash of the block.
        height (int): Absolute height of the block, height 0 is the genesis block.
    """

    hash: CCD_BlockHash
    height: int


class CCD_BlockInfo(BaseModel):
    """Information about a block.

    GRPC documentation: [concordium.v2.BlockInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockInfo)

    Attributes:
        arrive_time (Optional[CCD_TimeStamp]): Time the block was verified.
        validator (Optional[int]): ID of the validator of this block.
        hash (CCD_BlockHash): Hash of the block.
        height (int): Absolute height of the block.
        last_finalized_block (CCD_BlockHash): The last finalized block when this block was baked.
        parent_block (CCD_BlockHash): The parent block hash.
        receive_time (Optional[CCD_TimeStamp]): Time the block was received.
        slot_number (Optional[int]): The slot number in which the block was baked.
        slot_time (CCD_TimeStamp): Time of the slot in which the block was baked.
        era_block_height (int): The height relative to genesis.
        finalized (bool): Whether the block is finalized.
        genesis_index (int): The genesis index for this block.
        transaction_count (int): The number of transactions in the block.
        transactions_energy_cost (int): The total energy cost of the transactions in the block.
        transactions_size (int): The total size of the transactions in the block.
        transaction_hashes (Optional[list[CCD_TransactionHash]]): The hashes of the transactions in the block.
        state_hash (Optional[CCD_StateHash]): The state hash of the block.
        protocol_version (Optional[str]): The protocol version of the block.
        round (Optional[CCD_Round]): The round in which the block was created.
        epoch (Optional[CCD_Epoch]): The epoch in which the block was created.
    """

    arrive_time: Optional[CCD_TimeStamp] = None
    baker: Optional[int] = None
    hash: CCD_BlockHash
    height: int
    last_finalized_block: CCD_BlockHash
    parent_block: CCD_BlockHash
    receive_time: Optional[CCD_TimeStamp] = None
    slot_number: Optional[int] = None
    slot_time: CCD_TimeStamp
    era_block_height: int
    finalized: bool
    genesis_index: int
    transaction_count: int
    transactions_energy_cost: int
    transactions_size: int
    transaction_hashes: Optional[list[CCD_TransactionHash]] = None
    state_hash: Optional[CCD_StateHash] = None
    protocol_version: Optional[str] = None  # note this is not optional from the specification,
    # but this type is also used in retrieving blockInfo from MongoDB, where protocol_version
    # isn't stored for all blocks (currently).
    round: Optional[CCD_Round] = None
    epoch: Optional[CCD_Epoch] = None


class CCD_AccountTransactionEffects_None(BaseModel):
    """No effects other than payment from this transaction.
    The rejection reason indicates why the transaction failed.

    GRPC documentation: [concordium.v2.AccountTransactionEffects.None](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountTransactionEffects.None)

    Attributes:
        transaction_type (Optional[int]): Transaction type of a failed transaction, if known.
        reject_reason (CCD_RejectReason): Reason for rejection of the transaction.
    """

    transaction_type: Optional[int] = None
    reject_reason: CCD_RejectReason


class CCD_ContractInitializedEvent(BaseModel):
    """Data generated as part of initializing a single contract instance.

    GRPC documentation: [concordium.v2.ContractInitializedEvent](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ContractInitializedEvent)

    Attributes:
        contract_version (int): Contract version.
        origin_ref (CCD_ModuleRef): Module with the source code of the contract.
        address (CCD_ContractAddress): The newly assigned address of the contract.
        amount (microCCD): The amount the instance was initialized with.
        init_name (str): The name of the contract.
        events (list[CCD_ContractEvent]): Any contract events that might have been generated by the contract initialization.
        parameter (CCD_Parameter): The parameter passed to the initializer.
    """

    contract_version: int
    origin_ref: CCD_ModuleRef
    address: CCD_ContractAddress
    amount: microCCD = 0
    init_name: str
    events: list[CCD_ContractEvent]
    parameter: Optional[CCD_Parameter] = None


class CCD_InstanceUpdatedEvent(BaseModel):
    """Data generated as part of updating a single contract instance.
    In general a single Update transaction will generate one or more of these events, together with possibly some transfers.

    GRPC documentation: [concordium.v2.InstanceUpdatedEvent](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.InstanceUpdatedEvent)

    Attributes:
        contract_version (int): Contract version.
        address (CCD_ContractAddress): Address of the affected instance.
        instigator (CCD_Address): The origin of the message to the smart contract. This can be either an account or a smart contract.
        amount (microCCD): The amount the method was invoked with.
        parameter (CCD_Parameter): The parameter passed to the method.
        receive_name (CCD_ReceiveName): The name of the method that was executed.
        events (Optional[list[CCD_ContractEvent]]): Any contract events that might have been generated by the contract execution.
    """

    contract_version: int
    address: CCD_ContractAddress
    instigator: CCD_Address
    amount: microCCD
    parameter: CCD_Parameter
    receive_name: CCD_ReceiveName
    events: Optional[list[CCD_ContractEvent]] = None


class CCD_ContractTraceElement_Interrupted(BaseModel):
    """A contract was interrupted. This occurs when a contract invokes another contract or makes a transfer to an account.

    GRPC documentation: [concordium.v2.ContractTraceElement.Interrupted](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ContractTraceElement.Interrupted)

    Attributes:
        address (CCD_ContractAddress): The contract that was interrupted.
        events (list[CCD_ContractEvent]): The events generated up until the interruption.
    """

    address: CCD_ContractAddress
    events: list[CCD_ContractEvent]


class CCD_ContractTraceElement_Resumed(BaseModel):
    """A previously interrupted contract was resumed.

    GRPC documentation: [concordium.v2.ContractTraceElement.Resumed](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ContractTraceElement.Resumed)

    Attributes:
        address (CCD_ContractAddress): The address of the resumed contract.
        success (bool): Whether the execution was successful.
    """

    address: CCD_ContractAddress
    success: bool


class CCD_ContractTraceElement_Transferred(BaseModel):
    """A contract transferred an amount to an account.

    GRPC documentation: [concordium.v2.ContractTraceElement.Transferred](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ContractTraceElement.Transferred)

    Attributes:
        sender (CCD_ContractAddress): The contract that sent the amount.
        amount (microCCD): The amount that was transferred.
        receiver (CCD_AccountAddress): The account that received the amount.
    """

    sender: CCD_ContractAddress
    amount: microCCD
    receiver: CCD_AccountAddress


class CCD_ContractTraceElement_Upgraded(BaseModel):
    """A contract was upgraded from one module to another.

    GRPC documentation: [concordium.v2.ContractTraceElement.Upgraded](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ContractTraceElement.Upgraded)

    Attributes:
        address (CCD_ContractAddress): The contract that was upgraded.
        from_module (CCD_ModuleRef): The original module reference.
        to_module (CCD_ModuleRef): The new module reference.
    """

    model_config = ConfigDict(populate_by_name=True)
    address: CCD_ContractAddress
    from_module: CCD_ModuleRef = Field(..., alias="from")
    to_module: CCD_ModuleRef = Field(..., alias="to")
    model_config = {"populate_by_name": True}


class CCD_ContractTraceElement(BaseModel):
    """A trace element for a contract execution. Recording what happened during the execution.

    GRPC documentation: [concordium.v2.ContractTraceElement](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ContractTraceElement)

    Attributes:
        updated (Optional[CCD_InstanceUpdatedEvent]): A contract was updated.
        transferred (Optional[CCD_ContractTraceElement_Transferred]): A contract transferred an amount to an account.
        interrupted (Optional[CCD_ContractTraceElement_Interrupted]): A contract was interrupted.
        resumed (Optional[CCD_ContractTraceElement_Resumed]): A previously interrupted contract was resumed.
        upgraded (Optional[CCD_ContractTraceElement_Upgraded]): A contract was upgraded from one module to another.
    """

    updated: Optional[CCD_InstanceUpdatedEvent] = None
    transferred: Optional[CCD_ContractTraceElement_Transferred] = None
    interrupted: Optional[CCD_ContractTraceElement_Interrupted] = None
    resumed: Optional[CCD_ContractTraceElement_Resumed] = None
    upgraded: Optional[CCD_ContractTraceElement_Upgraded] = None


class CCD_ContractUpdateIssued(BaseModel):
    """The effects of executing a smart contract update transaction.
    Note that this will always be generated with at least one element in the effects list.
    If the execution failed the first element will be an `interrupted` message.

    GRPC documentation: [concordium.v2.ContractUpdateIssued](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ContractUpdateIssued)

    Attributes:
        effects (list[CCD_ContractTraceElement]): The effects of executing the contract update.
    """

    effects: list[CCD_ContractTraceElement]


class CCD_EncryptedAmountRemovedEvent(BaseModel):
    """Event generated when one or more encrypted amounts are consumed from the account.

    GRPC documentation: [concordium.v2.EncryptedAmountRemovedEvent](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.EncryptedAmountRemovedEvent)

    Attributes:
        account (CCD_AccountAddress): The affected account.
        new_amount (CCD_EncryptedAmount): The new self encrypted amount on the affected account.
        input_amount (CCD_EncryptedAmount): The input encrypted amount that was removed.
        up_to_index (int): The index indicating which amounts were used.
    """

    account: CCD_AccountAddress
    new_amount: CCD_EncryptedAmount
    input_amount: CCD_EncryptedAmount
    up_to_index: int


class CCD_NewEncryptedAmountEvent(BaseModel):
    """Event generated when a new self encrypted amount is added to an account.

    GRPC documentation: [concordium.v2.NewEncryptedAmountEvent](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.NewEncryptedAmountEvent)

    Attributes:
        receiver (CCD_AccountAddress): The affected account.
        new_index (int): The index at which this amount was added.
        encrypted_amount (CCD_EncryptedAmount): The encrypted amount that was added.
    """

    receiver: CCD_AccountAddress
    new_index: int
    encrypted_amount: CCD_EncryptedAmount


class CCD_AccountTransactionEffects_EncryptedAmountTransferred(BaseModel):
    """Event generated when transferring an encrypted amount.

    GRPC documentation: [concordium.v2.AccountTransactionEffects.EncryptedAmountTransferred](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountTransactionEffects.EncryptedAmountTransferred)

    Attributes:
        removed (Optional[CCD_EncryptedAmountRemovedEvent]): Event generated when removing an encrypted amount from the sender.
        added (Optional[CCD_NewEncryptedAmountEvent]): Event generated when adding the encrypted amount to the receiver.
        memo (Optional[CCD_Memo]): An optional memo attached to the transfer.
    """

    removed: Optional[CCD_EncryptedAmountRemovedEvent] = None
    added: Optional[CCD_NewEncryptedAmountEvent] = None
    memo: Optional[CCD_Memo] = None


class CCD_EncryptedSelfAmountAddedEvent(BaseModel):
    """Event generated when an account adds a self amount from public balance to encrypted balance.

    GRPC documentation: [concordium.v2.EncryptedSelfAmountAddedEvent](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.EncryptedSelfAmountAddedEvent)

    Attributes:
        account (CCD_AccountAddress): The affected account.
        new_amount (CCD_EncryptedAmount): The new encrypted amount on the affected account.
        amount (microCCD): The amount that was encrypted.
    """

    account: CCD_AccountAddress
    new_amount: CCD_EncryptedAmount
    amount: microCCD


class CCD_AccountTransactionEffects_TransferredToPublic(BaseModel):
    """Event generated when an amount is transferred from encrypted to public balance.

    GRPC documentation: [concordium.v2.AccountTransactionEffects.TransferredToPublic](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountTransactionEffects.TransferredToPublic)

    Attributes:
        removed (CCD_EncryptedAmountRemovedEvent): Event generated when removing an encrypted amount.
        amount (microCCD): The amount that was made public.
    """

    removed: CCD_EncryptedAmountRemovedEvent
    amount: microCCD


class CCD_AccountTransactionEffects_CredentialsUpdated(BaseModel):
    """Event generated when account credentials are updated.

    GRPC documentation: [concordium.v2.AccountTransactionEffects.CredentialsUpdated](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountTransactionEffects.CredentialsUpdated)

    Attributes:
        new_cred_ids (list[CCD_CredentialRegistrationId]): The new credential IDs added to the account.
        removed_cred_ids (list[CCD_CredentialRegistrationId]): The credential IDs removed from the account.
        new_threshold (CCD_AccountThreshold): The new account threshold.
    """

    new_cred_ids: list[CCD_CredentialRegistrationId]
    removed_cred_ids: list[CCD_CredentialRegistrationId]
    new_threshold: CCD_AccountThreshold


class CCD_BakerKeysEvent(BaseModel):
    """Event containing all keys of a validator, including aggregation key.

    GRPC documentation: [concordium.v2.BakerKeysEvent](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerKeysEvent)

    Attributes:
        baker_id (CCD_BakerId): The validator ID.
        account (CCD_AccountAddress): The account address of the validator.
        sign_key (CCD_BakerSignatureVerifyKey): The key used to verify block and consensus signatures.
        election_key (CCD_BakerElectionVerifyKey): The key used to verify leadership proofs.
        aggregation_key (CCD_BakerAggregationVerifyKey): The key used to verify aggregation of finalization proofs.
    """

    baker_id: CCD_BakerId
    account: CCD_AccountAddress
    sign_key: CCD_BakerSignatureVerifyKey
    election_key: CCD_BakerElectionVerifyKey
    aggregation_key: CCD_BakerAggregationVerifyKey


class CCD_BakerAdded(BaseModel):
    """Data generated when a new validator is added.

    GRPC documentation: [concordium.v2.BakerEvent.BakerAdded](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerEvent.BakerAdded)

    Attributes:
        keys_event (CCD_BakerKeysEvent): Event containing all the keys of the validator.
        stake (microCCD): The initial stake amount.
        restake_earnings (bool): Whether the validator's earnings will be added to their stake.
    """

    keys_event: CCD_BakerKeysEvent
    stake: microCCD
    restake_earnings: bool


class CCD_BakerResumed(BaseModel):
    """Data generated when a baker is resumed after being suspended.

    GRPC documentation: [concordium.v2.BakerEvent.BakerResumed](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerEvent.BakerResumed)

    Attributes:
        baker_id (CCD_BakerId): The baker ID.
    """

    baker_id: CCD_BakerId


class CCD_AccountTransfer(BaseModel):
    """A simple transfer of CCD tokens from one account to another.

    GRPC documentation: [concordium.v2.AccountTransfer](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountTransfer)

    Attributes:
        amount (microCCD): The amount that was transferred.
        receiver (CCD_AccountAddress): The account that received the amount.
        memo (Optional[CCD_Memo]): An optional memo attached to the transfer.
    """

    amount: microCCD = 0
    receiver: CCD_AccountAddress
    memo: Optional[CCD_Memo] = None


class CCD_NewRelease(BaseModel):
    """A new individual release. Part of a single transfer with schedule transaction.

    GRPC documentation: [concordium.v2.NewRelease](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.NewRelease)

    Attributes:
        timestamp (CCD_TimeStamp): Effective time of the release in milliseconds since unix epoch.
        amount (microCCD): Amount to be released.
    """

    timestamp: CCD_TimeStamp
    amount: microCCD


class CCD_TransferredWithSchedule(BaseModel):
    """A transfer with schedule was performed. This is the result of a successful TransferWithSchedule transaction.

    GRPC documentation: [concordium.v2.AccountTransactionEffects.TransferredWithSchedule](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountTransactionEffects.TransferredWithSchedule)

    Attributes:
        receiver (CCD_AccountAddress): Receiver account.
        amount (list[CCD_NewRelease]): The list of releases. Ordered by increasing timestamp.
        memo (Optional[CCD_Memo]): Optional memo.
    """

    receiver: CCD_AccountAddress
    amount: list[CCD_NewRelease]
    memo: Optional[CCD_Memo] = None


class CCD_BakerStakeUpdatedData(BaseModel):
    """Validator stake updated data.

    GRPC documentation: [concordium.v2.BakerStakeUpdatedData](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerStakeUpdatedData)

    Attributes:
        baker_id (CCD_BakerId): Affected validator.
        new_stake (microCCD): New stake.
        increased (bool): A boolean which indicates whether it increased (true) or decreased (false).
    """

    baker_id: CCD_BakerId
    new_stake: microCCD
    increased: bool


class CCD_BakerStakeUpdated(BaseModel):
    """An account was deregistered as a validator. This is the result of a successful UpdateBakerStake transaction.

    GRPC documentation: [concordium.v2.AccountTransactionEffects.BakerStakeUpdated](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountTransactionEffects.BakerStakeUpdated)

    Attributes:
        update (Optional[CCD_BakerStakeUpdatedData]): If the stake was updated (that is, it changed and did not stay the same) then this is present, otherwise it is not present.
    """

    update: CCD_BakerStakeUpdatedData


class CCD_BakerStakeIncreased(BaseModel):
    """Validator stake increased.

    GRPC documentation: [concordium.v2.BakerEvent.BakerStakeIncreased](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerEvent.BakerStakeIncreased)

    Attributes:
        baker_id (CCD_BakerId): Validator's id.
        new_stake (microCCD): The new stake.
    """

    baker_id: CCD_BakerId
    new_stake: microCCD


class CCD_BakerStakeDecreased(BaseModel):
    """Validator stake decreased.

    GRPC documentation: [concordium.v2.BakerEvent.BakerStakeDecreased](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerEvent.BakerStakeDecreased)

    Attributes:
        baker_id (CCD_BakerId): Validator's id.
        new_stake (microCCD): The new stake.
    """

    baker_id: CCD_BakerId
    new_stake: microCCD


class CCD_BakerRestakeEarningsUpdated(BaseModel):
    """A validator's setting for restaking earnings was updated.

    GRPC documentation: [concordium.v2.BakerEvent.BakerRestakeEarningsUpdated](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerEvent.BakerRestakeEarningsUpdated)

    Attributes:
        baker_id (CCD_BakerId): Validator's id.
        restake_earnings (bool): The new value of the flag.
    """

    baker_id: CCD_BakerId
    restake_earnings: bool


class CCD_BakerSetOpenStatus(BaseModel):
    """Updated open status for a validator pool.

    GRPC documentation: [concordium.v2.BakerEvent.BakerSetOpenStatus](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerEvent.BakerSetOpenStatus)

    Attributes:
        baker_id (CCD_BakerId): Validator's id.
        open_status (OpenStatus): The new open status.
    """

    baker_id: CCD_BakerId
    open_status: CCD_OpenStatus


class CCD_BakerSetMetadataUrl(BaseModel):
    """Updated metadata url for a validator pool.

    GRPC documentation: [concordium.v2.BakerEvent.BakerSetMetadataUrl](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerEvent.BakerSetMetadataUrl)

    Attributes:
        baker_id (CCD_BakerId): Validator's id.
        url (str): The URL.
    """

    baker_id: CCD_BakerId
    url: str


class CCD_BakerSetTransactionFeeCommission(BaseModel):
    """Updated transaction fee commission for a validator pool.

    GRPC documentation: [concordium.v2.BakerEvent.BakerSetTransactionFeeCommission](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerEvent.BakerSetTransactionFeeCommission)

    Attributes:
        baker_id (CCD_BakerId): Validator's id.
        transaction_fee_commission (AmountFraction): The transaction fee commission.
    """

    baker_id: CCD_BakerId
    transaction_fee_commission: CCD_AmountFraction


class CCD_BakerSetBakingRewardCommission(BaseModel):
    """Updated baking reward commission for validator pool.

    GRPC documentation: [concordium.v2.BakerEvent.BakerSetBakingRewardCommission](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerEvent.BakerSetBakingRewardCommission)

    Attributes:
        baker_id (CCD_BakerId): Validator's id.
        baking_reward_commission (AmountFraction): The baking reward commission.
    """

    baker_id: CCD_BakerId
    baking_reward_commission: CCD_AmountFraction


class CCD_BakerSetFinalizationRewardCommission(BaseModel):
    """Updated finalization reward commission for validator pool.

    GRPC documentation: [concordium.v2.BakerEvent.BakerSetFinalizationRewardCommission](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerEvent.BakerSetFinalizationRewardCommission)

    Attributes:
        baker_id (CCD_BakerId): Validator's id.
        finalization_reward_commission (AmountFraction): The finalization reward commission.
    """

    baker_id: CCD_BakerId
    finalization_reward_commission: CCD_AmountFraction


class CCD_BakerEvent(BaseModel):
    """Events that may result from the ConfigureBaker transaction.

    GRPC documentation: [concordium.v2.BakerEvent](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerEvent)

    Attributes:
        baker_added (Optional[CCD_BakerAdded]): A validator was added.
        baker_removed (Optional[CCD_BakerId]): A validator was removed.
        baker_stake_increased (Optional[CCD_BakerStakeIncreased]): The validator's stake was increased.
        baker_stake_decreased (Optional[CCD_BakerStakeDecreased]): The validator's stake was decreased.
        baker_restake_earnings_updated (Optional[CCD_BakerRestakeEarningsUpdated]): The validator's setting for restaking earnings was updated.
        baker_keys_updated (Optional[CCD_BakerKeysEvent]): Validator keys were updated.
        baker_set_open_status (Optional[CCD_BakerSetOpenStatus]): The validator's open status was updated.
        baker_set_metadata_url (Optional[CCD_BakerSetMetadataUrl]): The validator's metadata URL was updated.
        baker_set_transaction_fee_commission (Optional[CCD_BakerSetTransactionFeeCommission]): The validator's transaction fee commission was updated.
        baker_set_baking_reward_commission (Optional[CCD_BakerSetBakingRewardCommission]): The validator's baking reward commission was updated.
        baker_set_finalization_reward_commission (Optional[CCD_BakerSetFinalizationRewardCommission]): The validator's finalization reward commission was updated.
        delegation_removed (Optional[CCD_DelegatorId]): An existing delegator was removed.
        baker_suspended (Optional[CCD_BakerId]): The validator's account has been suspended.
        baker_resumed (Optional[CCD_BakerId]): The validator's account has been resumed.
    """

    baker_added: Optional[CCD_BakerAdded] = None
    baker_removed: Optional[CCD_BakerId] = None
    delegation_removed: Optional[CCD_DelegatorId] = None
    baker_stake_increased: Optional[CCD_BakerStakeIncreased] = None
    baker_stake_decreased: Optional[CCD_BakerStakeDecreased] = None
    baker_restake_earnings_updated: Optional[CCD_BakerRestakeEarningsUpdated] = None
    baker_keys_updated: Optional[CCD_BakerKeysEvent] = None
    baker_set_open_status: Optional[CCD_BakerSetOpenStatus] = None
    baker_set_metadata_url: Optional[CCD_BakerSetMetadataUrl] = None
    baker_set_transaction_fee_commission: Optional[CCD_BakerSetTransactionFeeCommission] = None
    baker_set_baking_reward_commission: Optional[CCD_BakerSetBakingRewardCommission] = None
    baker_set_finalization_reward_commission: Optional[CCD_BakerSetFinalizationRewardCommission] = (
        None
    )
    baker_suspended: Optional[CCD_BakerId] = None
    baker_resumed: Optional[CCD_BakerId] = None


class CCD_BakerConfigured(BaseModel):
    """A validator was configured. The details of what happened are contained in the list of BakerEvents.

    GRPC documentation: [concordium.v2.AccountTransactionEffects.BakerConfigured](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountTransactionEffects.BakerConfigured)

    Attributes:
        events (list[CCD_BakerEvent]): The events detailing how the validator was configured.
    """

    events: list[CCD_BakerEvent]


class CCD_DelegationStakeIncreased(BaseModel):
    """The delegator's stake increased.

    GRPC documentation: [concordium.v2.DelegationEvent.DelegationStakeIncreased](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.DelegationEvent.DelegationStakeIncreased)

    Attributes:
        delegator_id (CCD_DelegatorId): Delegator's id.
        new_stake (microCCD): New stake.
    """

    delegator_id: CCD_DelegatorId
    new_stake: microCCD


class CCD_DelegationStakeDecreased(BaseModel):
    """The delegator's stake decreased.

    GRPC documentation: [concordium.v2.DelegationEvent.DelegationStakeDecreased](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.DelegationEvent.DelegationStakeDecreased)

    Attributes:
        delegator_id (CCD_DelegatorId): Delegator's id.
        new_stake (microCCD): New stake.
    """

    delegator_id: CCD_DelegatorId
    new_stake: microCCD


class CCD_DelegationSetRestakeEarnings(BaseModel):
    """The delegator's restaking setting was updated.

    GRPC documentation: [concordium.v2.DelegationEvent.DelegationSetRestakeEarnings](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.DelegationEvent.DelegationSetRestakeEarnings)

    Attributes:
        delegator_id (CCD_DelegatorId): Delegator's id.
        restake_earnings (bool): Whether earnings will be restaked.
    """

    delegator_id: CCD_DelegatorId
    restake_earnings: bool


class CCD_DelegationTarget(BaseModel):
    """Entity to which the account delegates a portion of its stake.

    GRPC documentation: [concordium.v2.DelegationTarget](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.DelegationTarget)

    Attributes:
        passive_delegation (Optional[bool]): Delegate passively, i.e., to no specific validator.
        validator (Optional[CCD_BakerId]): Delegate to a specific validator.
    """

    passive_delegation: Optional[bool] = None
    baker: Optional[CCD_BakerId] = None


class CCD_DelegationSetDelegationTarget(BaseModel):
    """The delegator's delegation target was updated.

    GRPC documentation: [concordium.v2.DelegationEvent.DelegationSetDelegationTarget](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.DelegationEvent.DelegationSetDelegationTarget)

    Attributes:
        delegator_id (CCD_DelegatorId): Delegator's id.
        delegation_target (CCD_DelegationTarget): New delegation target.
    """

    delegator_id: CCD_DelegatorId
    delegation_target: CCD_DelegationTarget


class CCD_DelegationEvent(BaseModel):
    """Events that may result from configuring delegation.

    GRPC documentation: [concordium.v2.DelegationEvent](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.DelegationEvent)

    Attributes:
        delegation_stake_increased (Optional[CCD_DelegationStakeIncreased]): The delegator's stake increased.
        delegation_stake_decreased (Optional[CCD_DelegationStakeDecreased]): The delegator's stake decreased.
        delegation_set_restake_earnings (Optional[CCD_DelegationSetRestakeEarnings]): The delegator's restaking setting was updated.
        delegation_set_delegation_target (Optional[CCD_DelegationSetDelegationTarget]): The delegator's delegation target was updated.
        delegation_added (Optional[CCD_DelegatorId]): A delegator was added.
        delegation_removed (Optional[CCD_DelegatorId]): A delegator was removed.
        baker_removed (Optional[CCD_BakerRemoved]): An existing validator was removed.
    """

    delegation_added: Optional[CCD_DelegatorId] = None
    delegation_removed: Optional[CCD_DelegatorId] = None
    baker_removed: Optional[CCD_BakerId] = None
    delegation_stake_increased: Optional[CCD_DelegationStakeIncreased] = None
    delegation_stake_decreased: Optional[CCD_DelegationStakeDecreased] = None
    delegation_set_restake_earnings: Optional[CCD_DelegationSetRestakeEarnings] = None
    delegation_set_delegation_target: Optional[CCD_DelegationSetDelegationTarget] = None


class CCD_DelegationConfigured(BaseModel):
    """An account configured delegation. The details of what happened are contained in the list of DelegationEvents.

    GRPC documentation: [concordium.v2.AccountTransactionEffects.DelegationConfigured](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountTransactionEffects.DelegationConfigured)

    Attributes:
        events (list[CCD_DelegationEvent]): The delegation events that occurred during configuration.
    """

    events: list[CCD_DelegationEvent]


class CCD_BakerEvent_BakerRestakeEarningsUpdated(BaseModel):
    """A validator's setting for restaking earnings was updated.

    GRPC documentation: [concordium.v2.BakerEvent.BakerRestakeEarningsUpdated](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerEvent.BakerRestakeEarningsUpdated)

    Attributes:
        baker_id (CCD_BakerId): Validator's id.
        restake_earnings (bool): The new value of the flag.
    """

    baker_id: CCD_BakerId
    restake_earnings: bool


class CCD_AccountTransactionEffects(BaseModel):
    """Effects of an account transaction. All variants except `None` correspond to a unique transaction that was successful.

    GRPC documentation: [concordium.v2.AccountTransactionEffects](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountTransactionEffects)

    Attributes:
        none (Optional[CCD_AccountTransactionEffects_None]): No effects other than payment from this transaction.
        module_deployed (Optional[CCD_ModuleRef]): A smart contract module with the attached reference was deployed.
        contract_initialized (Optional[CCD_ContractInitializedEvent]): A smart contract was initialized.
        contract_update_issued (Optional[CCD_ContractUpdateIssued]): A smart contract instance update was issued.
        account_transfer (Optional[CCD_AccountTransfer]): A simple account to account transfer occurred.
        baker_added (Optional[CCD_BakerAdded]): A validator was added.
        baker_removed (Optional[CCD_BakerId]): A validator was removed.
        baker_stake_updated (Optional[CCD_BakerStakeUpdated]): A validator's stake was updated.
        baker_restake_earnings_updated (Optional[CCD_BakerEvent_BakerRestakeEarningsUpdated]): A validator's restake earnings setting was updated.
        baker_keys_updated (Optional[CCD_BakerKeysEvent]): A validator's keys were updated.
        encrypted_amount_transferred (Optional[CCD_AccountTransactionEffects_EncryptedAmountTransferred]): An encrypted amount was transferred.
        transferred_to_encrypted (Optional[CCD_EncryptedSelfAmountAddedEvent]): An account transferred part of its public balance to its encrypted balance.
        transferred_to_public (Optional[CCD_AccountTransactionEffects_TransferredToPublic]): An account transferred part of its encrypted balance to its public balance.
        transferred_with_schedule (Optional[CCD_TransferredWithSchedule]): A transfer with a release schedule was made.
        credential_keys_updated (Optional[CCD_CredentialRegistrationId]): Keys of a specific credential were updated.
        credentials_updated (Optional[CCD_AccountTransactionEffects_CredentialsUpdated]): Account credentials were updated.
        data_registered (Optional[CCD_RegisteredData]): Some data was registered on the chain.
        baker_configured (Optional[CCD_BakerConfigured]): A validator was configured.
        delegation_configured (Optional[CCD_DelegationConfigured]): A delegator was configured.
    """

    none: Optional[CCD_AccountTransactionEffects_None] = None
    module_deployed: Optional[CCD_ModuleRef] = None
    contract_initialized: Optional[CCD_ContractInitializedEvent] = None
    contract_update_issued: Optional[CCD_ContractUpdateIssued] = None
    account_transfer: Optional[CCD_AccountTransfer] = None
    baker_added: Optional[CCD_BakerAdded] = None
    baker_removed: Optional[CCD_BakerId] = None
    baker_stake_updated: Optional[CCD_BakerStakeUpdated] = None
    baker_restake_earnings_updated: Optional[CCD_BakerEvent_BakerRestakeEarningsUpdated] = None
    baker_keys_updated: Optional[CCD_BakerKeysEvent] = None
    encrypted_amount_transferred: Optional[
        CCD_AccountTransactionEffects_EncryptedAmountTransferred
    ] = None
    transferred_to_encrypted: Optional[CCD_EncryptedSelfAmountAddedEvent] = None
    transferred_to_public: Optional[CCD_AccountTransactionEffects_TransferredToPublic] = None
    transferred_with_schedule: Optional[CCD_TransferredWithSchedule] = None
    credential_keys_updated: Optional[CCD_CredentialRegistrationId] = None
    credentials_updated: Optional[CCD_AccountTransactionEffects_CredentialsUpdated] = None
    data_registered: Optional[CCD_RegisteredData] = None
    baker_configured: Optional[CCD_BakerConfigured] = None
    delegation_configured: Optional[CCD_DelegationConfigured] = None
    token_update_effect: Optional[CCD_TokenEffect] = None


class CCD_AccountTransactionDetails(BaseModel):
    """Details about an account transaction.

    GRPC documentation: [concordium.v2.AccountTransactionDetails](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountTransactionDetails)

    Attributes:
        cost (microCCD): The cost of the transaction. Paid by the sender.
        sender (CCD_AccountAddress): The sender of the transaction.
        effects (CCD_AccountTransactionEffects): The effects of the transaction.
    """

    cost: microCCD
    sender: CCD_AccountAddress
    outcome: str
    effects: CCD_AccountTransactionEffects


class CCD_AccountCreationDetails(BaseModel):
    """Details of an account creation. These transactions are free, and we only ever get a response for them if the account is created, hence no failure cases.

    GRPC documentation: [concordium.v2.AccountCreationDetails](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountCreationDetails)

    Attributes:
        credential_type (CredentialType): Whether this is an initial or normal account.
        address (CCD_AccountAddress): Address of the newly created account.
        reg_id (CCD_CredentialRegistrationId): Credential registration ID of the first credential.
    """

    credential_type: int
    address: CCD_AccountAddress
    reg_id: CCD_CredentialRegistrationId


class CCD_ProtocolUpdate(BaseModel):
    """A protocol update.

    GRPC documentation: [concordium.v2.ProtocolUpdate](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ProtocolUpdate)

    Attributes:
        message_ (str): A brief message about the update.
        specification_url (str): A URL of a document describing the update.
        specificationHash (CCD_Sha256Hash): SHA256 hash of the specification document.
        specification_auxiliary_data (bytes): Auxiliary data whose interpretation is defined by the new specification.
    """

    message_: str
    specification_url: str
    specificationHash: CCD_Sha256Hash
    specification_auxiliary_data: Optional[bytes] = None


class CCD_Ratio(BaseModel):
    """Represents a ratio, i.e., 'numerator / denominator'.

    GRPC documentation: [concordium.v2.Ratio](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.Ratio)

    Attributes:
        numerator (int): The numerator.
        denominator (int): The denominator.
    """

    # note these are ints, but need to convert to str
    # as the int amounts are sometimes too big (for MongoDB)
    numerator: str
    denominator: str


class CCD_ExchangeRate(BaseModel):
    """Represents an exchange rate. Value is numerator/denominator.

    GRPC documentation: [concordium.v2.ExchangeRate](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ExchangeRate)

    Attributes:
        value (CCD_Ratio): The ratio representing the exchange rate.
    """

    numerator: str
    denominator: str


class CCD_MintRate(BaseModel):
    """A minting rate of CCD. The value is `mantissa * 10^(-exponent)`.

    GRPC documentation: [concordium.v2.MintRate](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.MintRate)

    Attributes:
        mantissa (int): The mantissa part of the mint rate.
        exponent (int): The exponent part of the mint rate. This will never exceed 255 and can thus be stored in a single byte.
    """

    mantissa: int
    exponent: int


class CCD_MintDistributionCpv0(BaseModel):
    """The minting rate and the distribution of newly-minted CCD among bakers, finalizers, and the foundation account. It must be the case that baking_reward + finalization_reward <= 1. The remaining amount is the platform development charge.

    GRPC documentation: [concordium.v2.MintDistributionCpv0](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.MintDistributionCpv0)

    Attributes:
        mint_per_slot (CCD_MintRate): Mint rate per slot.
        baking_reward (CCD_AmountFraction): The fraction of newly created CCD allocated to validator rewards.
        finalization_reward (CCD_AmountFraction): The fraction of newly created CCD allocated to finalization rewards.
    """

    mint_per_slot: CCD_MintRate
    baking_reward: CCD_AmountFraction
    finalization_reward: CCD_AmountFraction


class CCD_TransactionFeeDistribution(BaseModel):
    """Parameters determining the distribution of transaction fees.

    GRPC documentation: [concordium.v2.TransactionFeeDistribution](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.TransactionFeeDistribution)

    Attributes:
        validator (CCD_AmountFraction): The fraction allocated to the validator.
        gas_account (CCD_AmountFraction): The fraction allocated to the GAS account.
    """

    baker: Optional[CCD_AmountFraction] = None
    gas_account: Optional[CCD_AmountFraction] = None


class CCD_GasRewards(BaseModel):
    """Distribution of gas rewards for chain parameters version 0 and 1.

    GRPC documentation: [concordium.v2.GasRewards](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.GasRewards)

    Attributes:
        validator (CCD_AmountFraction): The fraction paid to the validator.
        finalization_proof (CCD_AmountFraction): Fraction paid for including a finalization proof in a block.
        account_creation (CCD_AmountFraction): Fraction paid for including each account creation transaction in a block.
        chain_update (CCD_AmountFraction): Fraction paid for including an update transaction in a block.
    """

    baker: CCD_AmountFraction
    finalization_proof: CCD_AmountFraction
    account_creation: CCD_AmountFraction
    chain_update: CCD_AmountFraction


class CCD_HigherLevelKeys(BaseModel):
    """Represents root or level 1 keys.

    GRPC documentation: [concordium.v2.HigherLevelKeys](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.HigherLevelKeys)

    Attributes:
        keys (list[CCD_UpdatePublicKey]): The keys.
        threshold (CCD_UpdateKeysThreshold): The number of keys needed to make a chain update.
    """

    keys: list[CCD_UpdatePublicKey]
    threshold: CCD_UpdateKeysThreshold


class CCD_AccessStructure(BaseModel):
    """An access structure which specifies which UpdatePublicKeys in a HigherLevelKeys that are allowed to make chain update of a specific type. The threshold defines the minimum number of allowed keys needed to make the actual update.

    GRPC documentation: [concordium.v2.AccessStructure](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccessStructure)

    Attributes:
        access_public_keys (list[CCD_UpdateKeysIndex]): Unique indexes into the set of keys in AuthorizationV0.
        access_threshold (CCD_UpdateKeysThreshold): Number of keys required to authorize an update.
    """

    access_public_keys: list[CCD_UpdateKeysIndex]
    access_threshold: CCD_UpdateKeysThreshold


class CCD_AuthorizationsV0(BaseModel):
    """The set of keys authorized for chain updates, together with access structures determining which keys are authorized for which update types.

    GRPC documentation: [concordium.v2.AuthorizationsV0](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AuthorizationsV0)

    Attributes:
        keys (list[CCD_UpdatePublicKey]): The set of keys authorized for chain updates.
        emergency (CCD_AccessStructure): New emergency keys.
        protocol (CCD_AccessStructure): New protocol update keys.
        parameter_consensus (CCD_AccessStructure): Access structure for updating the consensus parameters. Previously, this was the election difficulty.
        parameter_euro_per_energy (CCD_AccessStructure): Access structure for updating the euro per energy.
        parameter_micro_CCD_per_euro (CCD_AccessStructure): Access structure for updating the micro CCD per euro.
        parameter_foundation_account (CCD_AccessStructure): Access structure for updating the foundation account.
        parameter_mint_distribution (CCD_AccessStructure): Access structure for updating the mint distribution.
        parameter_transaction_fee_distribution (CCD_AccessStructure): Access structure for updating the transaction fee distribution.
        parameter_gas_rewards (CCD_AccessStructure): Access structure for updating the gas rewards.
        pool_parameters (CCD_AccessStructure): Access structure for updating the pool parameters. For V0 this is only the validator stake threshold, for V1 there are more.
        add_anonymity_revoker (CCD_AccessStructure): Access structure for adding new anonymity revokers.
        add_identity_provider (CCD_AccessStructure): Access structure for adding new identity providers.
    """

    keys: list[CCD_UpdatePublicKey]
    emergency: CCD_AccessStructure
    protocol: CCD_AccessStructure
    parameter_consensus: Optional[CCD_AccessStructure] = None
    parameter_euro_per_energy: CCD_AccessStructure
    parameter_micro_CCD_per_euro: CCD_AccessStructure
    parameter_foundation_account: CCD_AccessStructure
    parameter_mint_distribution: CCD_AccessStructure
    parameter_transaction_fee_distribution: CCD_AccessStructure
    parameter_gas_rewards: CCD_AccessStructure
    pool_parameters: CCD_AccessStructure
    add_anonymity_revoker: CCD_AccessStructure
    add_identity_provider: CCD_AccessStructure


class CCD_AuthorizationsV1(BaseModel):
    """The set of keys authorized for chain updates, together with access structures determining which keys are authorized for which update types. Similar to AuthorizationsV0 except that a few more keys can be updated.

    GRPC documentation: [concordium.v2.AuthorizationsV1](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AuthorizationsV1)

    Attributes:
        v0 (CCD_AuthorizationsV0): The base V0 authorizations.
        parameter_cooldown (CCD_AccessStructure): Access structure for updating the cooldown periods related to baking and delegation.
        parameter_time (CCD_AccessStructure): Access structure for updating the length of the reward period.
    """

    v0: CCD_AuthorizationsV0
    parameter_cooldown: CCD_AccessStructure
    parameter_time: CCD_AccessStructure


class CCD_RootUpdate(BaseModel):
    """Root updates are the highest kind of key updates. They can update every other set of keys, even themselves. They can only be performed by Root level keys.

    GRPC documentation: [concordium.v2.RootUpdate](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RootUpdate)

    Attributes:
        root_keys_update (Optional[CCD_HigherLevelKeys]): The root keys were updated.
        level_1_keys_update (Optional[CCD_HigherLevelKeys]): The level 1 keys were updated.
        level_2_keys_update_v0 (Optional[CCD_AuthorizationsV0]): The level 2 keys were updated.
        level_2_keys_update_v1 (Optional[CCD_AuthorizationsV1]): The level 2 keys were updated. This is similar to level_2_keys_update_v0 except that a few more keys can be updated.
    """

    root_keys_update: CCD_HigherLevelKeys
    level_1_keys_update: CCD_HigherLevelKeys
    level_2_keys_update_v0: CCD_AuthorizationsV0
    level_2_keys_update_v1: CCD_AuthorizationsV1


class CCD_Level1Update(BaseModel):
    """Level 1 updates are the intermediate update kind. They can update themselves or level 2 keys. They can only be performed by level 1 keys.

    GRPC documentation: [concordium.v2.Level1Update](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.Level1Update)

    Attributes:
        level_1_keys_update (Optional[CCD_HigherLevelKeys]): The level 1 keys were updated.
        level_2_keys_update_v0 (Optional[CCD_AuthorizationsV0]): The level 2 keys were updated.
        level_2_keys_update_v1 (Optional[CCD_AuthorizationsV1]): The level 2 keys were updated. This is similar to level_2_keys_update_v0 except that a few more keys can be updated.
    """

    level_1_keys_update: Optional[CCD_HigherLevelKeys] = None
    level_2_keys_update_v0: Optional[CCD_AuthorizationsV0] = None
    level_2_keys_update_v1: Optional[CCD_AuthorizationsV1] = None


class CCD_Description(BaseModel):
    """Description either of an anonymity revoker or identity provider. Metadata that should be visible on the chain.

    GRPC documentation: [concordium.v2.Description](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.Description)

    Attributes:
        name (str): The name.
        url (str): A link to more information about the anonymity revoker or identity provider.
        description (str): A free form description of the revoker or provider.
    """

    name: str
    url: str
    description: str


class CCD_ArInfo(BaseModel):
    """Information on a single anonymity revoker help by the identity provider. Typically an identity provider will hold more than one.

    GRPC documentation: [concordium.v2.ArInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ArInfo)

    Attributes:
        identity (CCD_ArIdentity): Identity of the anonymity revoker on the chain. This defines their evaluation point for secret sharing, and thus it cannot be 0.
        description (CCD_Description): Description of the anonymity revoker.
        public_key (CCD_ArPublicKey): Elgamal encryption key of the anonymity revoker.
    """

    identity: CCD_ArIdentity
    description: CCD_Description
    public_key: CCD_ArPublicKey


class CCD_IpInfo(BaseModel):
    """Public information about an identity provider.

    GRPC documentation: [concordium.v2.IpInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.IpInfo)

    Attributes:
        identity (CCD_IpIdentity): Unique identifier of the identity provider.
        description (CCD_Description): Description of the identity provider.
        verify_key (CCD_IpVerifyKey): Pointcheval-Sanders public key of the identity provider.
        cdi_verify_key (CCD_IpCdiVerifyKey): Ed25519 public key of the identity provider.
    """

    identity: CCD_IpIdentity
    description: CCD_Description
    verify_key: CCD_IpVerifyKey
    cdi_verify_key: CCD_IpCdiVerifyKey


class CCD_CooldownParametersCpv1(BaseModel):
    """Parameters related to cooldown periods for pool owners and delegators.

    GRPC documentation: [concordium.v2.CooldownParametersCpv1](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.CooldownParametersCpv1)

    Attributes:
        pool_owner_cooldown (CCD_DurationSeconds): Number of seconds that pool owners must cooldown when reducing their equity capital or closing the pool.
        delegator_cooldown (CCD_DurationSeconds): Number of seconds that a delegator must cooldown when reducing their delegated stake.
    """

    pool_owner_cooldown: Optional[CCD_DurationSeconds] = None
    delegator_cooldown: Optional[CCD_DurationSeconds] = None


class CCD_InclusiveRangeAmountFraction(BaseModel):
    """Inclusive range of amount fractions.

    GRPC documentation: [concordium.v2.InclusiveRangeAmountFraction](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.InclusiveRangeAmountFraction)

    Attributes:
        min (CCD_AmountFraction): The minimum value of the range.
        max_ (CCD_AmountFraction): The maximum value of the range.
    """

    min: CCD_AmountFraction
    max_: CCD_AmountFraction


class CCD_CommissionRanges(BaseModel):
    """Ranges of allowed commission values that pools may choose from.

    GRPC documentation: [concordium.v2.CommissionRanges](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.CommissionRanges)

    Attributes:
        finalization (CCD_InclusiveRangeAmountFraction): The range of allowed finalization commissions.
        baking (CCD_InclusiveRangeAmountFraction): The range of allowed validator commissions.
        transaction (CCD_InclusiveRangeAmountFraction): The range of allowed transaction commissions.
    """

    finalization: CCD_InclusiveRangeAmountFraction
    baking: CCD_InclusiveRangeAmountFraction
    transaction: CCD_InclusiveRangeAmountFraction


class CCD_CapitalBound(BaseModel):
    """A bound on the relative share of the total staked capital that a validator can have as its stake. This is required to be greater than 0.

    GRPC documentation: [concordium.v2.CapitalBound](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.CapitalBound)

    Attributes:
        value (CCD_AmountFraction): The fraction representing the capital bound.
    """

    value: CCD_AmountFraction


class CCD_LeverageFactor(BaseModel):
    """A leverage factor.

    GRPC documentation: [concordium.v2.LeverageFactor](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.LeverageFactor)

    Attributes:
        value (CCD_Ratio): The ratio representing the leverage factor.
    """

    value: CCD_Ratio


class CCD_PoolParametersCpv1(BaseModel):
    """Parameters related to staking pools.

    GRPC documentation: [concordium.v2.PoolParametersCpv1](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.PoolParametersCpv1)

    Attributes:
        passive_finalization_commission (AmountFraction): Fraction of finalization rewards charged by the passive delegation.
        passive_baking_commission (AmountFraction): Fraction of block production rewards charged by the passive delegation.
        passive_transaction_commission (AmountFraction): Fraction of transaction rewards charged by the L-pool.
        commission_bounds (CCD_CommissionRanges): Bounds on the commission rates that may be charged by bakers.
        minimum_equity_capital (microCCD): Minimum equity capital required for a new validator.
        capital_bound (CCD_CapitalBound): Maximum fraction of the total staked capital that a new validator can have.
        leverage_bound (CCD_LeverageFactor): The maximum leverage that a validator can have as a ratio of total stake to equity capital.
    """

    passive_finalization_commission: CCD_AmountFraction
    passive_baking_commission: CCD_AmountFraction
    passive_transaction_commission: CCD_AmountFraction
    commission_bounds: CCD_CommissionRanges
    minimum_equity_capital: microCCD
    capital_bound: CCD_CapitalBound
    leverage_bound: CCD_LeverageFactor


class CCD_RewardPeriodLength(BaseModel):
    """Length of a reward period in epochs. Must always be a strictly positive number.

    GRPC documentation: [concordium.v2.RewardPeriodLength](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RewardPeriodLength)

    Attributes:
        value (Epoch): The number of epochs in the reward period.
    """

    value: CCD_Epoch


class CCD_TimeParametersCpv1(BaseModel):
    """The time parameters are introduced as of protocol version 4, and consist of the reward period length and the mint rate per payday. These are coupled as a change to either affects the overall rate of minting.

    GRPC documentation: [concordium.v2.TimeParametersCpv1](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.TimeParametersCpv1)

    Attributes:
        reward_period_length (RewardPeriodLength): Length of the reward period in epochs.
        mint_per_payday (CCD_MintRate): The rate at which CCDs are minted at each payday.
    """

    reward_period_length: CCD_Epoch
    mint_per_payday: CCD_MintRate


class CCD_MintDistributionCpv1(BaseModel):
    """Distribution of mint rewards for protocol version 4 and onwards.

    GRPC documentation: [concordium.v2.MintDistributionCpv1](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.MintDistributionCpv1)

    Attributes:
        baking_reward (AmountFraction): The fraction of newly created CCD allocated to validator rewards.
        finalization_reward (AmountFraction): The fraction of newly created CCD allocated to finalization rewards.
    """

    baking_reward: CCD_AmountFraction
    finalization_reward: CCD_AmountFraction


class CCD_BakerStakeThreshold(BaseModel):
    """Minimum stake needed to become a validator. This only applies to protocol version 1-3.

    GRPC documentation: [concordium.v2.BakerStakeThreshold](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerStakeThreshold)

    Attributes:
        baker_stake_threshold (Amount): Minimum threshold required for registering as a validator.
    """

    baker_stake_threshold: microCCD


class CCD_TransactionType(BaseModel):
    """Different types of transactions that can be submitted to the chain.

    GRPC documentation: [concordium.v2.TransactionType](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.TransactionType)
    """

    type: str
    contents: str
    additional_data: Optional[str] = None  # used for PLT event


class CCD_FinalizationCommitteeParameters(BaseModel):
    """Finalization committee parameters used from protocol version 6.

    GRPC documentation: [concordium.v2.FinalizationCommitteeParameters](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.FinalizationCommitteeParameters)

    Attributes:
        minimum_finalizers (uint32): The minimum size of a finalization committee before finalizer_relative_stake_threshold takes effect.
        maximum_finalizers (uint32): The maximum size of a finalization committee.
        finalizer_relative_stake_threshold (CCD_AmountFraction): The threshold for determining the stake required for being eligible for the finalization committee. The amount is given by total stake in pools * finalizer_relative_stake_threshold.
    """

    minimum_finalizers: int
    maximum_finalizers: int
    finalizer_relative_stake_threshold: CCD_AmountFraction


class CCD_ValidatorScoreParameters(BaseModel):
    """Parameters used by the validator scoring system for determining validator suspensions.

    GRPC documentation: [concordium.v2.ValidatorScoreParameters](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ValidatorScoreParameters)

    Attributes:
        maximum_missed_rounds (int): The maximal number of missed rounds before a validator gets suspended.
    """

    maximum_missed_rounds: int


class CCD_UpdatePayload(BaseModel):
    """The payload of a chain update.

    GRPC documentation: [concordium.v2.UpdatePayload](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.UpdatePayload)

    Attributes:
        protocol_update (Optional[CCD_ProtocolUpdate]): The protocol version was updated.
        election_difficulty_update (Optional[CCD_ElectionDifficulty]): The election difficulty was updated.
        euro_per_energy_update (Optional[CCD_ExchangeRate]): The euro per energy exchange rate was updated.
        micro_ccd_per_euro_update (Optional[CCD_ExchangeRate]): The microCCD per euro exchange rate was updated.
        foundation_account_update (Optional[CCD_AccountAddress]): The foundation account address was updated.
        mint_distribution_update (Optional[CCD_MintDistributionCpv0]): The mint distribution was updated (protocol version 1-3).
        transaction_fee_distribution_update (Optional[CCD_TransactionFeeDistribution]): The transaction fee distribution was updated.
        gas_rewards_update (Optional[CCD_GasRewards]): The gas rewards were updated (protocol version 1-5).
        baker_stake_threshold_update (Optional[CCD_BakerStakeThreshold]): The minimum amount of CCD needed to become a validator was updated.
        root_update (Optional[CCD_RootUpdate]): The root keys were updated.
        level_1_update (Optional[CCD_Level1Update]): The level 1 keys were updated.
        add_anonymity_revoker_update (Optional[CCD_ArInfo]): An anonymity revoker was added.
        add_identity_provider_update (Optional[CCD_IpInfo]): An identity provider was added.
        cooldown_parameters_cpv_1_update (Optional[CCD_CooldownParametersCpv1]): The cooldown parameters were updated.
        pool_parameters_cpv_1_update (Optional[CCD_PoolParametersCpv1]): The pool parameters were updated.
        time_parameters_cpv_1_update (Optional[CCD_TimeParametersCpv1]): The time parameters were updated.
        mint_distribution_cpv_1_update (Optional[CCD_MintDistributionCpv1]): The mint distribution was updated (protocol version 4+).
        gas_rewards_cpv_2_update (Optional[CCD_GasRewardsCpv2]): The gas rewards were updated (protocol version 6+).
        timeout_parameters_update (Optional[CCD_TimeoutParameters]): The consensus timeouts were updated.
        min_block_time_update (Optional[CCD_Duration]): The minimum time between blocks was updated.
        block_energy_limit_update (Optional[CCD_Energy]): The block energy limit was updated.
        finalization_committee_parameters_update (Optional[CCD_FinalizationCommitteeParameters]): The finalization committee parameters were updated.
        validator_score_parameters_update (Optional[CCD_ValidatorScoreParameters]): The validator score parameters were updated.
    """

    protocol_update: Optional[CCD_ProtocolUpdate] = None
    election_difficulty_update: Optional[CCD_ElectionDifficulty] = None
    euro_per_energy_update: Optional[CCD_ExchangeRate] = None
    micro_ccd_per_euro_update: Optional[CCD_ExchangeRate] = None
    foundation_account_update: Optional[CCD_AccountAddress] = None
    mint_distribution_update: Optional[CCD_MintDistributionCpv0] = None
    transaction_fee_distribution_update: Optional[CCD_TransactionFeeDistribution] = None
    baker_stake_threshold_update: Optional[CCD_BakerStakeThreshold] = None
    root_update: Optional[CCD_RootUpdate] = None
    level_1_update: Optional[CCD_Level1Update] = None
    add_anonymity_revoker_update: Optional[CCD_ArInfo] = None
    add_identity_provider_update: Optional[CCD_IpInfo] = None
    cooldown_parameters_cpv_1_update: Optional[CCD_CooldownParametersCpv1] = None
    pool_parameters_cpv_1_update: Optional[CCD_PoolParametersCpv1] = None
    time_parameters_cpv_1_update: Optional[CCD_TimeParametersCpv1] = None
    mint_distribution_cpv_1_update: Optional[CCD_MintDistributionCpv1] = None
    finalization_committee_parameters_update: Optional[CCD_FinalizationCommitteeParameters] = None
    validator_score_parameters_update: Optional[CCD_ValidatorScoreParameters] = None
    create_plt_update: Optional[CCD_CreatePLT] = None


class CCD_UpdateDetails(BaseModel):
    """Details of an update instruction. These are free, and we only ever get a response for them if the update is successfully enqueued, hence no failure cases.

    GRPC documentation: [concordium.v2.UpdateDetails](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.UpdateDetails)

    Attributes:
        effective_time (TransactionTime): The time at which the update will be effective.
        payload (UpdatePayload): The payload for the update.
    """

    effective_time: Optional[CCD_TransactionTime] = None
    payload: CCD_UpdatePayload


class CCD_ShortBlockInfo(BaseModel):
    """
    CCD_ShortBlockInfo represents some info for a block (custom class).

    Attributes:
        height (int): The height of the block in the blockchain.
        hash (CCD_BlockHash): The hash of the block.
        slot_time (CCD_TimeStamp): The timestamp of the block slot.
    """

    height: int
    hash: CCD_BlockHash
    slot_time: CCD_TimeStamp


class CCD_BlockItemSummary(BaseModel):
    """Summary of the outcome of a block item in structured form. The summary determines which transaction type it was.

    GRPC documentation: [concordium.v2.BlockItemSummary](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockItemSummary)

    Attributes:
        index (TransactionIndex): Index of the transaction in the block where it is included.
        energy_cost (Energy): The amount of NRG the transaction cost.
        hash (CCD_TransactionHash): Hash of the transaction.
        account_transaction (Optional[CCD_AccountTransactionDetails]): Details about an account transaction.
        account_creation (Optional[CCD_AccountCreationDetails]): Details about an account creation.
        update (Optional[CCD_UpdateDetails]): Details about a chain update.
        block_info (Optional[CCD_ShortBlockInfo]): Short information about the block.
        recognized_sender_id (Optional[str]): The recognized sender ID if available.
    """

    index: int = 0
    energy_cost: Optional[int] = None
    hash: CCD_TransactionHash
    type: Optional[CCD_TransactionType] = None
    account_transaction: Optional[CCD_AccountTransactionDetails] = None
    account_creation: Optional[CCD_AccountCreationDetails] = None
    update: Optional[CCD_UpdateDetails] = None
    token_creation: Optional[CCD_TokenCreationDetails] = None
    block_info: Optional[CCD_ShortBlockInfo] = None
    recognized_sender_id: Optional[str] = None


class CCD_Block(BaseModel):
    """
    CCD_Block represents the txs from a block (custom class)

    Attributes:
        transaction_summaries (list[CCD_BlockItemSummary]): A list of summaries for each transaction in the block.
    """

    transaction_summaries: list[CCD_BlockItemSummary]


class CCD_Release(BaseModel):
    """An individual release of a locked balance.

    GRPC documentation: [concordium.v2.Release](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.Release)

    Attributes:
        timestamp (CCD_TimeStamp): Effective time of the release in milliseconds since unix epoch.
        amount (microCCD): Amount to be released.
        transactions (list[CCD_TransactionHash]): List of transaction hashes that contribute a balance to this release.
    """

    timestamp: CCD_TimeStamp
    amount: microCCD
    transactions: list[CCD_TransactionHash]


class CCD_ReleaseSchedule(BaseModel):
    """State of the account's release schedule. This is the balance of the account that is owned by the account, but cannot be used until the release point.

    GRPC documentation: [concordium.v2.ReleaseSchedule](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ReleaseSchedule)

    Attributes:
        total (microCCD): Total amount locked in the release schedule.
        schedules (list[CCD_Release]): A list of releases, ordered by increasing timestamp.
    """

    schedules: list[CCD_Release]
    total: int


class CCD_BakerInfo(BaseModel):
    """Information about a validator.

    GRPC documentation: [concordium.v2.BakerInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakerInfo)

    Attributes:
        baker_id (CCD_BakerId): Identity of the validator. This is actually the account index of the account controlling the validator.
        election_key (CCD_BakerElectionVerifyKey): Validator's public key used to check whether they won the lottery or not.
        signature_key (CCD_BakerSignatureVerifyKey): Validator's public key used to check that they are indeed the ones who produced the block.
        aggregation_key (CCD_BakerAggregationVerifyKey): Validator's public key used to check signatures on finalization records. This is only used if the validator has sufficient stake to participate in finalization.
    """

    aggregation_key: str
    election_key: str
    baker_id: int
    signature_key: str


class CCD_AccountStakingInfo_Baker(BaseModel):
    """Information about an account that is registered as a validator.

    GRPC documentation: [concordium.v2.AccountStakingInfo.Validator](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountStakingInfo.Validator)

    Attributes:
        staked_amount (microCCD): Amount staked at present.
        restake_earnings (bool): Whether rewards paid to the validator are automatically restaked.
        baker_info (CCD_BakerInfo): Information about the validator that is staking.
        pending_change (Optional[CCD_StakePendingChange]): Any pending change to the delegated stake.
        pool_info (Optional[CCD_BakerPoolInfo]): Present if the account is currently a validator, i.e., it is in the baking committee of the current epoch.
        is_suspended (bool): Whether the account is currently suspended. Has meaning from protocol version 8 onwards. In protocol version 8 it signals whether an account has been suspended and is not participating in the consensus algorithm. For protocol version < 8 the flag will always be false.
    """

    baker_info: CCD_BakerInfo
    pool_info: CCD_BakerPoolInfo
    pending_change: CCD_StakePendingChange
    restake_earnings: bool
    staked_amount: microCCD
    is_suspended: Optional[bool] = None


# class CCD_DelegationTarget(BaseModel):
#     baker_id: int = None
#     passive: str = None


class CCD_AccountStakingInfo_Delegator(BaseModel):
    """Information about an account that has delegated its stake to a pool.

    GRPC documentation: [concordium.v2.AccountStakingInfo.Delegator](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountStakingInfo.Delegator)

    Attributes:
        staked_amount (microCCD): The amount that the account delegates.
        restake_earnings (bool): Whether the earnings are automatically added to the staked amount.
        target (CCD_DelegationTarget): The entity to which the account delegates.
        pending_change (Optional[CCD_StakePendingChange]): If present, any pending change to the delegated stake.
    """

    target: CCD_DelegationTarget
    pending_change: CCD_StakePendingChange
    restake_earnings: bool
    staked_amount: microCCD


class CCD_AccountStakingInfo(BaseModel):
    """Information about the account stake, if the account is either a validator or a delegator.

    GRPC documentation: [concordium.v2.AccountStakingInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountStakingInfo)

    Attributes:
        validator (Optional[CCD_AccountStakingInfo_Baker]): The account is a validator.
        delegator (Optional[CCD_AccountStakingInfo_Delegator]): The account is a delegator.
    """

    baker: Optional[CCD_AccountStakingInfo_Baker] = None
    delegator: Optional[CCD_AccountStakingInfo_Delegator] = None


class CCD_AccountVerifyKey(BaseModel):
    """A public key used to verify transaction signatures from an account.

    GRPC documentation: [concordium.v2.AccountVerifyKey](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountVerifyKey)

    Attributes:
        ed25519_key (Optional[str]): An Ed25519 public key.
    """

    ed25519_key: Optional[str] = None


# class CCD_KeysEntry(BaseModel):
#     key: int
#     value: CCD_AccountVerifyKey


class CCD_CredentialPublicKeys(BaseModel):
    """Public keys of a single credential.

    GRPC documentation: [concordium.v2.CredentialPublicKeys](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.CredentialPublicKeys)

    Attributes:
        keys (dict[int, CCD_AccountVerifyKey]): Map of key indexes to account verification keys.
        threshold (CCD_SignatureThreshold): The number of signatures required to sign.
    """

    keys: dict[str, CCD_AccountVerifyKey]
    threshold: CCD_SignatureThreshold


class CCD_WinningBaker(BaseModel):
    """Details of which validator won the lottery in a given round in consensus version 1.

    GRPC documentation: [concordium.v2.WinningBaker](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.WinningBaker)

    Attributes:
        round (int): The round number.
        winnter (int): The validator that won the round.
        present (bool): True if the validator produced a block in this round on the finalized chain, and False otherwise.
    """

    round: CCD_Round
    winner: CCD_BakerId
    present: bool


class CCD_YearMonth(BaseModel):
    """Representation of the pair of a year and month.

    GRPC documentation: [concordium.v2.YearMonth](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.YearMonth)

    Attributes:
        year (int): The year value.
        month (int): The month value.
    """

    year: int
    month: int


class CCD_Policy(BaseModel):
    """Policy on a credential.

    GRPC documentation: [concordium.v2.Policy](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.Policy)

    Attributes:
        created_at (CCD_YearMonth): The year and month when the identity object from which the credential is derived was created.
        valid_to (CCD_YearMonth): The last year and month when the credential is still valid. After this expires an account can no longer be created from the credential.
        attributes (dict[str, CCD_Policy_Attributes]): Mapping from attribute tags to attribute values. Attribute tags are always representable in a single u8, attribute values are never more than 31 bytes in length.
    """

    created_at: CCD_YearMonth
    valid_to: CCD_YearMonth
    attributes: dict[str, CCD_Policy_Attributes]


class CCD_InitialCredentialValues(BaseModel):
    """Values contained in an initial credential.

    GRPC documentation: [concordium.v2.InitialCredentialValues](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.InitialCredentialValues)

    Attributes:
        keys (CCD_CredentialPublicKeys): Public keys of the credential.
        cred_id (CCD_CredentialRegistrationId): Its registration ID.
        ip_id (CCD_IdentityProviderIdentity): The identity provider who signed the identity object from which this credential is derived.
        policy (CCD_Policy): Policy of this credential.
    """

    credential_public_keys: CCD_CredentialPublicKeys
    cred_id: CCD_CredentialRegistrationId
    ip_id: CCD_IdentityProviderIdentity
    policy: CCD_Policy


class CCD_ChainArData(BaseModel):
    """Data relating to a single anonymity revoker sent by the account holder to the chain.

    GRPC documentation: [concordium.v2.ChainArData](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ChainArData)

    Attributes:
        enc_id_cred_pub_share (str): Share of the encryption of IdCredPub.
    """

    enc_id_cred_pub_share: str


# class CCD_ArDataEntry(BaseModel):
#     key: int
#     value: CCD_ChainArData


class CCD_CredentialCommitments_AttributesEntry(BaseModel):
    """A map entry in CredentialCommitments.attributes, containing commitments to attributes which have not been revealed in the policy.

    GRPC documentation: [concordium.v2.CredentialCommitments](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.CredentialCommitments)

    Attributes:
        key (int): The attribute tag.
        value (CCD_Commitment): The commitment to the attribute.
    """

    key: int
    value: CCD_Commitment


class CCD_CredentialCommitments(BaseModel):
    """Commitments that are part of a normal credential.

    GRPC documentation: [concordium.v2.CredentialCommitments](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.CredentialCommitments)

    Attributes:
        prf (CCD_Commitment): Commitment to the PRF key.
        cred_counter (CCD_Commitment): Commitment to the counter used to generate the credential registration id.
        max_accounts (CCD_Commitment): Commitment to the `max_accounts` value, which determines the maximum number of credentials that may be created from the identity object.
        attributes (dict[int, CCD_Commitment]): Commitments to the attributes which have not been revealed in the policy.
        id_cred_sec_sharing_coeff (list[CCD_Commitment]): List of commitments to the coefficients of the sharing polynomial. This polynomial is used in a shared encryption of `id_cred_pub` among the anonymity revokers.
    """

    prf: CCD_Commitment
    cred_counter: CCD_Commitment
    max_accounts: CCD_Commitment
    attributes: dict[str, CCD_Commitment]
    id_cred_sec_sharing_coeff: list[CCD_Commitment]


class CCD_NormalCredentialValues(BaseModel):
    """Values contained in a normal (non-initial) credential.

    GRPC documentation: [concordium.v2.NormalCredentialValues](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.NormalCredentialValues)

    Attributes:
        keys (CCD_CredentialPublicKeys): Public keys of the credential.
        cred_id (CCD_CredentialRegistrationId): Its registration ID.
        ip_id (CCD_IdentityProviderIdentity): The identity provider who signed the identity object from which this credential is derived.
        policy (CCD_Policy): Policy of this credential.
        ar_threshold (ArThreshold): The number of anonymity revokers that must work together to revoke the anonymity of the credential holder.
        ar_data (dict[int, CCD_ChainArData]): Mapping from anonymity revoker identities to revocation data for the given anonymity revoker.
        commitments (CCD_CredentialCommitments): Commitments to attributes which have not been revealed.
    """

    credential_public_keys: CCD_CredentialPublicKeys
    cred_id: CCD_CredentialRegistrationId
    ip_id: CCD_IdentityProviderIdentity
    policy: CCD_Policy
    ar_threshold: CCD_ArThreshold
    ar_data: dict[str, CCD_ChainArData]
    commitments: CCD_CredentialCommitments


class CCD_AccountCredential(BaseModel):
    """Credential that is part of an account.

    GRPC documentation: [concordium.v2.AccountCredential](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountCredential)

    Attributes:
        initial (Optional[CCD_InitialCredentialValues]): Values for an initial credential that creates an account.
        normal (Optional[CCD_NormalCredentialValues]): Values for a normal (non-initial) credential.
    """

    initial: Optional[CCD_InitialCredentialValues] = None
    normal: Optional[CCD_NormalCredentialValues] = None


class CCD_EncryptedBalance(BaseModel):
    """The encrypted balance of a CCD account.

    GRPC documentation: [concordium.v2.EncryptedBalance](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.EncryptedBalance)

    Attributes:
        self_amount (CCD_EncryptedAmount): Encrypted amount that is a result of this account's actions. In particular this list includes the aggregate of remaining amounts from transfers to public balance, transfers to other accounts, and encrypted amounts transferred from public balance.
        start_index (int): Starting index for incoming encrypted amounts. If an aggregated amount is present then this index is associated with such an amount and the list of incoming encrypted amounts starts at index `start_index + 1`.
        aggregated_amount (Optional[CCD_EncryptedAmount]): If present, the amount that has resulted from aggregating other amounts.
        num_aggregated (Optional[int]): The number of aggregated amounts (must be at least 2 if present). Present if and only if aggregated_amount is present.
        incoming_amounts (list[CCD_EncryptedAmount]): Amounts starting at start_index (or at start_index + 1 if there is an aggregated amount present). They are assumed to be numbered sequentially. The length of this list is bounded by the maximum number of incoming amounts on the accounts, which is currently 32.
    """

    self_amount: CCD_EncryptedAmount
    start_index: int
    aggregated_amount: Optional[CCD_EncryptedAmount] = None
    num_aggregated: Optional[int] = None
    incoming_amounts: list[CCD_EncryptedAmount]


class CCD_Cooldown(BaseModel):
    """The stake on the account that is in cooldown. When stake is removed from a validator or delegator (from protocol version 7) it first enters the pre-pre-cooldown state. The next time the stake snapshot is taken (at the epoch transition before a payday) it enters the pre-cooldown state. At the subsequent payday, it enters the cooldown state. At the payday after the end of the cooldown period, the stake is finally released.

    GRPC documentation: [concordium.v2.Cooldown](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.Cooldown)

    Attributes:
        end_time (CCD_TimeStamp): The time in milliseconds since the Unix epoch when the cooldown period ends.
        amount (microCCD): The amount that is in cooldown and set to be released at the end of the cooldown period.
        status (CoolDownStatus): The status of the cooldown (0=COOLDOWN, 1=PRE_COOLDOWN, 2=PRE_PRE_COOLDOWN).
    """

    end_time: CCD_TimeStamp
    amount: microCCD
    status: CoolDownStatus


class CCD_AccountInfo(BaseModel):
    """Information about the account at a particular point in time.

    GRPC documentation: [concordium.v2.AccountInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountInfo)

    Attributes:
        sequence_number (CCD_SequenceNumber): Next sequence number to be used for transactions signed from this account.
        amount (microCCD): Current (unencrypted) balance of the account.
        schedule (CCD_ReleaseSchedule): Release schedule for any locked up amount. This could be an empty release schedule.
        creds (dict[int, CCD_AccountCredential]): Map of all currently active credentials on the account. This includes public keys that can sign for the given credentials, as well as any revealed attributes. This map always contains a credential with index 0.
        threshold (CCD_AccountThreshold): Lower bound on how many credentials must sign any given transaction from this account.
        encrypted_balance (CCD_EncryptedBalance): The encrypted balance of the account.
        encryption_key (CCD_EncryptionKey): The public key for sending encrypted balances to the account.
        index (CCD_AccountIndex): Internal index of the account. Accounts on the chain get sequential indices. These should generally not be used outside of the chain, the account address is meant to be used to refer to accounts.
        stake (Optional[CCD_AccountStakingInfo]): Present if the account is a validator or delegator. In that case it is the information about the validator or delegator.
        address (CCD_AccountAddress): Canonical address of the account. This is derived from the first credential that created the account.
        cooldowns (list[CCD_Cooldown]): The stake on the account that is in cooldown.
        available_balance (microCCD): The available (unencrypted) balance of the account that can be transferred or used to pay for transactions.
    """

    address: str
    amount: microCCD
    stake: Optional[CCD_AccountStakingInfo] = None
    credentials: dict[str, CCD_AccountCredential]
    encrypted_balance: CCD_EncryptedBalance
    encryption_key: str
    index: int
    schedule: Optional[CCD_ReleaseSchedule] = None
    threshold: int
    sequence_number: Optional[CCD_SequenceNumber] = None
    available_balance: Optional[microCCD] = None
    cooldowns: Optional[list[CCD_Cooldown]] = None
    tokens: Optional[list[CCD_Token]] = None


class CCD_TokenomicsInfo_V0(BaseModel):
    """Version 0 tokenomics information.

    GRPC documentation: [concordium.v2.TokenomicsInfo.V0](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.TokenomicsInfo.V0)

    Attributes:
        total_amount (microCCD): The total CCD in existence.
        total_encrypted_amount (microCCD): The total CCD in encrypted balances.
        baking_reward_account (microCCD): The amount in the baking reward account.
        finalization_reward_account (microCCD): The amount in the finalization reward account.
        gas_account (microCCD): The amount in the GAS account.
        protocol_version (int): The protocol version.
    """

    total_amount: microCCD
    total_encrypted_amount: microCCD
    baking_reward_account: microCCD
    finalization_reward_account: microCCD
    gas_account: microCCD
    protocol_version: int


class CCD_TokenomicsInfo_V1(BaseModel):
    """Version 1 tokenomics information.

    GRPC documentation: [concordium.v2.TokenomicsInfo.V1](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.TokenomicsInfo.V1)

    Attributes:
        total_amount (microCCD): The total CCD in existence.
        total_encrypted_amount (microCCD): The total CCD in encrypted balances.
        baking_reward_account (microCCD): The amount in the baking reward account.
        finalization_reward_account (microCCD): The amount in the finalization reward account.
        gas_account (microCCD): The amount in the GAS account.
        foundation_transaction_rewards (microCCD): The transaction reward fraction accruing to the foundation (to be paid at next payday).
        next_payday_time (CCD_TimeStamp): The time of the next payday.
        next_payday_mint_rate (CCD_MintRate): The rate at which CCD will be minted (as a proportion of the total supply) at the next payday.
        total_staked_capital (microCCD): The total capital put up as stake by bakers and delegators.
        protocol_version (ProtocolVersion): The protocol version.
    """

    total_amount: microCCD
    total_encrypted_amount: microCCD
    baking_reward_account: microCCD
    finalization_reward_account: microCCD
    gas_account: microCCD
    foundation_transaction_rewards: microCCD
    next_payday_time: CCD_TimeStamp
    next_payday_mint_rate: CCD_MintRate
    total_staked_capital: microCCD
    protocol_version: int


class CCD_TokenomicsInfo(BaseModel):
    """Contains information related to tokenomics at the end of a given block.

    GRPC documentation: [concordium.v2.TokenomicsInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.TokenomicsInfo)

    Attributes:
        v0 (Optional[CCD_TokenomicsInfo_V0]): Version 0 tokenomics information, used in earlier protocol versions.
        v1 (Optional[CCD_TokenomicsInfo_V1]): Version 1 tokenomics information, used in later protocol versions.
    """

    v0: Optional[CCD_TokenomicsInfo_V0] = None
    v1: Optional[CCD_TokenomicsInfo_V1] = None


class CCD_InstanceInfo_V0(BaseModel):
    """Version 0 smart contract instance information.

    GRPC documentation: [concordium.v2.InstanceInfo.V0](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.InstanceInfo.V0)

    Attributes:
        model (CCD_ContractStateV0): The state of the instance.
        owner (CCD_AccountAddress): The account address which deployed the instance.
        amount (microCCD): The amount of CCD tokens in the balance of the instance.
        methods (list[CCD_ReceiveName]): A list of endpoints exposed by the instance.
        name (CCD_InitName): The name of the smart contract of the instance.
        source_module (CCD_ModuleRef): The module reference for the smart contract module of the instance.
    """

    model: CCD_ContractStateV0
    owner: CCD_AccountAddress
    amount: microCCD
    methods: list[CCD_ReceiveName]
    name: CCD_InitName
    source_module: CCD_ModuleRef


class CCD_InstanceInfo_V1(BaseModel):
    """Version 1 smart contract instance information.

    GRPC documentation: [concordium.v2.InstanceInfo.V1](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.InstanceInfo.V1)

    Attributes:
        owner (CCD_AccountAddress): The account address which deployed the instance.
        amount (microCCD): The amount of CCD tokens in the balance of the instance.
        methods (list[CCD_ReceiveName]): A list of endpoints exposed by the instance.
        name (CCD_InitName): The name of the smart contract of the instance.
        source_module (CCD_ModuleRef): The module reference for the smart contract module of the instance.
    """

    owner: CCD_AccountAddress
    amount: microCCD
    methods: list[CCD_ReceiveName]
    name: CCD_InitName
    source_module: CCD_ModuleRef


class CCD_InstanceInfo(BaseModel):
    """Information about a smart contract instance.

    GRPC documentation: [concordium.v2.InstanceInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.InstanceInfo)

    Attributes:
        v0 (Optional[CCD_InstanceInfo_V0]): Version 0 smart contract instance information.
        v1 (Optional[CCD_InstanceInfo_V1]): Version 1 smart contract instance information.
    """

    v0: CCD_InstanceInfo_V0
    v1: CCD_InstanceInfo_V1


class CCD_BlocksAtHeightResponse(BaseModel):
    """Response for GetBlocksAtHeight containing the live blocks at a given height.

    GRPC documentation: [concordium.v2.BlocksAtHeightResponse](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlocksAtHeightResponse)

    Attributes:
        blocks (list[CCD_BlockHash]): Live blocks at the given height.
    """

    blocks: list[CCD_BlockHash]


class CCD_BlockSpecialEvent_PaydayPoolReward(BaseModel):
    """Payment distributed to a pool or passive delegators.

    GRPC documentation: [concordium.v2.BlockSpecialEvent.PaydayPoolReward](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockSpecialEvent.PaydayPoolReward)

    Attributes:
        pool_owner (Optional[CCD_BakerId]): The pool owner (None for passive delegators).
        transaction_fees (microCCD): Accrued transaction fees for pool.
        baker_reward (microCCD): Accrued block production rewards for pool.
        finalization_reward (microCCD): Accrued finalization rewards for pool.
    """

    pool_owner: Optional[CCD_BakerId] = None
    transaction_fees: microCCD
    baker_reward: microCCD
    finalization_reward: microCCD


class CCD_BlockSpecialEvent_BlockAccrueReward(BaseModel):
    """Amounts accrued to accounts for each baked block.

    GRPC documentation: [concordium.v2.BlockSpecialEvent.BlockAccrueReward](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockSpecialEvent.BlockAccrueReward)

    Attributes:
        transaction_fees (microCCD): The total fees paid for transactions in the block.
        old_gas_account (microCCD): The old balance of the GAS account.
        new_gas_account (microCCD): The new balance of the GAS account.
        baker_reward (microCCD): The amount awarded to the validator.
        passive_reward (microCCD): The amount awarded to the passive delegators.
        foundation_charge (microCCD): The amount awarded to the foundation.
        validator (CCD_BakerId): The validator of the block, who will receive the award.
    """

    transaction_fees: microCCD
    old_gas_account: microCCD
    new_gas_account: microCCD
    baker_reward: microCCD
    passive_reward: microCCD
    foundation_charge: microCCD
    baker: CCD_BakerId


class CCD_BlockSpecialEvent_PaydayAccountReward(BaseModel):
    """Reward payment to the given account.

    GRPC documentation: [concordium.v2.BlockSpecialEvent.PaydayAccountReward](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockSpecialEvent.PaydayAccountReward)

    Attributes:
        account (CCD_AccountAddress): The account that got rewarded.
        transaction_fees (microCCD): The transaction fee reward at payday to the account.
        baker_reward (microCCD): The baking reward at payday to the account.
        finalization_reward (microCCD): The finalization reward at payday to the account.
    """

    account: CCD_AccountAddress
    transaction_fees: microCCD
    baker_reward: microCCD
    finalization_reward: microCCD


class CCD_BlockSpecialEvent_PaydayFoundationReward(BaseModel):
    """Foundation tax.

    GRPC documentation: [concordium.v2.BlockSpecialEvent.PaydayFoundationReward](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockSpecialEvent.PaydayFoundationReward)

    Attributes:
        foundation_account (CCD_AccountAddress): The account that got rewarded.
        development_charge (microCCD): The transaction fee reward at payday to the account.
    """

    foundation_account: CCD_AccountAddress
    development_charge: microCCD


class CCD_BlockSpecialEvent_BlockReward(BaseModel):
    """Disbursement of fees from a block between the GAS account, the validator, and the foundation. It should always be that:
    transaction_fees + old_gas_account = new_gas_account + baker_reward + foundation_charge

    GRPC documentation: [concordium.v2.BlockSpecialEvent.BlockReward](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockSpecialEvent.BlockReward)

    Attributes:
        transaction_fees (microCCD): The total fees paid for transactions in the block.
        old_gas_account (microCCD): The old balance of the GAS account.
        new_gas_account (microCCD): The new balance of the GAS account.
        baker_reward (microCCD): The amount awarded to the validator.
        foundation_charge (microCCD): The amount awarded to the foundation.
        validator (CCD_AccountAddress): The validator of the block, who receives the award.
        foundation_account (CCD_AccountAddress): The foundation account.
    """

    transaction_fees: microCCD
    old_gas_account: microCCD
    new_gas_account: microCCD
    baker_reward: microCCD
    foundation_charge: microCCD
    foundation_account: CCD_AccountAddress
    baker: CCD_AccountAddress


class CCD_BlockSpecialEvent_AccountAmounts_Entry(BaseModel):
    """Entry for mapping from an account address to an amount.

    GRPC documentation: [concordium.v2.BlockSpecialEvent.AccountAmounts.Entry](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockSpecialEvent.AccountAmounts.Entry)

    Attributes:
        account (CCD_AccountAddress): The account address.
        amount (microCCD): The amount associated with the account.
    """

    account: CCD_AccountAddress
    amount: microCCD


class CCD_BlockSpecialEvent_AccountAmounts(BaseModel):
    """A representation of a mapping from an account address to an amount.

    GRPC documentation: [concordium.v2.BlockSpecialEvent.AccountAmounts](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockSpecialEvent.AccountAmounts)

    Attributes:
        entries (list[CCD_BlockSpecialEvent_AccountAmounts_Entry]): List of entries mapping from account addresses to amounts.
    """

    entries: list[CCD_BlockSpecialEvent_AccountAmounts_Entry]


class CCD_BlockSpecialEvent_FinalizationRewards(BaseModel):
    """Payment to each finalizer on inclusion of a finalization record in a block.

    GRPC documentation: [concordium.v2.BlockSpecialEvent.FinalizationRewards](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockSpecialEvent.FinalizationRewards)

    Attributes:
        finalization_rewards (CCD_BlockSpecialEvent_AccountAmounts): The amount awarded to each finalizer.
        remainder (microCCD): The remaining balance of the finalization reward account.
    """

    finalization_rewards: CCD_BlockSpecialEvent_AccountAmounts
    remainder: microCCD


class CCD_BlockSpecialEvent_BakingRewards(BaseModel):
    """Payment to each validator of a previous epoch, in proportion to the number of blocks they contributed.

    GRPC documentation: [concordium.v2.BlockSpecialEvent.BakingRewards](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockSpecialEvent.BakingRewards)

    Attributes:
        baker_rewards (CCD_BlockSpecialEvent_AccountAmounts): The amount awarded to each validator.
        remainder (microCCD): The remaining balance of the validator reward account.
    """

    baker_rewards: CCD_BlockSpecialEvent_AccountAmounts
    remainder: microCCD


class CCD_BlockSpecialEvent_Mint(BaseModel):
    """Minting of new CCD.

    GRPC documentation: [concordium.v2.BlockSpecialEvent.Mint](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockSpecialEvent.Mint)

    Attributes:
        mint_baking_reward (microCCD): The amount allocated to the banking reward account.
        mint_finalization_reward (microCCD): The amount allocated to the finalization reward account.
        mint_platform_development_charge (microCCD): The amount allocated as the platform development charge.
        foundation_account (CCD_AccountAddress): The account to which the platform development charge is paid.
    """

    mint_baking_reward: microCCD
    mint_finalization_reward: microCCD
    mint_platform_development_charge: microCCD
    foundation_account: CCD_AccountAddress


class CCD_BlockSpecialEvent_ValidatorSuspended(BaseModel):
    """The id of a validator that got suspended due to too many missed rounds.

    GRPC documentation: [concordium.v2.BlockSpecialEvent.ValidatorSuspended](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockSpecialEvent.ValidatorSuspended)

    Attributes:
        bakerId (CCD_BakerId): The id of the suspended validator.
        account (CCD_AccountAddress): The account of the suspended validator.
    """

    bakerId: Optional[CCD_BakerId] = None
    baker_id: Optional[CCD_BakerId] = None
    account: CCD_AccountAddress


class CCD_BlockSpecialEvent_ValidatorPrimedForSuspension(BaseModel):
    """The id of a validator that is primed for suspension at the next snapshot epoch due to too many missed rounds.

    GRPC documentation: [concordium.v2.BlockSpecialEvent.ValidatorPrimedForSuspension](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockSpecialEvent.ValidatorPrimedForSuspension)

    Attributes:
        bakerId (CCD_BakerId): The id of the primed validator.
        account (CCD_AccountAddress): The account of the primed validator.
    """

    bakerId: Optional[CCD_BakerId] = None
    baker_id: Optional[CCD_BakerId] = None
    account: CCD_AccountAddress


class CCD_BlockSpecialEvent(BaseModel):
    """A protocol generated event that is not directly caused by a transaction. This includes minting new CCD, rewarding different bakers and delegators, etc.

    GRPC documentation: [concordium.v2.BlockSpecialEvent](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockSpecialEvent)

    Attributes:
        baking_rewards (Optional[CCD_BlockSpecialEvent_BakingRewards]): Payment to each validator of a previous epoch, in proportion to blocks contributed.
        mint (Optional[CCD_BlockSpecialEvent_Mint]): Minting of new CCD.
        finalization_rewards (Optional[CCD_BlockSpecialEvent_FinalizationRewards]): Payment to each finalizer on inclusion of a finalization record.
        block_reward (Optional[CCD_BlockSpecialEvent_BlockReward]): Disbursement of fees from a block between GAS account, validator, and foundation.
        payday_foundation_reward (Optional[CCD_BlockSpecialEvent_PaydayFoundationReward]): Foundation tax.
        payday_account_reward (Optional[CCD_BlockSpecialEvent_PaydayAccountReward]): Reward payment to a given account.
        block_accrue_reward (Optional[CCD_BlockSpecialEvent_BlockAccrueReward]): Amounts accrued to accounts for each baked block.
        payday_pool_reward (Optional[CCD_BlockSpecialEvent_PaydayPoolReward]): Payment distributed to a pool or passive delegators.
        validator_suspended (Optional[CCD_BlockSpecialEvent_ValidatorSuspended]): A validator that got suspended due to too many missed rounds.
        validator_primed_for_suspension (Optional[CCD_BlockSpecialEvent_ValidatorPrimedForSuspension]): A validator primed for suspension at next snapshot epoch.
    """

    baking_rewards: Optional[CCD_BlockSpecialEvent_BakingRewards] = None
    mint: Optional[CCD_BlockSpecialEvent_Mint] = None
    finalization_rewards: Optional[CCD_BlockSpecialEvent_FinalizationRewards] = None
    block_reward: Optional[CCD_BlockSpecialEvent_BlockReward] = None
    payday_foundation_reward: Optional[CCD_BlockSpecialEvent_PaydayFoundationReward] = None
    payday_account_reward: Optional[CCD_BlockSpecialEvent_PaydayAccountReward] = None
    block_accrue_reward: Optional[CCD_BlockSpecialEvent_BlockAccrueReward] = None
    payday_pool_reward: Optional[CCD_BlockSpecialEvent_PaydayPoolReward] = None
    validator_suspended: Optional[CCD_BlockSpecialEvent_ValidatorSuspended] = None
    validator_primed_for_suspension: Optional[
        CCD_BlockSpecialEvent_ValidatorPrimedForSuspension
    ] = None


class CCD_PendingUpdate(BaseModel):
    """A pending update.

    GRPC documentation: [concordium.v2.PendingUpdate](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.PendingUpdate)

    Attributes:
        effective_time (TransactionTime): The effective time of the update.
        root_keys (Optional[CCD_HigherLevelKeys]): Updates to the root keys.
        level1_keys (Optional[CCD_HigherLevelKeys]): Updates to the level 1 keys.
        level2_keys_cpv_0 (Optional[CCD_AuthorizationsV0]): Updates to the level 2 keys.
        level2_keys_cpv_1 (Optional[CCD_AuthorizationsV1]): Updates to the level 2 keys.
        protocol (Optional[CCD_ProtocolUpdate]): Protocol updates.
        election_difficulty (Optional[CCD_ElectionDifficulty]): Updates to the election difficulty parameter.
        euro_per_energy (Optional[CCD_ExchangeRate]): Updates to the euro:energy exchange rate.
        micro_ccd_per_euro (Optional[CCD_ExchangeRate]): Updates to the CCD:EUR exchange rate.
        foundation_account (Optional[CCD_AccountAddress]): Updates to the foundation account.
        mint_distribution_cpv_0 (Optional[CCD_MintDistributionCpv0]): Updates to the mint distribution (prior to protocol version 4).
        mint_distribution_cpv_1 (Optional[CCD_MintDistributionCpv1]): Updates to the mint distribution (protocol version 4+).
        transaction_fee_distribution (Optional[CCD_TransactionFeeDistribution]): Updates to the transaction fee distribution.
        gas_rewards (Optional[CCD_GasRewards]): Updates to the GAS rewards.
        pool_parameters_cpv_0 (Optional[CCD_BakerStakeThreshold]): Updates validator stake threshold (prior to protocol version 4).
        pool_parameters_cpv_1 (Optional[CCD_PoolParametersCpv1]): Updates pool parameters (protocol version 4+).
        add_anonymity_revoker (Optional[CCD_ArInfo]): Adds a new anonymity revoker.
        add_identity_provider (Optional[CCD_IpInfo]): Adds a new identity provider.
        cooldown_parameters (Optional[CCD_CooldownParametersCpv1]): Updates to cooldown parameters for chain parameters.
        time_parameters (Optional[CCD_TimeParametersCpv1]): Updates to time parameters.
        gas_rewards_cpv_2 (Optional[CCD_GasRewardsCpv2]): Updates to the GAS rewards (protocol version 6+).
        timeout_parameters (Optional[CCD_TimeoutParameters]): Updates to the consensus timeouts.
        min_block_time (Optional[CCD_Duration]): Updates to the minimum time between blocks.
        block_energy_limit (Optional[CCD_Energy]): Updates to the block energy limit.
        finalization_committee_parameters (Optional[CCD_FinalizationCommitteeParameters]): Updates to the finalization committee.
        validator_score_parameters (Optional[CCD_ValidatorScoreParameters]): Updates to the validator score parameters.
    """

    effective_time: CCD_TransactionTime
    root_keys: Optional[CCD_HigherLevelKeys] = None
    level1_keys: Optional[CCD_HigherLevelKeys] = None
    level2_keys_cpc_0: Optional[CCD_AuthorizationsV0] = None
    level2_keys_cpc_1: Optional[CCD_AuthorizationsV1] = None
    protocol: Optional[CCD_ProtocolUpdate] = None
    election_difficulty: Optional[CCD_ElectionDifficulty] = None
    euro_per_energy: Optional[CCD_ExchangeRate] = None
    micro_ccd_per_euro: Optional[CCD_ExchangeRate] = None
    foundation_account: Optional[CCD_AccountAddress] = None
    mint_distribution_cpv_0: Optional[CCD_MintDistributionCpv0] = None
    mint_distribution_cpv_1: Optional[CCD_MintDistributionCpv1] = None
    transaction_fee_distribution: Optional[CCD_TransactionFeeDistribution] = None
    gas_rewards: CCD_GasRewards
    pool_parameters_cpv_0: Optional[CCD_BakerStakeThreshold] = None
    pool_parameters_cpv_1: Optional[CCD_PoolParametersCpv1] = None
    add_anonymity_revoker: Optional[CCD_ArInfo] = None
    add_identity_provider: Optional[CCD_IpInfo] = None
    cooldown_parameters: Optional[CCD_CooldownParametersCpv1] = None
    pool_parameters_cpv_1_update: Optional[CCD_PoolParametersCpv1] = None
    time_parameters: Optional[CCD_TimeParametersCpv1] = None
    validator_score_parameters: Optional[CCD_ValidatorScoreParameters] = None


class CCD_ElectionInfo_Baker(BaseModel):
    """Information about an individual validator's lottery power.

    GRPC documentation: [concordium.v2.ElectionInfo.Validator](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ElectionInfo.Validator)

    Attributes:
        validator (CCD_BakerId): The ID of the validator.
        account (CCD_AccountAddress): The account address of the validator.
        lottery_power (float): The lottery power of the validator, rounded to the nearest representable "double".
    """

    baker: CCD_BakerId
    account: CCD_AccountAddress
    lottery_power: float


class CCD_ElectionInfo(BaseModel):
    """Contains information related to validator election for a particular block.

    GRPC documentation: [concordium.v2.ElectionInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ElectionInfo)

    Attributes:
        election_difficulty (Optional[CCD_ElectionDifficulty]): Baking lottery election difficulty. Present only in protocol versions 1-5.
        election_nonce (CCD_LeadershipElectionNonce): Current leadership election nonce for the lottery.
        baker_election_info (list[CCD_ElectionInfo_Baker]): List of the currently eligible bakers.
    """

    election_difficulty: Optional[CCD_ElectionDifficulty] = None
    election_nonce: CCD_LeadershipElectionNonce
    baker_election_info: list[CCD_ElectionInfo_Baker]


class CCD_ConsensusInfo(BaseModel):
    """Contains the consensus information in a node.

    GRPC documentation: [concordium.v2.ConsensusInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ConsensusInfo)

    Attributes:
        best_block (CCD_BlockHash): Hash of the best/last block in the tree.
        genesis_block (CCD_BlockHash): Hash of the genesis block.
        genesis_time (CCD_TimeStamp): Time of the genesis block.
        slot_duration (Optional[CCD_Duration]): Duration of a slot (present in protocol versions < 6).
        epoch_duration (CCD_Duration): Duration of an epoch.
        last_finalized_block (CCD_BlockHash): Hash of the last finalized block.
        best_block_height (int): Block height of the best block.
        last_finalized_block_height (int): Block height of the last finalized block.
        blocks_received_count (int): Number of blocks received.
        block_last_received_time (Optional[CCD_TimeStamp]): Time the last block was received.
        block_receive_latency_ema (float): Exponential moving average of block receive latency.
        block_receive_latency_emsd (float): Exponential moving standard deviation of block receive latency.
        block_receive_period_ema (Optional[float]): Exponential moving average of block receive period.
        block_receive_period_emsd (Optional[float]): Exponential moving standard deviation of block receive period.
        blocks_verified_count (int): Number of blocks verified.
        block_last_arrived_time (Optional[CCD_TimeStamp]): Time the last block arrived.
        block_arrive_latency_ema (float): Exponential moving average of block arrive latency.
        block_arrive_latency_emsd (float): Exponential moving standard deviation of block arrive latency.
        block_arrive_period_ema (Optional[float]): Exponential moving average of block arrive period.
        block_arrive_period_emsd (Optional[float]): Exponential moving standard deviation of block arrive period.
        transactions_per_block_ema (float): Exponential moving average of transactions per block.
        transactions_per_block_emsd (float): Exponential moving standard deviation of transactions per block.
        finalization_count (int): Number of finalized blocks.
        last_finalized_time (Optional[CCD_TimeStamp]): Time of last finalization.
        finalization_period_ema (Optional[float]): Exponential moving average of finalization period.
        finalization_period_emsd (Optional[float]): Exponential moving standard deviation of finalization period.
        protocol_version (str): Protocol version.
        genesis_index (int): Index of the genesis block.
        current_era_genesis_block (CCD_BlockHash): Hash of the genesis block of the current era.
        current_era_genesis_time (CCD_TimeStamp): Time of the genesis block of the current era.
        current_timeout_duration (Optional[CCD_Duration]): Current timeout duration in consensus.
        current_round (Optional[CCD_Round]): Current round in consensus.
        current_epoch (Optional[int]): Current epoch in consensus.
        trigger_block_time (Optional[CCD_TimeStamp]): Time when the trigger block was received.
    """

    best_block: CCD_BlockHash
    genesis_block: CCD_BlockHash
    genesis_time: CCD_TimeStamp
    slot_duration: Optional[CCD_Duration] = None
    epoch_duration: CCD_Duration
    last_finalized_block: CCD_BlockHash
    best_block_height: int
    last_finalized_block_height: int
    blocks_received_count: int
    block_last_received_time: Optional[CCD_TimeStamp] = None
    block_receive_latency_ema: float
    block_receive_latency_emsd: float
    block_receive_period_ema: Optional[float] = None
    block_receive_period_emsd: Optional[float] = None
    blocks_verified_count: int
    block_last_arrived_time: Optional[CCD_TimeStamp] = None
    block_arrive_latency_ema: float
    block_arrive_latency_emsd: float
    block_arrive_period_ema: Optional[float] = None
    block_arrive_period_emsd: Optional[float] = None
    transactions_per_block_ema: float
    transactions_per_block_emsd: float
    finalization_count: int
    last_finalized_time: Optional[CCD_TimeStamp] = None
    finalization_period_ema: Optional[float] = None
    finalization_period_emsd: Optional[float] = None
    protocol_version: str
    genesis_index: int
    current_era_genesis_block: CCD_BlockHash
    current_era_genesis_time: CCD_TimeStamp
    current_timeout_duration: Optional[CCD_Duration] = None
    current_round: Optional[CCD_Round] = None
    current_epoch: Optional[int] = None
    trigger_block_time: Optional[CCD_TimeStamp] = None


class CCD_VersionedModuleSource(BaseModel):
    """Different versions of smart contract module sources.

    GRPC documentation: [concordium.v2.VersionedModuleSource](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.VersionedModuleSource)

    Attributes:
        v0 (Optional[CCD_VersionedModuleSource_ModuleSourceV0]): Version 0 module source format.
        v1 (Optional[CCD_VersionedModuleSource_ModuleSourceV1]): Version 1 module source format, which includes source code.
    """

    v0: Optional[CCD_VersionedModuleSource_ModuleSourceV0] = None
    v1: Optional[CCD_VersionedModuleSource_ModuleSourceV1] = None


class CCD_NextUpdateSequenceNumbers(BaseModel):
    """The sequence numbers that will be used for the next updates of each kind.

    GRPC documentation: [concordium.v2.NextUpdateSequenceNumbers](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.NextUpdateSequenceNumbers)

    Attributes:
        root_keys (Optional[CCD_SequenceNumber]): Next sequence number for root keys updates.
        level1_keys (Optional[CCD_SequenceNumber]): Next sequence number for level 1 keys updates.
        level2_keys (Optional[CCD_SequenceNumber]): Next sequence number for level 2 keys updates.
        protocol (Optional[CCD_SequenceNumber]): Next sequence number for protocol updates.
        election_difficulty (Optional[CCD_SequenceNumber]): Next sequence number for election difficulty updates.
        euro_per_energy (Optional[CCD_SequenceNumber]): Next sequence number for euro per energy rate updates.
        micro_ccd_per_euro (Optional[CCD_SequenceNumber]): Next sequence number for microCCD per euro rate updates.
        foundation_account (Optional[CCD_SequenceNumber]): Next sequence number for foundation account updates.
        mint_distribution (Optional[CCD_SequenceNumber]): Next sequence number for mint distribution updates.
        transaction_fee_distribution (Optional[CCD_SequenceNumber]): Next sequence number for transaction fee distribution updates.
        gas_rewards (Optional[CCD_SequenceNumber]): Next sequence number for gas rewards updates.
        pool_parameters (Optional[CCD_SequenceNumber]): Next sequence number for pool parameters updates.
        add_anonymity_revoker (Optional[CCD_SequenceNumber]): Next sequence number for adding anonymity revokers.
        add_identity_provider (Optional[CCD_SequenceNumber]): Next sequence number for adding identity providers.
        cooldown_parameters (Optional[CCD_SequenceNumber]): Next sequence number for cooldown parameters updates.
        time_parameters (Optional[CCD_SequenceNumber]): Next sequence number for time parameters updates.
        timeout_parameters (Optional[CCD_SequenceNumber]): Next sequence number for timeout parameters updates.
        min_block_time (Optional[CCD_SequenceNumber]): Next sequence number for minimum block time updates.
        block_energy_limit (Optional[CCD_SequenceNumber]): Next sequence number for block energy limit updates.
        finalization_committee_parameters (Optional[CCD_SequenceNumber]): Next sequence number for finalization committee parameter updates.
        validator_score_parameters (Optional[CCD_SequenceNumber]): Next sequence number for validator score parameter updates.
    """

    root_keys: Optional[CCD_SequenceNumber] = None
    level1_keys: Optional[CCD_SequenceNumber] = None
    level2_keys: Optional[CCD_SequenceNumber] = None
    protocol: Optional[CCD_SequenceNumber] = None
    election_difficulty: Optional[CCD_SequenceNumber] = None
    euro_per_energy: Optional[CCD_SequenceNumber] = None
    micro_ccd_per_euro: Optional[CCD_SequenceNumber] = None
    foundation_account: Optional[CCD_SequenceNumber] = None
    mint_distribution: Optional[CCD_SequenceNumber] = None
    transaction_fee_distribution: Optional[CCD_SequenceNumber] = None
    gas_rewards: Optional[CCD_SequenceNumber] = None
    pool_parameters: Optional[CCD_SequenceNumber] = None
    add_anonymity_revoker: Optional[CCD_SequenceNumber] = None
    add_identity_provider: Optional[CCD_SequenceNumber] = None
    cooldown_parameters: Optional[CCD_SequenceNumber] = None
    time_parameters: Optional[CCD_SequenceNumber] = None
    timeout_parameters: Optional[CCD_SequenceNumber] = None
    min_block_time: Optional[CCD_SequenceNumber] = None
    block_energy_limit: Optional[CCD_SequenceNumber] = None
    finalization_committee_parameters: Optional[CCD_SequenceNumber] = None
    validator_score_parameters: Optional[CCD_SequenceNumber] = None
    protocol_level_tokens: Optional[CCD_SequenceNumber] = None


class CCD_ChainParametersV0(BaseModel):
    """Chain parameters that apply in protocol versions 1-3.

    GRPC documentation: [concordium.v2.ChainParametersV0](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ChainParametersV0)

    Attributes:
        election_difficulty (CCD_ElectionDifficulty): The election difficulty used for the validator lottery.
        euro_per_energy (CCD_ExchangeRate): Exchange rate of euro per energy unit.
        micro_ccd_per_euro (CCD_ExchangeRate): Exchange rate of micro CCD per euro.
        baker_cooldown_epochs (CCD_Epoch): Number of epochs a validator must cooldown when removing their stake.
        account_creation_limit (CCD_CredentialsPerBlockLimit): Maximum number of credentials that can be created per block.
        mint_distribution (CCD_MintDistributionCpv0): Parameters for the distribution of newly minted CCD.
        transaction_fee_distribution (CCD_TransactionFeeDistribution): Parameters for the distribution of transaction fees.
        gas_rewards (CCD_GasRewards): Parameters for the distribution of execution costs (gas).
        foundation_account (CCD_AccountAddress): The foundation account address.
        minimum_threshold_for_baking (microCCD): Minimum stake required to become a validator.
        root_keys (CCD_HigherLevelKeys): The root update keys.
        level1_keys (CCD_HigherLevelKeys): The level 1 update keys.
        level2_keys (CCD_AuthorizationsV0): The level 2 update keys and authorizations.
    """

    election_difficulty: CCD_ElectionDifficulty
    euro_per_energy: CCD_ExchangeRate
    micro_ccd_per_euro: CCD_ExchangeRate
    baker_cooldown_epochs: CCD_Epoch
    account_creation_limit: CCD_CredentialsPerBlockLimit
    mint_distribution: CCD_MintDistributionCpv0
    transaction_fee_distribution: CCD_TransactionFeeDistribution
    gas_rewards: CCD_GasRewards
    foundation_account: CCD_AccountAddress
    minimum_threshold_for_baking: microCCD
    root_keys: CCD_HigherLevelKeys
    level1_keys: CCD_HigherLevelKeys
    level2_keys: CCD_AuthorizationsV0


class CCD_ChainParametersV1(BaseModel):
    """Chain parameters that apply from protocol version 4 onwards.

    GRPC documentation: [concordium.v2.ChainParametersV1](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ChainParametersV1)

    Attributes:
        election_difficulty (CCD_ElectionDifficulty): The election difficulty used for the validator lottery.
        euro_per_energy (CCD_ExchangeRate): Exchange rate of euro per energy unit.
        micro_ccd_per_euro (CCD_ExchangeRate): Exchange rate of micro CCD per euro.
        cooldown_parameters (CCD_CooldownParametersCpv1): Parameters related to cooldown periods.
        time_parameters (CCD_TimeParametersCpv1): Parameters related to time settings.
        account_creation_limit (CCD_CredentialsPerBlockLimit): Maximum number of credentials that can be created per block.
        mint_distribution (CCD_MintDistributionCpv1): Parameters for the distribution of newly minted CCD.
        transaction_fee_distribution (CCD_TransactionFeeDistribution): Parameters for the distribution of transaction fees.
        gas_rewards (CCD_GasRewards): Parameters for the distribution of execution costs (gas).
        foundation_account (CCD_AccountAddress): The foundation account address.
        pool_parameters (CCD_PoolParametersCpv1): Parameters related to staking pools.
        root_keys (CCD_HigherLevelKeys): The root update keys.
        level1_keys (CCD_HigherLevelKeys): The level 1 update keys.
        level2_keys (CCD_AuthorizationsV1): The level 2 update keys and authorizations.
    """

    election_difficulty: CCD_ElectionDifficulty
    euro_per_energy: CCD_ExchangeRate
    micro_ccd_per_euro: CCD_ExchangeRate
    cooldown_parameters: CCD_CooldownParametersCpv1
    time_parameters: CCD_TimeParametersCpv1
    account_creation_limit: CCD_CredentialsPerBlockLimit
    mint_distribution: CCD_MintDistributionCpv1
    transaction_fee_distribution: CCD_TransactionFeeDistribution
    gas_rewards: CCD_GasRewards
    foundation_account: CCD_AccountAddress
    pool_parameters: CCD_PoolParametersCpv1
    root_keys: CCD_HigherLevelKeys
    level1_keys: CCD_HigherLevelKeys
    level2_keys: CCD_AuthorizationsV1


class CCD_TimeOutParameters(BaseModel):
    """Parameters controlling timeouts in the consensus protocol.

    GRPC documentation: [concordium.v2.TimeoutParameters](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.TimeoutParameters)

    Attributes:
        timeout_base (CCD_Duration): The base duration for timeouts.
        timeout_increase (CCD_Ratio): The multiplicative factor by which timeout increases each time a timeout triggers.
        timeout_decrease (CCD_Ratio): The multiplicative factor by which timeout decreases after a successful round.
    """

    timeout_base: CCD_Duration
    timeout_increase: CCD_Ratio
    timeout_decrease: CCD_Ratio


class CCD_ConsensusParametersV1(BaseModel):
    """Parameters controlling consensus from protocol version 6 onwards.

    GRPC documentation: [concordium.v2.ConsensusParametersV1](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ConsensusParametersV1)

    Attributes:
        timeout_parameters (CCD_TimeOutParameters): Parameters controlling consensus timeouts.
        min_block_time (CCD_Duration): Minimum time between blocks.
        block_energy_limit (CCD_Energy): Maximum amount of energy that can be used in a block.
    """

    timeout_parameters: CCD_TimeOutParameters
    min_block_time: CCD_Duration
    block_energy_limit: CCD_Energy


class CCD_GasRewardsV2(BaseModel):
    """Distribution of gas rewards for chain parameters version 6 and onwards.

    GRPC documentation: [concordium.v2.GasRewardsV2](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.GasRewardsV2)

    Attributes:
        validator (CCD_AmountFraction): The fraction paid to the validator.
        account_creation (CCD_AmountFraction): Fraction paid for including each account creation transaction in a block.
        chain_update (CCD_AmountFraction): Fraction paid for including an update transaction in a block.
    """

    baker: CCD_AmountFraction
    account_creation: CCD_AmountFraction
    chain_update: CCD_AmountFraction


class CCD_ChainParametersV2(BaseModel):
    """Chain parameters that apply from protocol version 6 onwards.

    GRPC documentation: [concordium.v2.ChainParametersV2](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ChainParametersV2)

    Attributes:
        consensus_parameters (CCD_ConsensusParametersV1): Parameters controlling consensus from protocol version 6 onwards.
        euro_per_energy (CCD_ExchangeRate): Exchange rate of euro per energy unit.
        micro_ccd_per_euro (CCD_ExchangeRate): Exchange rate of micro CCD per euro.
        cooldown_parameters (CCD_CooldownParametersCpv1): Parameters related to cooldown periods.
        time_parameters (CCD_TimeParametersCpv1): Parameters related to time settings.
        account_creation_limit (CCD_CredentialsPerBlockLimit): Maximum number of credentials that can be created per block.
        mint_distribution (CCD_MintDistributionCpv1): Parameters for the distribution of newly minted CCD.
        transaction_fee_distribution (CCD_TransactionFeeDistribution): Parameters for the distribution of transaction fees.
        gas_rewards (CCD_GasRewardsV2): Parameters for the distribution of execution costs (gas).
        foundation_account (CCD_AccountAddress): The foundation account address.
        pool_parameters (CCD_PoolParametersCpv1): Parameters related to staking pools.
        root_keys (CCD_HigherLevelKeys): The root update keys.
        level1_keys (CCD_HigherLevelKeys): The level 1 update keys.
        level2_keys (CCD_AuthorizationsV1): The level 2 update keys and authorizations.
        finalization_committee_parameters (CCD_FinalizationCommitteeParameters): Parameters for the finalization committee.
    """

    consensus_parameters: CCD_ConsensusParametersV1
    euro_per_energy: CCD_ExchangeRate
    micro_ccd_per_euro: CCD_ExchangeRate
    cooldown_parameters: CCD_CooldownParametersCpv1
    time_parameters: CCD_TimeParametersCpv1
    account_creation_limit: CCD_CredentialsPerBlockLimit
    mint_distribution: CCD_MintDistributionCpv1
    transaction_fee_distribution: CCD_TransactionFeeDistribution
    gas_rewards: CCD_GasRewardsV2
    foundation_account: CCD_AccountAddress
    pool_parameters: CCD_PoolParametersCpv1
    root_keys: CCD_HigherLevelKeys
    level1_keys: CCD_HigherLevelKeys
    level2_keys: CCD_AuthorizationsV1
    finalization_committee_parameters: CCD_FinalizationCommitteeParameters


class CCD_ChainParametersV3(BaseModel):
    """Chain parameters that apply from protocol version 8 onwards.

    GRPC documentation: [concordium.v2.ChainParametersV3](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ChainParametersV3)

    Attributes:
        consensus_parameters (CCD_ConsensusParametersV1): Parameters controlling consensus from protocol version 6 onwards.
        euro_per_energy (CCD_ExchangeRate): Exchange rate of euro per energy unit.
        micro_ccd_per_euro (CCD_ExchangeRate): Exchange rate of micro CCD per euro.
        cooldown_parameters (CCD_CooldownParametersCpv1): Parameters related to cooldown periods.
        time_parameters (CCD_TimeParametersCpv1): Parameters related to time settings.
        account_creation_limit (CCD_CredentialsPerBlockLimit): Maximum number of credentials that can be created per block.
        mint_distribution (CCD_MintDistributionCpv1): Parameters for the distribution of newly minted CCD.
        transaction_fee_distribution (CCD_TransactionFeeDistribution): Parameters for the distribution of transaction fees.
        gas_rewards (CCD_GasRewardsV2): Parameters for the distribution of execution costs (gas).
        foundation_account (CCD_AccountAddress): The foundation account address.
        pool_parameters (CCD_PoolParametersCpv1): Parameters related to staking pools.
        root_keys (CCD_HigherLevelKeys): The root update keys.
        level1_keys (CCD_HigherLevelKeys): The level 1 update keys.
        level2_keys (CCD_AuthorizationsV1): The level 2 update keys and authorizations.
        finalization_committee_parameters (CCD_FinalizationCommitteeParameters): Parameters for the finalization committee.
        validator_score_parameters (CCD_ValidatorScoreParameters): Parameters for validator scoring.
    """

    consensus_parameters: CCD_ConsensusParametersV1
    euro_per_energy: CCD_ExchangeRate
    micro_ccd_per_euro: CCD_ExchangeRate
    cooldown_parameters: CCD_CooldownParametersCpv1
    time_parameters: CCD_TimeParametersCpv1
    account_creation_limit: CCD_CredentialsPerBlockLimit
    mint_distribution: CCD_MintDistributionCpv1
    transaction_fee_distribution: CCD_TransactionFeeDistribution
    gas_rewards: CCD_GasRewardsV2
    foundation_account: CCD_AccountAddress
    pool_parameters: CCD_PoolParametersCpv1
    root_keys: CCD_HigherLevelKeys
    level1_keys: CCD_HigherLevelKeys
    level2_keys: CCD_AuthorizationsV1
    finalization_committee_parameters: CCD_FinalizationCommitteeParameters
    validator_score_parameters: CCD_ValidatorScoreParameters


class CCD_ChainParameters(BaseModel):
    """Contains the chain parameters for a particular version of the protocol.

    GRPC documentation: [concordium.v2.ChainParameters](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ChainParameters)

    Attributes:
        v0 (Optional[CCD_ChainParametersV0]): Chain parameters for protocol versions 1-3.
        v1 (Optional[CCD_ChainParametersV1]): Chain parameters for protocol version 4.
        v2 (Optional[CCD_ChainParametersV2]): Chain parameters for protocol versions 5-7.
        v3 (Optional[CCD_ChainParametersV3]): Chain parameters for protocol version 8 onwards.
    """

    v0: Optional[CCD_ChainParametersV0] = None
    v1: Optional[CCD_ChainParametersV1] = None
    v2: Optional[CCD_ChainParametersV2] = None
    v3: Optional[CCD_ChainParametersV3] = None


class CCD_InvokeInstanceResponse_Success(BaseModel):
    """Success response when invoking a smart contract instance.

    GRPC documentation: [concordium.v2.InvokeInstanceResponse.Success](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.InvokeInstanceResponse.Success)

    Attributes:
        return_value (bytes): The return value from the contract execution.
        used_energy (CCD_Energy): The amount of energy used during execution.
        effects (list[CCD_ContractTraceElement]): List of effects produced by the execution.
    """

    return_value: bytes
    used_energy: CCD_Energy
    effects: list[CCD_ContractTraceElement]


class CCD_InvokeInstanceResponse_Failure(BaseModel):
    """Failure response when invoking a smart contract instance.

    GRPC documentation: [concordium.v2.InvokeInstanceResponse.Failure](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.InvokeInstanceResponse.Failure)

    Attributes:
        return_value (bytes): The return value from the failed contract execution.
        used_energy (CCD_Energy): The amount of energy used during execution.
        reason (CCD_RejectReason): The reason why the execution failed.
    """

    return_value: bytes
    used_energy: CCD_Energy
    reason: CCD_RejectReason


class CCD_InvokeInstanceResponse(BaseModel):
    """Response from invoking a smart contract instance. Contains either success or failure information.

    GRPC documentation: [concordium.v2.InvokeInstanceResponse](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.InvokeInstanceResponse)

    Attributes:
        success (Optional[CCD_InvokeInstanceResponse_Success]): Details of a successful invocation.
        failure (Optional[CCD_InvokeInstanceResponse_Failure]): Details of a failed invocation.
    """

    success: CCD_InvokeInstanceResponse_Success
    failure: CCD_InvokeInstanceResponse_Failure


class CCD_BlockComplete(BaseModel):
    """
    CCD_BlockComplete represents a complete block, including `block_info`, `transaction_summaries` and `special_events`, as well as `logged_events`.

    Attributes:
        block_info (CCD_BlockInfo): Information about the block.
        transaction_summaries (list[CCD_BlockItemSummary]): Summaries of the transactions in the block.
        special_events (list[CCD_BlockSpecialEvent]): Special events associated with the block.
        logged_events (Optional[list]): Logged events related to the block. This attribute is optional and may lead to circular imports if specific types are used.
        net (Optional[str]): Network identifier. This attribute is optional.
    """

    block_info: CCD_BlockInfo
    transaction_summaries: list[CCD_BlockItemSummary]
    special_events: list[CCD_BlockSpecialEvent]
    # This leads to circular import
    # logged_events: Optional[list[MongoTypeLoggedEvent]] = None
    logged_events: Optional[list] = None

    net: Optional[str] = None


class CCD_QuorumMessage(BaseModel):
    """A message in the consensus quorum carrying finalization information.

    GRPC documentation: [concordium.v2.QuorumMessage](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.QuorumMessage)

    Attributes:
        signature (CCD_QuorumSignature): The signature of the quorum message.
        block (CCD_BlockHash): The hash of the block associated with the quorum message.
        finalizer (CCD_FinalizerIndex): The index of the finalizer who created this message.
        round (CCD_Round): The round number of consensus.
        epoch (CCD_Epoch): The epoch number in consensus.
    """

    signature: CCD_QuorumSignature
    block: CCD_BlockHash
    finalizer: CCD_FinalizerIndex
    round: CCD_Round
    epoch: CCD_Epoch


class CCD_RawQuorumCertificate(BaseModel):
    """A raw quorum certificate from the consensus protocol.

    GRPC documentation: [concordium.v2.RawQuorumCertificate](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RawQuorumCertificate)

    Attributes:
        block_hash (CCD_BlockHash): The block hash this certificate certifies.
        round (CCD_Round): The round in which the certificate was created.
        epoch (CCD_Epoch): The epoch in which the certificate was created.
        aggregate_signature (CCD_QuorumSignature): The aggregate signature of all signers.
        signatories (list[CCD_FinalizerIndex]): The indices of the finalizers who signed this certificate.
    """

    block_hash: CCD_BlockHash
    round: CCD_Round
    epoch: CCD_Epoch
    aggregate_signature: CCD_QuorumSignature
    signatories: list[CCD_FinalizerIndex]


class CCD_TimeoutMessage(BaseModel):
    """A timeout message in the consensus protocol.

    GRPC documentation: [concordium.v2.TimeoutMessage](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.TimeoutMessage)

    Attributes:
        finalizer ([CCD_FinalizerIndex]): The index of the finalizer who created this message.
        round (CCD_Round): The round number this timeout is for.
        epoch (CCD_Epoch): The epoch this timeout is for.
        quorum_certificate (CCD_RawQuorumCertificate): The highest quorum certificate known to the sender.
        signature (CCD_TimeoutSignature): The signature on this timeout message.
        message_signature (CCD_BlockSignature): The block signature on this timeout message.
    """

    finalizer: CCD_FinalizerIndex
    round: CCD_Round
    epoch: CCD_Epoch
    quorum_certificate: CCD_RawQuorumCertificate
    signature: CCD_TimeoutSignature
    message_signature: CCD_BlockSignature


class CCD_RawFinalizerRound(BaseModel):
    """A round of finalization in the consensus protocol.

    GRPC documentation: [concordium.v2.RawFinalizerRound](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RawFinalizerRound)

    Attributes:
        round (CCD_Round): The round number in the consensus protocol.
        finalizers (list[CCD_FinalizerIndex]): The list of finalizer indices who participated in this round.
    """

    round: CCD_Round
    finalizers: list[CCD_FinalizerIndex]


class CCD_RawTimeoutCertificate(BaseModel):
    """A raw timeout certificate in the consensus protocol.

    GRPC documentation: [concordium.v2.RawTimeoutCertificate](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RawTimeoutCertificate)

    Attributes:
        round (CCD_Round): The round associated with the timeout certificate.
        min_epoch (CCD_Epoch): The minimum epoch associated with the timeout certificate.
        qc_rounds_first_epoch (list[CCD_RawFinalizerRound]): List of finalizer rounds for the first epoch.
        qc_rounds_second_epoch (list[CCD_RawFinalizerRound]): List of finalizer rounds for the second epoch.
        aggregate_signature (CCD_TimeoutSignature): The aggregated signature by the finalization committee that witnessed the 'round' timed out.
    """

    round: CCD_Round
    min_epoch: CCD_Epoch
    qc_rounds_first_epoch: list[CCD_RawFinalizerRound]
    qc_rounds_second_epoch: list[CCD_RawFinalizerRound]
    aggregate_signature: CCD_TimeoutSignature


class CCD_PersistentRoundStatus(BaseModel):
    """Persistent status of a consensus round that is stored to disk.

    GRPC documentation: [concordium.v2.PersistentRoundStatus](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.PersistentRoundStatus)

    Attributes:
        last_signed_quorum_message (Optional[CCD_QuorumMessage]): The last signed quorum message.
        last_signed_timeout_message (Optional[CCD_TimeoutMessage]): The last signed timeout message.
        last_baked_round (CCD_Round): The last round in which this node baked a block. Note that in the GRPC implementation, if the value equals the default value, it's not sent. Hence an Optional with default value of False.
        latest_timeout (Optional[CCD_RawTimeoutCertificate]): The latest timeout certificate.
    """

    last_signed_quorum_message: Optional[CCD_QuorumMessage] = None
    last_signed_timeout_message: Optional[CCD_TimeoutMessage] = None
    last_baked_round: Optional[CCD_Round] = False
    latest_timeout: Optional[CCD_RawTimeoutCertificate] = None


class CCD_RoundTimeout(BaseModel):
    """A timeout event in the consensus protocol.

    GRPC documentation: [concordium.v2.RoundTimeout](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RoundTimeout)

    Attributes:
        timeout_certificate (CCD_RawTimeoutCertificate): The certificate indicating that a timeout has occurred.
        quorum_certificate (CCD_RawQuorumCertificate): The highest quorum certificate known when the timeout occurred.
    """

    timeout_certificate: CCD_RawTimeoutCertificate
    quorum_certificate: CCD_RawQuorumCertificate


class CCD_RawFinalizationEntry(BaseModel):
    """A raw finalization entry in the consensus protocol.

    GRPC documentation: [concordium.v2.RawFinalizationEntry](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RawFinalizationEntry)

    Attributes:
        finalized_qc (CCD_RawQuorumCertificate): The certificate that proves finalization of a block.
        successor_qc (CCD_RawQuorumCertificate): The certificate that proves there is a successor block.
        successor_proof (CCD_SuccessorProof): The proof that the successor block is valid.
    """

    finalized_qc: CCD_RawQuorumCertificate
    successor_qc: CCD_RawQuorumCertificate
    successor_proof: CCD_SuccessorProof


class CCD_RoundStatus(BaseModel):
    """Status of a round in the consensus protocol.

    GRPC documentation: [concordium.v2.RoundStatus](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RoundStatus)

    Attributes:
        current_round (CCD_Round): The current round in progress.
        highest_certified_block (CCD_RawQuorumCertificate): The highest certified block in the current round.
        previous_round_timeout (Optional[CCD_RoundTimeout]): The timeout of the previous round, if any.
        current_epoch (CCD_Epoch): The current epoch in progress.
        last_epoch_finalization_entry (Optional[CCD_RawFinalizationEntry]): The finalization entry of the last epoch, if any.
        current_timeout (CCD_Duration): The current timeout duration.
    """

    current_round: CCD_Round
    highest_certified_block: CCD_RawQuorumCertificate
    previous_round_timeout: Optional[CCD_RoundTimeout] = None
    round_eligible_to_bake: Optional[bool] = None
    current_epoch: CCD_Epoch
    last_epoch_finalization_entry: Optional[CCD_RawFinalizationEntry] = None
    current_timeout: CCD_Duration


class CCD_BlockTableSummary(BaseModel):
    """Summary information about the block table in the node.

    GRPC documentation: [concordium.v2.BlockTableSummary](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BlockTableSummary)

    Attributes:
        dead_block_cache_size (int): The size of the dead block cache. Note that inn the GRPC implementation, if the value equals the default value, it's not sent. Hence an Optional with default value of 0.
        live_blocks (list[CCD_BlockHash]): A list of live block hashes.
    """

    dead_block_cache_size: Optional[int] = 0
    live_blocks: list[CCD_BlockHash]


class CCD_BranchBlocks(BaseModel):
    """A list of blocks at a specific branch height.

    GRPC documentation: [concordium.v2.BranchBlocks](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BranchBlocks)

    Attributes:
        blocks_at_branch_height (list[CCD_BlockHash]): A list of block hashes at the branch height.
    """

    blocks_at_branch_height: list[CCD_BlockHash]


class CCD_RoundExistingBlock(BaseModel):
    """An existing block in the consensus round.

    GRPC documentation: [concordium.v2.RoundExistingBlock](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RoundExistingBlock)

    Attributes:
        round (CCD_Round): The round number in which the block exists.
        validator (CCD_BakerId): The validator who created this block.
        block (CCD_BlockHash): The hash of the block.
    """

    round: CCD_Round
    baker: CCD_BakerId
    block: CCD_BlockHash


class CCD_RoundExistingQC(BaseModel):
    """A quorum certificate that exists for a round in consensus.

    GRPC documentation: [concordium.v2.RoundExistingQC](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.RoundExistingQC)

    Attributes:
        round (CCD_Round): The round number for which the QC exists.
        epoch (CCD_Epoch): The epoch in which the QC was created.
    """

    round: CCD_Round
    epoch: CCD_Epoch


class CCD_FullBakerInfo(BaseModel):
    """Full information about a validator, including identity and keys.

    GRPC documentation: [concordium.v2.FullBakerInfo](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.FullBakerInfo)

    Attributes:
        baker_identity (Optional[CCD_BakerId]): The identity of the validator.
        election_verify_key (CCD_BakerElectionVerifyKey): The election verification key of the validator.
        signature_verify_key (CCD_BakerSignatureVerifyKey): The signature verification key of the validator.
        aggregation_verify_key (CCD_BakerAggregationVerifyKey): The aggregation verification key of the validator.
        stake (microCCD): The stake of the validator in microCCD.
    """

    baker_identity: CCD_BakerId
    election_verify_key: CCD_BakerElectionVerifyKey
    signature_verify_key: CCD_BakerSignatureVerifyKey
    aggregation_verify_key: CCD_BakerAggregationVerifyKey
    stake: microCCD


class CCD_BakersAndFinalizers(BaseModel):
    """The set of bakers and finalizers in a particular consensus epoch.

    GRPC documentation: [concordium.v2.BakersAndFinalizers](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.BakersAndFinalizers)

    Attributes:
        bakers ([list[CCD_FullBakerInfo]): List of all bakers and their stakes and keys.
        finalizers ([list[CCD_BakerId]): List of validator IDs who are finalizers.
        baker_total_stake ([microCCD): The total stake of all bakers.
        finalizer_total_stake ([microCCD): The total stake of all finalizers.
        finalization_committee_hash ([CCD_FinalizationCommitteeHash): Hash of the finalization committee.
    """

    bakers: list[CCD_FullBakerInfo]
    finalizers: list[CCD_BakerId]
    baker_total_stake: microCCD
    finalizer_total_stake: microCCD
    finalization_committee_hash: CCD_FinalizationCommitteeHash


class CCD_EpochBakers(BaseModel):
    """The bakers and finalizers for different epochs.

    GRPC documentation: [concordium.v2.EpochBakers](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.EpochBakers)

    Attributes:
        previous_epoch_bakers (CCD_BakersAndFinalizers): Bakers and finalizers for the previous epoch.
        current_epoch_bakers (Optional[CCD_BakersAndFinalizers]): Bakers and finalizers for the current epoch.
        next_epoch_bakers (Optional[CCD_BakersAndFinalizers]): Bakers and finalizers for the next epoch.
        next_payday (CCD_Epoch): The next payday epoch.
    """

    previous_epoch_bakers: CCD_BakersAndFinalizers
    current_epoch_bakers: Optional[CCD_BakersAndFinalizers] = None
    next_epoch_bakers: Optional[CCD_BakersAndFinalizers] = None
    next_payday: CCD_Epoch


class CCD_TimeoutMessages(BaseModel):
    """Lists of timeout messages for different epochs.

    GRPC documentation: [concordium.v2.TimeoutMessages](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.TimeoutMessages)

    Attributes:
        first_epoch (CCD_Epoch): The first epoch for which we have timeout messages.
        first_epoch_timeouts (list[CCD_TimeoutMessage]): Timeout messages from the first epoch.
        second_epoch_timeouts (list[CCD_TimeoutMessage]): Timeout messages from the second epoch.
    """

    first_epoch: CCD_Epoch
    first_epoch_timeouts: list[CCD_TimeoutMessage]
    second_epoch_timeouts: list[CCD_TimeoutMessage]


class CCD_AggregatedSignatures(BaseModel):
    """Aggregated signatures from a set of finalizers.

    GRPC documentation: [concordium.v2.AggregatedSignatures](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AggregatedSignatures)

    Attributes:
        signed_block (CCD_BlockHash): The hash of the signed block.
        signature_weight (microCCD): The weight of the signature.
        aggregate_signature (CCD_QuorumSignature): The aggregate signature.
        signatories (list[CCD_FinalizerIndex]): The list of finalizer indices who signed the block.
    """

    signed_block: CCD_BlockHash
    signature_weight: microCCD
    aggregate_signature: CCD_QuorumSignature
    signatories: list[CCD_FinalizerIndex]


class CCD_QuorumMessages(BaseModel):
    """A collection of quorum messages and aggregated signatures in the consensus protocol.

    GRPC documentation: [concordium.v2.QuorumMessages](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.QuorumMessages)

    Attributes:
        quorum_messages (list[CCD_QuorumMessage]): A list of quorum messages from individual finalizers.
        aggregated_signatures (list[CCD_AggregatedSignatures]): A list of aggregated signatures from sets of finalizers.
    """

    quorum_messages: list[CCD_QuorumMessage]
    aggregated_signatures: list[CCD_AggregatedSignatures]


class CCD_ConsensusDetailedStatusQuery(BaseModel):
    """Query for detailed consensus status at a specific genesis index.

    GRPC documentation: [concordium.v2.ConsensusDetailedStatusQuery](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ConsensusDetailedStatusQuery)

    Attributes:
        genesis_index (Optional[int]): The genesis index for which to get the consensus status. If not provided, uses the current era.
    """

    genesis_index: Optional[int] = None


class CCD_ConsensusDetailedStatus(BaseModel):
    """Detailed status of consensus at a particular point in time.

    GRPC documentation: [concordium.v2.ConsensusDetailedStatus](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.ConsensusDetailedStatus)

    Attributes:
        genesis_block (CCD_BlockHash): The hash of the genesis block.
        persistent_round_status (CCD_PersistentRoundStatus): The status of the persistent round.
        round_status (CCD_RoundStatus): The status of the current round.
        non_finalized_transaction_count (int): The count of non-finalized transactions. Note that in the GRPC implementation, if the value equals the default value, it's not sent. Hence an Optional with default value of 0.
        transaction_table_purge_counter (int): The counter for purging the transaction table. Note that in the GRPC implementation, if the value equals the default value, it's not sent. Hence an Optional with default value of 0.
        block_table (CCD_BlockTableSummary): Summary of the block table.
        branches (list[CCD_BranchBlocks]): List of branch blocks.
        round_existing_blocks (list[CCD_RoundExistingBlock]): List of existing blocks in the current round.
        round_existing_qcs (list[CCD_RoundExistingQC]): List of existing QCs in the current round.
        genesis_block_height (int): The height of the genesis block.
        last_finalized_block (CCD_BlockHash): The hash of the last finalized block.
        last_finalized_block_height (int): The height of the last finalized block.
        latest_finalization_entry (Optional[CCD_RawFinalizationEntry]): The latest finalization entry.
        epoch_bakers (CCD_EpochBakers): The epoch bakers.
        timeout_messages (Optional[CCD_TimeoutMessages]): The timeout messages.
        terminal_block (Optional[CCD_BlockHash]): The hash of the terminal block.
    """

    genesis_block: CCD_BlockHash
    persistent_round_status: CCD_PersistentRoundStatus
    round_status: CCD_RoundStatus
    non_finalized_transaction_count: Optional[int] = 0
    transaction_table_purge_counter: Optional[int] = 0
    block_table: CCD_BlockTableSummary
    branches: list[CCD_BranchBlocks]
    round_existing_blocks: list[CCD_RoundExistingBlock]
    round_existing_qcs: list[CCD_RoundExistingQC]
    genesis_block_height: int
    last_finalized_block: CCD_BlockHash
    last_finalized_block_height: int
    latest_finalization_entry: Optional[CCD_RawFinalizationEntry] = None
    epoch_bakers: CCD_EpochBakers
    timeout_messages: Optional[CCD_TimeoutMessages] = None
    terminal_block: Optional[CCD_BlockHash] = None


class CCD_AccountPending(BaseModel):
    """Information about a pending account.

    GRPC documentation: [concordium.v2.AccountPending](https://docs.concordium.com/concordium-grpc-api/#concordium.v2.AccountPending)

    Attributes:
        account_index (CCD_AccountIndex): The index of the pending account.
        first_timestamp (CCD_TimeStamp): The timestamp of when the account was first seen.
    """

    account_index: CCD_AccountIndex
    first_timestamp: CCD_TimeStamp
