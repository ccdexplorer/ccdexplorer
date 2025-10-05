from . import kernel_pb2 as _kernel_pb2
from . import protocol_level_tokens_pb2 as _protocol_level_tokens_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OpenStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OPEN_STATUS_OPEN_FOR_ALL: _ClassVar[OpenStatus]
    OPEN_STATUS_CLOSED_FOR_NEW: _ClassVar[OpenStatus]
    OPEN_STATUS_CLOSED_FOR_ALL: _ClassVar[OpenStatus]

class ContractVersion(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    V0: _ClassVar[ContractVersion]
    V1: _ClassVar[ContractVersion]

class CredentialType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CREDENTIAL_TYPE_INITIAL: _ClassVar[CredentialType]
    CREDENTIAL_TYPE_NORMAL: _ClassVar[CredentialType]

class UpdateType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    UPDATE_PROTOCOL: _ClassVar[UpdateType]
    UPDATE_ELECTION_DIFFICULTY: _ClassVar[UpdateType]
    UPDATE_EURO_PER_ENERGY: _ClassVar[UpdateType]
    UPDATE_MICRO_CCD_PER_EURO: _ClassVar[UpdateType]
    UPDATE_FOUNDATION_ACCOUNT: _ClassVar[UpdateType]
    UPDATE_MINT_DISTRIBUTION: _ClassVar[UpdateType]
    UPDATE_TRANSACTION_FEE_DISTRIBUTION: _ClassVar[UpdateType]
    UPDATE_GAS_REWARDS: _ClassVar[UpdateType]
    UPDATE_POOL_PARAMETERS: _ClassVar[UpdateType]
    ADD_ANONYMITY_REVOKER: _ClassVar[UpdateType]
    ADD_IDENTITY_PROVIDER: _ClassVar[UpdateType]
    UPDATE_ROOT_KEYS: _ClassVar[UpdateType]
    UPDATE_LEVEL1_KEYS: _ClassVar[UpdateType]
    UPDATE_LEVEL2_KEYS: _ClassVar[UpdateType]
    UPDATE_COOLDOWN_PARAMETERS: _ClassVar[UpdateType]
    UPDATE_TIME_PARAMETERS: _ClassVar[UpdateType]
    UPDATE_TIMEOUT_PARAMETERS: _ClassVar[UpdateType]
    UPDATE_MIN_BLOCK_TIME: _ClassVar[UpdateType]
    UPDATE_BLOCK_ENERGY_LIMIT: _ClassVar[UpdateType]
    UPDATE_FINALIZATION_COMMITTEE_PARAMETERS: _ClassVar[UpdateType]
    UPDATE_VALIDATOR_SCORE_PARAMETERS: _ClassVar[UpdateType]
    UPDATE_CREATE_PLT: _ClassVar[UpdateType]

class TransactionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DEPLOY_MODULE: _ClassVar[TransactionType]
    INIT_CONTRACT: _ClassVar[TransactionType]
    UPDATE: _ClassVar[TransactionType]
    TRANSFER: _ClassVar[TransactionType]
    ADD_BAKER: _ClassVar[TransactionType]
    REMOVE_BAKER: _ClassVar[TransactionType]
    UPDATE_BAKER_STAKE: _ClassVar[TransactionType]
    UPDATE_BAKER_RESTAKE_EARNINGS: _ClassVar[TransactionType]
    UPDATE_BAKER_KEYS: _ClassVar[TransactionType]
    UPDATE_CREDENTIAL_KEYS: _ClassVar[TransactionType]
    ENCRYPTED_AMOUNT_TRANSFER: _ClassVar[TransactionType]
    TRANSFER_TO_ENCRYPTED: _ClassVar[TransactionType]
    TRANSFER_TO_PUBLIC: _ClassVar[TransactionType]
    TRANSFER_WITH_SCHEDULE: _ClassVar[TransactionType]
    UPDATE_CREDENTIALS: _ClassVar[TransactionType]
    REGISTER_DATA: _ClassVar[TransactionType]
    TRANSFER_WITH_MEMO: _ClassVar[TransactionType]
    ENCRYPTED_AMOUNT_TRANSFER_WITH_MEMO: _ClassVar[TransactionType]
    TRANSFER_WITH_SCHEDULE_AND_MEMO: _ClassVar[TransactionType]
    CONFIGURE_BAKER: _ClassVar[TransactionType]
    CONFIGURE_DELEGATION: _ClassVar[TransactionType]
    TOKEN_UPDATE: _ClassVar[TransactionType]

class ProtocolVersion(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PROTOCOL_VERSION_1: _ClassVar[ProtocolVersion]
    PROTOCOL_VERSION_2: _ClassVar[ProtocolVersion]
    PROTOCOL_VERSION_3: _ClassVar[ProtocolVersion]
    PROTOCOL_VERSION_4: _ClassVar[ProtocolVersion]
    PROTOCOL_VERSION_5: _ClassVar[ProtocolVersion]
    PROTOCOL_VERSION_6: _ClassVar[ProtocolVersion]
    PROTOCOL_VERSION_7: _ClassVar[ProtocolVersion]
    PROTOCOL_VERSION_8: _ClassVar[ProtocolVersion]
    PROTOCOL_VERSION_9: _ClassVar[ProtocolVersion]
OPEN_STATUS_OPEN_FOR_ALL: OpenStatus
OPEN_STATUS_CLOSED_FOR_NEW: OpenStatus
OPEN_STATUS_CLOSED_FOR_ALL: OpenStatus
V0: ContractVersion
V1: ContractVersion
CREDENTIAL_TYPE_INITIAL: CredentialType
CREDENTIAL_TYPE_NORMAL: CredentialType
UPDATE_PROTOCOL: UpdateType
UPDATE_ELECTION_DIFFICULTY: UpdateType
UPDATE_EURO_PER_ENERGY: UpdateType
UPDATE_MICRO_CCD_PER_EURO: UpdateType
UPDATE_FOUNDATION_ACCOUNT: UpdateType
UPDATE_MINT_DISTRIBUTION: UpdateType
UPDATE_TRANSACTION_FEE_DISTRIBUTION: UpdateType
UPDATE_GAS_REWARDS: UpdateType
UPDATE_POOL_PARAMETERS: UpdateType
ADD_ANONYMITY_REVOKER: UpdateType
ADD_IDENTITY_PROVIDER: UpdateType
UPDATE_ROOT_KEYS: UpdateType
UPDATE_LEVEL1_KEYS: UpdateType
UPDATE_LEVEL2_KEYS: UpdateType
UPDATE_COOLDOWN_PARAMETERS: UpdateType
UPDATE_TIME_PARAMETERS: UpdateType
UPDATE_TIMEOUT_PARAMETERS: UpdateType
UPDATE_MIN_BLOCK_TIME: UpdateType
UPDATE_BLOCK_ENERGY_LIMIT: UpdateType
UPDATE_FINALIZATION_COMMITTEE_PARAMETERS: UpdateType
UPDATE_VALIDATOR_SCORE_PARAMETERS: UpdateType
UPDATE_CREATE_PLT: UpdateType
DEPLOY_MODULE: TransactionType
INIT_CONTRACT: TransactionType
UPDATE: TransactionType
TRANSFER: TransactionType
ADD_BAKER: TransactionType
REMOVE_BAKER: TransactionType
UPDATE_BAKER_STAKE: TransactionType
UPDATE_BAKER_RESTAKE_EARNINGS: TransactionType
UPDATE_BAKER_KEYS: TransactionType
UPDATE_CREDENTIAL_KEYS: TransactionType
ENCRYPTED_AMOUNT_TRANSFER: TransactionType
TRANSFER_TO_ENCRYPTED: TransactionType
TRANSFER_TO_PUBLIC: TransactionType
TRANSFER_WITH_SCHEDULE: TransactionType
UPDATE_CREDENTIALS: TransactionType
REGISTER_DATA: TransactionType
TRANSFER_WITH_MEMO: TransactionType
ENCRYPTED_AMOUNT_TRANSFER_WITH_MEMO: TransactionType
TRANSFER_WITH_SCHEDULE_AND_MEMO: TransactionType
CONFIGURE_BAKER: TransactionType
CONFIGURE_DELEGATION: TransactionType
TOKEN_UPDATE: TransactionType
PROTOCOL_VERSION_1: ProtocolVersion
PROTOCOL_VERSION_2: ProtocolVersion
PROTOCOL_VERSION_3: ProtocolVersion
PROTOCOL_VERSION_4: ProtocolVersion
PROTOCOL_VERSION_5: ProtocolVersion
PROTOCOL_VERSION_6: ProtocolVersion
PROTOCOL_VERSION_7: ProtocolVersion
PROTOCOL_VERSION_8: ProtocolVersion
PROTOCOL_VERSION_9: ProtocolVersion

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class BlockHash(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class Sha256Hash(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class TransactionHash(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class StateHash(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class AbsoluteBlockHeight(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class BlockHeight(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class BakerId(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class AccountIndex(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class ModuleRef(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class VersionedModuleSource(_message.Message):
    __slots__ = ("v0", "v1")
    class ModuleSourceV0(_message.Message):
        __slots__ = ("value",)
        VALUE_FIELD_NUMBER: _ClassVar[int]
        value: bytes
        def __init__(self, value: _Optional[bytes] = ...) -> None: ...
    class ModuleSourceV1(_message.Message):
        __slots__ = ("value",)
        VALUE_FIELD_NUMBER: _ClassVar[int]
        value: bytes
        def __init__(self, value: _Optional[bytes] = ...) -> None: ...
    V0_FIELD_NUMBER: _ClassVar[int]
    V1_FIELD_NUMBER: _ClassVar[int]
    v0: VersionedModuleSource.ModuleSourceV0
    v1: VersionedModuleSource.ModuleSourceV1
    def __init__(self, v0: _Optional[_Union[VersionedModuleSource.ModuleSourceV0, _Mapping]] = ..., v1: _Optional[_Union[VersionedModuleSource.ModuleSourceV1, _Mapping]] = ...) -> None: ...

class Timestamp(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class Release(_message.Message):
    __slots__ = ("timestamp", "amount", "transactions")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    TRANSACTIONS_FIELD_NUMBER: _ClassVar[int]
    timestamp: Timestamp
    amount: Amount
    transactions: _containers.RepeatedCompositeFieldContainer[TransactionHash]
    def __init__(self, timestamp: _Optional[_Union[Timestamp, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ..., transactions: _Optional[_Iterable[_Union[TransactionHash, _Mapping]]] = ...) -> None: ...

class NewRelease(_message.Message):
    __slots__ = ("timestamp", "amount")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    timestamp: Timestamp
    amount: Amount
    def __init__(self, timestamp: _Optional[_Union[Timestamp, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...

class ReleaseSchedule(_message.Message):
    __slots__ = ("total", "schedules")
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    SCHEDULES_FIELD_NUMBER: _ClassVar[int]
    total: Amount
    schedules: _containers.RepeatedCompositeFieldContainer[Release]
    def __init__(self, total: _Optional[_Union[Amount, _Mapping]] = ..., schedules: _Optional[_Iterable[_Union[Release, _Mapping]]] = ...) -> None: ...

class EncryptedAmount(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class EncryptedBalance(_message.Message):
    __slots__ = ("self_amount", "start_index", "aggregated_amount", "num_aggregated", "incoming_amounts")
    SELF_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    START_INDEX_FIELD_NUMBER: _ClassVar[int]
    AGGREGATED_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    NUM_AGGREGATED_FIELD_NUMBER: _ClassVar[int]
    INCOMING_AMOUNTS_FIELD_NUMBER: _ClassVar[int]
    self_amount: EncryptedAmount
    start_index: int
    aggregated_amount: EncryptedAmount
    num_aggregated: int
    incoming_amounts: _containers.RepeatedCompositeFieldContainer[EncryptedAmount]
    def __init__(self, self_amount: _Optional[_Union[EncryptedAmount, _Mapping]] = ..., start_index: _Optional[int] = ..., aggregated_amount: _Optional[_Union[EncryptedAmount, _Mapping]] = ..., num_aggregated: _Optional[int] = ..., incoming_amounts: _Optional[_Iterable[_Union[EncryptedAmount, _Mapping]]] = ...) -> None: ...

class DelegationTarget(_message.Message):
    __slots__ = ("passive", "baker")
    PASSIVE_FIELD_NUMBER: _ClassVar[int]
    BAKER_FIELD_NUMBER: _ClassVar[int]
    passive: Empty
    baker: BakerId
    def __init__(self, passive: _Optional[_Union[Empty, _Mapping]] = ..., baker: _Optional[_Union[BakerId, _Mapping]] = ...) -> None: ...

class BakerElectionVerifyKey(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class BakerSignatureVerifyKey(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class BakerAggregationVerifyKey(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class BakerInfo(_message.Message):
    __slots__ = ("baker_id", "election_key", "signature_key", "aggregation_key")
    BAKER_ID_FIELD_NUMBER: _ClassVar[int]
    ELECTION_KEY_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_KEY_FIELD_NUMBER: _ClassVar[int]
    AGGREGATION_KEY_FIELD_NUMBER: _ClassVar[int]
    baker_id: BakerId
    election_key: BakerElectionVerifyKey
    signature_key: BakerSignatureVerifyKey
    aggregation_key: BakerAggregationVerifyKey
    def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ..., election_key: _Optional[_Union[BakerElectionVerifyKey, _Mapping]] = ..., signature_key: _Optional[_Union[BakerSignatureVerifyKey, _Mapping]] = ..., aggregation_key: _Optional[_Union[BakerAggregationVerifyKey, _Mapping]] = ...) -> None: ...

class StakePendingChange(_message.Message):
    __slots__ = ("reduce", "remove")
    class Reduce(_message.Message):
        __slots__ = ("new_stake", "effective_time")
        NEW_STAKE_FIELD_NUMBER: _ClassVar[int]
        EFFECTIVE_TIME_FIELD_NUMBER: _ClassVar[int]
        new_stake: Amount
        effective_time: Timestamp
        def __init__(self, new_stake: _Optional[_Union[Amount, _Mapping]] = ..., effective_time: _Optional[_Union[Timestamp, _Mapping]] = ...) -> None: ...
    REDUCE_FIELD_NUMBER: _ClassVar[int]
    REMOVE_FIELD_NUMBER: _ClassVar[int]
    reduce: StakePendingChange.Reduce
    remove: Timestamp
    def __init__(self, reduce: _Optional[_Union[StakePendingChange.Reduce, _Mapping]] = ..., remove: _Optional[_Union[Timestamp, _Mapping]] = ...) -> None: ...

class AmountFraction(_message.Message):
    __slots__ = ("parts_per_hundred_thousand",)
    PARTS_PER_HUNDRED_THOUSAND_FIELD_NUMBER: _ClassVar[int]
    parts_per_hundred_thousand: int
    def __init__(self, parts_per_hundred_thousand: _Optional[int] = ...) -> None: ...

class CommissionRates(_message.Message):
    __slots__ = ("finalization", "baking", "transaction")
    FINALIZATION_FIELD_NUMBER: _ClassVar[int]
    BAKING_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_FIELD_NUMBER: _ClassVar[int]
    finalization: AmountFraction
    baking: AmountFraction
    transaction: AmountFraction
    def __init__(self, finalization: _Optional[_Union[AmountFraction, _Mapping]] = ..., baking: _Optional[_Union[AmountFraction, _Mapping]] = ..., transaction: _Optional[_Union[AmountFraction, _Mapping]] = ...) -> None: ...

class BakerPoolInfo(_message.Message):
    __slots__ = ("open_status", "url", "commission_rates")
    OPEN_STATUS_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    COMMISSION_RATES_FIELD_NUMBER: _ClassVar[int]
    open_status: OpenStatus
    url: str
    commission_rates: CommissionRates
    def __init__(self, open_status: _Optional[_Union[OpenStatus, str]] = ..., url: _Optional[str] = ..., commission_rates: _Optional[_Union[CommissionRates, _Mapping]] = ...) -> None: ...

class AccountStakingInfo(_message.Message):
    __slots__ = ("baker", "delegator")
    class Baker(_message.Message):
        __slots__ = ("staked_amount", "restake_earnings", "baker_info", "pending_change", "pool_info", "is_suspended")
        STAKED_AMOUNT_FIELD_NUMBER: _ClassVar[int]
        RESTAKE_EARNINGS_FIELD_NUMBER: _ClassVar[int]
        BAKER_INFO_FIELD_NUMBER: _ClassVar[int]
        PENDING_CHANGE_FIELD_NUMBER: _ClassVar[int]
        POOL_INFO_FIELD_NUMBER: _ClassVar[int]
        IS_SUSPENDED_FIELD_NUMBER: _ClassVar[int]
        staked_amount: Amount
        restake_earnings: bool
        baker_info: BakerInfo
        pending_change: StakePendingChange
        pool_info: BakerPoolInfo
        is_suspended: bool
        def __init__(self, staked_amount: _Optional[_Union[Amount, _Mapping]] = ..., restake_earnings: bool = ..., baker_info: _Optional[_Union[BakerInfo, _Mapping]] = ..., pending_change: _Optional[_Union[StakePendingChange, _Mapping]] = ..., pool_info: _Optional[_Union[BakerPoolInfo, _Mapping]] = ..., is_suspended: bool = ...) -> None: ...
    class Delegator(_message.Message):
        __slots__ = ("staked_amount", "restake_earnings", "target", "pending_change")
        STAKED_AMOUNT_FIELD_NUMBER: _ClassVar[int]
        RESTAKE_EARNINGS_FIELD_NUMBER: _ClassVar[int]
        TARGET_FIELD_NUMBER: _ClassVar[int]
        PENDING_CHANGE_FIELD_NUMBER: _ClassVar[int]
        staked_amount: Amount
        restake_earnings: bool
        target: DelegationTarget
        pending_change: StakePendingChange
        def __init__(self, staked_amount: _Optional[_Union[Amount, _Mapping]] = ..., restake_earnings: bool = ..., target: _Optional[_Union[DelegationTarget, _Mapping]] = ..., pending_change: _Optional[_Union[StakePendingChange, _Mapping]] = ...) -> None: ...
    BAKER_FIELD_NUMBER: _ClassVar[int]
    DELEGATOR_FIELD_NUMBER: _ClassVar[int]
    baker: AccountStakingInfo.Baker
    delegator: AccountStakingInfo.Delegator
    def __init__(self, baker: _Optional[_Union[AccountStakingInfo.Baker, _Mapping]] = ..., delegator: _Optional[_Union[AccountStakingInfo.Delegator, _Mapping]] = ...) -> None: ...

class SequenceNumber(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class UpdateSequenceNumber(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class Amount(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class CredentialIndex(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class SignatureThreshold(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class AccountThreshold(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class EncryptionKey(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class Address(_message.Message):
    __slots__ = ("account", "contract")
    ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    CONTRACT_FIELD_NUMBER: _ClassVar[int]
    account: _kernel_pb2.AccountAddress
    contract: ContractAddress
    def __init__(self, account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., contract: _Optional[_Union[ContractAddress, _Mapping]] = ...) -> None: ...

class AccountVerifyKey(_message.Message):
    __slots__ = ("ed25519_key",)
    ED25519_KEY_FIELD_NUMBER: _ClassVar[int]
    ed25519_key: bytes
    def __init__(self, ed25519_key: _Optional[bytes] = ...) -> None: ...

class CredentialPublicKeys(_message.Message):
    __slots__ = ("keys", "threshold")
    class KeysEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: AccountVerifyKey
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[AccountVerifyKey, _Mapping]] = ...) -> None: ...
    KEYS_FIELD_NUMBER: _ClassVar[int]
    THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.MessageMap[int, AccountVerifyKey]
    threshold: SignatureThreshold
    def __init__(self, keys: _Optional[_Mapping[int, AccountVerifyKey]] = ..., threshold: _Optional[_Union[SignatureThreshold, _Mapping]] = ...) -> None: ...

class CredentialRegistrationId(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class IdentityProviderIdentity(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class YearMonth(_message.Message):
    __slots__ = ("year", "month")
    YEAR_FIELD_NUMBER: _ClassVar[int]
    MONTH_FIELD_NUMBER: _ClassVar[int]
    year: int
    month: int
    def __init__(self, year: _Optional[int] = ..., month: _Optional[int] = ...) -> None: ...

class Policy(_message.Message):
    __slots__ = ("created_at", "valid_to", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: bytes
        def __init__(self, key: _Optional[int] = ..., value: _Optional[bytes] = ...) -> None: ...
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    VALID_TO_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    created_at: YearMonth
    valid_to: YearMonth
    attributes: _containers.ScalarMap[int, bytes]
    def __init__(self, created_at: _Optional[_Union[YearMonth, _Mapping]] = ..., valid_to: _Optional[_Union[YearMonth, _Mapping]] = ..., attributes: _Optional[_Mapping[int, bytes]] = ...) -> None: ...

class InitialCredentialValues(_message.Message):
    __slots__ = ("keys", "cred_id", "ip_id", "policy")
    KEYS_FIELD_NUMBER: _ClassVar[int]
    CRED_ID_FIELD_NUMBER: _ClassVar[int]
    IP_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_FIELD_NUMBER: _ClassVar[int]
    keys: CredentialPublicKeys
    cred_id: CredentialRegistrationId
    ip_id: IdentityProviderIdentity
    policy: Policy
    def __init__(self, keys: _Optional[_Union[CredentialPublicKeys, _Mapping]] = ..., cred_id: _Optional[_Union[CredentialRegistrationId, _Mapping]] = ..., ip_id: _Optional[_Union[IdentityProviderIdentity, _Mapping]] = ..., policy: _Optional[_Union[Policy, _Mapping]] = ...) -> None: ...

class ChainArData(_message.Message):
    __slots__ = ("enc_id_cred_pub_share",)
    ENC_ID_CRED_PUB_SHARE_FIELD_NUMBER: _ClassVar[int]
    enc_id_cred_pub_share: bytes
    def __init__(self, enc_id_cred_pub_share: _Optional[bytes] = ...) -> None: ...

class ArThreshold(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class Commitment(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class CredentialCommitments(_message.Message):
    __slots__ = ("prf", "cred_counter", "max_accounts", "attributes", "id_cred_sec_sharing_coeff")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: Commitment
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[Commitment, _Mapping]] = ...) -> None: ...
    PRF_FIELD_NUMBER: _ClassVar[int]
    CRED_COUNTER_FIELD_NUMBER: _ClassVar[int]
    MAX_ACCOUNTS_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    ID_CRED_SEC_SHARING_COEFF_FIELD_NUMBER: _ClassVar[int]
    prf: Commitment
    cred_counter: Commitment
    max_accounts: Commitment
    attributes: _containers.MessageMap[int, Commitment]
    id_cred_sec_sharing_coeff: _containers.RepeatedCompositeFieldContainer[Commitment]
    def __init__(self, prf: _Optional[_Union[Commitment, _Mapping]] = ..., cred_counter: _Optional[_Union[Commitment, _Mapping]] = ..., max_accounts: _Optional[_Union[Commitment, _Mapping]] = ..., attributes: _Optional[_Mapping[int, Commitment]] = ..., id_cred_sec_sharing_coeff: _Optional[_Iterable[_Union[Commitment, _Mapping]]] = ...) -> None: ...

class NormalCredentialValues(_message.Message):
    __slots__ = ("keys", "cred_id", "ip_id", "policy", "ar_threshold", "ar_data", "commitments")
    class ArDataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: ChainArData
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[ChainArData, _Mapping]] = ...) -> None: ...
    KEYS_FIELD_NUMBER: _ClassVar[int]
    CRED_ID_FIELD_NUMBER: _ClassVar[int]
    IP_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_FIELD_NUMBER: _ClassVar[int]
    AR_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    AR_DATA_FIELD_NUMBER: _ClassVar[int]
    COMMITMENTS_FIELD_NUMBER: _ClassVar[int]
    keys: CredentialPublicKeys
    cred_id: CredentialRegistrationId
    ip_id: IdentityProviderIdentity
    policy: Policy
    ar_threshold: ArThreshold
    ar_data: _containers.MessageMap[int, ChainArData]
    commitments: CredentialCommitments
    def __init__(self, keys: _Optional[_Union[CredentialPublicKeys, _Mapping]] = ..., cred_id: _Optional[_Union[CredentialRegistrationId, _Mapping]] = ..., ip_id: _Optional[_Union[IdentityProviderIdentity, _Mapping]] = ..., policy: _Optional[_Union[Policy, _Mapping]] = ..., ar_threshold: _Optional[_Union[ArThreshold, _Mapping]] = ..., ar_data: _Optional[_Mapping[int, ChainArData]] = ..., commitments: _Optional[_Union[CredentialCommitments, _Mapping]] = ...) -> None: ...

class AccountCredential(_message.Message):
    __slots__ = ("initial", "normal")
    INITIAL_FIELD_NUMBER: _ClassVar[int]
    NORMAL_FIELD_NUMBER: _ClassVar[int]
    initial: InitialCredentialValues
    normal: NormalCredentialValues
    def __init__(self, initial: _Optional[_Union[InitialCredentialValues, _Mapping]] = ..., normal: _Optional[_Union[NormalCredentialValues, _Mapping]] = ...) -> None: ...

class Cooldown(_message.Message):
    __slots__ = ("end_time", "amount", "status")
    class CooldownStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        COOLDOWN: _ClassVar[Cooldown.CooldownStatus]
        PRE_COOLDOWN: _ClassVar[Cooldown.CooldownStatus]
        PRE_PRE_COOLDOWN: _ClassVar[Cooldown.CooldownStatus]
    COOLDOWN: Cooldown.CooldownStatus
    PRE_COOLDOWN: Cooldown.CooldownStatus
    PRE_PRE_COOLDOWN: Cooldown.CooldownStatus
    END_TIME_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    end_time: Timestamp
    amount: Amount
    status: Cooldown.CooldownStatus
    def __init__(self, end_time: _Optional[_Union[Timestamp, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ..., status: _Optional[_Union[Cooldown.CooldownStatus, str]] = ...) -> None: ...

class TokenInfo(_message.Message):
    __slots__ = ("token_id", "token_state")
    TOKEN_ID_FIELD_NUMBER: _ClassVar[int]
    TOKEN_STATE_FIELD_NUMBER: _ClassVar[int]
    token_id: _protocol_level_tokens_pb2.TokenId
    token_state: _protocol_level_tokens_pb2.TokenState
    def __init__(self, token_id: _Optional[_Union[_protocol_level_tokens_pb2.TokenId, _Mapping]] = ..., token_state: _Optional[_Union[_protocol_level_tokens_pb2.TokenState, _Mapping]] = ...) -> None: ...

class AccountInfo(_message.Message):
    __slots__ = ("sequence_number", "amount", "schedule", "creds", "threshold", "encrypted_balance", "encryption_key", "index", "stake", "address", "cooldowns", "available_balance", "tokens")
    class Token(_message.Message):
        __slots__ = ("token_id", "token_account_state")
        TOKEN_ID_FIELD_NUMBER: _ClassVar[int]
        TOKEN_ACCOUNT_STATE_FIELD_NUMBER: _ClassVar[int]
        token_id: _protocol_level_tokens_pb2.TokenId
        token_account_state: _protocol_level_tokens_pb2.TokenAccountState
        def __init__(self, token_id: _Optional[_Union[_protocol_level_tokens_pb2.TokenId, _Mapping]] = ..., token_account_state: _Optional[_Union[_protocol_level_tokens_pb2.TokenAccountState, _Mapping]] = ...) -> None: ...
    class CredsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: AccountCredential
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[AccountCredential, _Mapping]] = ...) -> None: ...
    SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    CREDS_FIELD_NUMBER: _ClassVar[int]
    THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTED_BALANCE_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTION_KEY_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    STAKE_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    COOLDOWNS_FIELD_NUMBER: _ClassVar[int]
    AVAILABLE_BALANCE_FIELD_NUMBER: _ClassVar[int]
    TOKENS_FIELD_NUMBER: _ClassVar[int]
    sequence_number: SequenceNumber
    amount: Amount
    schedule: ReleaseSchedule
    creds: _containers.MessageMap[int, AccountCredential]
    threshold: AccountThreshold
    encrypted_balance: EncryptedBalance
    encryption_key: EncryptionKey
    index: AccountIndex
    stake: AccountStakingInfo
    address: _kernel_pb2.AccountAddress
    cooldowns: _containers.RepeatedCompositeFieldContainer[Cooldown]
    available_balance: Amount
    tokens: _containers.RepeatedCompositeFieldContainer[AccountInfo.Token]
    def __init__(self, sequence_number: _Optional[_Union[SequenceNumber, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ..., schedule: _Optional[_Union[ReleaseSchedule, _Mapping]] = ..., creds: _Optional[_Mapping[int, AccountCredential]] = ..., threshold: _Optional[_Union[AccountThreshold, _Mapping]] = ..., encrypted_balance: _Optional[_Union[EncryptedBalance, _Mapping]] = ..., encryption_key: _Optional[_Union[EncryptionKey, _Mapping]] = ..., index: _Optional[_Union[AccountIndex, _Mapping]] = ..., stake: _Optional[_Union[AccountStakingInfo, _Mapping]] = ..., address: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., cooldowns: _Optional[_Iterable[_Union[Cooldown, _Mapping]]] = ..., available_balance: _Optional[_Union[Amount, _Mapping]] = ..., tokens: _Optional[_Iterable[_Union[AccountInfo.Token, _Mapping]]] = ...) -> None: ...

class BlockHashInput(_message.Message):
    __slots__ = ("best", "last_final", "given", "absolute_height", "relative_height")
    class RelativeHeight(_message.Message):
        __slots__ = ("genesis_index", "height", "restrict")
        GENESIS_INDEX_FIELD_NUMBER: _ClassVar[int]
        HEIGHT_FIELD_NUMBER: _ClassVar[int]
        RESTRICT_FIELD_NUMBER: _ClassVar[int]
        genesis_index: GenesisIndex
        height: BlockHeight
        restrict: bool
        def __init__(self, genesis_index: _Optional[_Union[GenesisIndex, _Mapping]] = ..., height: _Optional[_Union[BlockHeight, _Mapping]] = ..., restrict: bool = ...) -> None: ...
    BEST_FIELD_NUMBER: _ClassVar[int]
    LAST_FINAL_FIELD_NUMBER: _ClassVar[int]
    GIVEN_FIELD_NUMBER: _ClassVar[int]
    ABSOLUTE_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    RELATIVE_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    best: Empty
    last_final: Empty
    given: BlockHash
    absolute_height: AbsoluteBlockHeight
    relative_height: BlockHashInput.RelativeHeight
    def __init__(self, best: _Optional[_Union[Empty, _Mapping]] = ..., last_final: _Optional[_Union[Empty, _Mapping]] = ..., given: _Optional[_Union[BlockHash, _Mapping]] = ..., absolute_height: _Optional[_Union[AbsoluteBlockHeight, _Mapping]] = ..., relative_height: _Optional[_Union[BlockHashInput.RelativeHeight, _Mapping]] = ...) -> None: ...

class EpochRequest(_message.Message):
    __slots__ = ("relative_epoch", "block_hash")
    class RelativeEpoch(_message.Message):
        __slots__ = ("genesis_index", "epoch")
        GENESIS_INDEX_FIELD_NUMBER: _ClassVar[int]
        EPOCH_FIELD_NUMBER: _ClassVar[int]
        genesis_index: GenesisIndex
        epoch: Epoch
        def __init__(self, genesis_index: _Optional[_Union[GenesisIndex, _Mapping]] = ..., epoch: _Optional[_Union[Epoch, _Mapping]] = ...) -> None: ...
    RELATIVE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    relative_epoch: EpochRequest.RelativeEpoch
    block_hash: BlockHashInput
    def __init__(self, relative_epoch: _Optional[_Union[EpochRequest.RelativeEpoch, _Mapping]] = ..., block_hash: _Optional[_Union[BlockHashInput, _Mapping]] = ...) -> None: ...

class AccountIdentifierInput(_message.Message):
    __slots__ = ("address", "cred_id", "account_index")
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    CRED_ID_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_INDEX_FIELD_NUMBER: _ClassVar[int]
    address: _kernel_pb2.AccountAddress
    cred_id: CredentialRegistrationId
    account_index: AccountIndex
    def __init__(self, address: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., cred_id: _Optional[_Union[CredentialRegistrationId, _Mapping]] = ..., account_index: _Optional[_Union[AccountIndex, _Mapping]] = ...) -> None: ...

class AccountInfoRequest(_message.Message):
    __slots__ = ("block_hash", "account_identifier")
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_IDENTIFIER_FIELD_NUMBER: _ClassVar[int]
    block_hash: BlockHashInput
    account_identifier: AccountIdentifierInput
    def __init__(self, block_hash: _Optional[_Union[BlockHashInput, _Mapping]] = ..., account_identifier: _Optional[_Union[AccountIdentifierInput, _Mapping]] = ...) -> None: ...

class TokenInfoRequest(_message.Message):
    __slots__ = ("block_hash", "token_id")
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    TOKEN_ID_FIELD_NUMBER: _ClassVar[int]
    block_hash: BlockHashInput
    token_id: _protocol_level_tokens_pb2.TokenId
    def __init__(self, block_hash: _Optional[_Union[BlockHashInput, _Mapping]] = ..., token_id: _Optional[_Union[_protocol_level_tokens_pb2.TokenId, _Mapping]] = ...) -> None: ...

class FinalizedBlockInfo(_message.Message):
    __slots__ = ("hash", "height")
    HASH_FIELD_NUMBER: _ClassVar[int]
    HEIGHT_FIELD_NUMBER: _ClassVar[int]
    hash: BlockHash
    height: AbsoluteBlockHeight
    def __init__(self, hash: _Optional[_Union[BlockHash, _Mapping]] = ..., height: _Optional[_Union[AbsoluteBlockHeight, _Mapping]] = ...) -> None: ...

class AncestorsRequest(_message.Message):
    __slots__ = ("block_hash", "amount")
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    block_hash: BlockHashInput
    amount: int
    def __init__(self, block_hash: _Optional[_Union[BlockHashInput, _Mapping]] = ..., amount: _Optional[int] = ...) -> None: ...

class ModuleSourceRequest(_message.Message):
    __slots__ = ("block_hash", "module_ref")
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    MODULE_REF_FIELD_NUMBER: _ClassVar[int]
    block_hash: BlockHashInput
    module_ref: ModuleRef
    def __init__(self, block_hash: _Optional[_Union[BlockHashInput, _Mapping]] = ..., module_ref: _Optional[_Union[ModuleRef, _Mapping]] = ...) -> None: ...

class ContractAddress(_message.Message):
    __slots__ = ("index", "subindex")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    SUBINDEX_FIELD_NUMBER: _ClassVar[int]
    index: int
    subindex: int
    def __init__(self, index: _Optional[int] = ..., subindex: _Optional[int] = ...) -> None: ...

class InstanceInfoRequest(_message.Message):
    __slots__ = ("block_hash", "address")
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    block_hash: BlockHashInput
    address: ContractAddress
    def __init__(self, block_hash: _Optional[_Union[BlockHashInput, _Mapping]] = ..., address: _Optional[_Union[ContractAddress, _Mapping]] = ...) -> None: ...

class InstanceInfo(_message.Message):
    __slots__ = ("v0", "v1")
    class V0(_message.Message):
        __slots__ = ("model", "owner", "amount", "methods", "name", "source_module")
        MODEL_FIELD_NUMBER: _ClassVar[int]
        OWNER_FIELD_NUMBER: _ClassVar[int]
        AMOUNT_FIELD_NUMBER: _ClassVar[int]
        METHODS_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        SOURCE_MODULE_FIELD_NUMBER: _ClassVar[int]
        model: ContractStateV0
        owner: _kernel_pb2.AccountAddress
        amount: Amount
        methods: _containers.RepeatedCompositeFieldContainer[ReceiveName]
        name: InitName
        source_module: ModuleRef
        def __init__(self, model: _Optional[_Union[ContractStateV0, _Mapping]] = ..., owner: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ..., methods: _Optional[_Iterable[_Union[ReceiveName, _Mapping]]] = ..., name: _Optional[_Union[InitName, _Mapping]] = ..., source_module: _Optional[_Union[ModuleRef, _Mapping]] = ...) -> None: ...
    class V1(_message.Message):
        __slots__ = ("owner", "amount", "methods", "name", "source_module")
        OWNER_FIELD_NUMBER: _ClassVar[int]
        AMOUNT_FIELD_NUMBER: _ClassVar[int]
        METHODS_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        SOURCE_MODULE_FIELD_NUMBER: _ClassVar[int]
        owner: _kernel_pb2.AccountAddress
        amount: Amount
        methods: _containers.RepeatedCompositeFieldContainer[ReceiveName]
        name: InitName
        source_module: ModuleRef
        def __init__(self, owner: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ..., methods: _Optional[_Iterable[_Union[ReceiveName, _Mapping]]] = ..., name: _Optional[_Union[InitName, _Mapping]] = ..., source_module: _Optional[_Union[ModuleRef, _Mapping]] = ...) -> None: ...
    V0_FIELD_NUMBER: _ClassVar[int]
    V1_FIELD_NUMBER: _ClassVar[int]
    v0: InstanceInfo.V0
    v1: InstanceInfo.V1
    def __init__(self, v0: _Optional[_Union[InstanceInfo.V0, _Mapping]] = ..., v1: _Optional[_Union[InstanceInfo.V1, _Mapping]] = ...) -> None: ...

class InstanceStateKVPair(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    value: bytes
    def __init__(self, key: _Optional[bytes] = ..., value: _Optional[bytes] = ...) -> None: ...

class InstanceStateLookupRequest(_message.Message):
    __slots__ = ("block_hash", "address", "key")
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    block_hash: BlockHashInput
    address: ContractAddress
    key: bytes
    def __init__(self, block_hash: _Optional[_Union[BlockHashInput, _Mapping]] = ..., address: _Optional[_Union[ContractAddress, _Mapping]] = ..., key: _Optional[bytes] = ...) -> None: ...

class InstanceStateValueAtKey(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class ReceiveName(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class InitName(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class Parameter(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class ContractStateV0(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class BlockItemStatus(_message.Message):
    __slots__ = ("received", "committed", "finalized")
    class Committed(_message.Message):
        __slots__ = ("outcomes",)
        OUTCOMES_FIELD_NUMBER: _ClassVar[int]
        outcomes: _containers.RepeatedCompositeFieldContainer[BlockItemSummaryInBlock]
        def __init__(self, outcomes: _Optional[_Iterable[_Union[BlockItemSummaryInBlock, _Mapping]]] = ...) -> None: ...
    class Finalized(_message.Message):
        __slots__ = ("outcome",)
        OUTCOME_FIELD_NUMBER: _ClassVar[int]
        outcome: BlockItemSummaryInBlock
        def __init__(self, outcome: _Optional[_Union[BlockItemSummaryInBlock, _Mapping]] = ...) -> None: ...
    RECEIVED_FIELD_NUMBER: _ClassVar[int]
    COMMITTED_FIELD_NUMBER: _ClassVar[int]
    FINALIZED_FIELD_NUMBER: _ClassVar[int]
    received: Empty
    committed: BlockItemStatus.Committed
    finalized: BlockItemStatus.Finalized
    def __init__(self, received: _Optional[_Union[Empty, _Mapping]] = ..., committed: _Optional[_Union[BlockItemStatus.Committed, _Mapping]] = ..., finalized: _Optional[_Union[BlockItemStatus.Finalized, _Mapping]] = ...) -> None: ...

class BlockItemSummaryInBlock(_message.Message):
    __slots__ = ("block_hash", "outcome")
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    block_hash: BlockHash
    outcome: BlockItemSummary
    def __init__(self, block_hash: _Optional[_Union[BlockHash, _Mapping]] = ..., outcome: _Optional[_Union[BlockItemSummary, _Mapping]] = ...) -> None: ...

class Energy(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class Slot(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class NextAccountSequenceNumber(_message.Message):
    __slots__ = ("sequence_number", "all_final")
    SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    ALL_FINAL_FIELD_NUMBER: _ClassVar[int]
    sequence_number: SequenceNumber
    all_final: bool
    def __init__(self, sequence_number: _Optional[_Union[SequenceNumber, _Mapping]] = ..., all_final: bool = ...) -> None: ...

class Duration(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class RejectReason(_message.Message):
    __slots__ = ("module_not_wf", "module_hash_already_exists", "invalid_account_reference", "invalid_init_method", "invalid_receive_method", "invalid_module_reference", "invalid_contract_address", "runtime_failure", "amount_too_large", "serialization_failure", "out_of_energy", "rejected_init", "rejected_receive", "invalid_proof", "already_a_baker", "not_a_baker", "insufficient_balance_for_baker_stake", "stake_under_minimum_threshold_for_baking", "baker_in_cooldown", "duplicate_aggregation_key", "non_existent_credential_id", "key_index_already_in_use", "invalid_account_threshold", "invalid_credential_key_sign_threshold", "invalid_encrypted_amount_transfer_proof", "invalid_transfer_to_public_proof", "encrypted_amount_self_transfer", "invalid_index_on_encrypted_transfer", "zero_scheduledAmount", "non_increasing_schedule", "first_scheduled_release_expired", "scheduled_self_transfer", "invalid_credentials", "duplicate_cred_ids", "non_existent_cred_ids", "remove_first_credential", "credential_holder_did_not_sign", "not_allowed_multiple_credentials", "not_allowed_to_receive_encrypted", "not_allowed_to_handle_encrypted", "missing_baker_add_parameters", "finalization_reward_commission_not_in_range", "baking_reward_commission_not_in_range", "transaction_fee_commission_not_in_range", "already_a_delegator", "insufficient_balance_for_delegation_stake", "missing_delegation_add_parameters", "insufficient_delegation_stake", "delegator_in_cooldown", "not_a_delegator", "delegation_target_not_a_baker", "stake_over_maximum_threshold_for_pool", "pool_would_become_over_delegated", "pool_closed", "non_existent_token_id", "token_update_transaction_failed")
    class InvalidInitMethod(_message.Message):
        __slots__ = ("module_ref", "init_name")
        MODULE_REF_FIELD_NUMBER: _ClassVar[int]
        INIT_NAME_FIELD_NUMBER: _ClassVar[int]
        module_ref: ModuleRef
        init_name: InitName
        def __init__(self, module_ref: _Optional[_Union[ModuleRef, _Mapping]] = ..., init_name: _Optional[_Union[InitName, _Mapping]] = ...) -> None: ...
    class InvalidReceiveMethod(_message.Message):
        __slots__ = ("module_ref", "receive_name")
        MODULE_REF_FIELD_NUMBER: _ClassVar[int]
        RECEIVE_NAME_FIELD_NUMBER: _ClassVar[int]
        module_ref: ModuleRef
        receive_name: ReceiveName
        def __init__(self, module_ref: _Optional[_Union[ModuleRef, _Mapping]] = ..., receive_name: _Optional[_Union[ReceiveName, _Mapping]] = ...) -> None: ...
    class AmountTooLarge(_message.Message):
        __slots__ = ("address", "amount")
        ADDRESS_FIELD_NUMBER: _ClassVar[int]
        AMOUNT_FIELD_NUMBER: _ClassVar[int]
        address: Address
        amount: Amount
        def __init__(self, address: _Optional[_Union[Address, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...
    class RejectedInit(_message.Message):
        __slots__ = ("reject_reason",)
        REJECT_REASON_FIELD_NUMBER: _ClassVar[int]
        reject_reason: int
        def __init__(self, reject_reason: _Optional[int] = ...) -> None: ...
    class RejectedReceive(_message.Message):
        __slots__ = ("reject_reason", "contract_address", "receive_name", "parameter")
        REJECT_REASON_FIELD_NUMBER: _ClassVar[int]
        CONTRACT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
        RECEIVE_NAME_FIELD_NUMBER: _ClassVar[int]
        PARAMETER_FIELD_NUMBER: _ClassVar[int]
        reject_reason: int
        contract_address: ContractAddress
        receive_name: ReceiveName
        parameter: Parameter
        def __init__(self, reject_reason: _Optional[int] = ..., contract_address: _Optional[_Union[ContractAddress, _Mapping]] = ..., receive_name: _Optional[_Union[ReceiveName, _Mapping]] = ..., parameter: _Optional[_Union[Parameter, _Mapping]] = ...) -> None: ...
    class DuplicateCredIds(_message.Message):
        __slots__ = ("ids",)
        IDS_FIELD_NUMBER: _ClassVar[int]
        ids: _containers.RepeatedCompositeFieldContainer[CredentialRegistrationId]
        def __init__(self, ids: _Optional[_Iterable[_Union[CredentialRegistrationId, _Mapping]]] = ...) -> None: ...
    class NonExistentCredIds(_message.Message):
        __slots__ = ("ids",)
        IDS_FIELD_NUMBER: _ClassVar[int]
        ids: _containers.RepeatedCompositeFieldContainer[CredentialRegistrationId]
        def __init__(self, ids: _Optional[_Iterable[_Union[CredentialRegistrationId, _Mapping]]] = ...) -> None: ...
    MODULE_NOT_WF_FIELD_NUMBER: _ClassVar[int]
    MODULE_HASH_ALREADY_EXISTS_FIELD_NUMBER: _ClassVar[int]
    INVALID_ACCOUNT_REFERENCE_FIELD_NUMBER: _ClassVar[int]
    INVALID_INIT_METHOD_FIELD_NUMBER: _ClassVar[int]
    INVALID_RECEIVE_METHOD_FIELD_NUMBER: _ClassVar[int]
    INVALID_MODULE_REFERENCE_FIELD_NUMBER: _ClassVar[int]
    INVALID_CONTRACT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    RUNTIME_FAILURE_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_TOO_LARGE_FIELD_NUMBER: _ClassVar[int]
    SERIALIZATION_FAILURE_FIELD_NUMBER: _ClassVar[int]
    OUT_OF_ENERGY_FIELD_NUMBER: _ClassVar[int]
    REJECTED_INIT_FIELD_NUMBER: _ClassVar[int]
    REJECTED_RECEIVE_FIELD_NUMBER: _ClassVar[int]
    INVALID_PROOF_FIELD_NUMBER: _ClassVar[int]
    ALREADY_A_BAKER_FIELD_NUMBER: _ClassVar[int]
    NOT_A_BAKER_FIELD_NUMBER: _ClassVar[int]
    INSUFFICIENT_BALANCE_FOR_BAKER_STAKE_FIELD_NUMBER: _ClassVar[int]
    STAKE_UNDER_MINIMUM_THRESHOLD_FOR_BAKING_FIELD_NUMBER: _ClassVar[int]
    BAKER_IN_COOLDOWN_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_AGGREGATION_KEY_FIELD_NUMBER: _ClassVar[int]
    NON_EXISTENT_CREDENTIAL_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_INDEX_ALREADY_IN_USE_FIELD_NUMBER: _ClassVar[int]
    INVALID_ACCOUNT_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    INVALID_CREDENTIAL_KEY_SIGN_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    INVALID_ENCRYPTED_AMOUNT_TRANSFER_PROOF_FIELD_NUMBER: _ClassVar[int]
    INVALID_TRANSFER_TO_PUBLIC_PROOF_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTED_AMOUNT_SELF_TRANSFER_FIELD_NUMBER: _ClassVar[int]
    INVALID_INDEX_ON_ENCRYPTED_TRANSFER_FIELD_NUMBER: _ClassVar[int]
    ZERO_SCHEDULEDAMOUNT_FIELD_NUMBER: _ClassVar[int]
    NON_INCREASING_SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    FIRST_SCHEDULED_RELEASE_EXPIRED_FIELD_NUMBER: _ClassVar[int]
    SCHEDULED_SELF_TRANSFER_FIELD_NUMBER: _ClassVar[int]
    INVALID_CREDENTIALS_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_CRED_IDS_FIELD_NUMBER: _ClassVar[int]
    NON_EXISTENT_CRED_IDS_FIELD_NUMBER: _ClassVar[int]
    REMOVE_FIRST_CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_HOLDER_DID_NOT_SIGN_FIELD_NUMBER: _ClassVar[int]
    NOT_ALLOWED_MULTIPLE_CREDENTIALS_FIELD_NUMBER: _ClassVar[int]
    NOT_ALLOWED_TO_RECEIVE_ENCRYPTED_FIELD_NUMBER: _ClassVar[int]
    NOT_ALLOWED_TO_HANDLE_ENCRYPTED_FIELD_NUMBER: _ClassVar[int]
    MISSING_BAKER_ADD_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_REWARD_COMMISSION_NOT_IN_RANGE_FIELD_NUMBER: _ClassVar[int]
    BAKING_REWARD_COMMISSION_NOT_IN_RANGE_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_FEE_COMMISSION_NOT_IN_RANGE_FIELD_NUMBER: _ClassVar[int]
    ALREADY_A_DELEGATOR_FIELD_NUMBER: _ClassVar[int]
    INSUFFICIENT_BALANCE_FOR_DELEGATION_STAKE_FIELD_NUMBER: _ClassVar[int]
    MISSING_DELEGATION_ADD_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    INSUFFICIENT_DELEGATION_STAKE_FIELD_NUMBER: _ClassVar[int]
    DELEGATOR_IN_COOLDOWN_FIELD_NUMBER: _ClassVar[int]
    NOT_A_DELEGATOR_FIELD_NUMBER: _ClassVar[int]
    DELEGATION_TARGET_NOT_A_BAKER_FIELD_NUMBER: _ClassVar[int]
    STAKE_OVER_MAXIMUM_THRESHOLD_FOR_POOL_FIELD_NUMBER: _ClassVar[int]
    POOL_WOULD_BECOME_OVER_DELEGATED_FIELD_NUMBER: _ClassVar[int]
    POOL_CLOSED_FIELD_NUMBER: _ClassVar[int]
    NON_EXISTENT_TOKEN_ID_FIELD_NUMBER: _ClassVar[int]
    TOKEN_UPDATE_TRANSACTION_FAILED_FIELD_NUMBER: _ClassVar[int]
    module_not_wf: Empty
    module_hash_already_exists: ModuleRef
    invalid_account_reference: _kernel_pb2.AccountAddress
    invalid_init_method: RejectReason.InvalidInitMethod
    invalid_receive_method: RejectReason.InvalidReceiveMethod
    invalid_module_reference: ModuleRef
    invalid_contract_address: ContractAddress
    runtime_failure: Empty
    amount_too_large: RejectReason.AmountTooLarge
    serialization_failure: Empty
    out_of_energy: Empty
    rejected_init: RejectReason.RejectedInit
    rejected_receive: RejectReason.RejectedReceive
    invalid_proof: Empty
    already_a_baker: BakerId
    not_a_baker: _kernel_pb2.AccountAddress
    insufficient_balance_for_baker_stake: Empty
    stake_under_minimum_threshold_for_baking: Empty
    baker_in_cooldown: Empty
    duplicate_aggregation_key: BakerAggregationVerifyKey
    non_existent_credential_id: Empty
    key_index_already_in_use: Empty
    invalid_account_threshold: Empty
    invalid_credential_key_sign_threshold: Empty
    invalid_encrypted_amount_transfer_proof: Empty
    invalid_transfer_to_public_proof: Empty
    encrypted_amount_self_transfer: _kernel_pb2.AccountAddress
    invalid_index_on_encrypted_transfer: Empty
    zero_scheduledAmount: Empty
    non_increasing_schedule: Empty
    first_scheduled_release_expired: Empty
    scheduled_self_transfer: _kernel_pb2.AccountAddress
    invalid_credentials: Empty
    duplicate_cred_ids: RejectReason.DuplicateCredIds
    non_existent_cred_ids: RejectReason.NonExistentCredIds
    remove_first_credential: Empty
    credential_holder_did_not_sign: Empty
    not_allowed_multiple_credentials: Empty
    not_allowed_to_receive_encrypted: Empty
    not_allowed_to_handle_encrypted: Empty
    missing_baker_add_parameters: Empty
    finalization_reward_commission_not_in_range: Empty
    baking_reward_commission_not_in_range: Empty
    transaction_fee_commission_not_in_range: Empty
    already_a_delegator: Empty
    insufficient_balance_for_delegation_stake: Empty
    missing_delegation_add_parameters: Empty
    insufficient_delegation_stake: Empty
    delegator_in_cooldown: Empty
    not_a_delegator: _kernel_pb2.AccountAddress
    delegation_target_not_a_baker: BakerId
    stake_over_maximum_threshold_for_pool: Empty
    pool_would_become_over_delegated: Empty
    pool_closed: Empty
    non_existent_token_id: _protocol_level_tokens_pb2.TokenId
    token_update_transaction_failed: _protocol_level_tokens_pb2.TokenModuleRejectReason
    def __init__(self, module_not_wf: _Optional[_Union[Empty, _Mapping]] = ..., module_hash_already_exists: _Optional[_Union[ModuleRef, _Mapping]] = ..., invalid_account_reference: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., invalid_init_method: _Optional[_Union[RejectReason.InvalidInitMethod, _Mapping]] = ..., invalid_receive_method: _Optional[_Union[RejectReason.InvalidReceiveMethod, _Mapping]] = ..., invalid_module_reference: _Optional[_Union[ModuleRef, _Mapping]] = ..., invalid_contract_address: _Optional[_Union[ContractAddress, _Mapping]] = ..., runtime_failure: _Optional[_Union[Empty, _Mapping]] = ..., amount_too_large: _Optional[_Union[RejectReason.AmountTooLarge, _Mapping]] = ..., serialization_failure: _Optional[_Union[Empty, _Mapping]] = ..., out_of_energy: _Optional[_Union[Empty, _Mapping]] = ..., rejected_init: _Optional[_Union[RejectReason.RejectedInit, _Mapping]] = ..., rejected_receive: _Optional[_Union[RejectReason.RejectedReceive, _Mapping]] = ..., invalid_proof: _Optional[_Union[Empty, _Mapping]] = ..., already_a_baker: _Optional[_Union[BakerId, _Mapping]] = ..., not_a_baker: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., insufficient_balance_for_baker_stake: _Optional[_Union[Empty, _Mapping]] = ..., stake_under_minimum_threshold_for_baking: _Optional[_Union[Empty, _Mapping]] = ..., baker_in_cooldown: _Optional[_Union[Empty, _Mapping]] = ..., duplicate_aggregation_key: _Optional[_Union[BakerAggregationVerifyKey, _Mapping]] = ..., non_existent_credential_id: _Optional[_Union[Empty, _Mapping]] = ..., key_index_already_in_use: _Optional[_Union[Empty, _Mapping]] = ..., invalid_account_threshold: _Optional[_Union[Empty, _Mapping]] = ..., invalid_credential_key_sign_threshold: _Optional[_Union[Empty, _Mapping]] = ..., invalid_encrypted_amount_transfer_proof: _Optional[_Union[Empty, _Mapping]] = ..., invalid_transfer_to_public_proof: _Optional[_Union[Empty, _Mapping]] = ..., encrypted_amount_self_transfer: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., invalid_index_on_encrypted_transfer: _Optional[_Union[Empty, _Mapping]] = ..., zero_scheduledAmount: _Optional[_Union[Empty, _Mapping]] = ..., non_increasing_schedule: _Optional[_Union[Empty, _Mapping]] = ..., first_scheduled_release_expired: _Optional[_Union[Empty, _Mapping]] = ..., scheduled_self_transfer: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., invalid_credentials: _Optional[_Union[Empty, _Mapping]] = ..., duplicate_cred_ids: _Optional[_Union[RejectReason.DuplicateCredIds, _Mapping]] = ..., non_existent_cred_ids: _Optional[_Union[RejectReason.NonExistentCredIds, _Mapping]] = ..., remove_first_credential: _Optional[_Union[Empty, _Mapping]] = ..., credential_holder_did_not_sign: _Optional[_Union[Empty, _Mapping]] = ..., not_allowed_multiple_credentials: _Optional[_Union[Empty, _Mapping]] = ..., not_allowed_to_receive_encrypted: _Optional[_Union[Empty, _Mapping]] = ..., not_allowed_to_handle_encrypted: _Optional[_Union[Empty, _Mapping]] = ..., missing_baker_add_parameters: _Optional[_Union[Empty, _Mapping]] = ..., finalization_reward_commission_not_in_range: _Optional[_Union[Empty, _Mapping]] = ..., baking_reward_commission_not_in_range: _Optional[_Union[Empty, _Mapping]] = ..., transaction_fee_commission_not_in_range: _Optional[_Union[Empty, _Mapping]] = ..., already_a_delegator: _Optional[_Union[Empty, _Mapping]] = ..., insufficient_balance_for_delegation_stake: _Optional[_Union[Empty, _Mapping]] = ..., missing_delegation_add_parameters: _Optional[_Union[Empty, _Mapping]] = ..., insufficient_delegation_stake: _Optional[_Union[Empty, _Mapping]] = ..., delegator_in_cooldown: _Optional[_Union[Empty, _Mapping]] = ..., not_a_delegator: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., delegation_target_not_a_baker: _Optional[_Union[BakerId, _Mapping]] = ..., stake_over_maximum_threshold_for_pool: _Optional[_Union[Empty, _Mapping]] = ..., pool_would_become_over_delegated: _Optional[_Union[Empty, _Mapping]] = ..., pool_closed: _Optional[_Union[Empty, _Mapping]] = ..., non_existent_token_id: _Optional[_Union[_protocol_level_tokens_pb2.TokenId, _Mapping]] = ..., token_update_transaction_failed: _Optional[_Union[_protocol_level_tokens_pb2.TokenModuleRejectReason, _Mapping]] = ...) -> None: ...

class ContractInitializedEvent(_message.Message):
    __slots__ = ("contract_version", "origin_ref", "address", "amount", "init_name", "events", "parameter")
    CONTRACT_VERSION_FIELD_NUMBER: _ClassVar[int]
    ORIGIN_REF_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    INIT_NAME_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_FIELD_NUMBER: _ClassVar[int]
    contract_version: ContractVersion
    origin_ref: ModuleRef
    address: ContractAddress
    amount: Amount
    init_name: InitName
    events: _containers.RepeatedCompositeFieldContainer[ContractEvent]
    parameter: Parameter
    def __init__(self, contract_version: _Optional[_Union[ContractVersion, str]] = ..., origin_ref: _Optional[_Union[ModuleRef, _Mapping]] = ..., address: _Optional[_Union[ContractAddress, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ..., init_name: _Optional[_Union[InitName, _Mapping]] = ..., events: _Optional[_Iterable[_Union[ContractEvent, _Mapping]]] = ..., parameter: _Optional[_Union[Parameter, _Mapping]] = ...) -> None: ...

class ContractEvent(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class InstanceUpdatedEvent(_message.Message):
    __slots__ = ("contract_version", "address", "instigator", "amount", "parameter", "receive_name", "events")
    CONTRACT_VERSION_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    INSTIGATOR_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_FIELD_NUMBER: _ClassVar[int]
    RECEIVE_NAME_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    contract_version: ContractVersion
    address: ContractAddress
    instigator: Address
    amount: Amount
    parameter: Parameter
    receive_name: ReceiveName
    events: _containers.RepeatedCompositeFieldContainer[ContractEvent]
    def __init__(self, contract_version: _Optional[_Union[ContractVersion, str]] = ..., address: _Optional[_Union[ContractAddress, _Mapping]] = ..., instigator: _Optional[_Union[Address, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ..., parameter: _Optional[_Union[Parameter, _Mapping]] = ..., receive_name: _Optional[_Union[ReceiveName, _Mapping]] = ..., events: _Optional[_Iterable[_Union[ContractEvent, _Mapping]]] = ...) -> None: ...

class ContractTraceElement(_message.Message):
    __slots__ = ("updated", "transferred", "interrupted", "resumed", "upgraded")
    class Transferred(_message.Message):
        __slots__ = ("sender", "amount", "receiver")
        SENDER_FIELD_NUMBER: _ClassVar[int]
        AMOUNT_FIELD_NUMBER: _ClassVar[int]
        RECEIVER_FIELD_NUMBER: _ClassVar[int]
        sender: ContractAddress
        amount: Amount
        receiver: _kernel_pb2.AccountAddress
        def __init__(self, sender: _Optional[_Union[ContractAddress, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ..., receiver: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ...) -> None: ...
    class Interrupted(_message.Message):
        __slots__ = ("address", "events")
        ADDRESS_FIELD_NUMBER: _ClassVar[int]
        EVENTS_FIELD_NUMBER: _ClassVar[int]
        address: ContractAddress
        events: _containers.RepeatedCompositeFieldContainer[ContractEvent]
        def __init__(self, address: _Optional[_Union[ContractAddress, _Mapping]] = ..., events: _Optional[_Iterable[_Union[ContractEvent, _Mapping]]] = ...) -> None: ...
    class Resumed(_message.Message):
        __slots__ = ("address", "success")
        ADDRESS_FIELD_NUMBER: _ClassVar[int]
        SUCCESS_FIELD_NUMBER: _ClassVar[int]
        address: ContractAddress
        success: bool
        def __init__(self, address: _Optional[_Union[ContractAddress, _Mapping]] = ..., success: bool = ...) -> None: ...
    class Upgraded(_message.Message):
        __slots__ = ("address", "to")
        ADDRESS_FIELD_NUMBER: _ClassVar[int]
        FROM_FIELD_NUMBER: _ClassVar[int]
        TO_FIELD_NUMBER: _ClassVar[int]
        address: ContractAddress
        to: ModuleRef
        def __init__(self, address: _Optional[_Union[ContractAddress, _Mapping]] = ..., to: _Optional[_Union[ModuleRef, _Mapping]] = ..., **kwargs) -> None: ...
    UPDATED_FIELD_NUMBER: _ClassVar[int]
    TRANSFERRED_FIELD_NUMBER: _ClassVar[int]
    INTERRUPTED_FIELD_NUMBER: _ClassVar[int]
    RESUMED_FIELD_NUMBER: _ClassVar[int]
    UPGRADED_FIELD_NUMBER: _ClassVar[int]
    updated: InstanceUpdatedEvent
    transferred: ContractTraceElement.Transferred
    interrupted: ContractTraceElement.Interrupted
    resumed: ContractTraceElement.Resumed
    upgraded: ContractTraceElement.Upgraded
    def __init__(self, updated: _Optional[_Union[InstanceUpdatedEvent, _Mapping]] = ..., transferred: _Optional[_Union[ContractTraceElement.Transferred, _Mapping]] = ..., interrupted: _Optional[_Union[ContractTraceElement.Interrupted, _Mapping]] = ..., resumed: _Optional[_Union[ContractTraceElement.Resumed, _Mapping]] = ..., upgraded: _Optional[_Union[ContractTraceElement.Upgraded, _Mapping]] = ...) -> None: ...

class BakerKeysEvent(_message.Message):
    __slots__ = ("baker_id", "account", "sign_key", "election_key", "aggregation_key")
    BAKER_ID_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    SIGN_KEY_FIELD_NUMBER: _ClassVar[int]
    ELECTION_KEY_FIELD_NUMBER: _ClassVar[int]
    AGGREGATION_KEY_FIELD_NUMBER: _ClassVar[int]
    baker_id: BakerId
    account: _kernel_pb2.AccountAddress
    sign_key: BakerSignatureVerifyKey
    election_key: BakerElectionVerifyKey
    aggregation_key: BakerAggregationVerifyKey
    def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ..., account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., sign_key: _Optional[_Union[BakerSignatureVerifyKey, _Mapping]] = ..., election_key: _Optional[_Union[BakerElectionVerifyKey, _Mapping]] = ..., aggregation_key: _Optional[_Union[BakerAggregationVerifyKey, _Mapping]] = ...) -> None: ...

class BakerStakeUpdatedData(_message.Message):
    __slots__ = ("baker_id", "new_stake", "increased")
    BAKER_ID_FIELD_NUMBER: _ClassVar[int]
    NEW_STAKE_FIELD_NUMBER: _ClassVar[int]
    INCREASED_FIELD_NUMBER: _ClassVar[int]
    baker_id: BakerId
    new_stake: Amount
    increased: bool
    def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ..., new_stake: _Optional[_Union[Amount, _Mapping]] = ..., increased: bool = ...) -> None: ...

class EncryptedAmountRemovedEvent(_message.Message):
    __slots__ = ("account", "new_amount", "input_amount", "up_to_index")
    ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    NEW_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    INPUT_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    UP_TO_INDEX_FIELD_NUMBER: _ClassVar[int]
    account: _kernel_pb2.AccountAddress
    new_amount: EncryptedAmount
    input_amount: EncryptedAmount
    up_to_index: int
    def __init__(self, account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., new_amount: _Optional[_Union[EncryptedAmount, _Mapping]] = ..., input_amount: _Optional[_Union[EncryptedAmount, _Mapping]] = ..., up_to_index: _Optional[int] = ...) -> None: ...

class NewEncryptedAmountEvent(_message.Message):
    __slots__ = ("receiver", "new_index", "encrypted_amount")
    RECEIVER_FIELD_NUMBER: _ClassVar[int]
    NEW_INDEX_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTED_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    receiver: _kernel_pb2.AccountAddress
    new_index: int
    encrypted_amount: EncryptedAmount
    def __init__(self, receiver: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., new_index: _Optional[int] = ..., encrypted_amount: _Optional[_Union[EncryptedAmount, _Mapping]] = ...) -> None: ...

class EncryptedSelfAmountAddedEvent(_message.Message):
    __slots__ = ("account", "new_amount", "amount")
    ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    NEW_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    account: _kernel_pb2.AccountAddress
    new_amount: EncryptedAmount
    amount: Amount
    def __init__(self, account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., new_amount: _Optional[_Union[EncryptedAmount, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...

class RegisteredData(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class BakerEvent(_message.Message):
    __slots__ = ("baker_added", "baker_removed", "baker_stake_increased", "baker_stake_decreased", "baker_restake_earnings_updated", "baker_keys_updated", "baker_set_open_status", "baker_set_metadata_url", "baker_set_transaction_fee_commission", "baker_set_baking_reward_commission", "baker_set_finalization_reward_commission", "delegation_removed", "baker_suspended", "baker_resumed")
    class BakerAdded(_message.Message):
        __slots__ = ("keys_event", "stake", "restake_earnings")
        KEYS_EVENT_FIELD_NUMBER: _ClassVar[int]
        STAKE_FIELD_NUMBER: _ClassVar[int]
        RESTAKE_EARNINGS_FIELD_NUMBER: _ClassVar[int]
        keys_event: BakerKeysEvent
        stake: Amount
        restake_earnings: bool
        def __init__(self, keys_event: _Optional[_Union[BakerKeysEvent, _Mapping]] = ..., stake: _Optional[_Union[Amount, _Mapping]] = ..., restake_earnings: bool = ...) -> None: ...
    class BakerStakeIncreased(_message.Message):
        __slots__ = ("baker_id", "new_stake")
        BAKER_ID_FIELD_NUMBER: _ClassVar[int]
        NEW_STAKE_FIELD_NUMBER: _ClassVar[int]
        baker_id: BakerId
        new_stake: Amount
        def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ..., new_stake: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...
    class BakerStakeDecreased(_message.Message):
        __slots__ = ("baker_id", "new_stake")
        BAKER_ID_FIELD_NUMBER: _ClassVar[int]
        NEW_STAKE_FIELD_NUMBER: _ClassVar[int]
        baker_id: BakerId
        new_stake: Amount
        def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ..., new_stake: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...
    class BakerRestakeEarningsUpdated(_message.Message):
        __slots__ = ("baker_id", "restake_earnings")
        BAKER_ID_FIELD_NUMBER: _ClassVar[int]
        RESTAKE_EARNINGS_FIELD_NUMBER: _ClassVar[int]
        baker_id: BakerId
        restake_earnings: bool
        def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ..., restake_earnings: bool = ...) -> None: ...
    class BakerSetOpenStatus(_message.Message):
        __slots__ = ("baker_id", "open_status")
        BAKER_ID_FIELD_NUMBER: _ClassVar[int]
        OPEN_STATUS_FIELD_NUMBER: _ClassVar[int]
        baker_id: BakerId
        open_status: OpenStatus
        def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ..., open_status: _Optional[_Union[OpenStatus, str]] = ...) -> None: ...
    class BakerSetMetadataUrl(_message.Message):
        __slots__ = ("baker_id", "url")
        BAKER_ID_FIELD_NUMBER: _ClassVar[int]
        URL_FIELD_NUMBER: _ClassVar[int]
        baker_id: BakerId
        url: str
        def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ..., url: _Optional[str] = ...) -> None: ...
    class BakerSetTransactionFeeCommission(_message.Message):
        __slots__ = ("baker_id", "transaction_fee_commission")
        BAKER_ID_FIELD_NUMBER: _ClassVar[int]
        TRANSACTION_FEE_COMMISSION_FIELD_NUMBER: _ClassVar[int]
        baker_id: BakerId
        transaction_fee_commission: AmountFraction
        def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ..., transaction_fee_commission: _Optional[_Union[AmountFraction, _Mapping]] = ...) -> None: ...
    class BakerSetBakingRewardCommission(_message.Message):
        __slots__ = ("baker_id", "baking_reward_commission")
        BAKER_ID_FIELD_NUMBER: _ClassVar[int]
        BAKING_REWARD_COMMISSION_FIELD_NUMBER: _ClassVar[int]
        baker_id: BakerId
        baking_reward_commission: AmountFraction
        def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ..., baking_reward_commission: _Optional[_Union[AmountFraction, _Mapping]] = ...) -> None: ...
    class BakerSetFinalizationRewardCommission(_message.Message):
        __slots__ = ("baker_id", "finalization_reward_commission")
        BAKER_ID_FIELD_NUMBER: _ClassVar[int]
        FINALIZATION_REWARD_COMMISSION_FIELD_NUMBER: _ClassVar[int]
        baker_id: BakerId
        finalization_reward_commission: AmountFraction
        def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ..., finalization_reward_commission: _Optional[_Union[AmountFraction, _Mapping]] = ...) -> None: ...
    class DelegationRemoved(_message.Message):
        __slots__ = ("delegator_id",)
        DELEGATOR_ID_FIELD_NUMBER: _ClassVar[int]
        delegator_id: DelegatorId
        def __init__(self, delegator_id: _Optional[_Union[DelegatorId, _Mapping]] = ...) -> None: ...
    class BakerSuspended(_message.Message):
        __slots__ = ("baker_id",)
        BAKER_ID_FIELD_NUMBER: _ClassVar[int]
        baker_id: BakerId
        def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ...) -> None: ...
    class BakerResumed(_message.Message):
        __slots__ = ("baker_id",)
        BAKER_ID_FIELD_NUMBER: _ClassVar[int]
        baker_id: BakerId
        def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ...) -> None: ...
    BAKER_ADDED_FIELD_NUMBER: _ClassVar[int]
    BAKER_REMOVED_FIELD_NUMBER: _ClassVar[int]
    BAKER_STAKE_INCREASED_FIELD_NUMBER: _ClassVar[int]
    BAKER_STAKE_DECREASED_FIELD_NUMBER: _ClassVar[int]
    BAKER_RESTAKE_EARNINGS_UPDATED_FIELD_NUMBER: _ClassVar[int]
    BAKER_KEYS_UPDATED_FIELD_NUMBER: _ClassVar[int]
    BAKER_SET_OPEN_STATUS_FIELD_NUMBER: _ClassVar[int]
    BAKER_SET_METADATA_URL_FIELD_NUMBER: _ClassVar[int]
    BAKER_SET_TRANSACTION_FEE_COMMISSION_FIELD_NUMBER: _ClassVar[int]
    BAKER_SET_BAKING_REWARD_COMMISSION_FIELD_NUMBER: _ClassVar[int]
    BAKER_SET_FINALIZATION_REWARD_COMMISSION_FIELD_NUMBER: _ClassVar[int]
    DELEGATION_REMOVED_FIELD_NUMBER: _ClassVar[int]
    BAKER_SUSPENDED_FIELD_NUMBER: _ClassVar[int]
    BAKER_RESUMED_FIELD_NUMBER: _ClassVar[int]
    baker_added: BakerEvent.BakerAdded
    baker_removed: BakerId
    baker_stake_increased: BakerEvent.BakerStakeIncreased
    baker_stake_decreased: BakerEvent.BakerStakeDecreased
    baker_restake_earnings_updated: BakerEvent.BakerRestakeEarningsUpdated
    baker_keys_updated: BakerKeysEvent
    baker_set_open_status: BakerEvent.BakerSetOpenStatus
    baker_set_metadata_url: BakerEvent.BakerSetMetadataUrl
    baker_set_transaction_fee_commission: BakerEvent.BakerSetTransactionFeeCommission
    baker_set_baking_reward_commission: BakerEvent.BakerSetBakingRewardCommission
    baker_set_finalization_reward_commission: BakerEvent.BakerSetFinalizationRewardCommission
    delegation_removed: BakerEvent.DelegationRemoved
    baker_suspended: BakerEvent.BakerSuspended
    baker_resumed: BakerEvent.BakerResumed
    def __init__(self, baker_added: _Optional[_Union[BakerEvent.BakerAdded, _Mapping]] = ..., baker_removed: _Optional[_Union[BakerId, _Mapping]] = ..., baker_stake_increased: _Optional[_Union[BakerEvent.BakerStakeIncreased, _Mapping]] = ..., baker_stake_decreased: _Optional[_Union[BakerEvent.BakerStakeDecreased, _Mapping]] = ..., baker_restake_earnings_updated: _Optional[_Union[BakerEvent.BakerRestakeEarningsUpdated, _Mapping]] = ..., baker_keys_updated: _Optional[_Union[BakerKeysEvent, _Mapping]] = ..., baker_set_open_status: _Optional[_Union[BakerEvent.BakerSetOpenStatus, _Mapping]] = ..., baker_set_metadata_url: _Optional[_Union[BakerEvent.BakerSetMetadataUrl, _Mapping]] = ..., baker_set_transaction_fee_commission: _Optional[_Union[BakerEvent.BakerSetTransactionFeeCommission, _Mapping]] = ..., baker_set_baking_reward_commission: _Optional[_Union[BakerEvent.BakerSetBakingRewardCommission, _Mapping]] = ..., baker_set_finalization_reward_commission: _Optional[_Union[BakerEvent.BakerSetFinalizationRewardCommission, _Mapping]] = ..., delegation_removed: _Optional[_Union[BakerEvent.DelegationRemoved, _Mapping]] = ..., baker_suspended: _Optional[_Union[BakerEvent.BakerSuspended, _Mapping]] = ..., baker_resumed: _Optional[_Union[BakerEvent.BakerResumed, _Mapping]] = ...) -> None: ...

class DelegatorId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: AccountIndex
    def __init__(self, id: _Optional[_Union[AccountIndex, _Mapping]] = ...) -> None: ...

class DelegationEvent(_message.Message):
    __slots__ = ("delegation_stake_increased", "delegation_stake_decreased", "delegation_set_restake_earnings", "delegation_set_delegation_target", "delegation_added", "delegation_removed", "baker_removed")
    class DelegationStakeIncreased(_message.Message):
        __slots__ = ("delegator_id", "new_stake")
        DELEGATOR_ID_FIELD_NUMBER: _ClassVar[int]
        NEW_STAKE_FIELD_NUMBER: _ClassVar[int]
        delegator_id: DelegatorId
        new_stake: Amount
        def __init__(self, delegator_id: _Optional[_Union[DelegatorId, _Mapping]] = ..., new_stake: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...
    class DelegationStakeDecreased(_message.Message):
        __slots__ = ("delegator_id", "new_stake")
        DELEGATOR_ID_FIELD_NUMBER: _ClassVar[int]
        NEW_STAKE_FIELD_NUMBER: _ClassVar[int]
        delegator_id: DelegatorId
        new_stake: Amount
        def __init__(self, delegator_id: _Optional[_Union[DelegatorId, _Mapping]] = ..., new_stake: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...
    class DelegationSetRestakeEarnings(_message.Message):
        __slots__ = ("delegator_id", "restake_earnings")
        DELEGATOR_ID_FIELD_NUMBER: _ClassVar[int]
        RESTAKE_EARNINGS_FIELD_NUMBER: _ClassVar[int]
        delegator_id: DelegatorId
        restake_earnings: bool
        def __init__(self, delegator_id: _Optional[_Union[DelegatorId, _Mapping]] = ..., restake_earnings: bool = ...) -> None: ...
    class DelegationSetDelegationTarget(_message.Message):
        __slots__ = ("delegator_id", "delegation_target")
        DELEGATOR_ID_FIELD_NUMBER: _ClassVar[int]
        DELEGATION_TARGET_FIELD_NUMBER: _ClassVar[int]
        delegator_id: DelegatorId
        delegation_target: DelegationTarget
        def __init__(self, delegator_id: _Optional[_Union[DelegatorId, _Mapping]] = ..., delegation_target: _Optional[_Union[DelegationTarget, _Mapping]] = ...) -> None: ...
    class BakerRemoved(_message.Message):
        __slots__ = ("baker_id",)
        BAKER_ID_FIELD_NUMBER: _ClassVar[int]
        baker_id: BakerId
        def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ...) -> None: ...
    DELEGATION_STAKE_INCREASED_FIELD_NUMBER: _ClassVar[int]
    DELEGATION_STAKE_DECREASED_FIELD_NUMBER: _ClassVar[int]
    DELEGATION_SET_RESTAKE_EARNINGS_FIELD_NUMBER: _ClassVar[int]
    DELEGATION_SET_DELEGATION_TARGET_FIELD_NUMBER: _ClassVar[int]
    DELEGATION_ADDED_FIELD_NUMBER: _ClassVar[int]
    DELEGATION_REMOVED_FIELD_NUMBER: _ClassVar[int]
    BAKER_REMOVED_FIELD_NUMBER: _ClassVar[int]
    delegation_stake_increased: DelegationEvent.DelegationStakeIncreased
    delegation_stake_decreased: DelegationEvent.DelegationStakeDecreased
    delegation_set_restake_earnings: DelegationEvent.DelegationSetRestakeEarnings
    delegation_set_delegation_target: DelegationEvent.DelegationSetDelegationTarget
    delegation_added: DelegatorId
    delegation_removed: DelegatorId
    baker_removed: DelegationEvent.BakerRemoved
    def __init__(self, delegation_stake_increased: _Optional[_Union[DelegationEvent.DelegationStakeIncreased, _Mapping]] = ..., delegation_stake_decreased: _Optional[_Union[DelegationEvent.DelegationStakeDecreased, _Mapping]] = ..., delegation_set_restake_earnings: _Optional[_Union[DelegationEvent.DelegationSetRestakeEarnings, _Mapping]] = ..., delegation_set_delegation_target: _Optional[_Union[DelegationEvent.DelegationSetDelegationTarget, _Mapping]] = ..., delegation_added: _Optional[_Union[DelegatorId, _Mapping]] = ..., delegation_removed: _Optional[_Union[DelegatorId, _Mapping]] = ..., baker_removed: _Optional[_Union[DelegationEvent.BakerRemoved, _Mapping]] = ...) -> None: ...

class AccountTransactionEffects(_message.Message):
    __slots__ = ("none", "module_deployed", "contract_initialized", "contract_update_issued", "account_transfer", "baker_added", "baker_removed", "baker_stake_updated", "baker_restake_earnings_updated", "baker_keys_updated", "encrypted_amount_transferred", "transferred_to_encrypted", "transferred_to_public", "transferred_with_schedule", "credential_keys_updated", "credentials_updated", "data_registered", "baker_configured", "delegation_configured", "token_update_effect")
    class None(_message.Message):
        __slots__ = ("transaction_type", "reject_reason")
        TRANSACTION_TYPE_FIELD_NUMBER: _ClassVar[int]
        REJECT_REASON_FIELD_NUMBER: _ClassVar[int]
        transaction_type: TransactionType
        reject_reason: RejectReason
        def __init__(self, transaction_type: _Optional[_Union[TransactionType, str]] = ..., reject_reason: _Optional[_Union[RejectReason, _Mapping]] = ...) -> None: ...
    class ContractUpdateIssued(_message.Message):
        __slots__ = ("effects",)
        EFFECTS_FIELD_NUMBER: _ClassVar[int]
        effects: _containers.RepeatedCompositeFieldContainer[ContractTraceElement]
        def __init__(self, effects: _Optional[_Iterable[_Union[ContractTraceElement, _Mapping]]] = ...) -> None: ...
    class AccountTransfer(_message.Message):
        __slots__ = ("amount", "receiver", "memo")
        AMOUNT_FIELD_NUMBER: _ClassVar[int]
        RECEIVER_FIELD_NUMBER: _ClassVar[int]
        MEMO_FIELD_NUMBER: _ClassVar[int]
        amount: Amount
        receiver: _kernel_pb2.AccountAddress
        memo: _kernel_pb2.Memo
        def __init__(self, amount: _Optional[_Union[Amount, _Mapping]] = ..., receiver: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., memo: _Optional[_Union[_kernel_pb2.Memo, _Mapping]] = ...) -> None: ...
    class BakerStakeUpdated(_message.Message):
        __slots__ = ("update",)
        UPDATE_FIELD_NUMBER: _ClassVar[int]
        update: BakerStakeUpdatedData
        def __init__(self, update: _Optional[_Union[BakerStakeUpdatedData, _Mapping]] = ...) -> None: ...
    class EncryptedAmountTransferred(_message.Message):
        __slots__ = ("removed", "added", "memo")
        REMOVED_FIELD_NUMBER: _ClassVar[int]
        ADDED_FIELD_NUMBER: _ClassVar[int]
        MEMO_FIELD_NUMBER: _ClassVar[int]
        removed: EncryptedAmountRemovedEvent
        added: NewEncryptedAmountEvent
        memo: _kernel_pb2.Memo
        def __init__(self, removed: _Optional[_Union[EncryptedAmountRemovedEvent, _Mapping]] = ..., added: _Optional[_Union[NewEncryptedAmountEvent, _Mapping]] = ..., memo: _Optional[_Union[_kernel_pb2.Memo, _Mapping]] = ...) -> None: ...
    class TransferredToPublic(_message.Message):
        __slots__ = ("removed", "amount")
        REMOVED_FIELD_NUMBER: _ClassVar[int]
        AMOUNT_FIELD_NUMBER: _ClassVar[int]
        removed: EncryptedAmountRemovedEvent
        amount: Amount
        def __init__(self, removed: _Optional[_Union[EncryptedAmountRemovedEvent, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...
    class TransferredWithSchedule(_message.Message):
        __slots__ = ("receiver", "amount", "memo")
        RECEIVER_FIELD_NUMBER: _ClassVar[int]
        AMOUNT_FIELD_NUMBER: _ClassVar[int]
        MEMO_FIELD_NUMBER: _ClassVar[int]
        receiver: _kernel_pb2.AccountAddress
        amount: _containers.RepeatedCompositeFieldContainer[NewRelease]
        memo: _kernel_pb2.Memo
        def __init__(self, receiver: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., amount: _Optional[_Iterable[_Union[NewRelease, _Mapping]]] = ..., memo: _Optional[_Union[_kernel_pb2.Memo, _Mapping]] = ...) -> None: ...
    class CredentialsUpdated(_message.Message):
        __slots__ = ("new_cred_ids", "removed_cred_ids", "new_threshold")
        NEW_CRED_IDS_FIELD_NUMBER: _ClassVar[int]
        REMOVED_CRED_IDS_FIELD_NUMBER: _ClassVar[int]
        NEW_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
        new_cred_ids: _containers.RepeatedCompositeFieldContainer[CredentialRegistrationId]
        removed_cred_ids: _containers.RepeatedCompositeFieldContainer[CredentialRegistrationId]
        new_threshold: AccountThreshold
        def __init__(self, new_cred_ids: _Optional[_Iterable[_Union[CredentialRegistrationId, _Mapping]]] = ..., removed_cred_ids: _Optional[_Iterable[_Union[CredentialRegistrationId, _Mapping]]] = ..., new_threshold: _Optional[_Union[AccountThreshold, _Mapping]] = ...) -> None: ...
    class BakerConfigured(_message.Message):
        __slots__ = ("events",)
        EVENTS_FIELD_NUMBER: _ClassVar[int]
        events: _containers.RepeatedCompositeFieldContainer[BakerEvent]
        def __init__(self, events: _Optional[_Iterable[_Union[BakerEvent, _Mapping]]] = ...) -> None: ...
    class DelegationConfigured(_message.Message):
        __slots__ = ("events",)
        EVENTS_FIELD_NUMBER: _ClassVar[int]
        events: _containers.RepeatedCompositeFieldContainer[DelegationEvent]
        def __init__(self, events: _Optional[_Iterable[_Union[DelegationEvent, _Mapping]]] = ...) -> None: ...
    NONE_FIELD_NUMBER: _ClassVar[int]
    MODULE_DEPLOYED_FIELD_NUMBER: _ClassVar[int]
    CONTRACT_INITIALIZED_FIELD_NUMBER: _ClassVar[int]
    CONTRACT_UPDATE_ISSUED_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_TRANSFER_FIELD_NUMBER: _ClassVar[int]
    BAKER_ADDED_FIELD_NUMBER: _ClassVar[int]
    BAKER_REMOVED_FIELD_NUMBER: _ClassVar[int]
    BAKER_STAKE_UPDATED_FIELD_NUMBER: _ClassVar[int]
    BAKER_RESTAKE_EARNINGS_UPDATED_FIELD_NUMBER: _ClassVar[int]
    BAKER_KEYS_UPDATED_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTED_AMOUNT_TRANSFERRED_FIELD_NUMBER: _ClassVar[int]
    TRANSFERRED_TO_ENCRYPTED_FIELD_NUMBER: _ClassVar[int]
    TRANSFERRED_TO_PUBLIC_FIELD_NUMBER: _ClassVar[int]
    TRANSFERRED_WITH_SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_KEYS_UPDATED_FIELD_NUMBER: _ClassVar[int]
    CREDENTIALS_UPDATED_FIELD_NUMBER: _ClassVar[int]
    DATA_REGISTERED_FIELD_NUMBER: _ClassVar[int]
    BAKER_CONFIGURED_FIELD_NUMBER: _ClassVar[int]
    DELEGATION_CONFIGURED_FIELD_NUMBER: _ClassVar[int]
    TOKEN_UPDATE_EFFECT_FIELD_NUMBER: _ClassVar[int]
    none: getattr(AccountTransactionEffects, 'None')
    module_deployed: ModuleRef
    contract_initialized: ContractInitializedEvent
    contract_update_issued: AccountTransactionEffects.ContractUpdateIssued
    account_transfer: AccountTransactionEffects.AccountTransfer
    baker_added: BakerEvent.BakerAdded
    baker_removed: BakerId
    baker_stake_updated: AccountTransactionEffects.BakerStakeUpdated
    baker_restake_earnings_updated: BakerEvent.BakerRestakeEarningsUpdated
    baker_keys_updated: BakerKeysEvent
    encrypted_amount_transferred: AccountTransactionEffects.EncryptedAmountTransferred
    transferred_to_encrypted: EncryptedSelfAmountAddedEvent
    transferred_to_public: AccountTransactionEffects.TransferredToPublic
    transferred_with_schedule: AccountTransactionEffects.TransferredWithSchedule
    credential_keys_updated: CredentialRegistrationId
    credentials_updated: AccountTransactionEffects.CredentialsUpdated
    data_registered: RegisteredData
    baker_configured: AccountTransactionEffects.BakerConfigured
    delegation_configured: AccountTransactionEffects.DelegationConfigured
    token_update_effect: _protocol_level_tokens_pb2.TokenEffect
    def __init__(self, none: _Optional[_Union[getattr(AccountTransactionEffects, 'None'), _Mapping]] = ..., module_deployed: _Optional[_Union[ModuleRef, _Mapping]] = ..., contract_initialized: _Optional[_Union[ContractInitializedEvent, _Mapping]] = ..., contract_update_issued: _Optional[_Union[AccountTransactionEffects.ContractUpdateIssued, _Mapping]] = ..., account_transfer: _Optional[_Union[AccountTransactionEffects.AccountTransfer, _Mapping]] = ..., baker_added: _Optional[_Union[BakerEvent.BakerAdded, _Mapping]] = ..., baker_removed: _Optional[_Union[BakerId, _Mapping]] = ..., baker_stake_updated: _Optional[_Union[AccountTransactionEffects.BakerStakeUpdated, _Mapping]] = ..., baker_restake_earnings_updated: _Optional[_Union[BakerEvent.BakerRestakeEarningsUpdated, _Mapping]] = ..., baker_keys_updated: _Optional[_Union[BakerKeysEvent, _Mapping]] = ..., encrypted_amount_transferred: _Optional[_Union[AccountTransactionEffects.EncryptedAmountTransferred, _Mapping]] = ..., transferred_to_encrypted: _Optional[_Union[EncryptedSelfAmountAddedEvent, _Mapping]] = ..., transferred_to_public: _Optional[_Union[AccountTransactionEffects.TransferredToPublic, _Mapping]] = ..., transferred_with_schedule: _Optional[_Union[AccountTransactionEffects.TransferredWithSchedule, _Mapping]] = ..., credential_keys_updated: _Optional[_Union[CredentialRegistrationId, _Mapping]] = ..., credentials_updated: _Optional[_Union[AccountTransactionEffects.CredentialsUpdated, _Mapping]] = ..., data_registered: _Optional[_Union[RegisteredData, _Mapping]] = ..., baker_configured: _Optional[_Union[AccountTransactionEffects.BakerConfigured, _Mapping]] = ..., delegation_configured: _Optional[_Union[AccountTransactionEffects.DelegationConfigured, _Mapping]] = ..., token_update_effect: _Optional[_Union[_protocol_level_tokens_pb2.TokenEffect, _Mapping]] = ...) -> None: ...

class ElectionDifficulty(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: AmountFraction
    def __init__(self, value: _Optional[_Union[AmountFraction, _Mapping]] = ...) -> None: ...

class TimeoutParameters(_message.Message):
    __slots__ = ("timeout_base", "timeout_increase", "timeout_decrease")
    TIMEOUT_BASE_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_INCREASE_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_DECREASE_FIELD_NUMBER: _ClassVar[int]
    timeout_base: Duration
    timeout_increase: Ratio
    timeout_decrease: Ratio
    def __init__(self, timeout_base: _Optional[_Union[Duration, _Mapping]] = ..., timeout_increase: _Optional[_Union[Ratio, _Mapping]] = ..., timeout_decrease: _Optional[_Union[Ratio, _Mapping]] = ...) -> None: ...

class FinalizationCommitteeParameters(_message.Message):
    __slots__ = ("minimum_finalizers", "maximum_finalizers", "finalizer_relative_stake_threshold")
    MINIMUM_FINALIZERS_FIELD_NUMBER: _ClassVar[int]
    MAXIMUM_FINALIZERS_FIELD_NUMBER: _ClassVar[int]
    FINALIZER_RELATIVE_STAKE_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    minimum_finalizers: int
    maximum_finalizers: int
    finalizer_relative_stake_threshold: AmountFraction
    def __init__(self, minimum_finalizers: _Optional[int] = ..., maximum_finalizers: _Optional[int] = ..., finalizer_relative_stake_threshold: _Optional[_Union[AmountFraction, _Mapping]] = ...) -> None: ...

class ValidatorScoreParameters(_message.Message):
    __slots__ = ("maximum_missed_rounds",)
    MAXIMUM_MISSED_ROUNDS_FIELD_NUMBER: _ClassVar[int]
    maximum_missed_rounds: int
    def __init__(self, maximum_missed_rounds: _Optional[int] = ...) -> None: ...

class ConsensusParametersV1(_message.Message):
    __slots__ = ("timeout_parameters", "min_block_time", "block_energy_limit")
    TIMEOUT_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    MIN_BLOCK_TIME_FIELD_NUMBER: _ClassVar[int]
    BLOCK_ENERGY_LIMIT_FIELD_NUMBER: _ClassVar[int]
    timeout_parameters: TimeoutParameters
    min_block_time: Duration
    block_energy_limit: Energy
    def __init__(self, timeout_parameters: _Optional[_Union[TimeoutParameters, _Mapping]] = ..., min_block_time: _Optional[_Union[Duration, _Mapping]] = ..., block_energy_limit: _Optional[_Union[Energy, _Mapping]] = ...) -> None: ...

class ExchangeRate(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: Ratio
    def __init__(self, value: _Optional[_Union[Ratio, _Mapping]] = ...) -> None: ...

class Ratio(_message.Message):
    __slots__ = ("numerator", "denominator")
    NUMERATOR_FIELD_NUMBER: _ClassVar[int]
    DENOMINATOR_FIELD_NUMBER: _ClassVar[int]
    numerator: int
    denominator: int
    def __init__(self, numerator: _Optional[int] = ..., denominator: _Optional[int] = ...) -> None: ...

class UpdatePublicKey(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class UpdateKeysThreshold(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class UpdateKeysIndex(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class HigherLevelKeys(_message.Message):
    __slots__ = ("keys", "threshold")
    KEYS_FIELD_NUMBER: _ClassVar[int]
    THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.RepeatedCompositeFieldContainer[UpdatePublicKey]
    threshold: UpdateKeysThreshold
    def __init__(self, keys: _Optional[_Iterable[_Union[UpdatePublicKey, _Mapping]]] = ..., threshold: _Optional[_Union[UpdateKeysThreshold, _Mapping]] = ...) -> None: ...

class AccessStructure(_message.Message):
    __slots__ = ("access_public_keys", "access_threshold")
    ACCESS_PUBLIC_KEYS_FIELD_NUMBER: _ClassVar[int]
    ACCESS_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    access_public_keys: _containers.RepeatedCompositeFieldContainer[UpdateKeysIndex]
    access_threshold: UpdateKeysThreshold
    def __init__(self, access_public_keys: _Optional[_Iterable[_Union[UpdateKeysIndex, _Mapping]]] = ..., access_threshold: _Optional[_Union[UpdateKeysThreshold, _Mapping]] = ...) -> None: ...

class AuthorizationsV0(_message.Message):
    __slots__ = ("keys", "emergency", "protocol", "parameter_consensus", "parameter_euro_per_energy", "parameter_micro_CCD_per_euro", "parameter_foundation_account", "parameter_mint_distribution", "parameter_transaction_fee_distribution", "parameter_gas_rewards", "pool_parameters", "add_anonymity_revoker", "add_identity_provider")
    KEYS_FIELD_NUMBER: _ClassVar[int]
    EMERGENCY_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_CONSENSUS_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_EURO_PER_ENERGY_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_MICRO_CCD_PER_EURO_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_FOUNDATION_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_MINT_DISTRIBUTION_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_TRANSACTION_FEE_DISTRIBUTION_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_GAS_REWARDS_FIELD_NUMBER: _ClassVar[int]
    POOL_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    ADD_ANONYMITY_REVOKER_FIELD_NUMBER: _ClassVar[int]
    ADD_IDENTITY_PROVIDER_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.RepeatedCompositeFieldContainer[UpdatePublicKey]
    emergency: AccessStructure
    protocol: AccessStructure
    parameter_consensus: AccessStructure
    parameter_euro_per_energy: AccessStructure
    parameter_micro_CCD_per_euro: AccessStructure
    parameter_foundation_account: AccessStructure
    parameter_mint_distribution: AccessStructure
    parameter_transaction_fee_distribution: AccessStructure
    parameter_gas_rewards: AccessStructure
    pool_parameters: AccessStructure
    add_anonymity_revoker: AccessStructure
    add_identity_provider: AccessStructure
    def __init__(self, keys: _Optional[_Iterable[_Union[UpdatePublicKey, _Mapping]]] = ..., emergency: _Optional[_Union[AccessStructure, _Mapping]] = ..., protocol: _Optional[_Union[AccessStructure, _Mapping]] = ..., parameter_consensus: _Optional[_Union[AccessStructure, _Mapping]] = ..., parameter_euro_per_energy: _Optional[_Union[AccessStructure, _Mapping]] = ..., parameter_micro_CCD_per_euro: _Optional[_Union[AccessStructure, _Mapping]] = ..., parameter_foundation_account: _Optional[_Union[AccessStructure, _Mapping]] = ..., parameter_mint_distribution: _Optional[_Union[AccessStructure, _Mapping]] = ..., parameter_transaction_fee_distribution: _Optional[_Union[AccessStructure, _Mapping]] = ..., parameter_gas_rewards: _Optional[_Union[AccessStructure, _Mapping]] = ..., pool_parameters: _Optional[_Union[AccessStructure, _Mapping]] = ..., add_anonymity_revoker: _Optional[_Union[AccessStructure, _Mapping]] = ..., add_identity_provider: _Optional[_Union[AccessStructure, _Mapping]] = ...) -> None: ...

class AuthorizationsV1(_message.Message):
    __slots__ = ("v0", "parameter_cooldown", "parameter_time", "create_plt")
    V0_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_COOLDOWN_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_TIME_FIELD_NUMBER: _ClassVar[int]
    CREATE_PLT_FIELD_NUMBER: _ClassVar[int]
    v0: AuthorizationsV0
    parameter_cooldown: AccessStructure
    parameter_time: AccessStructure
    create_plt: AccessStructure
    def __init__(self, v0: _Optional[_Union[AuthorizationsV0, _Mapping]] = ..., parameter_cooldown: _Optional[_Union[AccessStructure, _Mapping]] = ..., parameter_time: _Optional[_Union[AccessStructure, _Mapping]] = ..., create_plt: _Optional[_Union[AccessStructure, _Mapping]] = ...) -> None: ...

class Description(_message.Message):
    __slots__ = ("name", "url", "description")
    NAME_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    name: str
    url: str
    description: str
    def __init__(self, name: _Optional[str] = ..., url: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class ArInfo(_message.Message):
    __slots__ = ("identity", "description", "public_key")
    class ArIdentity(_message.Message):
        __slots__ = ("value",)
        VALUE_FIELD_NUMBER: _ClassVar[int]
        value: int
        def __init__(self, value: _Optional[int] = ...) -> None: ...
    class ArPublicKey(_message.Message):
        __slots__ = ("value",)
        VALUE_FIELD_NUMBER: _ClassVar[int]
        value: bytes
        def __init__(self, value: _Optional[bytes] = ...) -> None: ...
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PUBLIC_KEY_FIELD_NUMBER: _ClassVar[int]
    identity: ArInfo.ArIdentity
    description: Description
    public_key: ArInfo.ArPublicKey
    def __init__(self, identity: _Optional[_Union[ArInfo.ArIdentity, _Mapping]] = ..., description: _Optional[_Union[Description, _Mapping]] = ..., public_key: _Optional[_Union[ArInfo.ArPublicKey, _Mapping]] = ...) -> None: ...

class IpIdentity(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class IpInfo(_message.Message):
    __slots__ = ("identity", "description", "verify_key", "cdi_verify_key")
    class IpVerifyKey(_message.Message):
        __slots__ = ("value",)
        VALUE_FIELD_NUMBER: _ClassVar[int]
        value: bytes
        def __init__(self, value: _Optional[bytes] = ...) -> None: ...
    class IpCdiVerifyKey(_message.Message):
        __slots__ = ("value",)
        VALUE_FIELD_NUMBER: _ClassVar[int]
        value: bytes
        def __init__(self, value: _Optional[bytes] = ...) -> None: ...
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    VERIFY_KEY_FIELD_NUMBER: _ClassVar[int]
    CDI_VERIFY_KEY_FIELD_NUMBER: _ClassVar[int]
    identity: IpIdentity
    description: Description
    verify_key: IpInfo.IpVerifyKey
    cdi_verify_key: IpInfo.IpCdiVerifyKey
    def __init__(self, identity: _Optional[_Union[IpIdentity, _Mapping]] = ..., description: _Optional[_Union[Description, _Mapping]] = ..., verify_key: _Optional[_Union[IpInfo.IpVerifyKey, _Mapping]] = ..., cdi_verify_key: _Optional[_Union[IpInfo.IpCdiVerifyKey, _Mapping]] = ...) -> None: ...

class DurationSeconds(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class InclusiveRangeAmountFraction(_message.Message):
    __slots__ = ("min", "max_")
    MIN_FIELD_NUMBER: _ClassVar[int]
    MAX__FIELD_NUMBER: _ClassVar[int]
    min: AmountFraction
    max_: AmountFraction
    def __init__(self, min: _Optional[_Union[AmountFraction, _Mapping]] = ..., max_: _Optional[_Union[AmountFraction, _Mapping]] = ...) -> None: ...

class CommissionRanges(_message.Message):
    __slots__ = ("finalization", "baking", "transaction")
    FINALIZATION_FIELD_NUMBER: _ClassVar[int]
    BAKING_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_FIELD_NUMBER: _ClassVar[int]
    finalization: InclusiveRangeAmountFraction
    baking: InclusiveRangeAmountFraction
    transaction: InclusiveRangeAmountFraction
    def __init__(self, finalization: _Optional[_Union[InclusiveRangeAmountFraction, _Mapping]] = ..., baking: _Optional[_Union[InclusiveRangeAmountFraction, _Mapping]] = ..., transaction: _Optional[_Union[InclusiveRangeAmountFraction, _Mapping]] = ...) -> None: ...

class CapitalBound(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: AmountFraction
    def __init__(self, value: _Optional[_Union[AmountFraction, _Mapping]] = ...) -> None: ...

class LeverageFactor(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: Ratio
    def __init__(self, value: _Optional[_Union[Ratio, _Mapping]] = ...) -> None: ...

class Epoch(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class Round(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class RewardPeriodLength(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: Epoch
    def __init__(self, value: _Optional[_Union[Epoch, _Mapping]] = ...) -> None: ...

class MintRate(_message.Message):
    __slots__ = ("mantissa", "exponent")
    MANTISSA_FIELD_NUMBER: _ClassVar[int]
    EXPONENT_FIELD_NUMBER: _ClassVar[int]
    mantissa: int
    exponent: int
    def __init__(self, mantissa: _Optional[int] = ..., exponent: _Optional[int] = ...) -> None: ...

class CooldownParametersCpv1(_message.Message):
    __slots__ = ("pool_owner_cooldown", "delegator_cooldown")
    POOL_OWNER_COOLDOWN_FIELD_NUMBER: _ClassVar[int]
    DELEGATOR_COOLDOWN_FIELD_NUMBER: _ClassVar[int]
    pool_owner_cooldown: DurationSeconds
    delegator_cooldown: DurationSeconds
    def __init__(self, pool_owner_cooldown: _Optional[_Union[DurationSeconds, _Mapping]] = ..., delegator_cooldown: _Optional[_Union[DurationSeconds, _Mapping]] = ...) -> None: ...

class PoolParametersCpv1(_message.Message):
    __slots__ = ("passive_finalization_commission", "passive_baking_commission", "passive_transaction_commission", "commission_bounds", "minimum_equity_capital", "capital_bound", "leverage_bound")
    PASSIVE_FINALIZATION_COMMISSION_FIELD_NUMBER: _ClassVar[int]
    PASSIVE_BAKING_COMMISSION_FIELD_NUMBER: _ClassVar[int]
    PASSIVE_TRANSACTION_COMMISSION_FIELD_NUMBER: _ClassVar[int]
    COMMISSION_BOUNDS_FIELD_NUMBER: _ClassVar[int]
    MINIMUM_EQUITY_CAPITAL_FIELD_NUMBER: _ClassVar[int]
    CAPITAL_BOUND_FIELD_NUMBER: _ClassVar[int]
    LEVERAGE_BOUND_FIELD_NUMBER: _ClassVar[int]
    passive_finalization_commission: AmountFraction
    passive_baking_commission: AmountFraction
    passive_transaction_commission: AmountFraction
    commission_bounds: CommissionRanges
    minimum_equity_capital: Amount
    capital_bound: CapitalBound
    leverage_bound: LeverageFactor
    def __init__(self, passive_finalization_commission: _Optional[_Union[AmountFraction, _Mapping]] = ..., passive_baking_commission: _Optional[_Union[AmountFraction, _Mapping]] = ..., passive_transaction_commission: _Optional[_Union[AmountFraction, _Mapping]] = ..., commission_bounds: _Optional[_Union[CommissionRanges, _Mapping]] = ..., minimum_equity_capital: _Optional[_Union[Amount, _Mapping]] = ..., capital_bound: _Optional[_Union[CapitalBound, _Mapping]] = ..., leverage_bound: _Optional[_Union[LeverageFactor, _Mapping]] = ...) -> None: ...

class TimeParametersCpv1(_message.Message):
    __slots__ = ("reward_period_length", "mint_per_payday")
    REWARD_PERIOD_LENGTH_FIELD_NUMBER: _ClassVar[int]
    MINT_PER_PAYDAY_FIELD_NUMBER: _ClassVar[int]
    reward_period_length: RewardPeriodLength
    mint_per_payday: MintRate
    def __init__(self, reward_period_length: _Optional[_Union[RewardPeriodLength, _Mapping]] = ..., mint_per_payday: _Optional[_Union[MintRate, _Mapping]] = ...) -> None: ...

class MintDistributionCpv1(_message.Message):
    __slots__ = ("baking_reward", "finalization_reward")
    BAKING_REWARD_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_REWARD_FIELD_NUMBER: _ClassVar[int]
    baking_reward: AmountFraction
    finalization_reward: AmountFraction
    def __init__(self, baking_reward: _Optional[_Union[AmountFraction, _Mapping]] = ..., finalization_reward: _Optional[_Union[AmountFraction, _Mapping]] = ...) -> None: ...

class ProtocolUpdate(_message.Message):
    __slots__ = ("message_", "specification_url", "specificationHash", "specification_auxiliary_data")
    MESSAGE__FIELD_NUMBER: _ClassVar[int]
    SPECIFICATION_URL_FIELD_NUMBER: _ClassVar[int]
    SPECIFICATIONHASH_FIELD_NUMBER: _ClassVar[int]
    SPECIFICATION_AUXILIARY_DATA_FIELD_NUMBER: _ClassVar[int]
    message_: str
    specification_url: str
    specificationHash: Sha256Hash
    specification_auxiliary_data: bytes
    def __init__(self, message_: _Optional[str] = ..., specification_url: _Optional[str] = ..., specificationHash: _Optional[_Union[Sha256Hash, _Mapping]] = ..., specification_auxiliary_data: _Optional[bytes] = ...) -> None: ...

class MintDistributionCpv0(_message.Message):
    __slots__ = ("mint_per_slot", "baking_reward", "finalization_reward")
    MINT_PER_SLOT_FIELD_NUMBER: _ClassVar[int]
    BAKING_REWARD_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_REWARD_FIELD_NUMBER: _ClassVar[int]
    mint_per_slot: MintRate
    baking_reward: AmountFraction
    finalization_reward: AmountFraction
    def __init__(self, mint_per_slot: _Optional[_Union[MintRate, _Mapping]] = ..., baking_reward: _Optional[_Union[AmountFraction, _Mapping]] = ..., finalization_reward: _Optional[_Union[AmountFraction, _Mapping]] = ...) -> None: ...

class TransactionFeeDistribution(_message.Message):
    __slots__ = ("baker", "gas_account")
    BAKER_FIELD_NUMBER: _ClassVar[int]
    GAS_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    baker: AmountFraction
    gas_account: AmountFraction
    def __init__(self, baker: _Optional[_Union[AmountFraction, _Mapping]] = ..., gas_account: _Optional[_Union[AmountFraction, _Mapping]] = ...) -> None: ...

class GasRewards(_message.Message):
    __slots__ = ("baker", "finalization_proof", "account_creation", "chain_update")
    BAKER_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_PROOF_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_CREATION_FIELD_NUMBER: _ClassVar[int]
    CHAIN_UPDATE_FIELD_NUMBER: _ClassVar[int]
    baker: AmountFraction
    finalization_proof: AmountFraction
    account_creation: AmountFraction
    chain_update: AmountFraction
    def __init__(self, baker: _Optional[_Union[AmountFraction, _Mapping]] = ..., finalization_proof: _Optional[_Union[AmountFraction, _Mapping]] = ..., account_creation: _Optional[_Union[AmountFraction, _Mapping]] = ..., chain_update: _Optional[_Union[AmountFraction, _Mapping]] = ...) -> None: ...

class GasRewardsCpv2(_message.Message):
    __slots__ = ("baker", "account_creation", "chain_update")
    BAKER_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_CREATION_FIELD_NUMBER: _ClassVar[int]
    CHAIN_UPDATE_FIELD_NUMBER: _ClassVar[int]
    baker: AmountFraction
    account_creation: AmountFraction
    chain_update: AmountFraction
    def __init__(self, baker: _Optional[_Union[AmountFraction, _Mapping]] = ..., account_creation: _Optional[_Union[AmountFraction, _Mapping]] = ..., chain_update: _Optional[_Union[AmountFraction, _Mapping]] = ...) -> None: ...

class BakerStakeThreshold(_message.Message):
    __slots__ = ("baker_stake_threshold",)
    BAKER_STAKE_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    baker_stake_threshold: Amount
    def __init__(self, baker_stake_threshold: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...

class RootUpdate(_message.Message):
    __slots__ = ("root_keys_update", "level_1_keys_update", "level_2_keys_update_v0", "level_2_keys_update_v1")
    ROOT_KEYS_UPDATE_FIELD_NUMBER: _ClassVar[int]
    LEVEL_1_KEYS_UPDATE_FIELD_NUMBER: _ClassVar[int]
    LEVEL_2_KEYS_UPDATE_V0_FIELD_NUMBER: _ClassVar[int]
    LEVEL_2_KEYS_UPDATE_V1_FIELD_NUMBER: _ClassVar[int]
    root_keys_update: HigherLevelKeys
    level_1_keys_update: HigherLevelKeys
    level_2_keys_update_v0: AuthorizationsV0
    level_2_keys_update_v1: AuthorizationsV1
    def __init__(self, root_keys_update: _Optional[_Union[HigherLevelKeys, _Mapping]] = ..., level_1_keys_update: _Optional[_Union[HigherLevelKeys, _Mapping]] = ..., level_2_keys_update_v0: _Optional[_Union[AuthorizationsV0, _Mapping]] = ..., level_2_keys_update_v1: _Optional[_Union[AuthorizationsV1, _Mapping]] = ...) -> None: ...

class Level1Update(_message.Message):
    __slots__ = ("level_1_keys_update", "level_2_keys_update_v0", "level_2_keys_update_v1")
    LEVEL_1_KEYS_UPDATE_FIELD_NUMBER: _ClassVar[int]
    LEVEL_2_KEYS_UPDATE_V0_FIELD_NUMBER: _ClassVar[int]
    LEVEL_2_KEYS_UPDATE_V1_FIELD_NUMBER: _ClassVar[int]
    level_1_keys_update: HigherLevelKeys
    level_2_keys_update_v0: AuthorizationsV0
    level_2_keys_update_v1: AuthorizationsV1
    def __init__(self, level_1_keys_update: _Optional[_Union[HigherLevelKeys, _Mapping]] = ..., level_2_keys_update_v0: _Optional[_Union[AuthorizationsV0, _Mapping]] = ..., level_2_keys_update_v1: _Optional[_Union[AuthorizationsV1, _Mapping]] = ...) -> None: ...

class UpdatePayload(_message.Message):
    __slots__ = ("protocol_update", "election_difficulty_update", "euro_per_energy_update", "micro_ccd_per_euro_update", "foundation_account_update", "mint_distribution_update", "transaction_fee_distribution_update", "gas_rewards_update", "baker_stake_threshold_update", "root_update", "level_1_update", "add_anonymity_revoker_update", "add_identity_provider_update", "cooldown_parameters_cpv_1_update", "pool_parameters_cpv_1_update", "time_parameters_cpv_1_update", "mint_distribution_cpv_1_update", "gas_rewards_cpv_2_update", "timeout_parameters_update", "min_block_time_update", "block_energy_limit_update", "finalization_committee_parameters_update", "validator_score_parameters_update", "create_plt_update")
    PROTOCOL_UPDATE_FIELD_NUMBER: _ClassVar[int]
    ELECTION_DIFFICULTY_UPDATE_FIELD_NUMBER: _ClassVar[int]
    EURO_PER_ENERGY_UPDATE_FIELD_NUMBER: _ClassVar[int]
    MICRO_CCD_PER_EURO_UPDATE_FIELD_NUMBER: _ClassVar[int]
    FOUNDATION_ACCOUNT_UPDATE_FIELD_NUMBER: _ClassVar[int]
    MINT_DISTRIBUTION_UPDATE_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_FEE_DISTRIBUTION_UPDATE_FIELD_NUMBER: _ClassVar[int]
    GAS_REWARDS_UPDATE_FIELD_NUMBER: _ClassVar[int]
    BAKER_STAKE_THRESHOLD_UPDATE_FIELD_NUMBER: _ClassVar[int]
    ROOT_UPDATE_FIELD_NUMBER: _ClassVar[int]
    LEVEL_1_UPDATE_FIELD_NUMBER: _ClassVar[int]
    ADD_ANONYMITY_REVOKER_UPDATE_FIELD_NUMBER: _ClassVar[int]
    ADD_IDENTITY_PROVIDER_UPDATE_FIELD_NUMBER: _ClassVar[int]
    COOLDOWN_PARAMETERS_CPV_1_UPDATE_FIELD_NUMBER: _ClassVar[int]
    POOL_PARAMETERS_CPV_1_UPDATE_FIELD_NUMBER: _ClassVar[int]
    TIME_PARAMETERS_CPV_1_UPDATE_FIELD_NUMBER: _ClassVar[int]
    MINT_DISTRIBUTION_CPV_1_UPDATE_FIELD_NUMBER: _ClassVar[int]
    GAS_REWARDS_CPV_2_UPDATE_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_PARAMETERS_UPDATE_FIELD_NUMBER: _ClassVar[int]
    MIN_BLOCK_TIME_UPDATE_FIELD_NUMBER: _ClassVar[int]
    BLOCK_ENERGY_LIMIT_UPDATE_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_COMMITTEE_PARAMETERS_UPDATE_FIELD_NUMBER: _ClassVar[int]
    VALIDATOR_SCORE_PARAMETERS_UPDATE_FIELD_NUMBER: _ClassVar[int]
    CREATE_PLT_UPDATE_FIELD_NUMBER: _ClassVar[int]
    protocol_update: ProtocolUpdate
    election_difficulty_update: ElectionDifficulty
    euro_per_energy_update: ExchangeRate
    micro_ccd_per_euro_update: ExchangeRate
    foundation_account_update: _kernel_pb2.AccountAddress
    mint_distribution_update: MintDistributionCpv0
    transaction_fee_distribution_update: TransactionFeeDistribution
    gas_rewards_update: GasRewards
    baker_stake_threshold_update: BakerStakeThreshold
    root_update: RootUpdate
    level_1_update: Level1Update
    add_anonymity_revoker_update: ArInfo
    add_identity_provider_update: IpInfo
    cooldown_parameters_cpv_1_update: CooldownParametersCpv1
    pool_parameters_cpv_1_update: PoolParametersCpv1
    time_parameters_cpv_1_update: TimeParametersCpv1
    mint_distribution_cpv_1_update: MintDistributionCpv1
    gas_rewards_cpv_2_update: GasRewardsCpv2
    timeout_parameters_update: TimeoutParameters
    min_block_time_update: Duration
    block_energy_limit_update: Energy
    finalization_committee_parameters_update: FinalizationCommitteeParameters
    validator_score_parameters_update: ValidatorScoreParameters
    create_plt_update: _protocol_level_tokens_pb2.CreatePLT
    def __init__(self, protocol_update: _Optional[_Union[ProtocolUpdate, _Mapping]] = ..., election_difficulty_update: _Optional[_Union[ElectionDifficulty, _Mapping]] = ..., euro_per_energy_update: _Optional[_Union[ExchangeRate, _Mapping]] = ..., micro_ccd_per_euro_update: _Optional[_Union[ExchangeRate, _Mapping]] = ..., foundation_account_update: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., mint_distribution_update: _Optional[_Union[MintDistributionCpv0, _Mapping]] = ..., transaction_fee_distribution_update: _Optional[_Union[TransactionFeeDistribution, _Mapping]] = ..., gas_rewards_update: _Optional[_Union[GasRewards, _Mapping]] = ..., baker_stake_threshold_update: _Optional[_Union[BakerStakeThreshold, _Mapping]] = ..., root_update: _Optional[_Union[RootUpdate, _Mapping]] = ..., level_1_update: _Optional[_Union[Level1Update, _Mapping]] = ..., add_anonymity_revoker_update: _Optional[_Union[ArInfo, _Mapping]] = ..., add_identity_provider_update: _Optional[_Union[IpInfo, _Mapping]] = ..., cooldown_parameters_cpv_1_update: _Optional[_Union[CooldownParametersCpv1, _Mapping]] = ..., pool_parameters_cpv_1_update: _Optional[_Union[PoolParametersCpv1, _Mapping]] = ..., time_parameters_cpv_1_update: _Optional[_Union[TimeParametersCpv1, _Mapping]] = ..., mint_distribution_cpv_1_update: _Optional[_Union[MintDistributionCpv1, _Mapping]] = ..., gas_rewards_cpv_2_update: _Optional[_Union[GasRewardsCpv2, _Mapping]] = ..., timeout_parameters_update: _Optional[_Union[TimeoutParameters, _Mapping]] = ..., min_block_time_update: _Optional[_Union[Duration, _Mapping]] = ..., block_energy_limit_update: _Optional[_Union[Energy, _Mapping]] = ..., finalization_committee_parameters_update: _Optional[_Union[FinalizationCommitteeParameters, _Mapping]] = ..., validator_score_parameters_update: _Optional[_Union[ValidatorScoreParameters, _Mapping]] = ..., create_plt_update: _Optional[_Union[_protocol_level_tokens_pb2.CreatePLT, _Mapping]] = ...) -> None: ...

class AccountTransactionDetails(_message.Message):
    __slots__ = ("cost", "sender", "effects")
    COST_FIELD_NUMBER: _ClassVar[int]
    SENDER_FIELD_NUMBER: _ClassVar[int]
    EFFECTS_FIELD_NUMBER: _ClassVar[int]
    cost: Amount
    sender: _kernel_pb2.AccountAddress
    effects: AccountTransactionEffects
    def __init__(self, cost: _Optional[_Union[Amount, _Mapping]] = ..., sender: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., effects: _Optional[_Union[AccountTransactionEffects, _Mapping]] = ...) -> None: ...

class AccountCreationDetails(_message.Message):
    __slots__ = ("credential_type", "address", "reg_id")
    CREDENTIAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    REG_ID_FIELD_NUMBER: _ClassVar[int]
    credential_type: CredentialType
    address: _kernel_pb2.AccountAddress
    reg_id: CredentialRegistrationId
    def __init__(self, credential_type: _Optional[_Union[CredentialType, str]] = ..., address: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., reg_id: _Optional[_Union[CredentialRegistrationId, _Mapping]] = ...) -> None: ...

class TransactionTime(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class UpdateDetails(_message.Message):
    __slots__ = ("effective_time", "payload")
    EFFECTIVE_TIME_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    effective_time: TransactionTime
    payload: UpdatePayload
    def __init__(self, effective_time: _Optional[_Union[TransactionTime, _Mapping]] = ..., payload: _Optional[_Union[UpdatePayload, _Mapping]] = ...) -> None: ...

class BlockItemSummary(_message.Message):
    __slots__ = ("index", "energy_cost", "hash", "account_transaction", "account_creation", "update", "token_creation")
    class TransactionIndex(_message.Message):
        __slots__ = ("value",)
        VALUE_FIELD_NUMBER: _ClassVar[int]
        value: int
        def __init__(self, value: _Optional[int] = ...) -> None: ...
    INDEX_FIELD_NUMBER: _ClassVar[int]
    ENERGY_COST_FIELD_NUMBER: _ClassVar[int]
    HASH_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_TRANSACTION_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_CREATION_FIELD_NUMBER: _ClassVar[int]
    UPDATE_FIELD_NUMBER: _ClassVar[int]
    TOKEN_CREATION_FIELD_NUMBER: _ClassVar[int]
    index: BlockItemSummary.TransactionIndex
    energy_cost: Energy
    hash: TransactionHash
    account_transaction: AccountTransactionDetails
    account_creation: AccountCreationDetails
    update: UpdateDetails
    token_creation: _protocol_level_tokens_pb2.TokenCreationDetails
    def __init__(self, index: _Optional[_Union[BlockItemSummary.TransactionIndex, _Mapping]] = ..., energy_cost: _Optional[_Union[Energy, _Mapping]] = ..., hash: _Optional[_Union[TransactionHash, _Mapping]] = ..., account_transaction: _Optional[_Union[AccountTransactionDetails, _Mapping]] = ..., account_creation: _Optional[_Union[AccountCreationDetails, _Mapping]] = ..., update: _Optional[_Union[UpdateDetails, _Mapping]] = ..., token_creation: _Optional[_Union[_protocol_level_tokens_pb2.TokenCreationDetails, _Mapping]] = ...) -> None: ...

class GenesisIndex(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class ConsensusInfo(_message.Message):
    __slots__ = ("best_block", "genesis_block", "genesis_time", "slot_duration", "epoch_duration", "last_finalized_block", "best_block_height", "last_finalized_block_height", "blocks_received_count", "block_last_received_time", "block_receive_latency_ema", "block_receive_latency_emsd", "block_receive_period_ema", "block_receive_period_emsd", "blocks_verified_count", "block_last_arrived_time", "block_arrive_latency_ema", "block_arrive_latency_emsd", "block_arrive_period_ema", "block_arrive_period_emsd", "transactions_per_block_ema", "transactions_per_block_emsd", "finalization_count", "last_finalized_time", "finalization_period_ema", "finalization_period_emsd", "protocol_version", "genesis_index", "current_era_genesis_block", "current_era_genesis_time", "current_timeout_duration", "current_round", "current_epoch", "trigger_block_time")
    BEST_BLOCK_FIELD_NUMBER: _ClassVar[int]
    GENESIS_BLOCK_FIELD_NUMBER: _ClassVar[int]
    GENESIS_TIME_FIELD_NUMBER: _ClassVar[int]
    SLOT_DURATION_FIELD_NUMBER: _ClassVar[int]
    EPOCH_DURATION_FIELD_NUMBER: _ClassVar[int]
    LAST_FINALIZED_BLOCK_FIELD_NUMBER: _ClassVar[int]
    BEST_BLOCK_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    LAST_FINALIZED_BLOCK_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    BLOCKS_RECEIVED_COUNT_FIELD_NUMBER: _ClassVar[int]
    BLOCK_LAST_RECEIVED_TIME_FIELD_NUMBER: _ClassVar[int]
    BLOCK_RECEIVE_LATENCY_EMA_FIELD_NUMBER: _ClassVar[int]
    BLOCK_RECEIVE_LATENCY_EMSD_FIELD_NUMBER: _ClassVar[int]
    BLOCK_RECEIVE_PERIOD_EMA_FIELD_NUMBER: _ClassVar[int]
    BLOCK_RECEIVE_PERIOD_EMSD_FIELD_NUMBER: _ClassVar[int]
    BLOCKS_VERIFIED_COUNT_FIELD_NUMBER: _ClassVar[int]
    BLOCK_LAST_ARRIVED_TIME_FIELD_NUMBER: _ClassVar[int]
    BLOCK_ARRIVE_LATENCY_EMA_FIELD_NUMBER: _ClassVar[int]
    BLOCK_ARRIVE_LATENCY_EMSD_FIELD_NUMBER: _ClassVar[int]
    BLOCK_ARRIVE_PERIOD_EMA_FIELD_NUMBER: _ClassVar[int]
    BLOCK_ARRIVE_PERIOD_EMSD_FIELD_NUMBER: _ClassVar[int]
    TRANSACTIONS_PER_BLOCK_EMA_FIELD_NUMBER: _ClassVar[int]
    TRANSACTIONS_PER_BLOCK_EMSD_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_COUNT_FIELD_NUMBER: _ClassVar[int]
    LAST_FINALIZED_TIME_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_PERIOD_EMA_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_PERIOD_EMSD_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    GENESIS_INDEX_FIELD_NUMBER: _ClassVar[int]
    CURRENT_ERA_GENESIS_BLOCK_FIELD_NUMBER: _ClassVar[int]
    CURRENT_ERA_GENESIS_TIME_FIELD_NUMBER: _ClassVar[int]
    CURRENT_TIMEOUT_DURATION_FIELD_NUMBER: _ClassVar[int]
    CURRENT_ROUND_FIELD_NUMBER: _ClassVar[int]
    CURRENT_EPOCH_FIELD_NUMBER: _ClassVar[int]
    TRIGGER_BLOCK_TIME_FIELD_NUMBER: _ClassVar[int]
    best_block: BlockHash
    genesis_block: BlockHash
    genesis_time: Timestamp
    slot_duration: Duration
    epoch_duration: Duration
    last_finalized_block: BlockHash
    best_block_height: AbsoluteBlockHeight
    last_finalized_block_height: AbsoluteBlockHeight
    blocks_received_count: int
    block_last_received_time: Timestamp
    block_receive_latency_ema: float
    block_receive_latency_emsd: float
    block_receive_period_ema: float
    block_receive_period_emsd: float
    blocks_verified_count: int
    block_last_arrived_time: Timestamp
    block_arrive_latency_ema: float
    block_arrive_latency_emsd: float
    block_arrive_period_ema: float
    block_arrive_period_emsd: float
    transactions_per_block_ema: float
    transactions_per_block_emsd: float
    finalization_count: int
    last_finalized_time: Timestamp
    finalization_period_ema: float
    finalization_period_emsd: float
    protocol_version: ProtocolVersion
    genesis_index: GenesisIndex
    current_era_genesis_block: BlockHash
    current_era_genesis_time: Timestamp
    current_timeout_duration: Duration
    current_round: Round
    current_epoch: Epoch
    trigger_block_time: Timestamp
    def __init__(self, best_block: _Optional[_Union[BlockHash, _Mapping]] = ..., genesis_block: _Optional[_Union[BlockHash, _Mapping]] = ..., genesis_time: _Optional[_Union[Timestamp, _Mapping]] = ..., slot_duration: _Optional[_Union[Duration, _Mapping]] = ..., epoch_duration: _Optional[_Union[Duration, _Mapping]] = ..., last_finalized_block: _Optional[_Union[BlockHash, _Mapping]] = ..., best_block_height: _Optional[_Union[AbsoluteBlockHeight, _Mapping]] = ..., last_finalized_block_height: _Optional[_Union[AbsoluteBlockHeight, _Mapping]] = ..., blocks_received_count: _Optional[int] = ..., block_last_received_time: _Optional[_Union[Timestamp, _Mapping]] = ..., block_receive_latency_ema: _Optional[float] = ..., block_receive_latency_emsd: _Optional[float] = ..., block_receive_period_ema: _Optional[float] = ..., block_receive_period_emsd: _Optional[float] = ..., blocks_verified_count: _Optional[int] = ..., block_last_arrived_time: _Optional[_Union[Timestamp, _Mapping]] = ..., block_arrive_latency_ema: _Optional[float] = ..., block_arrive_latency_emsd: _Optional[float] = ..., block_arrive_period_ema: _Optional[float] = ..., block_arrive_period_emsd: _Optional[float] = ..., transactions_per_block_ema: _Optional[float] = ..., transactions_per_block_emsd: _Optional[float] = ..., finalization_count: _Optional[int] = ..., last_finalized_time: _Optional[_Union[Timestamp, _Mapping]] = ..., finalization_period_ema: _Optional[float] = ..., finalization_period_emsd: _Optional[float] = ..., protocol_version: _Optional[_Union[ProtocolVersion, str]] = ..., genesis_index: _Optional[_Union[GenesisIndex, _Mapping]] = ..., current_era_genesis_block: _Optional[_Union[BlockHash, _Mapping]] = ..., current_era_genesis_time: _Optional[_Union[Timestamp, _Mapping]] = ..., current_timeout_duration: _Optional[_Union[Duration, _Mapping]] = ..., current_round: _Optional[_Union[Round, _Mapping]] = ..., current_epoch: _Optional[_Union[Epoch, _Mapping]] = ..., trigger_block_time: _Optional[_Union[Timestamp, _Mapping]] = ...) -> None: ...

class ArrivedBlockInfo(_message.Message):
    __slots__ = ("hash", "height")
    HASH_FIELD_NUMBER: _ClassVar[int]
    HEIGHT_FIELD_NUMBER: _ClassVar[int]
    hash: BlockHash
    height: AbsoluteBlockHeight
    def __init__(self, hash: _Optional[_Union[BlockHash, _Mapping]] = ..., height: _Optional[_Union[AbsoluteBlockHeight, _Mapping]] = ...) -> None: ...

class CryptographicParameters(_message.Message):
    __slots__ = ("genesis_string", "bulletproof_generators", "on_chain_commitment_key")
    GENESIS_STRING_FIELD_NUMBER: _ClassVar[int]
    BULLETPROOF_GENERATORS_FIELD_NUMBER: _ClassVar[int]
    ON_CHAIN_COMMITMENT_KEY_FIELD_NUMBER: _ClassVar[int]
    genesis_string: str
    bulletproof_generators: bytes
    on_chain_commitment_key: bytes
    def __init__(self, genesis_string: _Optional[str] = ..., bulletproof_generators: _Optional[bytes] = ..., on_chain_commitment_key: _Optional[bytes] = ...) -> None: ...

class BlockInfo(_message.Message):
    __slots__ = ("hash", "height", "parent_block", "last_finalized_block", "genesis_index", "era_block_height", "receive_time", "arrive_time", "slot_number", "slot_time", "baker", "finalized", "transaction_count", "transactions_energy_cost", "transactions_size", "state_hash", "protocol_version", "round", "epoch")
    HASH_FIELD_NUMBER: _ClassVar[int]
    HEIGHT_FIELD_NUMBER: _ClassVar[int]
    PARENT_BLOCK_FIELD_NUMBER: _ClassVar[int]
    LAST_FINALIZED_BLOCK_FIELD_NUMBER: _ClassVar[int]
    GENESIS_INDEX_FIELD_NUMBER: _ClassVar[int]
    ERA_BLOCK_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    RECEIVE_TIME_FIELD_NUMBER: _ClassVar[int]
    ARRIVE_TIME_FIELD_NUMBER: _ClassVar[int]
    SLOT_NUMBER_FIELD_NUMBER: _ClassVar[int]
    SLOT_TIME_FIELD_NUMBER: _ClassVar[int]
    BAKER_FIELD_NUMBER: _ClassVar[int]
    FINALIZED_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_COUNT_FIELD_NUMBER: _ClassVar[int]
    TRANSACTIONS_ENERGY_COST_FIELD_NUMBER: _ClassVar[int]
    TRANSACTIONS_SIZE_FIELD_NUMBER: _ClassVar[int]
    STATE_HASH_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    ROUND_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    hash: BlockHash
    height: AbsoluteBlockHeight
    parent_block: BlockHash
    last_finalized_block: BlockHash
    genesis_index: GenesisIndex
    era_block_height: BlockHeight
    receive_time: Timestamp
    arrive_time: Timestamp
    slot_number: Slot
    slot_time: Timestamp
    baker: BakerId
    finalized: bool
    transaction_count: int
    transactions_energy_cost: Energy
    transactions_size: int
    state_hash: StateHash
    protocol_version: ProtocolVersion
    round: Round
    epoch: Epoch
    def __init__(self, hash: _Optional[_Union[BlockHash, _Mapping]] = ..., height: _Optional[_Union[AbsoluteBlockHeight, _Mapping]] = ..., parent_block: _Optional[_Union[BlockHash, _Mapping]] = ..., last_finalized_block: _Optional[_Union[BlockHash, _Mapping]] = ..., genesis_index: _Optional[_Union[GenesisIndex, _Mapping]] = ..., era_block_height: _Optional[_Union[BlockHeight, _Mapping]] = ..., receive_time: _Optional[_Union[Timestamp, _Mapping]] = ..., arrive_time: _Optional[_Union[Timestamp, _Mapping]] = ..., slot_number: _Optional[_Union[Slot, _Mapping]] = ..., slot_time: _Optional[_Union[Timestamp, _Mapping]] = ..., baker: _Optional[_Union[BakerId, _Mapping]] = ..., finalized: bool = ..., transaction_count: _Optional[int] = ..., transactions_energy_cost: _Optional[_Union[Energy, _Mapping]] = ..., transactions_size: _Optional[int] = ..., state_hash: _Optional[_Union[StateHash, _Mapping]] = ..., protocol_version: _Optional[_Union[ProtocolVersion, str]] = ..., round: _Optional[_Union[Round, _Mapping]] = ..., epoch: _Optional[_Union[Epoch, _Mapping]] = ...) -> None: ...

class PoolInfoRequest(_message.Message):
    __slots__ = ("block_hash", "baker")
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    BAKER_FIELD_NUMBER: _ClassVar[int]
    block_hash: BlockHashInput
    baker: BakerId
    def __init__(self, block_hash: _Optional[_Union[BlockHashInput, _Mapping]] = ..., baker: _Optional[_Union[BakerId, _Mapping]] = ...) -> None: ...

class PoolPendingChange(_message.Message):
    __slots__ = ("reduce", "remove")
    class Reduce(_message.Message):
        __slots__ = ("reduced_equity_capital", "effective_time")
        REDUCED_EQUITY_CAPITAL_FIELD_NUMBER: _ClassVar[int]
        EFFECTIVE_TIME_FIELD_NUMBER: _ClassVar[int]
        reduced_equity_capital: Amount
        effective_time: Timestamp
        def __init__(self, reduced_equity_capital: _Optional[_Union[Amount, _Mapping]] = ..., effective_time: _Optional[_Union[Timestamp, _Mapping]] = ...) -> None: ...
    class Remove(_message.Message):
        __slots__ = ("effective_time",)
        EFFECTIVE_TIME_FIELD_NUMBER: _ClassVar[int]
        effective_time: Timestamp
        def __init__(self, effective_time: _Optional[_Union[Timestamp, _Mapping]] = ...) -> None: ...
    REDUCE_FIELD_NUMBER: _ClassVar[int]
    REMOVE_FIELD_NUMBER: _ClassVar[int]
    reduce: PoolPendingChange.Reduce
    remove: PoolPendingChange.Remove
    def __init__(self, reduce: _Optional[_Union[PoolPendingChange.Reduce, _Mapping]] = ..., remove: _Optional[_Union[PoolPendingChange.Remove, _Mapping]] = ...) -> None: ...

class PoolCurrentPaydayInfo(_message.Message):
    __slots__ = ("blocks_baked", "finalization_live", "transaction_fees_earned", "effective_stake", "lottery_power", "baker_equity_capital", "delegated_capital", "commission_rates", "is_primed_for_suspension", "missed_rounds")
    BLOCKS_BAKED_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_LIVE_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_FEES_EARNED_FIELD_NUMBER: _ClassVar[int]
    EFFECTIVE_STAKE_FIELD_NUMBER: _ClassVar[int]
    LOTTERY_POWER_FIELD_NUMBER: _ClassVar[int]
    BAKER_EQUITY_CAPITAL_FIELD_NUMBER: _ClassVar[int]
    DELEGATED_CAPITAL_FIELD_NUMBER: _ClassVar[int]
    COMMISSION_RATES_FIELD_NUMBER: _ClassVar[int]
    IS_PRIMED_FOR_SUSPENSION_FIELD_NUMBER: _ClassVar[int]
    MISSED_ROUNDS_FIELD_NUMBER: _ClassVar[int]
    blocks_baked: int
    finalization_live: bool
    transaction_fees_earned: Amount
    effective_stake: Amount
    lottery_power: float
    baker_equity_capital: Amount
    delegated_capital: Amount
    commission_rates: CommissionRates
    is_primed_for_suspension: bool
    missed_rounds: int
    def __init__(self, blocks_baked: _Optional[int] = ..., finalization_live: bool = ..., transaction_fees_earned: _Optional[_Union[Amount, _Mapping]] = ..., effective_stake: _Optional[_Union[Amount, _Mapping]] = ..., lottery_power: _Optional[float] = ..., baker_equity_capital: _Optional[_Union[Amount, _Mapping]] = ..., delegated_capital: _Optional[_Union[Amount, _Mapping]] = ..., commission_rates: _Optional[_Union[CommissionRates, _Mapping]] = ..., is_primed_for_suspension: bool = ..., missed_rounds: _Optional[int] = ...) -> None: ...

class PoolInfoResponse(_message.Message):
    __slots__ = ("baker", "address", "equity_capital", "delegated_capital", "delegated_capital_cap", "pool_info", "equity_pending_change", "current_payday_info", "all_pool_total_capital", "is_suspended")
    BAKER_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    EQUITY_CAPITAL_FIELD_NUMBER: _ClassVar[int]
    DELEGATED_CAPITAL_FIELD_NUMBER: _ClassVar[int]
    DELEGATED_CAPITAL_CAP_FIELD_NUMBER: _ClassVar[int]
    POOL_INFO_FIELD_NUMBER: _ClassVar[int]
    EQUITY_PENDING_CHANGE_FIELD_NUMBER: _ClassVar[int]
    CURRENT_PAYDAY_INFO_FIELD_NUMBER: _ClassVar[int]
    ALL_POOL_TOTAL_CAPITAL_FIELD_NUMBER: _ClassVar[int]
    IS_SUSPENDED_FIELD_NUMBER: _ClassVar[int]
    baker: BakerId
    address: _kernel_pb2.AccountAddress
    equity_capital: Amount
    delegated_capital: Amount
    delegated_capital_cap: Amount
    pool_info: BakerPoolInfo
    equity_pending_change: PoolPendingChange
    current_payday_info: PoolCurrentPaydayInfo
    all_pool_total_capital: Amount
    is_suspended: bool
    def __init__(self, baker: _Optional[_Union[BakerId, _Mapping]] = ..., address: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., equity_capital: _Optional[_Union[Amount, _Mapping]] = ..., delegated_capital: _Optional[_Union[Amount, _Mapping]] = ..., delegated_capital_cap: _Optional[_Union[Amount, _Mapping]] = ..., pool_info: _Optional[_Union[BakerPoolInfo, _Mapping]] = ..., equity_pending_change: _Optional[_Union[PoolPendingChange, _Mapping]] = ..., current_payday_info: _Optional[_Union[PoolCurrentPaydayInfo, _Mapping]] = ..., all_pool_total_capital: _Optional[_Union[Amount, _Mapping]] = ..., is_suspended: bool = ...) -> None: ...

class PassiveDelegationInfo(_message.Message):
    __slots__ = ("delegated_capital", "commission_rates", "current_payday_transaction_fees_earned", "current_payday_delegated_capital", "all_pool_total_capital")
    DELEGATED_CAPITAL_FIELD_NUMBER: _ClassVar[int]
    COMMISSION_RATES_FIELD_NUMBER: _ClassVar[int]
    CURRENT_PAYDAY_TRANSACTION_FEES_EARNED_FIELD_NUMBER: _ClassVar[int]
    CURRENT_PAYDAY_DELEGATED_CAPITAL_FIELD_NUMBER: _ClassVar[int]
    ALL_POOL_TOTAL_CAPITAL_FIELD_NUMBER: _ClassVar[int]
    delegated_capital: Amount
    commission_rates: CommissionRates
    current_payday_transaction_fees_earned: Amount
    current_payday_delegated_capital: Amount
    all_pool_total_capital: Amount
    def __init__(self, delegated_capital: _Optional[_Union[Amount, _Mapping]] = ..., commission_rates: _Optional[_Union[CommissionRates, _Mapping]] = ..., current_payday_transaction_fees_earned: _Optional[_Union[Amount, _Mapping]] = ..., current_payday_delegated_capital: _Optional[_Union[Amount, _Mapping]] = ..., all_pool_total_capital: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...

class BlocksAtHeightRequest(_message.Message):
    __slots__ = ("absolute", "relative")
    class Absolute(_message.Message):
        __slots__ = ("height",)
        HEIGHT_FIELD_NUMBER: _ClassVar[int]
        height: AbsoluteBlockHeight
        def __init__(self, height: _Optional[_Union[AbsoluteBlockHeight, _Mapping]] = ...) -> None: ...
    class Relative(_message.Message):
        __slots__ = ("genesis_index", "height", "restrict")
        GENESIS_INDEX_FIELD_NUMBER: _ClassVar[int]
        HEIGHT_FIELD_NUMBER: _ClassVar[int]
        RESTRICT_FIELD_NUMBER: _ClassVar[int]
        genesis_index: GenesisIndex
        height: BlockHeight
        restrict: bool
        def __init__(self, genesis_index: _Optional[_Union[GenesisIndex, _Mapping]] = ..., height: _Optional[_Union[BlockHeight, _Mapping]] = ..., restrict: bool = ...) -> None: ...
    ABSOLUTE_FIELD_NUMBER: _ClassVar[int]
    RELATIVE_FIELD_NUMBER: _ClassVar[int]
    absolute: BlocksAtHeightRequest.Absolute
    relative: BlocksAtHeightRequest.Relative
    def __init__(self, absolute: _Optional[_Union[BlocksAtHeightRequest.Absolute, _Mapping]] = ..., relative: _Optional[_Union[BlocksAtHeightRequest.Relative, _Mapping]] = ...) -> None: ...

class BlocksAtHeightResponse(_message.Message):
    __slots__ = ("blocks",)
    BLOCKS_FIELD_NUMBER: _ClassVar[int]
    blocks: _containers.RepeatedCompositeFieldContainer[BlockHash]
    def __init__(self, blocks: _Optional[_Iterable[_Union[BlockHash, _Mapping]]] = ...) -> None: ...

class TokenomicsInfo(_message.Message):
    __slots__ = ("v0", "v1")
    class V0(_message.Message):
        __slots__ = ("total_amount", "total_encrypted_amount", "baking_reward_account", "finalization_reward_account", "gas_account", "protocol_version")
        TOTAL_AMOUNT_FIELD_NUMBER: _ClassVar[int]
        TOTAL_ENCRYPTED_AMOUNT_FIELD_NUMBER: _ClassVar[int]
        BAKING_REWARD_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        FINALIZATION_REWARD_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        GAS_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
        total_amount: Amount
        total_encrypted_amount: Amount
        baking_reward_account: Amount
        finalization_reward_account: Amount
        gas_account: Amount
        protocol_version: ProtocolVersion
        def __init__(self, total_amount: _Optional[_Union[Amount, _Mapping]] = ..., total_encrypted_amount: _Optional[_Union[Amount, _Mapping]] = ..., baking_reward_account: _Optional[_Union[Amount, _Mapping]] = ..., finalization_reward_account: _Optional[_Union[Amount, _Mapping]] = ..., gas_account: _Optional[_Union[Amount, _Mapping]] = ..., protocol_version: _Optional[_Union[ProtocolVersion, str]] = ...) -> None: ...
    class V1(_message.Message):
        __slots__ = ("total_amount", "total_encrypted_amount", "baking_reward_account", "finalization_reward_account", "gas_account", "foundation_transaction_rewards", "next_payday_time", "next_payday_mint_rate", "total_staked_capital", "protocol_version")
        TOTAL_AMOUNT_FIELD_NUMBER: _ClassVar[int]
        TOTAL_ENCRYPTED_AMOUNT_FIELD_NUMBER: _ClassVar[int]
        BAKING_REWARD_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        FINALIZATION_REWARD_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        GAS_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        FOUNDATION_TRANSACTION_REWARDS_FIELD_NUMBER: _ClassVar[int]
        NEXT_PAYDAY_TIME_FIELD_NUMBER: _ClassVar[int]
        NEXT_PAYDAY_MINT_RATE_FIELD_NUMBER: _ClassVar[int]
        TOTAL_STAKED_CAPITAL_FIELD_NUMBER: _ClassVar[int]
        PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
        total_amount: Amount
        total_encrypted_amount: Amount
        baking_reward_account: Amount
        finalization_reward_account: Amount
        gas_account: Amount
        foundation_transaction_rewards: Amount
        next_payday_time: Timestamp
        next_payday_mint_rate: MintRate
        total_staked_capital: Amount
        protocol_version: ProtocolVersion
        def __init__(self, total_amount: _Optional[_Union[Amount, _Mapping]] = ..., total_encrypted_amount: _Optional[_Union[Amount, _Mapping]] = ..., baking_reward_account: _Optional[_Union[Amount, _Mapping]] = ..., finalization_reward_account: _Optional[_Union[Amount, _Mapping]] = ..., gas_account: _Optional[_Union[Amount, _Mapping]] = ..., foundation_transaction_rewards: _Optional[_Union[Amount, _Mapping]] = ..., next_payday_time: _Optional[_Union[Timestamp, _Mapping]] = ..., next_payday_mint_rate: _Optional[_Union[MintRate, _Mapping]] = ..., total_staked_capital: _Optional[_Union[Amount, _Mapping]] = ..., protocol_version: _Optional[_Union[ProtocolVersion, str]] = ...) -> None: ...
    V0_FIELD_NUMBER: _ClassVar[int]
    V1_FIELD_NUMBER: _ClassVar[int]
    v0: TokenomicsInfo.V0
    v1: TokenomicsInfo.V1
    def __init__(self, v0: _Optional[_Union[TokenomicsInfo.V0, _Mapping]] = ..., v1: _Optional[_Union[TokenomicsInfo.V1, _Mapping]] = ...) -> None: ...

class InvokeInstanceRequest(_message.Message):
    __slots__ = ("block_hash", "invoker", "instance", "amount", "entrypoint", "parameter", "energy")
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    INVOKER_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    ENTRYPOINT_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_FIELD_NUMBER: _ClassVar[int]
    ENERGY_FIELD_NUMBER: _ClassVar[int]
    block_hash: BlockHashInput
    invoker: Address
    instance: ContractAddress
    amount: Amount
    entrypoint: ReceiveName
    parameter: Parameter
    energy: Energy
    def __init__(self, block_hash: _Optional[_Union[BlockHashInput, _Mapping]] = ..., invoker: _Optional[_Union[Address, _Mapping]] = ..., instance: _Optional[_Union[ContractAddress, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ..., entrypoint: _Optional[_Union[ReceiveName, _Mapping]] = ..., parameter: _Optional[_Union[Parameter, _Mapping]] = ..., energy: _Optional[_Union[Energy, _Mapping]] = ...) -> None: ...

class InvokeInstanceResponse(_message.Message):
    __slots__ = ("success", "failure")
    class Failure(_message.Message):
        __slots__ = ("return_value", "used_energy", "reason")
        RETURN_VALUE_FIELD_NUMBER: _ClassVar[int]
        USED_ENERGY_FIELD_NUMBER: _ClassVar[int]
        REASON_FIELD_NUMBER: _ClassVar[int]
        return_value: bytes
        used_energy: Energy
        reason: RejectReason
        def __init__(self, return_value: _Optional[bytes] = ..., used_energy: _Optional[_Union[Energy, _Mapping]] = ..., reason: _Optional[_Union[RejectReason, _Mapping]] = ...) -> None: ...
    class Success(_message.Message):
        __slots__ = ("return_value", "used_energy", "effects")
        RETURN_VALUE_FIELD_NUMBER: _ClassVar[int]
        USED_ENERGY_FIELD_NUMBER: _ClassVar[int]
        EFFECTS_FIELD_NUMBER: _ClassVar[int]
        return_value: bytes
        used_energy: Energy
        effects: _containers.RepeatedCompositeFieldContainer[ContractTraceElement]
        def __init__(self, return_value: _Optional[bytes] = ..., used_energy: _Optional[_Union[Energy, _Mapping]] = ..., effects: _Optional[_Iterable[_Union[ContractTraceElement, _Mapping]]] = ...) -> None: ...
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    success: InvokeInstanceResponse.Success
    failure: InvokeInstanceResponse.Failure
    def __init__(self, success: _Optional[_Union[InvokeInstanceResponse.Success, _Mapping]] = ..., failure: _Optional[_Union[InvokeInstanceResponse.Failure, _Mapping]] = ...) -> None: ...

class GetPoolDelegatorsRequest(_message.Message):
    __slots__ = ("block_hash", "baker")
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    BAKER_FIELD_NUMBER: _ClassVar[int]
    block_hash: BlockHashInput
    baker: BakerId
    def __init__(self, block_hash: _Optional[_Union[BlockHashInput, _Mapping]] = ..., baker: _Optional[_Union[BakerId, _Mapping]] = ...) -> None: ...

class DelegatorInfo(_message.Message):
    __slots__ = ("account", "stake", "pending_change")
    ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    STAKE_FIELD_NUMBER: _ClassVar[int]
    PENDING_CHANGE_FIELD_NUMBER: _ClassVar[int]
    account: _kernel_pb2.AccountAddress
    stake: Amount
    pending_change: StakePendingChange
    def __init__(self, account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., stake: _Optional[_Union[Amount, _Mapping]] = ..., pending_change: _Optional[_Union[StakePendingChange, _Mapping]] = ...) -> None: ...

class DelegatorRewardPeriodInfo(_message.Message):
    __slots__ = ("account", "stake")
    ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    STAKE_FIELD_NUMBER: _ClassVar[int]
    account: _kernel_pb2.AccountAddress
    stake: Amount
    def __init__(self, account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., stake: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...

class Branch(_message.Message):
    __slots__ = ("block_hash", "children")
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    block_hash: BlockHash
    children: _containers.RepeatedCompositeFieldContainer[Branch]
    def __init__(self, block_hash: _Optional[_Union[BlockHash, _Mapping]] = ..., children: _Optional[_Iterable[_Union[Branch, _Mapping]]] = ...) -> None: ...

class LeadershipElectionNonce(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class ElectionInfo(_message.Message):
    __slots__ = ("election_difficulty", "election_nonce", "baker_election_info")
    class Baker(_message.Message):
        __slots__ = ("baker", "account", "lottery_power")
        BAKER_FIELD_NUMBER: _ClassVar[int]
        ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        LOTTERY_POWER_FIELD_NUMBER: _ClassVar[int]
        baker: BakerId
        account: _kernel_pb2.AccountAddress
        lottery_power: float
        def __init__(self, baker: _Optional[_Union[BakerId, _Mapping]] = ..., account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., lottery_power: _Optional[float] = ...) -> None: ...
    ELECTION_DIFFICULTY_FIELD_NUMBER: _ClassVar[int]
    ELECTION_NONCE_FIELD_NUMBER: _ClassVar[int]
    BAKER_ELECTION_INFO_FIELD_NUMBER: _ClassVar[int]
    election_difficulty: ElectionDifficulty
    election_nonce: LeadershipElectionNonce
    baker_election_info: _containers.RepeatedCompositeFieldContainer[ElectionInfo.Baker]
    def __init__(self, election_difficulty: _Optional[_Union[ElectionDifficulty, _Mapping]] = ..., election_nonce: _Optional[_Union[LeadershipElectionNonce, _Mapping]] = ..., baker_election_info: _Optional[_Iterable[_Union[ElectionInfo.Baker, _Mapping]]] = ...) -> None: ...

class BlockSpecialEvent(_message.Message):
    __slots__ = ("baking_rewards", "mint", "finalization_rewards", "block_reward", "payday_foundation_reward", "payday_account_reward", "block_accrue_reward", "payday_pool_reward", "validator_suspended", "validator_primed_for_suspension")
    class AccountAmounts(_message.Message):
        __slots__ = ("entries",)
        class Entry(_message.Message):
            __slots__ = ("account", "amount")
            ACCOUNT_FIELD_NUMBER: _ClassVar[int]
            AMOUNT_FIELD_NUMBER: _ClassVar[int]
            account: _kernel_pb2.AccountAddress
            amount: Amount
            def __init__(self, account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...
        ENTRIES_FIELD_NUMBER: _ClassVar[int]
        entries: _containers.RepeatedCompositeFieldContainer[BlockSpecialEvent.AccountAmounts.Entry]
        def __init__(self, entries: _Optional[_Iterable[_Union[BlockSpecialEvent.AccountAmounts.Entry, _Mapping]]] = ...) -> None: ...
    class BakingRewards(_message.Message):
        __slots__ = ("baker_rewards", "remainder")
        BAKER_REWARDS_FIELD_NUMBER: _ClassVar[int]
        REMAINDER_FIELD_NUMBER: _ClassVar[int]
        baker_rewards: BlockSpecialEvent.AccountAmounts
        remainder: Amount
        def __init__(self, baker_rewards: _Optional[_Union[BlockSpecialEvent.AccountAmounts, _Mapping]] = ..., remainder: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...
    class Mint(_message.Message):
        __slots__ = ("mint_baking_reward", "mint_finalization_reward", "mint_platform_development_charge", "foundation_account")
        MINT_BAKING_REWARD_FIELD_NUMBER: _ClassVar[int]
        MINT_FINALIZATION_REWARD_FIELD_NUMBER: _ClassVar[int]
        MINT_PLATFORM_DEVELOPMENT_CHARGE_FIELD_NUMBER: _ClassVar[int]
        FOUNDATION_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        mint_baking_reward: Amount
        mint_finalization_reward: Amount
        mint_platform_development_charge: Amount
        foundation_account: _kernel_pb2.AccountAddress
        def __init__(self, mint_baking_reward: _Optional[_Union[Amount, _Mapping]] = ..., mint_finalization_reward: _Optional[_Union[Amount, _Mapping]] = ..., mint_platform_development_charge: _Optional[_Union[Amount, _Mapping]] = ..., foundation_account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ...) -> None: ...
    class FinalizationRewards(_message.Message):
        __slots__ = ("finalization_rewards", "remainder")
        FINALIZATION_REWARDS_FIELD_NUMBER: _ClassVar[int]
        REMAINDER_FIELD_NUMBER: _ClassVar[int]
        finalization_rewards: BlockSpecialEvent.AccountAmounts
        remainder: Amount
        def __init__(self, finalization_rewards: _Optional[_Union[BlockSpecialEvent.AccountAmounts, _Mapping]] = ..., remainder: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...
    class BlockReward(_message.Message):
        __slots__ = ("transaction_fees", "old_gas_account", "new_gas_account", "baker_reward", "foundation_charge", "baker", "foundation_account")
        TRANSACTION_FEES_FIELD_NUMBER: _ClassVar[int]
        OLD_GAS_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        NEW_GAS_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        BAKER_REWARD_FIELD_NUMBER: _ClassVar[int]
        FOUNDATION_CHARGE_FIELD_NUMBER: _ClassVar[int]
        BAKER_FIELD_NUMBER: _ClassVar[int]
        FOUNDATION_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        transaction_fees: Amount
        old_gas_account: Amount
        new_gas_account: Amount
        baker_reward: Amount
        foundation_charge: Amount
        baker: _kernel_pb2.AccountAddress
        foundation_account: _kernel_pb2.AccountAddress
        def __init__(self, transaction_fees: _Optional[_Union[Amount, _Mapping]] = ..., old_gas_account: _Optional[_Union[Amount, _Mapping]] = ..., new_gas_account: _Optional[_Union[Amount, _Mapping]] = ..., baker_reward: _Optional[_Union[Amount, _Mapping]] = ..., foundation_charge: _Optional[_Union[Amount, _Mapping]] = ..., baker: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., foundation_account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ...) -> None: ...
    class PaydayFoundationReward(_message.Message):
        __slots__ = ("foundation_account", "development_charge")
        FOUNDATION_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        DEVELOPMENT_CHARGE_FIELD_NUMBER: _ClassVar[int]
        foundation_account: _kernel_pb2.AccountAddress
        development_charge: Amount
        def __init__(self, foundation_account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., development_charge: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...
    class PaydayAccountReward(_message.Message):
        __slots__ = ("account", "transaction_fees", "baker_reward", "finalization_reward")
        ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        TRANSACTION_FEES_FIELD_NUMBER: _ClassVar[int]
        BAKER_REWARD_FIELD_NUMBER: _ClassVar[int]
        FINALIZATION_REWARD_FIELD_NUMBER: _ClassVar[int]
        account: _kernel_pb2.AccountAddress
        transaction_fees: Amount
        baker_reward: Amount
        finalization_reward: Amount
        def __init__(self, account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., transaction_fees: _Optional[_Union[Amount, _Mapping]] = ..., baker_reward: _Optional[_Union[Amount, _Mapping]] = ..., finalization_reward: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...
    class BlockAccrueReward(_message.Message):
        __slots__ = ("transaction_fees", "old_gas_account", "new_gas_account", "baker_reward", "passive_reward", "foundation_charge", "baker")
        TRANSACTION_FEES_FIELD_NUMBER: _ClassVar[int]
        OLD_GAS_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        NEW_GAS_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        BAKER_REWARD_FIELD_NUMBER: _ClassVar[int]
        PASSIVE_REWARD_FIELD_NUMBER: _ClassVar[int]
        FOUNDATION_CHARGE_FIELD_NUMBER: _ClassVar[int]
        BAKER_FIELD_NUMBER: _ClassVar[int]
        transaction_fees: Amount
        old_gas_account: Amount
        new_gas_account: Amount
        baker_reward: Amount
        passive_reward: Amount
        foundation_charge: Amount
        baker: BakerId
        def __init__(self, transaction_fees: _Optional[_Union[Amount, _Mapping]] = ..., old_gas_account: _Optional[_Union[Amount, _Mapping]] = ..., new_gas_account: _Optional[_Union[Amount, _Mapping]] = ..., baker_reward: _Optional[_Union[Amount, _Mapping]] = ..., passive_reward: _Optional[_Union[Amount, _Mapping]] = ..., foundation_charge: _Optional[_Union[Amount, _Mapping]] = ..., baker: _Optional[_Union[BakerId, _Mapping]] = ...) -> None: ...
    class PaydayPoolReward(_message.Message):
        __slots__ = ("pool_owner", "transaction_fees", "baker_reward", "finalization_reward")
        POOL_OWNER_FIELD_NUMBER: _ClassVar[int]
        TRANSACTION_FEES_FIELD_NUMBER: _ClassVar[int]
        BAKER_REWARD_FIELD_NUMBER: _ClassVar[int]
        FINALIZATION_REWARD_FIELD_NUMBER: _ClassVar[int]
        pool_owner: BakerId
        transaction_fees: Amount
        baker_reward: Amount
        finalization_reward: Amount
        def __init__(self, pool_owner: _Optional[_Union[BakerId, _Mapping]] = ..., transaction_fees: _Optional[_Union[Amount, _Mapping]] = ..., baker_reward: _Optional[_Union[Amount, _Mapping]] = ..., finalization_reward: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...
    class ValidatorSuspended(_message.Message):
        __slots__ = ("baker_id", "account")
        BAKER_ID_FIELD_NUMBER: _ClassVar[int]
        ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        baker_id: BakerId
        account: _kernel_pb2.AccountAddress
        def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ..., account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ...) -> None: ...
    class ValidatorPrimedForSuspension(_message.Message):
        __slots__ = ("baker_id", "account")
        BAKER_ID_FIELD_NUMBER: _ClassVar[int]
        ACCOUNT_FIELD_NUMBER: _ClassVar[int]
        baker_id: BakerId
        account: _kernel_pb2.AccountAddress
        def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ..., account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ...) -> None: ...
    BAKING_REWARDS_FIELD_NUMBER: _ClassVar[int]
    MINT_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_REWARDS_FIELD_NUMBER: _ClassVar[int]
    BLOCK_REWARD_FIELD_NUMBER: _ClassVar[int]
    PAYDAY_FOUNDATION_REWARD_FIELD_NUMBER: _ClassVar[int]
    PAYDAY_ACCOUNT_REWARD_FIELD_NUMBER: _ClassVar[int]
    BLOCK_ACCRUE_REWARD_FIELD_NUMBER: _ClassVar[int]
    PAYDAY_POOL_REWARD_FIELD_NUMBER: _ClassVar[int]
    VALIDATOR_SUSPENDED_FIELD_NUMBER: _ClassVar[int]
    VALIDATOR_PRIMED_FOR_SUSPENSION_FIELD_NUMBER: _ClassVar[int]
    baking_rewards: BlockSpecialEvent.BakingRewards
    mint: BlockSpecialEvent.Mint
    finalization_rewards: BlockSpecialEvent.FinalizationRewards
    block_reward: BlockSpecialEvent.BlockReward
    payday_foundation_reward: BlockSpecialEvent.PaydayFoundationReward
    payday_account_reward: BlockSpecialEvent.PaydayAccountReward
    block_accrue_reward: BlockSpecialEvent.BlockAccrueReward
    payday_pool_reward: BlockSpecialEvent.PaydayPoolReward
    validator_suspended: BlockSpecialEvent.ValidatorSuspended
    validator_primed_for_suspension: BlockSpecialEvent.ValidatorPrimedForSuspension
    def __init__(self, baking_rewards: _Optional[_Union[BlockSpecialEvent.BakingRewards, _Mapping]] = ..., mint: _Optional[_Union[BlockSpecialEvent.Mint, _Mapping]] = ..., finalization_rewards: _Optional[_Union[BlockSpecialEvent.FinalizationRewards, _Mapping]] = ..., block_reward: _Optional[_Union[BlockSpecialEvent.BlockReward, _Mapping]] = ..., payday_foundation_reward: _Optional[_Union[BlockSpecialEvent.PaydayFoundationReward, _Mapping]] = ..., payday_account_reward: _Optional[_Union[BlockSpecialEvent.PaydayAccountReward, _Mapping]] = ..., block_accrue_reward: _Optional[_Union[BlockSpecialEvent.BlockAccrueReward, _Mapping]] = ..., payday_pool_reward: _Optional[_Union[BlockSpecialEvent.PaydayPoolReward, _Mapping]] = ..., validator_suspended: _Optional[_Union[BlockSpecialEvent.ValidatorSuspended, _Mapping]] = ..., validator_primed_for_suspension: _Optional[_Union[BlockSpecialEvent.ValidatorPrimedForSuspension, _Mapping]] = ...) -> None: ...

class PendingUpdate(_message.Message):
    __slots__ = ("effective_time", "root_keys", "level1_keys", "level2_keys_cpv_0", "level2_keys_cpv_1", "protocol", "election_difficulty", "euro_per_energy", "micro_ccd_per_euro", "foundation_account", "mint_distribution_cpv_0", "mint_distribution_cpv_1", "transaction_fee_distribution", "gas_rewards", "pool_parameters_cpv_0", "pool_parameters_cpv_1", "add_anonymity_revoker", "add_identity_provider", "cooldown_parameters", "time_parameters", "gas_rewards_cpv_2", "timeout_parameters", "min_block_time", "block_energy_limit", "finalization_committee_parameters", "validator_score_parameters")
    EFFECTIVE_TIME_FIELD_NUMBER: _ClassVar[int]
    ROOT_KEYS_FIELD_NUMBER: _ClassVar[int]
    LEVEL1_KEYS_FIELD_NUMBER: _ClassVar[int]
    LEVEL2_KEYS_CPV_0_FIELD_NUMBER: _ClassVar[int]
    LEVEL2_KEYS_CPV_1_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_FIELD_NUMBER: _ClassVar[int]
    ELECTION_DIFFICULTY_FIELD_NUMBER: _ClassVar[int]
    EURO_PER_ENERGY_FIELD_NUMBER: _ClassVar[int]
    MICRO_CCD_PER_EURO_FIELD_NUMBER: _ClassVar[int]
    FOUNDATION_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    MINT_DISTRIBUTION_CPV_0_FIELD_NUMBER: _ClassVar[int]
    MINT_DISTRIBUTION_CPV_1_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_FEE_DISTRIBUTION_FIELD_NUMBER: _ClassVar[int]
    GAS_REWARDS_FIELD_NUMBER: _ClassVar[int]
    POOL_PARAMETERS_CPV_0_FIELD_NUMBER: _ClassVar[int]
    POOL_PARAMETERS_CPV_1_FIELD_NUMBER: _ClassVar[int]
    ADD_ANONYMITY_REVOKER_FIELD_NUMBER: _ClassVar[int]
    ADD_IDENTITY_PROVIDER_FIELD_NUMBER: _ClassVar[int]
    COOLDOWN_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    TIME_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    GAS_REWARDS_CPV_2_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    MIN_BLOCK_TIME_FIELD_NUMBER: _ClassVar[int]
    BLOCK_ENERGY_LIMIT_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_COMMITTEE_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    VALIDATOR_SCORE_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    effective_time: TransactionTime
    root_keys: HigherLevelKeys
    level1_keys: HigherLevelKeys
    level2_keys_cpv_0: AuthorizationsV0
    level2_keys_cpv_1: AuthorizationsV1
    protocol: ProtocolUpdate
    election_difficulty: ElectionDifficulty
    euro_per_energy: ExchangeRate
    micro_ccd_per_euro: ExchangeRate
    foundation_account: _kernel_pb2.AccountAddress
    mint_distribution_cpv_0: MintDistributionCpv0
    mint_distribution_cpv_1: MintDistributionCpv1
    transaction_fee_distribution: TransactionFeeDistribution
    gas_rewards: GasRewards
    pool_parameters_cpv_0: BakerStakeThreshold
    pool_parameters_cpv_1: PoolParametersCpv1
    add_anonymity_revoker: ArInfo
    add_identity_provider: IpInfo
    cooldown_parameters: CooldownParametersCpv1
    time_parameters: TimeParametersCpv1
    gas_rewards_cpv_2: GasRewardsCpv2
    timeout_parameters: TimeoutParameters
    min_block_time: Duration
    block_energy_limit: Energy
    finalization_committee_parameters: FinalizationCommitteeParameters
    validator_score_parameters: ValidatorScoreParameters
    def __init__(self, effective_time: _Optional[_Union[TransactionTime, _Mapping]] = ..., root_keys: _Optional[_Union[HigherLevelKeys, _Mapping]] = ..., level1_keys: _Optional[_Union[HigherLevelKeys, _Mapping]] = ..., level2_keys_cpv_0: _Optional[_Union[AuthorizationsV0, _Mapping]] = ..., level2_keys_cpv_1: _Optional[_Union[AuthorizationsV1, _Mapping]] = ..., protocol: _Optional[_Union[ProtocolUpdate, _Mapping]] = ..., election_difficulty: _Optional[_Union[ElectionDifficulty, _Mapping]] = ..., euro_per_energy: _Optional[_Union[ExchangeRate, _Mapping]] = ..., micro_ccd_per_euro: _Optional[_Union[ExchangeRate, _Mapping]] = ..., foundation_account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., mint_distribution_cpv_0: _Optional[_Union[MintDistributionCpv0, _Mapping]] = ..., mint_distribution_cpv_1: _Optional[_Union[MintDistributionCpv1, _Mapping]] = ..., transaction_fee_distribution: _Optional[_Union[TransactionFeeDistribution, _Mapping]] = ..., gas_rewards: _Optional[_Union[GasRewards, _Mapping]] = ..., pool_parameters_cpv_0: _Optional[_Union[BakerStakeThreshold, _Mapping]] = ..., pool_parameters_cpv_1: _Optional[_Union[PoolParametersCpv1, _Mapping]] = ..., add_anonymity_revoker: _Optional[_Union[ArInfo, _Mapping]] = ..., add_identity_provider: _Optional[_Union[IpInfo, _Mapping]] = ..., cooldown_parameters: _Optional[_Union[CooldownParametersCpv1, _Mapping]] = ..., time_parameters: _Optional[_Union[TimeParametersCpv1, _Mapping]] = ..., gas_rewards_cpv_2: _Optional[_Union[GasRewardsCpv2, _Mapping]] = ..., timeout_parameters: _Optional[_Union[TimeoutParameters, _Mapping]] = ..., min_block_time: _Optional[_Union[Duration, _Mapping]] = ..., block_energy_limit: _Optional[_Union[Energy, _Mapping]] = ..., finalization_committee_parameters: _Optional[_Union[FinalizationCommitteeParameters, _Mapping]] = ..., validator_score_parameters: _Optional[_Union[ValidatorScoreParameters, _Mapping]] = ...) -> None: ...

class NextUpdateSequenceNumbers(_message.Message):
    __slots__ = ("root_keys", "level1_keys", "level2_keys", "protocol", "election_difficulty", "euro_per_energy", "micro_ccd_per_euro", "foundation_account", "mint_distribution", "transaction_fee_distribution", "gas_rewards", "pool_parameters", "add_anonymity_revoker", "add_identity_provider", "cooldown_parameters", "time_parameters", "timeout_parameters", "min_block_time", "block_energy_limit", "finalization_committee_parameters", "validator_score_parameters", "protocol_level_tokens")
    ROOT_KEYS_FIELD_NUMBER: _ClassVar[int]
    LEVEL1_KEYS_FIELD_NUMBER: _ClassVar[int]
    LEVEL2_KEYS_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_FIELD_NUMBER: _ClassVar[int]
    ELECTION_DIFFICULTY_FIELD_NUMBER: _ClassVar[int]
    EURO_PER_ENERGY_FIELD_NUMBER: _ClassVar[int]
    MICRO_CCD_PER_EURO_FIELD_NUMBER: _ClassVar[int]
    FOUNDATION_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    MINT_DISTRIBUTION_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_FEE_DISTRIBUTION_FIELD_NUMBER: _ClassVar[int]
    GAS_REWARDS_FIELD_NUMBER: _ClassVar[int]
    POOL_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    ADD_ANONYMITY_REVOKER_FIELD_NUMBER: _ClassVar[int]
    ADD_IDENTITY_PROVIDER_FIELD_NUMBER: _ClassVar[int]
    COOLDOWN_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    TIME_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    MIN_BLOCK_TIME_FIELD_NUMBER: _ClassVar[int]
    BLOCK_ENERGY_LIMIT_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_COMMITTEE_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    VALIDATOR_SCORE_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_LEVEL_TOKENS_FIELD_NUMBER: _ClassVar[int]
    root_keys: SequenceNumber
    level1_keys: SequenceNumber
    level2_keys: SequenceNumber
    protocol: SequenceNumber
    election_difficulty: SequenceNumber
    euro_per_energy: SequenceNumber
    micro_ccd_per_euro: SequenceNumber
    foundation_account: SequenceNumber
    mint_distribution: SequenceNumber
    transaction_fee_distribution: SequenceNumber
    gas_rewards: SequenceNumber
    pool_parameters: SequenceNumber
    add_anonymity_revoker: SequenceNumber
    add_identity_provider: SequenceNumber
    cooldown_parameters: SequenceNumber
    time_parameters: SequenceNumber
    timeout_parameters: SequenceNumber
    min_block_time: SequenceNumber
    block_energy_limit: SequenceNumber
    finalization_committee_parameters: SequenceNumber
    validator_score_parameters: SequenceNumber
    protocol_level_tokens: SequenceNumber
    def __init__(self, root_keys: _Optional[_Union[SequenceNumber, _Mapping]] = ..., level1_keys: _Optional[_Union[SequenceNumber, _Mapping]] = ..., level2_keys: _Optional[_Union[SequenceNumber, _Mapping]] = ..., protocol: _Optional[_Union[SequenceNumber, _Mapping]] = ..., election_difficulty: _Optional[_Union[SequenceNumber, _Mapping]] = ..., euro_per_energy: _Optional[_Union[SequenceNumber, _Mapping]] = ..., micro_ccd_per_euro: _Optional[_Union[SequenceNumber, _Mapping]] = ..., foundation_account: _Optional[_Union[SequenceNumber, _Mapping]] = ..., mint_distribution: _Optional[_Union[SequenceNumber, _Mapping]] = ..., transaction_fee_distribution: _Optional[_Union[SequenceNumber, _Mapping]] = ..., gas_rewards: _Optional[_Union[SequenceNumber, _Mapping]] = ..., pool_parameters: _Optional[_Union[SequenceNumber, _Mapping]] = ..., add_anonymity_revoker: _Optional[_Union[SequenceNumber, _Mapping]] = ..., add_identity_provider: _Optional[_Union[SequenceNumber, _Mapping]] = ..., cooldown_parameters: _Optional[_Union[SequenceNumber, _Mapping]] = ..., time_parameters: _Optional[_Union[SequenceNumber, _Mapping]] = ..., timeout_parameters: _Optional[_Union[SequenceNumber, _Mapping]] = ..., min_block_time: _Optional[_Union[SequenceNumber, _Mapping]] = ..., block_energy_limit: _Optional[_Union[SequenceNumber, _Mapping]] = ..., finalization_committee_parameters: _Optional[_Union[SequenceNumber, _Mapping]] = ..., validator_score_parameters: _Optional[_Union[SequenceNumber, _Mapping]] = ..., protocol_level_tokens: _Optional[_Union[SequenceNumber, _Mapping]] = ...) -> None: ...

class IpAddress(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class Port(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class IpSocketAddress(_message.Message):
    __slots__ = ("ip", "port")
    IP_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    ip: IpAddress
    port: Port
    def __init__(self, ip: _Optional[_Union[IpAddress, _Mapping]] = ..., port: _Optional[_Union[Port, _Mapping]] = ...) -> None: ...

class PeerId(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class BannedPeer(_message.Message):
    __slots__ = ("ip_address",)
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    ip_address: IpAddress
    def __init__(self, ip_address: _Optional[_Union[IpAddress, _Mapping]] = ...) -> None: ...

class BannedPeers(_message.Message):
    __slots__ = ("peers",)
    PEERS_FIELD_NUMBER: _ClassVar[int]
    peers: _containers.RepeatedCompositeFieldContainer[BannedPeer]
    def __init__(self, peers: _Optional[_Iterable[_Union[BannedPeer, _Mapping]]] = ...) -> None: ...

class PeerToBan(_message.Message):
    __slots__ = ("ip_address",)
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    ip_address: IpAddress
    def __init__(self, ip_address: _Optional[_Union[IpAddress, _Mapping]] = ...) -> None: ...

class DumpRequest(_message.Message):
    __slots__ = ("file", "raw")
    FILE_FIELD_NUMBER: _ClassVar[int]
    RAW_FIELD_NUMBER: _ClassVar[int]
    file: str
    raw: bool
    def __init__(self, file: _Optional[str] = ..., raw: bool = ...) -> None: ...

class PeersInfo(_message.Message):
    __slots__ = ("peers",)
    class Peer(_message.Message):
        __slots__ = ("peer_id", "socket_address", "network_stats", "bootstrapper", "node_catchup_status")
        class CatchupStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            UPTODATE: _ClassVar[PeersInfo.Peer.CatchupStatus]
            PENDING: _ClassVar[PeersInfo.Peer.CatchupStatus]
            CATCHINGUP: _ClassVar[PeersInfo.Peer.CatchupStatus]
        UPTODATE: PeersInfo.Peer.CatchupStatus
        PENDING: PeersInfo.Peer.CatchupStatus
        CATCHINGUP: PeersInfo.Peer.CatchupStatus
        class NetworkStats(_message.Message):
            __slots__ = ("packets_sent", "packets_received", "latency")
            PACKETS_SENT_FIELD_NUMBER: _ClassVar[int]
            PACKETS_RECEIVED_FIELD_NUMBER: _ClassVar[int]
            LATENCY_FIELD_NUMBER: _ClassVar[int]
            packets_sent: int
            packets_received: int
            latency: int
            def __init__(self, packets_sent: _Optional[int] = ..., packets_received: _Optional[int] = ..., latency: _Optional[int] = ...) -> None: ...
        PEER_ID_FIELD_NUMBER: _ClassVar[int]
        SOCKET_ADDRESS_FIELD_NUMBER: _ClassVar[int]
        NETWORK_STATS_FIELD_NUMBER: _ClassVar[int]
        BOOTSTRAPPER_FIELD_NUMBER: _ClassVar[int]
        NODE_CATCHUP_STATUS_FIELD_NUMBER: _ClassVar[int]
        peer_id: PeerId
        socket_address: IpSocketAddress
        network_stats: PeersInfo.Peer.NetworkStats
        bootstrapper: Empty
        node_catchup_status: PeersInfo.Peer.CatchupStatus
        def __init__(self, peer_id: _Optional[_Union[PeerId, _Mapping]] = ..., socket_address: _Optional[_Union[IpSocketAddress, _Mapping]] = ..., network_stats: _Optional[_Union[PeersInfo.Peer.NetworkStats, _Mapping]] = ..., bootstrapper: _Optional[_Union[Empty, _Mapping]] = ..., node_catchup_status: _Optional[_Union[PeersInfo.Peer.CatchupStatus, str]] = ...) -> None: ...
    PEERS_FIELD_NUMBER: _ClassVar[int]
    peers: _containers.RepeatedCompositeFieldContainer[PeersInfo.Peer]
    def __init__(self, peers: _Optional[_Iterable[_Union[PeersInfo.Peer, _Mapping]]] = ...) -> None: ...

class NodeInfo(_message.Message):
    __slots__ = ("peer_version", "local_time", "peer_uptime", "network_info", "bootstrapper", "node")
    class NetworkInfo(_message.Message):
        __slots__ = ("node_id", "peer_total_sent", "peer_total_received", "avg_bps_in", "avg_bps_out")
        NODE_ID_FIELD_NUMBER: _ClassVar[int]
        PEER_TOTAL_SENT_FIELD_NUMBER: _ClassVar[int]
        PEER_TOTAL_RECEIVED_FIELD_NUMBER: _ClassVar[int]
        AVG_BPS_IN_FIELD_NUMBER: _ClassVar[int]
        AVG_BPS_OUT_FIELD_NUMBER: _ClassVar[int]
        node_id: PeerId
        peer_total_sent: int
        peer_total_received: int
        avg_bps_in: int
        avg_bps_out: int
        def __init__(self, node_id: _Optional[_Union[PeerId, _Mapping]] = ..., peer_total_sent: _Optional[int] = ..., peer_total_received: _Optional[int] = ..., avg_bps_in: _Optional[int] = ..., avg_bps_out: _Optional[int] = ...) -> None: ...
    class BakerConsensusInfo(_message.Message):
        __slots__ = ("baker_id", "passive_committee_info", "active_baker_committee_info", "active_finalizer_committee_info")
        class PassiveCommitteeInfo(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            NOT_IN_COMMITTEE: _ClassVar[NodeInfo.BakerConsensusInfo.PassiveCommitteeInfo]
            ADDED_BUT_NOT_ACTIVE_IN_COMMITTEE: _ClassVar[NodeInfo.BakerConsensusInfo.PassiveCommitteeInfo]
            ADDED_BUT_WRONG_KEYS: _ClassVar[NodeInfo.BakerConsensusInfo.PassiveCommitteeInfo]
        NOT_IN_COMMITTEE: NodeInfo.BakerConsensusInfo.PassiveCommitteeInfo
        ADDED_BUT_NOT_ACTIVE_IN_COMMITTEE: NodeInfo.BakerConsensusInfo.PassiveCommitteeInfo
        ADDED_BUT_WRONG_KEYS: NodeInfo.BakerConsensusInfo.PassiveCommitteeInfo
        class ActiveBakerCommitteeInfo(_message.Message):
            __slots__ = ()
            def __init__(self) -> None: ...
        class ActiveFinalizerCommitteeInfo(_message.Message):
            __slots__ = ()
            def __init__(self) -> None: ...
        BAKER_ID_FIELD_NUMBER: _ClassVar[int]
        PASSIVE_COMMITTEE_INFO_FIELD_NUMBER: _ClassVar[int]
        ACTIVE_BAKER_COMMITTEE_INFO_FIELD_NUMBER: _ClassVar[int]
        ACTIVE_FINALIZER_COMMITTEE_INFO_FIELD_NUMBER: _ClassVar[int]
        baker_id: BakerId
        passive_committee_info: NodeInfo.BakerConsensusInfo.PassiveCommitteeInfo
        active_baker_committee_info: NodeInfo.BakerConsensusInfo.ActiveBakerCommitteeInfo
        active_finalizer_committee_info: NodeInfo.BakerConsensusInfo.ActiveFinalizerCommitteeInfo
        def __init__(self, baker_id: _Optional[_Union[BakerId, _Mapping]] = ..., passive_committee_info: _Optional[_Union[NodeInfo.BakerConsensusInfo.PassiveCommitteeInfo, str]] = ..., active_baker_committee_info: _Optional[_Union[NodeInfo.BakerConsensusInfo.ActiveBakerCommitteeInfo, _Mapping]] = ..., active_finalizer_committee_info: _Optional[_Union[NodeInfo.BakerConsensusInfo.ActiveFinalizerCommitteeInfo, _Mapping]] = ...) -> None: ...
    class Node(_message.Message):
        __slots__ = ("not_running", "passive", "active")
        NOT_RUNNING_FIELD_NUMBER: _ClassVar[int]
        PASSIVE_FIELD_NUMBER: _ClassVar[int]
        ACTIVE_FIELD_NUMBER: _ClassVar[int]
        not_running: Empty
        passive: Empty
        active: NodeInfo.BakerConsensusInfo
        def __init__(self, not_running: _Optional[_Union[Empty, _Mapping]] = ..., passive: _Optional[_Union[Empty, _Mapping]] = ..., active: _Optional[_Union[NodeInfo.BakerConsensusInfo, _Mapping]] = ...) -> None: ...
    PEER_VERSION_FIELD_NUMBER: _ClassVar[int]
    LOCAL_TIME_FIELD_NUMBER: _ClassVar[int]
    PEER_UPTIME_FIELD_NUMBER: _ClassVar[int]
    NETWORK_INFO_FIELD_NUMBER: _ClassVar[int]
    BOOTSTRAPPER_FIELD_NUMBER: _ClassVar[int]
    NODE_FIELD_NUMBER: _ClassVar[int]
    peer_version: str
    local_time: Timestamp
    peer_uptime: Duration
    network_info: NodeInfo.NetworkInfo
    bootstrapper: Empty
    node: NodeInfo.Node
    def __init__(self, peer_version: _Optional[str] = ..., local_time: _Optional[_Union[Timestamp, _Mapping]] = ..., peer_uptime: _Optional[_Union[Duration, _Mapping]] = ..., network_info: _Optional[_Union[NodeInfo.NetworkInfo, _Mapping]] = ..., bootstrapper: _Optional[_Union[Empty, _Mapping]] = ..., node: _Optional[_Union[NodeInfo.Node, _Mapping]] = ...) -> None: ...

class SendBlockItemRequest(_message.Message):
    __slots__ = ("account_transaction", "credential_deployment", "update_instruction")
    ACCOUNT_TRANSACTION_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_DEPLOYMENT_FIELD_NUMBER: _ClassVar[int]
    UPDATE_INSTRUCTION_FIELD_NUMBER: _ClassVar[int]
    account_transaction: AccountTransaction
    credential_deployment: CredentialDeployment
    update_instruction: UpdateInstruction
    def __init__(self, account_transaction: _Optional[_Union[AccountTransaction, _Mapping]] = ..., credential_deployment: _Optional[_Union[CredentialDeployment, _Mapping]] = ..., update_instruction: _Optional[_Union[UpdateInstruction, _Mapping]] = ...) -> None: ...

class CredentialDeployment(_message.Message):
    __slots__ = ("message_expiry", "raw_payload")
    MESSAGE_EXPIRY_FIELD_NUMBER: _ClassVar[int]
    RAW_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    message_expiry: TransactionTime
    raw_payload: bytes
    def __init__(self, message_expiry: _Optional[_Union[TransactionTime, _Mapping]] = ..., raw_payload: _Optional[bytes] = ...) -> None: ...

class Signature(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class SignatureMap(_message.Message):
    __slots__ = ("signatures",)
    class SignaturesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: Signature
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[Signature, _Mapping]] = ...) -> None: ...
    SIGNATURES_FIELD_NUMBER: _ClassVar[int]
    signatures: _containers.MessageMap[int, Signature]
    def __init__(self, signatures: _Optional[_Mapping[int, Signature]] = ...) -> None: ...

class AccountSignatureMap(_message.Message):
    __slots__ = ("signatures",)
    class SignaturesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: Signature
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[Signature, _Mapping]] = ...) -> None: ...
    SIGNATURES_FIELD_NUMBER: _ClassVar[int]
    signatures: _containers.MessageMap[int, Signature]
    def __init__(self, signatures: _Optional[_Mapping[int, Signature]] = ...) -> None: ...

class AccountTransactionSignature(_message.Message):
    __slots__ = ("signatures",)
    class SignaturesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: AccountSignatureMap
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[AccountSignatureMap, _Mapping]] = ...) -> None: ...
    SIGNATURES_FIELD_NUMBER: _ClassVar[int]
    signatures: _containers.MessageMap[int, AccountSignatureMap]
    def __init__(self, signatures: _Optional[_Mapping[int, AccountSignatureMap]] = ...) -> None: ...

class AccountTransactionHeader(_message.Message):
    __slots__ = ("sender", "sequence_number", "energy_amount", "expiry")
    SENDER_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    ENERGY_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_FIELD_NUMBER: _ClassVar[int]
    sender: _kernel_pb2.AccountAddress
    sequence_number: SequenceNumber
    energy_amount: Energy
    expiry: TransactionTime
    def __init__(self, sender: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., sequence_number: _Optional[_Union[SequenceNumber, _Mapping]] = ..., energy_amount: _Optional[_Union[Energy, _Mapping]] = ..., expiry: _Optional[_Union[TransactionTime, _Mapping]] = ...) -> None: ...

class InitContractPayload(_message.Message):
    __slots__ = ("amount", "module_ref", "init_name", "parameter")
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    MODULE_REF_FIELD_NUMBER: _ClassVar[int]
    INIT_NAME_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_FIELD_NUMBER: _ClassVar[int]
    amount: Amount
    module_ref: ModuleRef
    init_name: InitName
    parameter: Parameter
    def __init__(self, amount: _Optional[_Union[Amount, _Mapping]] = ..., module_ref: _Optional[_Union[ModuleRef, _Mapping]] = ..., init_name: _Optional[_Union[InitName, _Mapping]] = ..., parameter: _Optional[_Union[Parameter, _Mapping]] = ...) -> None: ...

class UpdateContractPayload(_message.Message):
    __slots__ = ("amount", "address", "receive_name", "parameter")
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    RECEIVE_NAME_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_FIELD_NUMBER: _ClassVar[int]
    amount: Amount
    address: ContractAddress
    receive_name: ReceiveName
    parameter: Parameter
    def __init__(self, amount: _Optional[_Union[Amount, _Mapping]] = ..., address: _Optional[_Union[ContractAddress, _Mapping]] = ..., receive_name: _Optional[_Union[ReceiveName, _Mapping]] = ..., parameter: _Optional[_Union[Parameter, _Mapping]] = ...) -> None: ...

class TransferPayload(_message.Message):
    __slots__ = ("amount", "receiver")
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    RECEIVER_FIELD_NUMBER: _ClassVar[int]
    amount: Amount
    receiver: _kernel_pb2.AccountAddress
    def __init__(self, amount: _Optional[_Union[Amount, _Mapping]] = ..., receiver: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ...) -> None: ...

class TransferWithMemoPayload(_message.Message):
    __slots__ = ("amount", "receiver", "memo")
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    RECEIVER_FIELD_NUMBER: _ClassVar[int]
    MEMO_FIELD_NUMBER: _ClassVar[int]
    amount: Amount
    receiver: _kernel_pb2.AccountAddress
    memo: _kernel_pb2.Memo
    def __init__(self, amount: _Optional[_Union[Amount, _Mapping]] = ..., receiver: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., memo: _Optional[_Union[_kernel_pb2.Memo, _Mapping]] = ...) -> None: ...

class AccountTransactionPayload(_message.Message):
    __slots__ = ("raw_payload", "deploy_module", "init_contract", "update_contract", "transfer", "transfer_with_memo", "register_data")
    RAW_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    DEPLOY_MODULE_FIELD_NUMBER: _ClassVar[int]
    INIT_CONTRACT_FIELD_NUMBER: _ClassVar[int]
    UPDATE_CONTRACT_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_WITH_MEMO_FIELD_NUMBER: _ClassVar[int]
    REGISTER_DATA_FIELD_NUMBER: _ClassVar[int]
    raw_payload: bytes
    deploy_module: VersionedModuleSource
    init_contract: InitContractPayload
    update_contract: UpdateContractPayload
    transfer: TransferPayload
    transfer_with_memo: TransferWithMemoPayload
    register_data: RegisteredData
    def __init__(self, raw_payload: _Optional[bytes] = ..., deploy_module: _Optional[_Union[VersionedModuleSource, _Mapping]] = ..., init_contract: _Optional[_Union[InitContractPayload, _Mapping]] = ..., update_contract: _Optional[_Union[UpdateContractPayload, _Mapping]] = ..., transfer: _Optional[_Union[TransferPayload, _Mapping]] = ..., transfer_with_memo: _Optional[_Union[TransferWithMemoPayload, _Mapping]] = ..., register_data: _Optional[_Union[RegisteredData, _Mapping]] = ...) -> None: ...

class PreAccountTransaction(_message.Message):
    __slots__ = ("header", "payload")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    header: AccountTransactionHeader
    payload: AccountTransactionPayload
    def __init__(self, header: _Optional[_Union[AccountTransactionHeader, _Mapping]] = ..., payload: _Optional[_Union[AccountTransactionPayload, _Mapping]] = ...) -> None: ...

class AccountTransaction(_message.Message):
    __slots__ = ("signature", "header", "payload")
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    HEADER_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    signature: AccountTransactionSignature
    header: AccountTransactionHeader
    payload: AccountTransactionPayload
    def __init__(self, signature: _Optional[_Union[AccountTransactionSignature, _Mapping]] = ..., header: _Optional[_Union[AccountTransactionHeader, _Mapping]] = ..., payload: _Optional[_Union[AccountTransactionPayload, _Mapping]] = ...) -> None: ...

class UpdateInstructionHeader(_message.Message):
    __slots__ = ("sequence_number", "effective_time", "timeout")
    SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    EFFECTIVE_TIME_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    sequence_number: UpdateSequenceNumber
    effective_time: TransactionTime
    timeout: TransactionTime
    def __init__(self, sequence_number: _Optional[_Union[UpdateSequenceNumber, _Mapping]] = ..., effective_time: _Optional[_Union[TransactionTime, _Mapping]] = ..., timeout: _Optional[_Union[TransactionTime, _Mapping]] = ...) -> None: ...

class UpdateInstructionPayload(_message.Message):
    __slots__ = ("raw_payload",)
    RAW_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    raw_payload: bytes
    def __init__(self, raw_payload: _Optional[bytes] = ...) -> None: ...

class UpdateInstruction(_message.Message):
    __slots__ = ("signatures", "header", "payload")
    SIGNATURES_FIELD_NUMBER: _ClassVar[int]
    HEADER_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    signatures: SignatureMap
    header: UpdateInstructionHeader
    payload: UpdateInstructionPayload
    def __init__(self, signatures: _Optional[_Union[SignatureMap, _Mapping]] = ..., header: _Optional[_Union[UpdateInstructionHeader, _Mapping]] = ..., payload: _Optional[_Union[UpdateInstructionPayload, _Mapping]] = ...) -> None: ...

class AccountTransactionSignHash(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class CredentialsPerBlockLimit(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class ChainParametersV0(_message.Message):
    __slots__ = ("election_difficulty", "euro_per_energy", "micro_ccd_per_euro", "baker_cooldown_epochs", "account_creation_limit", "mint_distribution", "transaction_fee_distribution", "gas_rewards", "foundation_account", "minimum_threshold_for_baking", "root_keys", "level1_keys", "level2_keys")
    ELECTION_DIFFICULTY_FIELD_NUMBER: _ClassVar[int]
    EURO_PER_ENERGY_FIELD_NUMBER: _ClassVar[int]
    MICRO_CCD_PER_EURO_FIELD_NUMBER: _ClassVar[int]
    BAKER_COOLDOWN_EPOCHS_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_CREATION_LIMIT_FIELD_NUMBER: _ClassVar[int]
    MINT_DISTRIBUTION_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_FEE_DISTRIBUTION_FIELD_NUMBER: _ClassVar[int]
    GAS_REWARDS_FIELD_NUMBER: _ClassVar[int]
    FOUNDATION_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    MINIMUM_THRESHOLD_FOR_BAKING_FIELD_NUMBER: _ClassVar[int]
    ROOT_KEYS_FIELD_NUMBER: _ClassVar[int]
    LEVEL1_KEYS_FIELD_NUMBER: _ClassVar[int]
    LEVEL2_KEYS_FIELD_NUMBER: _ClassVar[int]
    election_difficulty: ElectionDifficulty
    euro_per_energy: ExchangeRate
    micro_ccd_per_euro: ExchangeRate
    baker_cooldown_epochs: Epoch
    account_creation_limit: CredentialsPerBlockLimit
    mint_distribution: MintDistributionCpv0
    transaction_fee_distribution: TransactionFeeDistribution
    gas_rewards: GasRewards
    foundation_account: _kernel_pb2.AccountAddress
    minimum_threshold_for_baking: Amount
    root_keys: HigherLevelKeys
    level1_keys: HigherLevelKeys
    level2_keys: AuthorizationsV0
    def __init__(self, election_difficulty: _Optional[_Union[ElectionDifficulty, _Mapping]] = ..., euro_per_energy: _Optional[_Union[ExchangeRate, _Mapping]] = ..., micro_ccd_per_euro: _Optional[_Union[ExchangeRate, _Mapping]] = ..., baker_cooldown_epochs: _Optional[_Union[Epoch, _Mapping]] = ..., account_creation_limit: _Optional[_Union[CredentialsPerBlockLimit, _Mapping]] = ..., mint_distribution: _Optional[_Union[MintDistributionCpv0, _Mapping]] = ..., transaction_fee_distribution: _Optional[_Union[TransactionFeeDistribution, _Mapping]] = ..., gas_rewards: _Optional[_Union[GasRewards, _Mapping]] = ..., foundation_account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., minimum_threshold_for_baking: _Optional[_Union[Amount, _Mapping]] = ..., root_keys: _Optional[_Union[HigherLevelKeys, _Mapping]] = ..., level1_keys: _Optional[_Union[HigherLevelKeys, _Mapping]] = ..., level2_keys: _Optional[_Union[AuthorizationsV0, _Mapping]] = ...) -> None: ...

class ChainParametersV1(_message.Message):
    __slots__ = ("election_difficulty", "euro_per_energy", "micro_ccd_per_euro", "cooldown_parameters", "time_parameters", "account_creation_limit", "mint_distribution", "transaction_fee_distribution", "gas_rewards", "foundation_account", "pool_parameters", "root_keys", "level1_keys", "level2_keys")
    ELECTION_DIFFICULTY_FIELD_NUMBER: _ClassVar[int]
    EURO_PER_ENERGY_FIELD_NUMBER: _ClassVar[int]
    MICRO_CCD_PER_EURO_FIELD_NUMBER: _ClassVar[int]
    COOLDOWN_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    TIME_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_CREATION_LIMIT_FIELD_NUMBER: _ClassVar[int]
    MINT_DISTRIBUTION_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_FEE_DISTRIBUTION_FIELD_NUMBER: _ClassVar[int]
    GAS_REWARDS_FIELD_NUMBER: _ClassVar[int]
    FOUNDATION_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    POOL_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    ROOT_KEYS_FIELD_NUMBER: _ClassVar[int]
    LEVEL1_KEYS_FIELD_NUMBER: _ClassVar[int]
    LEVEL2_KEYS_FIELD_NUMBER: _ClassVar[int]
    election_difficulty: ElectionDifficulty
    euro_per_energy: ExchangeRate
    micro_ccd_per_euro: ExchangeRate
    cooldown_parameters: CooldownParametersCpv1
    time_parameters: TimeParametersCpv1
    account_creation_limit: CredentialsPerBlockLimit
    mint_distribution: MintDistributionCpv1
    transaction_fee_distribution: TransactionFeeDistribution
    gas_rewards: GasRewards
    foundation_account: _kernel_pb2.AccountAddress
    pool_parameters: PoolParametersCpv1
    root_keys: HigherLevelKeys
    level1_keys: HigherLevelKeys
    level2_keys: AuthorizationsV1
    def __init__(self, election_difficulty: _Optional[_Union[ElectionDifficulty, _Mapping]] = ..., euro_per_energy: _Optional[_Union[ExchangeRate, _Mapping]] = ..., micro_ccd_per_euro: _Optional[_Union[ExchangeRate, _Mapping]] = ..., cooldown_parameters: _Optional[_Union[CooldownParametersCpv1, _Mapping]] = ..., time_parameters: _Optional[_Union[TimeParametersCpv1, _Mapping]] = ..., account_creation_limit: _Optional[_Union[CredentialsPerBlockLimit, _Mapping]] = ..., mint_distribution: _Optional[_Union[MintDistributionCpv1, _Mapping]] = ..., transaction_fee_distribution: _Optional[_Union[TransactionFeeDistribution, _Mapping]] = ..., gas_rewards: _Optional[_Union[GasRewards, _Mapping]] = ..., foundation_account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., pool_parameters: _Optional[_Union[PoolParametersCpv1, _Mapping]] = ..., root_keys: _Optional[_Union[HigherLevelKeys, _Mapping]] = ..., level1_keys: _Optional[_Union[HigherLevelKeys, _Mapping]] = ..., level2_keys: _Optional[_Union[AuthorizationsV1, _Mapping]] = ...) -> None: ...

class ChainParametersV2(_message.Message):
    __slots__ = ("consensus_parameters", "euro_per_energy", "micro_ccd_per_euro", "cooldown_parameters", "time_parameters", "account_creation_limit", "mint_distribution", "transaction_fee_distribution", "gas_rewards", "foundation_account", "pool_parameters", "root_keys", "level1_keys", "level2_keys", "finalization_committee_parameters")
    CONSENSUS_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    EURO_PER_ENERGY_FIELD_NUMBER: _ClassVar[int]
    MICRO_CCD_PER_EURO_FIELD_NUMBER: _ClassVar[int]
    COOLDOWN_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    TIME_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_CREATION_LIMIT_FIELD_NUMBER: _ClassVar[int]
    MINT_DISTRIBUTION_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_FEE_DISTRIBUTION_FIELD_NUMBER: _ClassVar[int]
    GAS_REWARDS_FIELD_NUMBER: _ClassVar[int]
    FOUNDATION_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    POOL_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    ROOT_KEYS_FIELD_NUMBER: _ClassVar[int]
    LEVEL1_KEYS_FIELD_NUMBER: _ClassVar[int]
    LEVEL2_KEYS_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_COMMITTEE_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    consensus_parameters: ConsensusParametersV1
    euro_per_energy: ExchangeRate
    micro_ccd_per_euro: ExchangeRate
    cooldown_parameters: CooldownParametersCpv1
    time_parameters: TimeParametersCpv1
    account_creation_limit: CredentialsPerBlockLimit
    mint_distribution: MintDistributionCpv1
    transaction_fee_distribution: TransactionFeeDistribution
    gas_rewards: GasRewardsCpv2
    foundation_account: _kernel_pb2.AccountAddress
    pool_parameters: PoolParametersCpv1
    root_keys: HigherLevelKeys
    level1_keys: HigherLevelKeys
    level2_keys: AuthorizationsV1
    finalization_committee_parameters: FinalizationCommitteeParameters
    def __init__(self, consensus_parameters: _Optional[_Union[ConsensusParametersV1, _Mapping]] = ..., euro_per_energy: _Optional[_Union[ExchangeRate, _Mapping]] = ..., micro_ccd_per_euro: _Optional[_Union[ExchangeRate, _Mapping]] = ..., cooldown_parameters: _Optional[_Union[CooldownParametersCpv1, _Mapping]] = ..., time_parameters: _Optional[_Union[TimeParametersCpv1, _Mapping]] = ..., account_creation_limit: _Optional[_Union[CredentialsPerBlockLimit, _Mapping]] = ..., mint_distribution: _Optional[_Union[MintDistributionCpv1, _Mapping]] = ..., transaction_fee_distribution: _Optional[_Union[TransactionFeeDistribution, _Mapping]] = ..., gas_rewards: _Optional[_Union[GasRewardsCpv2, _Mapping]] = ..., foundation_account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., pool_parameters: _Optional[_Union[PoolParametersCpv1, _Mapping]] = ..., root_keys: _Optional[_Union[HigherLevelKeys, _Mapping]] = ..., level1_keys: _Optional[_Union[HigherLevelKeys, _Mapping]] = ..., level2_keys: _Optional[_Union[AuthorizationsV1, _Mapping]] = ..., finalization_committee_parameters: _Optional[_Union[FinalizationCommitteeParameters, _Mapping]] = ...) -> None: ...

class ChainParametersV3(_message.Message):
    __slots__ = ("consensus_parameters", "euro_per_energy", "micro_ccd_per_euro", "cooldown_parameters", "time_parameters", "account_creation_limit", "mint_distribution", "transaction_fee_distribution", "gas_rewards", "foundation_account", "pool_parameters", "root_keys", "level1_keys", "level2_keys", "finalization_committee_parameters", "validator_score_parameters")
    CONSENSUS_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    EURO_PER_ENERGY_FIELD_NUMBER: _ClassVar[int]
    MICRO_CCD_PER_EURO_FIELD_NUMBER: _ClassVar[int]
    COOLDOWN_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    TIME_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_CREATION_LIMIT_FIELD_NUMBER: _ClassVar[int]
    MINT_DISTRIBUTION_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_FEE_DISTRIBUTION_FIELD_NUMBER: _ClassVar[int]
    GAS_REWARDS_FIELD_NUMBER: _ClassVar[int]
    FOUNDATION_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    POOL_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    ROOT_KEYS_FIELD_NUMBER: _ClassVar[int]
    LEVEL1_KEYS_FIELD_NUMBER: _ClassVar[int]
    LEVEL2_KEYS_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_COMMITTEE_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    VALIDATOR_SCORE_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    consensus_parameters: ConsensusParametersV1
    euro_per_energy: ExchangeRate
    micro_ccd_per_euro: ExchangeRate
    cooldown_parameters: CooldownParametersCpv1
    time_parameters: TimeParametersCpv1
    account_creation_limit: CredentialsPerBlockLimit
    mint_distribution: MintDistributionCpv1
    transaction_fee_distribution: TransactionFeeDistribution
    gas_rewards: GasRewardsCpv2
    foundation_account: _kernel_pb2.AccountAddress
    pool_parameters: PoolParametersCpv1
    root_keys: HigherLevelKeys
    level1_keys: HigherLevelKeys
    level2_keys: AuthorizationsV1
    finalization_committee_parameters: FinalizationCommitteeParameters
    validator_score_parameters: ValidatorScoreParameters
    def __init__(self, consensus_parameters: _Optional[_Union[ConsensusParametersV1, _Mapping]] = ..., euro_per_energy: _Optional[_Union[ExchangeRate, _Mapping]] = ..., micro_ccd_per_euro: _Optional[_Union[ExchangeRate, _Mapping]] = ..., cooldown_parameters: _Optional[_Union[CooldownParametersCpv1, _Mapping]] = ..., time_parameters: _Optional[_Union[TimeParametersCpv1, _Mapping]] = ..., account_creation_limit: _Optional[_Union[CredentialsPerBlockLimit, _Mapping]] = ..., mint_distribution: _Optional[_Union[MintDistributionCpv1, _Mapping]] = ..., transaction_fee_distribution: _Optional[_Union[TransactionFeeDistribution, _Mapping]] = ..., gas_rewards: _Optional[_Union[GasRewardsCpv2, _Mapping]] = ..., foundation_account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., pool_parameters: _Optional[_Union[PoolParametersCpv1, _Mapping]] = ..., root_keys: _Optional[_Union[HigherLevelKeys, _Mapping]] = ..., level1_keys: _Optional[_Union[HigherLevelKeys, _Mapping]] = ..., level2_keys: _Optional[_Union[AuthorizationsV1, _Mapping]] = ..., finalization_committee_parameters: _Optional[_Union[FinalizationCommitteeParameters, _Mapping]] = ..., validator_score_parameters: _Optional[_Union[ValidatorScoreParameters, _Mapping]] = ...) -> None: ...

class ChainParameters(_message.Message):
    __slots__ = ("v0", "v1", "v2", "v3")
    V0_FIELD_NUMBER: _ClassVar[int]
    V1_FIELD_NUMBER: _ClassVar[int]
    V2_FIELD_NUMBER: _ClassVar[int]
    V3_FIELD_NUMBER: _ClassVar[int]
    v0: ChainParametersV0
    v1: ChainParametersV1
    v2: ChainParametersV2
    v3: ChainParametersV3
    def __init__(self, v0: _Optional[_Union[ChainParametersV0, _Mapping]] = ..., v1: _Optional[_Union[ChainParametersV1, _Mapping]] = ..., v2: _Optional[_Union[ChainParametersV2, _Mapping]] = ..., v3: _Optional[_Union[ChainParametersV3, _Mapping]] = ...) -> None: ...

class FinalizationSummaryParty(_message.Message):
    __slots__ = ("baker", "weight", "signed")
    BAKER_FIELD_NUMBER: _ClassVar[int]
    WEIGHT_FIELD_NUMBER: _ClassVar[int]
    SIGNED_FIELD_NUMBER: _ClassVar[int]
    baker: BakerId
    weight: int
    signed: bool
    def __init__(self, baker: _Optional[_Union[BakerId, _Mapping]] = ..., weight: _Optional[int] = ..., signed: bool = ...) -> None: ...

class FinalizationIndex(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class FinalizationSummary(_message.Message):
    __slots__ = ("block", "index", "delay", "finalizers")
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    DELAY_FIELD_NUMBER: _ClassVar[int]
    FINALIZERS_FIELD_NUMBER: _ClassVar[int]
    block: BlockHash
    index: FinalizationIndex
    delay: BlockHeight
    finalizers: _containers.RepeatedCompositeFieldContainer[FinalizationSummaryParty]
    def __init__(self, block: _Optional[_Union[BlockHash, _Mapping]] = ..., index: _Optional[_Union[FinalizationIndex, _Mapping]] = ..., delay: _Optional[_Union[BlockHeight, _Mapping]] = ..., finalizers: _Optional[_Iterable[_Union[FinalizationSummaryParty, _Mapping]]] = ...) -> None: ...

class BlockFinalizationSummary(_message.Message):
    __slots__ = ("none", "record")
    NONE_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    none: Empty
    record: FinalizationSummary
    def __init__(self, none: _Optional[_Union[Empty, _Mapping]] = ..., record: _Optional[_Union[FinalizationSummary, _Mapping]] = ...) -> None: ...

class BlockItem(_message.Message):
    __slots__ = ("hash", "account_transaction", "credential_deployment", "update_instruction")
    HASH_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_TRANSACTION_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_DEPLOYMENT_FIELD_NUMBER: _ClassVar[int]
    UPDATE_INSTRUCTION_FIELD_NUMBER: _ClassVar[int]
    hash: TransactionHash
    account_transaction: AccountTransaction
    credential_deployment: CredentialDeployment
    update_instruction: UpdateInstruction
    def __init__(self, hash: _Optional[_Union[TransactionHash, _Mapping]] = ..., account_transaction: _Optional[_Union[AccountTransaction, _Mapping]] = ..., credential_deployment: _Optional[_Union[CredentialDeployment, _Mapping]] = ..., update_instruction: _Optional[_Union[UpdateInstruction, _Mapping]] = ...) -> None: ...

class BakerRewardPeriodInfo(_message.Message):
    __slots__ = ("baker", "effective_stake", "commission_rates", "equity_capital", "delegated_capital", "is_finalizer")
    BAKER_FIELD_NUMBER: _ClassVar[int]
    EFFECTIVE_STAKE_FIELD_NUMBER: _ClassVar[int]
    COMMISSION_RATES_FIELD_NUMBER: _ClassVar[int]
    EQUITY_CAPITAL_FIELD_NUMBER: _ClassVar[int]
    DELEGATED_CAPITAL_FIELD_NUMBER: _ClassVar[int]
    IS_FINALIZER_FIELD_NUMBER: _ClassVar[int]
    baker: BakerInfo
    effective_stake: Amount
    commission_rates: CommissionRates
    equity_capital: Amount
    delegated_capital: Amount
    is_finalizer: bool
    def __init__(self, baker: _Optional[_Union[BakerInfo, _Mapping]] = ..., effective_stake: _Optional[_Union[Amount, _Mapping]] = ..., commission_rates: _Optional[_Union[CommissionRates, _Mapping]] = ..., equity_capital: _Optional[_Union[Amount, _Mapping]] = ..., delegated_capital: _Optional[_Union[Amount, _Mapping]] = ..., is_finalizer: bool = ...) -> None: ...

class QuorumSignature(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class QuorumCertificate(_message.Message):
    __slots__ = ("block_hash", "round", "epoch", "aggregate_signature", "signatories")
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    ROUND_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    SIGNATORIES_FIELD_NUMBER: _ClassVar[int]
    block_hash: BlockHash
    round: Round
    epoch: Epoch
    aggregate_signature: QuorumSignature
    signatories: _containers.RepeatedCompositeFieldContainer[BakerId]
    def __init__(self, block_hash: _Optional[_Union[BlockHash, _Mapping]] = ..., round: _Optional[_Union[Round, _Mapping]] = ..., epoch: _Optional[_Union[Epoch, _Mapping]] = ..., aggregate_signature: _Optional[_Union[QuorumSignature, _Mapping]] = ..., signatories: _Optional[_Iterable[_Union[BakerId, _Mapping]]] = ...) -> None: ...

class FinalizerRound(_message.Message):
    __slots__ = ("round", "finalizers")
    ROUND_FIELD_NUMBER: _ClassVar[int]
    FINALIZERS_FIELD_NUMBER: _ClassVar[int]
    round: Round
    finalizers: _containers.RepeatedCompositeFieldContainer[BakerId]
    def __init__(self, round: _Optional[_Union[Round, _Mapping]] = ..., finalizers: _Optional[_Iterable[_Union[BakerId, _Mapping]]] = ...) -> None: ...

class TimeoutSignature(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class TimeoutCertificate(_message.Message):
    __slots__ = ("round", "min_epoch", "qc_rounds_first_epoch", "qc_rounds_second_epoch", "aggregate_signature")
    ROUND_FIELD_NUMBER: _ClassVar[int]
    MIN_EPOCH_FIELD_NUMBER: _ClassVar[int]
    QC_ROUNDS_FIRST_EPOCH_FIELD_NUMBER: _ClassVar[int]
    QC_ROUNDS_SECOND_EPOCH_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    round: Round
    min_epoch: Epoch
    qc_rounds_first_epoch: _containers.RepeatedCompositeFieldContainer[FinalizerRound]
    qc_rounds_second_epoch: _containers.RepeatedCompositeFieldContainer[FinalizerRound]
    aggregate_signature: TimeoutSignature
    def __init__(self, round: _Optional[_Union[Round, _Mapping]] = ..., min_epoch: _Optional[_Union[Epoch, _Mapping]] = ..., qc_rounds_first_epoch: _Optional[_Iterable[_Union[FinalizerRound, _Mapping]]] = ..., qc_rounds_second_epoch: _Optional[_Iterable[_Union[FinalizerRound, _Mapping]]] = ..., aggregate_signature: _Optional[_Union[TimeoutSignature, _Mapping]] = ...) -> None: ...

class SuccessorProof(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class EpochFinalizationEntry(_message.Message):
    __slots__ = ("finalized_qc", "successor_qc", "successor_proof")
    FINALIZED_QC_FIELD_NUMBER: _ClassVar[int]
    SUCCESSOR_QC_FIELD_NUMBER: _ClassVar[int]
    SUCCESSOR_PROOF_FIELD_NUMBER: _ClassVar[int]
    finalized_qc: QuorumCertificate
    successor_qc: QuorumCertificate
    successor_proof: SuccessorProof
    def __init__(self, finalized_qc: _Optional[_Union[QuorumCertificate, _Mapping]] = ..., successor_qc: _Optional[_Union[QuorumCertificate, _Mapping]] = ..., successor_proof: _Optional[_Union[SuccessorProof, _Mapping]] = ...) -> None: ...

class BlockCertificates(_message.Message):
    __slots__ = ("quorum_certificate", "timeout_certificate", "epoch_finalization_entry")
    QUORUM_CERTIFICATE_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_CERTIFICATE_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FINALIZATION_ENTRY_FIELD_NUMBER: _ClassVar[int]
    quorum_certificate: QuorumCertificate
    timeout_certificate: TimeoutCertificate
    epoch_finalization_entry: EpochFinalizationEntry
    def __init__(self, quorum_certificate: _Optional[_Union[QuorumCertificate, _Mapping]] = ..., timeout_certificate: _Optional[_Union[TimeoutCertificate, _Mapping]] = ..., epoch_finalization_entry: _Optional[_Union[EpochFinalizationEntry, _Mapping]] = ...) -> None: ...

class WinningBaker(_message.Message):
    __slots__ = ("round", "winner", "present")
    ROUND_FIELD_NUMBER: _ClassVar[int]
    WINNER_FIELD_NUMBER: _ClassVar[int]
    PRESENT_FIELD_NUMBER: _ClassVar[int]
    round: Round
    winner: BakerId
    present: bool
    def __init__(self, round: _Optional[_Union[Round, _Mapping]] = ..., winner: _Optional[_Union[BakerId, _Mapping]] = ..., present: bool = ...) -> None: ...

class DryRunRequest(_message.Message):
    __slots__ = ("load_block_state", "state_query", "state_operation")
    LOAD_BLOCK_STATE_FIELD_NUMBER: _ClassVar[int]
    STATE_QUERY_FIELD_NUMBER: _ClassVar[int]
    STATE_OPERATION_FIELD_NUMBER: _ClassVar[int]
    load_block_state: BlockHashInput
    state_query: DryRunStateQuery
    state_operation: DryRunStateOperation
    def __init__(self, load_block_state: _Optional[_Union[BlockHashInput, _Mapping]] = ..., state_query: _Optional[_Union[DryRunStateQuery, _Mapping]] = ..., state_operation: _Optional[_Union[DryRunStateOperation, _Mapping]] = ...) -> None: ...

class DryRunStateQuery(_message.Message):
    __slots__ = ("get_account_info", "get_instance_info", "invoke_instance")
    GET_ACCOUNT_INFO_FIELD_NUMBER: _ClassVar[int]
    GET_INSTANCE_INFO_FIELD_NUMBER: _ClassVar[int]
    INVOKE_INSTANCE_FIELD_NUMBER: _ClassVar[int]
    get_account_info: AccountIdentifierInput
    get_instance_info: ContractAddress
    invoke_instance: DryRunInvokeInstance
    def __init__(self, get_account_info: _Optional[_Union[AccountIdentifierInput, _Mapping]] = ..., get_instance_info: _Optional[_Union[ContractAddress, _Mapping]] = ..., invoke_instance: _Optional[_Union[DryRunInvokeInstance, _Mapping]] = ...) -> None: ...

class DryRunInvokeInstance(_message.Message):
    __slots__ = ("invoker", "instance", "amount", "entrypoint", "parameter", "energy")
    INVOKER_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    ENTRYPOINT_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_FIELD_NUMBER: _ClassVar[int]
    ENERGY_FIELD_NUMBER: _ClassVar[int]
    invoker: Address
    instance: ContractAddress
    amount: Amount
    entrypoint: ReceiveName
    parameter: Parameter
    energy: Energy
    def __init__(self, invoker: _Optional[_Union[Address, _Mapping]] = ..., instance: _Optional[_Union[ContractAddress, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ..., entrypoint: _Optional[_Union[ReceiveName, _Mapping]] = ..., parameter: _Optional[_Union[Parameter, _Mapping]] = ..., energy: _Optional[_Union[Energy, _Mapping]] = ...) -> None: ...

class DryRunStateOperation(_message.Message):
    __slots__ = ("set_timestamp", "mint_to_account", "run_transaction")
    SET_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    MINT_TO_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    RUN_TRANSACTION_FIELD_NUMBER: _ClassVar[int]
    set_timestamp: Timestamp
    mint_to_account: DryRunMintToAccount
    run_transaction: DryRunTransaction
    def __init__(self, set_timestamp: _Optional[_Union[Timestamp, _Mapping]] = ..., mint_to_account: _Optional[_Union[DryRunMintToAccount, _Mapping]] = ..., run_transaction: _Optional[_Union[DryRunTransaction, _Mapping]] = ...) -> None: ...

class DryRunMintToAccount(_message.Message):
    __slots__ = ("account", "amount")
    ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    account: _kernel_pb2.AccountAddress
    amount: Amount
    def __init__(self, account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., amount: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...

class DryRunTransaction(_message.Message):
    __slots__ = ("sender", "energy_amount", "payload", "signatures")
    SENDER_FIELD_NUMBER: _ClassVar[int]
    ENERGY_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    SIGNATURES_FIELD_NUMBER: _ClassVar[int]
    sender: _kernel_pb2.AccountAddress
    energy_amount: Energy
    payload: AccountTransactionPayload
    signatures: _containers.RepeatedCompositeFieldContainer[DryRunSignature]
    def __init__(self, sender: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ..., energy_amount: _Optional[_Union[Energy, _Mapping]] = ..., payload: _Optional[_Union[AccountTransactionPayload, _Mapping]] = ..., signatures: _Optional[_Iterable[_Union[DryRunSignature, _Mapping]]] = ...) -> None: ...

class DryRunSignature(_message.Message):
    __slots__ = ("credential", "key")
    CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    credential: int
    key: int
    def __init__(self, credential: _Optional[int] = ..., key: _Optional[int] = ...) -> None: ...

class DryRunResponse(_message.Message):
    __slots__ = ("error", "success", "quota_remaining")
    ERROR_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    QUOTA_REMAINING_FIELD_NUMBER: _ClassVar[int]
    error: DryRunErrorResponse
    success: DryRunSuccessResponse
    quota_remaining: Energy
    def __init__(self, error: _Optional[_Union[DryRunErrorResponse, _Mapping]] = ..., success: _Optional[_Union[DryRunSuccessResponse, _Mapping]] = ..., quota_remaining: _Optional[_Union[Energy, _Mapping]] = ...) -> None: ...

class DryRunErrorResponse(_message.Message):
    __slots__ = ("no_state", "block_not_found", "account_not_found", "instance_not_found", "amount_over_limit", "balance_insufficient", "energy_insufficient", "invoke_failed")
    class NoState(_message.Message):
        __slots__ = ()
        def __init__(self) -> None: ...
    class BlockNotFound(_message.Message):
        __slots__ = ()
        def __init__(self) -> None: ...
    class AccountNotFound(_message.Message):
        __slots__ = ()
        def __init__(self) -> None: ...
    class InstanceNotFound(_message.Message):
        __slots__ = ()
        def __init__(self) -> None: ...
    class AmountOverLimit(_message.Message):
        __slots__ = ("amount_limit",)
        AMOUNT_LIMIT_FIELD_NUMBER: _ClassVar[int]
        amount_limit: Amount
        def __init__(self, amount_limit: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...
    class BalanceInsufficient(_message.Message):
        __slots__ = ("required_amount", "available_amount")
        REQUIRED_AMOUNT_FIELD_NUMBER: _ClassVar[int]
        AVAILABLE_AMOUNT_FIELD_NUMBER: _ClassVar[int]
        required_amount: Amount
        available_amount: Amount
        def __init__(self, required_amount: _Optional[_Union[Amount, _Mapping]] = ..., available_amount: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...
    class EnergyInsufficient(_message.Message):
        __slots__ = ("energy_required",)
        ENERGY_REQUIRED_FIELD_NUMBER: _ClassVar[int]
        energy_required: Energy
        def __init__(self, energy_required: _Optional[_Union[Energy, _Mapping]] = ...) -> None: ...
    class InvokeFailure(_message.Message):
        __slots__ = ("return_value", "used_energy", "reason")
        RETURN_VALUE_FIELD_NUMBER: _ClassVar[int]
        USED_ENERGY_FIELD_NUMBER: _ClassVar[int]
        REASON_FIELD_NUMBER: _ClassVar[int]
        return_value: bytes
        used_energy: Energy
        reason: RejectReason
        def __init__(self, return_value: _Optional[bytes] = ..., used_energy: _Optional[_Union[Energy, _Mapping]] = ..., reason: _Optional[_Union[RejectReason, _Mapping]] = ...) -> None: ...
    NO_STATE_FIELD_NUMBER: _ClassVar[int]
    BLOCK_NOT_FOUND_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_NOT_FOUND_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_NOT_FOUND_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_OVER_LIMIT_FIELD_NUMBER: _ClassVar[int]
    BALANCE_INSUFFICIENT_FIELD_NUMBER: _ClassVar[int]
    ENERGY_INSUFFICIENT_FIELD_NUMBER: _ClassVar[int]
    INVOKE_FAILED_FIELD_NUMBER: _ClassVar[int]
    no_state: DryRunErrorResponse.NoState
    block_not_found: DryRunErrorResponse.BlockNotFound
    account_not_found: DryRunErrorResponse.AccountNotFound
    instance_not_found: DryRunErrorResponse.InstanceNotFound
    amount_over_limit: DryRunErrorResponse.AmountOverLimit
    balance_insufficient: DryRunErrorResponse.BalanceInsufficient
    energy_insufficient: DryRunErrorResponse.EnergyInsufficient
    invoke_failed: DryRunErrorResponse.InvokeFailure
    def __init__(self, no_state: _Optional[_Union[DryRunErrorResponse.NoState, _Mapping]] = ..., block_not_found: _Optional[_Union[DryRunErrorResponse.BlockNotFound, _Mapping]] = ..., account_not_found: _Optional[_Union[DryRunErrorResponse.AccountNotFound, _Mapping]] = ..., instance_not_found: _Optional[_Union[DryRunErrorResponse.InstanceNotFound, _Mapping]] = ..., amount_over_limit: _Optional[_Union[DryRunErrorResponse.AmountOverLimit, _Mapping]] = ..., balance_insufficient: _Optional[_Union[DryRunErrorResponse.BalanceInsufficient, _Mapping]] = ..., energy_insufficient: _Optional[_Union[DryRunErrorResponse.EnergyInsufficient, _Mapping]] = ..., invoke_failed: _Optional[_Union[DryRunErrorResponse.InvokeFailure, _Mapping]] = ...) -> None: ...

class DryRunSuccessResponse(_message.Message):
    __slots__ = ("block_state_loaded", "account_info", "instance_info", "invoke_succeeded", "timestamp_set", "minted_to_account", "transaction_executed")
    class BlockStateLoaded(_message.Message):
        __slots__ = ("current_timestamp", "block_hash", "protocol_version")
        CURRENT_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
        BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
        PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
        current_timestamp: Timestamp
        block_hash: BlockHash
        protocol_version: ProtocolVersion
        def __init__(self, current_timestamp: _Optional[_Union[Timestamp, _Mapping]] = ..., block_hash: _Optional[_Union[BlockHash, _Mapping]] = ..., protocol_version: _Optional[_Union[ProtocolVersion, str]] = ...) -> None: ...
    class TimestampSet(_message.Message):
        __slots__ = ()
        def __init__(self) -> None: ...
    class MintedToAccount(_message.Message):
        __slots__ = ()
        def __init__(self) -> None: ...
    class TransactionExecuted(_message.Message):
        __slots__ = ("energy_cost", "details", "return_value")
        ENERGY_COST_FIELD_NUMBER: _ClassVar[int]
        DETAILS_FIELD_NUMBER: _ClassVar[int]
        RETURN_VALUE_FIELD_NUMBER: _ClassVar[int]
        energy_cost: Energy
        details: AccountTransactionDetails
        return_value: bytes
        def __init__(self, energy_cost: _Optional[_Union[Energy, _Mapping]] = ..., details: _Optional[_Union[AccountTransactionDetails, _Mapping]] = ..., return_value: _Optional[bytes] = ...) -> None: ...
    class InvokeSuccess(_message.Message):
        __slots__ = ("return_value", "used_energy", "effects")
        RETURN_VALUE_FIELD_NUMBER: _ClassVar[int]
        USED_ENERGY_FIELD_NUMBER: _ClassVar[int]
        EFFECTS_FIELD_NUMBER: _ClassVar[int]
        return_value: bytes
        used_energy: Energy
        effects: _containers.RepeatedCompositeFieldContainer[ContractTraceElement]
        def __init__(self, return_value: _Optional[bytes] = ..., used_energy: _Optional[_Union[Energy, _Mapping]] = ..., effects: _Optional[_Iterable[_Union[ContractTraceElement, _Mapping]]] = ...) -> None: ...
    BLOCK_STATE_LOADED_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_INFO_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_INFO_FIELD_NUMBER: _ClassVar[int]
    INVOKE_SUCCEEDED_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_SET_FIELD_NUMBER: _ClassVar[int]
    MINTED_TO_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_EXECUTED_FIELD_NUMBER: _ClassVar[int]
    block_state_loaded: DryRunSuccessResponse.BlockStateLoaded
    account_info: AccountInfo
    instance_info: InstanceInfo
    invoke_succeeded: DryRunSuccessResponse.InvokeSuccess
    timestamp_set: DryRunSuccessResponse.TimestampSet
    minted_to_account: DryRunSuccessResponse.MintedToAccount
    transaction_executed: DryRunSuccessResponse.TransactionExecuted
    def __init__(self, block_state_loaded: _Optional[_Union[DryRunSuccessResponse.BlockStateLoaded, _Mapping]] = ..., account_info: _Optional[_Union[AccountInfo, _Mapping]] = ..., instance_info: _Optional[_Union[InstanceInfo, _Mapping]] = ..., invoke_succeeded: _Optional[_Union[DryRunSuccessResponse.InvokeSuccess, _Mapping]] = ..., timestamp_set: _Optional[_Union[DryRunSuccessResponse.TimestampSet, _Mapping]] = ..., minted_to_account: _Optional[_Union[DryRunSuccessResponse.MintedToAccount, _Mapping]] = ..., transaction_executed: _Optional[_Union[DryRunSuccessResponse.TransactionExecuted, _Mapping]] = ...) -> None: ...

class FinalizerIndex(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class QuorumMessage(_message.Message):
    __slots__ = ("signature", "block", "finalizer", "round", "epoch")
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    FINALIZER_FIELD_NUMBER: _ClassVar[int]
    ROUND_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    signature: QuorumSignature
    block: BlockHash
    finalizer: FinalizerIndex
    round: Round
    epoch: Epoch
    def __init__(self, signature: _Optional[_Union[QuorumSignature, _Mapping]] = ..., block: _Optional[_Union[BlockHash, _Mapping]] = ..., finalizer: _Optional[_Union[FinalizerIndex, _Mapping]] = ..., round: _Optional[_Union[Round, _Mapping]] = ..., epoch: _Optional[_Union[Epoch, _Mapping]] = ...) -> None: ...

class RawQuorumCertificate(_message.Message):
    __slots__ = ("block_hash", "round", "epoch", "aggregate_signature", "signatories")
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    ROUND_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    SIGNATORIES_FIELD_NUMBER: _ClassVar[int]
    block_hash: BlockHash
    round: Round
    epoch: Epoch
    aggregate_signature: QuorumSignature
    signatories: _containers.RepeatedCompositeFieldContainer[FinalizerIndex]
    def __init__(self, block_hash: _Optional[_Union[BlockHash, _Mapping]] = ..., round: _Optional[_Union[Round, _Mapping]] = ..., epoch: _Optional[_Union[Epoch, _Mapping]] = ..., aggregate_signature: _Optional[_Union[QuorumSignature, _Mapping]] = ..., signatories: _Optional[_Iterable[_Union[FinalizerIndex, _Mapping]]] = ...) -> None: ...

class BlockSignature(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class TimeoutMessage(_message.Message):
    __slots__ = ("finalizer", "round", "epoch", "quorum_certificate", "signature", "message_signature")
    FINALIZER_FIELD_NUMBER: _ClassVar[int]
    ROUND_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    QUORUM_CERTIFICATE_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    finalizer: FinalizerIndex
    round: Round
    epoch: Epoch
    quorum_certificate: RawQuorumCertificate
    signature: TimeoutSignature
    message_signature: BlockSignature
    def __init__(self, finalizer: _Optional[_Union[FinalizerIndex, _Mapping]] = ..., round: _Optional[_Union[Round, _Mapping]] = ..., epoch: _Optional[_Union[Epoch, _Mapping]] = ..., quorum_certificate: _Optional[_Union[RawQuorumCertificate, _Mapping]] = ..., signature: _Optional[_Union[TimeoutSignature, _Mapping]] = ..., message_signature: _Optional[_Union[BlockSignature, _Mapping]] = ...) -> None: ...

class RawFinalizerRound(_message.Message):
    __slots__ = ("round", "finalizers")
    ROUND_FIELD_NUMBER: _ClassVar[int]
    FINALIZERS_FIELD_NUMBER: _ClassVar[int]
    round: Round
    finalizers: _containers.RepeatedCompositeFieldContainer[FinalizerIndex]
    def __init__(self, round: _Optional[_Union[Round, _Mapping]] = ..., finalizers: _Optional[_Iterable[_Union[FinalizerIndex, _Mapping]]] = ...) -> None: ...

class RawTimeoutCertificate(_message.Message):
    __slots__ = ("round", "min_epoch", "qc_rounds_first_epoch", "qc_rounds_second_epoch", "aggregate_signature")
    ROUND_FIELD_NUMBER: _ClassVar[int]
    MIN_EPOCH_FIELD_NUMBER: _ClassVar[int]
    QC_ROUNDS_FIRST_EPOCH_FIELD_NUMBER: _ClassVar[int]
    QC_ROUNDS_SECOND_EPOCH_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    round: Round
    min_epoch: Epoch
    qc_rounds_first_epoch: _containers.RepeatedCompositeFieldContainer[RawFinalizerRound]
    qc_rounds_second_epoch: _containers.RepeatedCompositeFieldContainer[RawFinalizerRound]
    aggregate_signature: TimeoutSignature
    def __init__(self, round: _Optional[_Union[Round, _Mapping]] = ..., min_epoch: _Optional[_Union[Epoch, _Mapping]] = ..., qc_rounds_first_epoch: _Optional[_Iterable[_Union[RawFinalizerRound, _Mapping]]] = ..., qc_rounds_second_epoch: _Optional[_Iterable[_Union[RawFinalizerRound, _Mapping]]] = ..., aggregate_signature: _Optional[_Union[TimeoutSignature, _Mapping]] = ...) -> None: ...

class PersistentRoundStatus(_message.Message):
    __slots__ = ("last_signed_quorum_message", "last_signed_timeout_message", "last_baked_round", "latest_timeout")
    LAST_SIGNED_QUORUM_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    LAST_SIGNED_TIMEOUT_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    LAST_BAKED_ROUND_FIELD_NUMBER: _ClassVar[int]
    LATEST_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    last_signed_quorum_message: QuorumMessage
    last_signed_timeout_message: TimeoutMessage
    last_baked_round: Round
    latest_timeout: RawTimeoutCertificate
    def __init__(self, last_signed_quorum_message: _Optional[_Union[QuorumMessage, _Mapping]] = ..., last_signed_timeout_message: _Optional[_Union[TimeoutMessage, _Mapping]] = ..., last_baked_round: _Optional[_Union[Round, _Mapping]] = ..., latest_timeout: _Optional[_Union[RawTimeoutCertificate, _Mapping]] = ...) -> None: ...

class RoundTimeout(_message.Message):
    __slots__ = ("timeout_certificate", "quorum_certificate")
    TIMEOUT_CERTIFICATE_FIELD_NUMBER: _ClassVar[int]
    QUORUM_CERTIFICATE_FIELD_NUMBER: _ClassVar[int]
    timeout_certificate: RawTimeoutCertificate
    quorum_certificate: RawQuorumCertificate
    def __init__(self, timeout_certificate: _Optional[_Union[RawTimeoutCertificate, _Mapping]] = ..., quorum_certificate: _Optional[_Union[RawQuorumCertificate, _Mapping]] = ...) -> None: ...

class RawFinalizationEntry(_message.Message):
    __slots__ = ("finalized_qc", "successor_qc", "successor_proof")
    FINALIZED_QC_FIELD_NUMBER: _ClassVar[int]
    SUCCESSOR_QC_FIELD_NUMBER: _ClassVar[int]
    SUCCESSOR_PROOF_FIELD_NUMBER: _ClassVar[int]
    finalized_qc: RawQuorumCertificate
    successor_qc: RawQuorumCertificate
    successor_proof: SuccessorProof
    def __init__(self, finalized_qc: _Optional[_Union[RawQuorumCertificate, _Mapping]] = ..., successor_qc: _Optional[_Union[RawQuorumCertificate, _Mapping]] = ..., successor_proof: _Optional[_Union[SuccessorProof, _Mapping]] = ...) -> None: ...

class RoundStatus(_message.Message):
    __slots__ = ("current_round", "highest_certified_block", "previous_round_timeout", "round_eligible_to_bake", "current_epoch", "last_epoch_finalization_entry", "current_timeout")
    CURRENT_ROUND_FIELD_NUMBER: _ClassVar[int]
    HIGHEST_CERTIFIED_BLOCK_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_ROUND_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    ROUND_ELIGIBLE_TO_BAKE_FIELD_NUMBER: _ClassVar[int]
    CURRENT_EPOCH_FIELD_NUMBER: _ClassVar[int]
    LAST_EPOCH_FINALIZATION_ENTRY_FIELD_NUMBER: _ClassVar[int]
    CURRENT_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    current_round: Round
    highest_certified_block: RawQuorumCertificate
    previous_round_timeout: RoundTimeout
    round_eligible_to_bake: bool
    current_epoch: Epoch
    last_epoch_finalization_entry: RawFinalizationEntry
    current_timeout: Duration
    def __init__(self, current_round: _Optional[_Union[Round, _Mapping]] = ..., highest_certified_block: _Optional[_Union[RawQuorumCertificate, _Mapping]] = ..., previous_round_timeout: _Optional[_Union[RoundTimeout, _Mapping]] = ..., round_eligible_to_bake: bool = ..., current_epoch: _Optional[_Union[Epoch, _Mapping]] = ..., last_epoch_finalization_entry: _Optional[_Union[RawFinalizationEntry, _Mapping]] = ..., current_timeout: _Optional[_Union[Duration, _Mapping]] = ...) -> None: ...

class BlockTableSummary(_message.Message):
    __slots__ = ("dead_block_cache_size", "live_blocks")
    DEAD_BLOCK_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    LIVE_BLOCKS_FIELD_NUMBER: _ClassVar[int]
    dead_block_cache_size: int
    live_blocks: _containers.RepeatedCompositeFieldContainer[BlockHash]
    def __init__(self, dead_block_cache_size: _Optional[int] = ..., live_blocks: _Optional[_Iterable[_Union[BlockHash, _Mapping]]] = ...) -> None: ...

class BranchBlocks(_message.Message):
    __slots__ = ("blocks_at_branch_height",)
    BLOCKS_AT_BRANCH_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    blocks_at_branch_height: _containers.RepeatedCompositeFieldContainer[BlockHash]
    def __init__(self, blocks_at_branch_height: _Optional[_Iterable[_Union[BlockHash, _Mapping]]] = ...) -> None: ...

class RoundExistingBlock(_message.Message):
    __slots__ = ("round", "baker", "block")
    ROUND_FIELD_NUMBER: _ClassVar[int]
    BAKER_FIELD_NUMBER: _ClassVar[int]
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    round: Round
    baker: BakerId
    block: BlockHash
    def __init__(self, round: _Optional[_Union[Round, _Mapping]] = ..., baker: _Optional[_Union[BakerId, _Mapping]] = ..., block: _Optional[_Union[BlockHash, _Mapping]] = ...) -> None: ...

class RoundExistingQC(_message.Message):
    __slots__ = ("round", "epoch")
    ROUND_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    round: Round
    epoch: Epoch
    def __init__(self, round: _Optional[_Union[Round, _Mapping]] = ..., epoch: _Optional[_Union[Epoch, _Mapping]] = ...) -> None: ...

class FullBakerInfo(_message.Message):
    __slots__ = ("baker_identity", "election_verify_key", "signature_verify_key", "aggregation_verify_key", "stake")
    BAKER_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    ELECTION_VERIFY_KEY_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_VERIFY_KEY_FIELD_NUMBER: _ClassVar[int]
    AGGREGATION_VERIFY_KEY_FIELD_NUMBER: _ClassVar[int]
    STAKE_FIELD_NUMBER: _ClassVar[int]
    baker_identity: BakerId
    election_verify_key: BakerElectionVerifyKey
    signature_verify_key: BakerSignatureVerifyKey
    aggregation_verify_key: BakerAggregationVerifyKey
    stake: Amount
    def __init__(self, baker_identity: _Optional[_Union[BakerId, _Mapping]] = ..., election_verify_key: _Optional[_Union[BakerElectionVerifyKey, _Mapping]] = ..., signature_verify_key: _Optional[_Union[BakerSignatureVerifyKey, _Mapping]] = ..., aggregation_verify_key: _Optional[_Union[BakerAggregationVerifyKey, _Mapping]] = ..., stake: _Optional[_Union[Amount, _Mapping]] = ...) -> None: ...

class FinalizationCommitteeHash(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class BakersAndFinalizers(_message.Message):
    __slots__ = ("bakers", "finalizers", "baker_total_stake", "finalizer_total_stake", "finalization_committee_hash")
    BAKERS_FIELD_NUMBER: _ClassVar[int]
    FINALIZERS_FIELD_NUMBER: _ClassVar[int]
    BAKER_TOTAL_STAKE_FIELD_NUMBER: _ClassVar[int]
    FINALIZER_TOTAL_STAKE_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_COMMITTEE_HASH_FIELD_NUMBER: _ClassVar[int]
    bakers: _containers.RepeatedCompositeFieldContainer[FullBakerInfo]
    finalizers: _containers.RepeatedCompositeFieldContainer[BakerId]
    baker_total_stake: Amount
    finalizer_total_stake: Amount
    finalization_committee_hash: FinalizationCommitteeHash
    def __init__(self, bakers: _Optional[_Iterable[_Union[FullBakerInfo, _Mapping]]] = ..., finalizers: _Optional[_Iterable[_Union[BakerId, _Mapping]]] = ..., baker_total_stake: _Optional[_Union[Amount, _Mapping]] = ..., finalizer_total_stake: _Optional[_Union[Amount, _Mapping]] = ..., finalization_committee_hash: _Optional[_Union[FinalizationCommitteeHash, _Mapping]] = ...) -> None: ...

class EpochBakers(_message.Message):
    __slots__ = ("previous_epoch_bakers", "current_epoch_bakers", "next_epoch_bakers", "next_payday")
    PREVIOUS_EPOCH_BAKERS_FIELD_NUMBER: _ClassVar[int]
    CURRENT_EPOCH_BAKERS_FIELD_NUMBER: _ClassVar[int]
    NEXT_EPOCH_BAKERS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAYDAY_FIELD_NUMBER: _ClassVar[int]
    previous_epoch_bakers: BakersAndFinalizers
    current_epoch_bakers: BakersAndFinalizers
    next_epoch_bakers: BakersAndFinalizers
    next_payday: Epoch
    def __init__(self, previous_epoch_bakers: _Optional[_Union[BakersAndFinalizers, _Mapping]] = ..., current_epoch_bakers: _Optional[_Union[BakersAndFinalizers, _Mapping]] = ..., next_epoch_bakers: _Optional[_Union[BakersAndFinalizers, _Mapping]] = ..., next_payday: _Optional[_Union[Epoch, _Mapping]] = ...) -> None: ...

class TimeoutMessages(_message.Message):
    __slots__ = ("first_epoch", "first_epoch_timeouts", "second_epoch_timeouts")
    FIRST_EPOCH_FIELD_NUMBER: _ClassVar[int]
    FIRST_EPOCH_TIMEOUTS_FIELD_NUMBER: _ClassVar[int]
    SECOND_EPOCH_TIMEOUTS_FIELD_NUMBER: _ClassVar[int]
    first_epoch: Epoch
    first_epoch_timeouts: _containers.RepeatedCompositeFieldContainer[TimeoutMessage]
    second_epoch_timeouts: _containers.RepeatedCompositeFieldContainer[TimeoutMessage]
    def __init__(self, first_epoch: _Optional[_Union[Epoch, _Mapping]] = ..., first_epoch_timeouts: _Optional[_Iterable[_Union[TimeoutMessage, _Mapping]]] = ..., second_epoch_timeouts: _Optional[_Iterable[_Union[TimeoutMessage, _Mapping]]] = ...) -> None: ...

class AggregatedSignatures(_message.Message):
    __slots__ = ("signed_block", "signature_weight", "aggregate_signature", "signatories")
    SIGNED_BLOCK_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_WEIGHT_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    SIGNATORIES_FIELD_NUMBER: _ClassVar[int]
    signed_block: BlockHash
    signature_weight: Amount
    aggregate_signature: QuorumSignature
    signatories: _containers.RepeatedCompositeFieldContainer[FinalizerIndex]
    def __init__(self, signed_block: _Optional[_Union[BlockHash, _Mapping]] = ..., signature_weight: _Optional[_Union[Amount, _Mapping]] = ..., aggregate_signature: _Optional[_Union[QuorumSignature, _Mapping]] = ..., signatories: _Optional[_Iterable[_Union[FinalizerIndex, _Mapping]]] = ...) -> None: ...

class QuorumMessages(_message.Message):
    __slots__ = ("quorum_messages", "aggregated_signatures")
    QUORUM_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    AGGREGATED_SIGNATURES_FIELD_NUMBER: _ClassVar[int]
    quorum_messages: _containers.RepeatedCompositeFieldContainer[QuorumMessage]
    aggregated_signatures: _containers.RepeatedCompositeFieldContainer[AggregatedSignatures]
    def __init__(self, quorum_messages: _Optional[_Iterable[_Union[QuorumMessage, _Mapping]]] = ..., aggregated_signatures: _Optional[_Iterable[_Union[AggregatedSignatures, _Mapping]]] = ...) -> None: ...

class ConsensusDetailedStatusQuery(_message.Message):
    __slots__ = ("genesis_index",)
    GENESIS_INDEX_FIELD_NUMBER: _ClassVar[int]
    genesis_index: GenesisIndex
    def __init__(self, genesis_index: _Optional[_Union[GenesisIndex, _Mapping]] = ...) -> None: ...

class ConsensusDetailedStatus(_message.Message):
    __slots__ = ("genesis_block", "persistent_round_status", "round_status", "non_finalized_transaction_count", "transaction_table_purge_counter", "block_table", "branches", "round_existing_blocks", "round_existing_qcs", "genesis_block_height", "last_finalized_block", "last_finalized_block_height", "latest_finalization_entry", "epoch_bakers", "timeout_messages", "terminal_block")
    GENESIS_BLOCK_FIELD_NUMBER: _ClassVar[int]
    PERSISTENT_ROUND_STATUS_FIELD_NUMBER: _ClassVar[int]
    ROUND_STATUS_FIELD_NUMBER: _ClassVar[int]
    NON_FINALIZED_TRANSACTION_COUNT_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_TABLE_PURGE_COUNTER_FIELD_NUMBER: _ClassVar[int]
    BLOCK_TABLE_FIELD_NUMBER: _ClassVar[int]
    BRANCHES_FIELD_NUMBER: _ClassVar[int]
    ROUND_EXISTING_BLOCKS_FIELD_NUMBER: _ClassVar[int]
    ROUND_EXISTING_QCS_FIELD_NUMBER: _ClassVar[int]
    GENESIS_BLOCK_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    LAST_FINALIZED_BLOCK_FIELD_NUMBER: _ClassVar[int]
    LAST_FINALIZED_BLOCK_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    LATEST_FINALIZATION_ENTRY_FIELD_NUMBER: _ClassVar[int]
    EPOCH_BAKERS_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_BLOCK_FIELD_NUMBER: _ClassVar[int]
    genesis_block: BlockHash
    persistent_round_status: PersistentRoundStatus
    round_status: RoundStatus
    non_finalized_transaction_count: int
    transaction_table_purge_counter: int
    block_table: BlockTableSummary
    branches: _containers.RepeatedCompositeFieldContainer[BranchBlocks]
    round_existing_blocks: _containers.RepeatedCompositeFieldContainer[RoundExistingBlock]
    round_existing_qcs: _containers.RepeatedCompositeFieldContainer[RoundExistingQC]
    genesis_block_height: AbsoluteBlockHeight
    last_finalized_block: BlockHash
    last_finalized_block_height: BlockHeight
    latest_finalization_entry: RawFinalizationEntry
    epoch_bakers: EpochBakers
    timeout_messages: TimeoutMessages
    terminal_block: BlockHash
    def __init__(self, genesis_block: _Optional[_Union[BlockHash, _Mapping]] = ..., persistent_round_status: _Optional[_Union[PersistentRoundStatus, _Mapping]] = ..., round_status: _Optional[_Union[RoundStatus, _Mapping]] = ..., non_finalized_transaction_count: _Optional[int] = ..., transaction_table_purge_counter: _Optional[int] = ..., block_table: _Optional[_Union[BlockTableSummary, _Mapping]] = ..., branches: _Optional[_Iterable[_Union[BranchBlocks, _Mapping]]] = ..., round_existing_blocks: _Optional[_Iterable[_Union[RoundExistingBlock, _Mapping]]] = ..., round_existing_qcs: _Optional[_Iterable[_Union[RoundExistingQC, _Mapping]]] = ..., genesis_block_height: _Optional[_Union[AbsoluteBlockHeight, _Mapping]] = ..., last_finalized_block: _Optional[_Union[BlockHash, _Mapping]] = ..., last_finalized_block_height: _Optional[_Union[BlockHeight, _Mapping]] = ..., latest_finalization_entry: _Optional[_Union[RawFinalizationEntry, _Mapping]] = ..., epoch_bakers: _Optional[_Union[EpochBakers, _Mapping]] = ..., timeout_messages: _Optional[_Union[TimeoutMessages, _Mapping]] = ..., terminal_block: _Optional[_Union[BlockHash, _Mapping]] = ...) -> None: ...

class AccountPending(_message.Message):
    __slots__ = ("account_index", "first_timestamp")
    ACCOUNT_INDEX_FIELD_NUMBER: _ClassVar[int]
    FIRST_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    account_index: AccountIndex
    first_timestamp: Timestamp
    def __init__(self, account_index: _Optional[_Union[AccountIndex, _Mapping]] = ..., first_timestamp: _Optional[_Union[Timestamp, _Mapping]] = ...) -> None: ...
